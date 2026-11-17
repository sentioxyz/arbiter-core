package objstore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// CollectionPrefix starts the name of every temporary named collection
// OpenEndpoint creates; the ClickHouse grants of a backup or restore user
// can be limited to NAMED COLLECTION ON si_backup_*.
const CollectionPrefix = "si_backup_"

// collectionDropTimeout bounds the DROP NAMED COLLECTION that runs after the
// run's context ended.
const collectionDropTimeout = 30 * time.Second

// collectionName is si_backup_<unix seconds>_<16 lowercase hex>. Its creation
// time is part of the name because system.named_collections has no creation
// time, and DropStaleCollections needs one.
var collectionName = regexp.MustCompile(`^si_backup_([0-9]{1,19})_[0-9a-f]{16}$`)

// randRead fills the nonce; tests replace it.
var randRead = rand.Read

// newCollectionName is a fresh si_backup_<unix seconds>_<nonce>, the nonce 8
// random bytes in hex.
func newCollectionName(now time.Time) (string, error) {
	var nonce [8]byte
	if _, err := randRead(nonce[:]); err != nil {
		return "", fmt.Errorf("objstore: named collection nonce: %w", err)
	}
	return CollectionPrefix + strconv.FormatInt(now.Unix(), 10) + "_" + hex.EncodeToString(nonce[:]), nil
}

// collectionCreated is the creation time a collection name carries, false
// for a name newCollectionName cannot produce.
func collectionCreated(name string) (time.Time, bool) {
	m := collectionName.FindStringSubmatch(name)
	if m == nil {
		return time.Time{}, false
	}
	sec, err := strconv.ParseInt(m[1], 10, 64)
	if err != nil {
		return time.Time{}, false
	}
	return time.Unix(sec, 0), true
}

// createCollectionSQL creates the named collection name over bucket b with
// c's key pair and session token. The URL ends in a slash, so an Endpoint
// path is relative to the bucket.
func createCollectionSQL(name string, b Bucket, c Credentials) string {
	return "CREATE NAMED COLLECTION " + name + " AS url = " + literal(b.URL()+"/") +
		", access_key_id = " + literal(c.AccessKeyID) +
		", secret_access_key = " + literal(c.SecretAccessKey) +
		", session_token = " + literal(c.SessionToken)
}

// dropCollectionSQL drops the named collection name, if it exists.
func dropCollectionSQL(name string) string {
	return "DROP NAMED COLLECTION IF EXISTS " + name
}

// OpenEndpoint is the Endpoint of b with c for the BACKUP and RESTORE
// statements of one run, and the function that ends it, which the caller
// must call on every exit path (defer it). For a static key pair it is the
// S3(url, key, secret) form, nothing is created and closing does nothing.
// For credentials with a session token it creates the temporary named
// collection si_backup_<unix seconds>_<nonce> (D4 of the si-backup R2
// design), and closing drops it, with a fresh bounded context when ctx has
// ended by then. A collection a killed process leaves behind is dropped by
// DropStaleCollections. Every error is redacted.
//
// An incremental BACKUP through a collection records its base in its
// .backup as S3(<collection>, '<base path>'), and ClickHouse resolves that
// name again when it restores the incremental backup. Once the collection
// is dropped, RESTORE of that incremental backup fails with "There is no
// named collection" (measured on 26.8.37.168), so with a session token only
// full backups restore today. Before 26.8 the backup engine does not send a
// collection's session_token at all.
func OpenEndpoint(ctx context.Context, conn clickhouse.Conn, b Bucket, c Credentials) (Endpoint, func() error, error) {
	return openEndpoint(ctx, conn, b, c, time.Now())
}

func openEndpoint(ctx context.Context, conn clickhouse.Conn, b Bucket, c Credentials, now time.Time) (Endpoint, func() error, error) {
	if err := c.validate(); err != nil {
		return Endpoint{}, nil, err
	}
	if c.SessionToken == "" {
		return staticEndpoint(b, c), func() error { return nil }, nil
	}
	name, err := newCollectionName(now)
	if err != nil {
		return Endpoint{}, nil, err
	}
	drop := func() error {
		dropCtx := ctx
		if ctx.Err() != nil {
			var cancel context.CancelFunc
			dropCtx, cancel = context.WithTimeout(context.WithoutCancel(ctx), collectionDropTimeout)
			defer cancel()
		}
		if err := conn.Exec(dropCtx, dropCollectionSQL(name)); err != nil {
			return c.statementError("objstore: drop named collection %s", err, name)
		}
		return nil
	}
	if err := conn.Exec(ctx, createCollectionSQL(name, b, c)); err != nil {
		// A CREATE that failed on the client side may still have reached the
		// server; dropping a collection that does not exist is a no-op.
		createErr := c.statementError("objstore: create named collection %s", err, name)
		if dropErr := drop(); dropErr != nil {
			return Endpoint{}, nil, errors.Join(createErr, dropErr)
		}
		return Endpoint{}, nil, createErr
	}
	return Endpoint{bucket: b, creds: c, collection: name}, drop, nil
}

// DropStaleCollections drops every temporary named collection OpenEndpoint
// created more than maxAge ago, as the name records it: what a run that was
// killed before its drop left behind. A backup or restore job calls it at
// start with maxAge at least the longest a run may last (its credentials'
// TTL), so it never drops the collection of a run still going. Names under
// CollectionPrefix that OpenEndpoint cannot produce are left alone. It
// returns the names it dropped. The user needs to see the collections in
// system.named_collections (SHOW NAMED COLLECTIONS on si_backup_*).
func DropStaleCollections(ctx context.Context, conn clickhouse.Conn, maxAge time.Duration) ([]string, error) {
	return dropStaleCollections(ctx, conn, maxAge, time.Now())
}

func dropStaleCollections(ctx context.Context, conn clickhouse.Conn, maxAge time.Duration, now time.Time) ([]string, error) {
	if maxAge <= 0 {
		return nil, fmt.Errorf("objstore: stale named collection age %s must be positive", maxAge)
	}
	rows, err := conn.Query(ctx, "SELECT name FROM system.named_collections WHERE startsWith(name, ?) ORDER BY name", CollectionPrefix)
	if err != nil {
		return nil, fmt.Errorf("objstore: list the %s* named collections: %w", CollectionPrefix, err)
	}
	var stale []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			rows.Close()
			return nil, fmt.Errorf("objstore: list the %s* named collections: %w", CollectionPrefix, err)
		}
		if created, ok := collectionCreated(name); ok && now.Sub(created) > maxAge {
			stale = append(stale, name)
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, fmt.Errorf("objstore: list the %s* named collections: %w", CollectionPrefix, err)
	}
	var dropped []string
	for _, name := range stale {
		if err := conn.Exec(ctx, dropCollectionSQL(name)); err != nil {
			return dropped, fmt.Errorf("objstore: drop stale named collection %s: %w", name, err)
		}
		dropped = append(dropped, name)
	}
	return dropped, nil
}

// closeLogged calls an OpenEndpoint close function and logs, never returns,
// its error: a named collection left behind does not undo the statement that
// used it, and DropStaleCollections removes it later.
func closeLogged(closeEndpoint func() error, logger *slog.Logger, args ...any) {
	if err := closeEndpoint(); err != nil {
		logger.Warn("objstore: could not drop a temporary named collection; DropStaleCollections removes it later",
			append(args, "error", err.Error())...)
	}
}
