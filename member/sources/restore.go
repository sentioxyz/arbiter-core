package sources

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/objstore"
)

const redacted = "[redacted]"

// RestoreIndexTimeout is the default deadline of the index read. ClickHouse
// 26.3 defaults s3_retry_attempts to 500, so an object store that refuses
// connections keeps the read retrying for many minutes (measured), and
// objstore.ReadIndexes replaces the context's query settings with its own, so
// the setting cannot be lowered for it. A context deadline survives:
// clickhouse-go sends it as max_execution_time, which stops the server's
// retries too, and the fetch is refused at the deadline (plan S3-C2 K35). The
// RESTORE and the copy INSERT run under the caller's context: once the index
// read succeeded the store is reachable, and a partition restore may be long,
// so they are bounded only by the installer's per-fetch deadline
// (install.Config.FetchTimeout), which covers the whole Fetch.
const RestoreIndexTimeout = 2 * time.Minute

// RestoreConfig is the member's read-only view of the per-owner backups of
// Plan S3-C1: Endpoint is http(s)://<host>[:port], Bucket the bucket name.
// The key pair comes only from the member's environment
// (STORAGE_INTEGRITY_MEMBER_RESTORE_ACCESS_KEY_ID and
// STORAGE_INTEGRITY_MEMBER_RESTORE_SECRET, contract §24.2); String, GoString
// and LogValue never print either part of it.
//
// With Credentials set, the static fields are not used: every Fetch asks it
// for the bucket and a key, for example short-lived R2 credentials from the
// si-backup credential broker (si-backup R2 design, D6 and §14).
type RestoreConfig struct {
	Endpoint, Bucket, AccessKeyID, Secret string
	// Credentials, when set, is called once per Fetch and supplies that
	// fetch's bucket URL and key in place of Endpoint, Bucket, AccessKeyID
	// and Secret; its error fails the fetch. nil keeps the static key pair,
	// whose statements are unchanged.
	Credentials RestoreCredentialsFunc
}

// RestoreCredentialsFunc supplies the bucket and the key of one Fetch. need is
// how long the fetch may run (the installer's per-fetch deadline), so the
// credentials it returns must stay valid at least that long.
type RestoreCredentialsFunc func(ctx context.Context, need time.Duration) (RestoreCredentials, error)

// RestoreCredentials is a RestoreCredentialsFunc's answer: BucketURL is
// http(s)://<host>[:port]/<bucket> (objstore.ParseBucket), Credentials the
// key, with its session token for temporary credentials. String, GoString
// and LogValue never print the secret or the token.
type RestoreCredentials struct {
	BucketURL   string
	Credentials objstore.Credentials
}

func (c RestoreCredentials) String() string {
	return fmt.Sprintf("RestoreCredentials{BucketURL: %q, Credentials: %s}", c.BucketURL, c.Credentials)
}

// GoString keeps %#v from printing the keys.
func (c RestoreCredentials) GoString() string { return c.String() }

func (c RestoreCredentials) LogValue() slog.Value {
	return slog.GroupValue(slog.String("bucket_url", c.BucketURL), slog.Any("credentials", c.Credentials))
}

func (c RestoreConfig) String() string {
	if c.Credentials != nil {
		return fmt.Sprintf("RestoreConfig{Credentials: func, AccessKeyID: %s, Secret: %s}", redacted, redacted)
	}
	return fmt.Sprintf("RestoreConfig{Endpoint: %q, Bucket: %q, AccessKeyID: %s, Secret: %s}", c.Endpoint, c.Bucket, redacted, redacted)
}

// GoString keeps %#v from printing the keys.
func (c RestoreConfig) GoString() string { return c.String() }

func (c RestoreConfig) LogValue() slog.Value {
	if c.Credentials != nil {
		return slog.GroupValue(slog.String("credentials", "func"),
			slog.String("access_key_id", redacted), slog.String("secret", redacted))
	}
	return slog.GroupValue(
		slog.String("endpoint", c.Endpoint), slog.String("bucket", c.Bucket),
		slog.String("access_key_id", redacted), slog.String("secret", redacted))
}

// RestoreSource restores a needed part from the owner's newest verified
// backup (housegate spec 2026-10-14 §8.6, plan S3-C1 contract §17): it reads
// the index objects under owner-<owner>/, picks the newest backup whose index
// lists a part of the needed physical table with the needed
// hash_of_all_files, restores that table's partition with objstore's
// RESTORE ... AS <scratch>_r PARTITIONS ... FROM S3(...) (the member's READ ON
// S3, K33), copies the needed part into the scratch with a local no-split
// INSERT ... SELECT ... WHERE _part = <name>, and drops <scratch>_r. RESTORE
// renames parts and keeps their hash_of_all_files (measured), so the part is
// found in <scratch>_r by its hash; copying it, rather than attaching it,
// keeps the scratch's own storage policy for ATTACH PARTITION ... FROM into
// the copy, and row hashes are content hashes, so the copy keeps the needed
// one. Parts of the restored partition that the need does not name never
// reach the scratch. A need that no backup lists is missed. The RESTORE runs
// synchronously: objstore's asynchronous restore polls system.backups, which
// the member's user may not read.
type RestoreSource struct {
	// IndexTimeout is the deadline of the index read; 0 is
	// RestoreIndexTimeout.
	IndexTimeout time.Duration
	// FetchTimeout is the need passed to RestoreConfig.Credentials: the
	// installer's per-fetch deadline. 0 takes what is left of the Fetch
	// context's deadline, or RestoreIndexTimeout without one.
	FetchTimeout time.Duration

	conn  clickhouse.Conn
	cfg   RestoreConfig
	owner uint64
}

// NewRestore restores from the backups of owner (its indexer id).
func NewRestore(conn clickhouse.Conn, cfg RestoreConfig, owner uint64) *RestoreSource {
	return &RestoreSource{conn: conn, cfg: cfg, owner: owner}
}

// Name is the source's metric label.
func (r *RestoreSource) Name() string { return SourceRestore }

func (r *RestoreSource) String() string {
	return fmt.Sprintf("RestoreSource{owner: %d, config: %s}", r.owner, r.cfg)
}

// GoString keeps %#v from printing the keys.
func (r *RestoreSource) GoString() string { return r.String() }

func (r *RestoreSource) LogValue() slog.Value {
	return slog.GroupValue(slog.Uint64("owner", r.owner), slog.Any("config", r.cfg))
}

// Fetch restores need into scratch. No error it returns carries a statement
// (the restore and index statements hold the key pair), and scrub removes
// either part of the key from any server message that echoes one.
func (r *RestoreSource) Fetch(ctx context.Context, need Need, schema payloadexec.TableSchema, scratch Scratch) Result {
	res := Result{Source: SourceRestore}
	if err := checkScratch(scratch); err != nil {
		res.Outcome, res.Err = OutcomeRefused, err
		return res
	}
	if need.Table == "" || need.PartPhysHash == "" {
		res.Outcome = OutcomeMissed
		return res
	}
	bucketURL := strings.TrimRight(r.cfg.Endpoint, "/") + "/" + r.cfg.Bucket
	creds := objstore.Credentials{AccessKeyID: r.cfg.AccessKeyID, SecretAccessKey: r.cfg.Secret}
	if r.cfg.Credentials != nil {
		got, err := r.cfg.Credentials(ctx, r.credentialNeed(ctx))
		if err != nil {
			return scrubKey(failure(res, "the restore credentials", err), got.Credentials)
		}
		bucketURL, creds = got.BucketURL, got.Credentials
	}
	bucket, err := objstore.ParseBucket(bucketURL)
	if err != nil {
		return scrubKey(failure(res, "the restore bucket", err), creds)
	}
	timeout := r.IndexTimeout
	if timeout <= 0 {
		timeout = RestoreIndexTimeout
	}
	indexCtx, cancel := context.WithTimeout(ctx, timeout)
	indexes, err := objstore.ReadIndexes(indexCtx, r.conn, bucket, r.owner, creds)
	cancel()
	if err != nil {
		return scrubKey(failure(res, fmt.Sprintf("read the backup indexes of indexer %d", r.owner), err), creds)
	}
	backup, base, ok := r.newestListing(indexes, need)
	if !ok {
		res.Outcome = OutcomeMissed
		return res
	}
	clause, err := objstore.PartitionClause(schema, need.PartitionID)
	if err != nil {
		return scrubKey(failure(res, "the restore partition", err), creds)
	}
	staged := Scratch{Database: scratch.Database, Table: scratch.Table + "_r"}
	dropTemporary(ctx, r.conn, staged)
	defer dropTemporary(ctx, r.conn, staged)
	// A key with a session token restores through a temporary named
	// collection; a static key pair through S3(url, key, secret), as before.
	endpoint, closeEndpoint, err := objstore.OpenEndpoint(ctx, r.conn, bucket, creds)
	if err != nil {
		return scrubKey(failure(res, "open the restore endpoint", err), creds)
	}
	restore := objstore.RestoreSQLAt(endpoint, r.owner, backup, base, need.Table, clause,
		objstore.Scratch{Database: staged.Database, Table: staged.Table})
	err = r.conn.Exec(ctx, restore)
	if dropErr := closeEndpoint(); dropErr != nil {
		slog.Warn("sources: could not drop a temporary named collection", "owner", r.owner, "backup", backup, "error", dropErr.Error())
	}
	if err != nil {
		return scrubKey(failure(res, fmt.Sprintf("restore %s partition %s from backup %s", need.Table, need.PartitionID, backup), err), creds)
	}
	name, rows, found, err := r.restoredPart(ctx, staged, need.PartPhysHash)
	if err != nil {
		return scrubKey(failure(res, "find the restored part", err), creds)
	}
	if !found {
		res.Outcome = OutcomeMissed
		return res
	}
	copyPart := "INSERT INTO " + scratch.qualified() + " SELECT * FROM " + staged.qualified() + " WHERE _part = " + literal(name)
	if err := r.conn.Exec(clickhouse.Context(ctx, clickhouse.WithSettings(NoSplitSettings(rows))), copyPart); err != nil {
		return scrubKey(failure(res, "copy the restored part "+name, err), creds)
	}
	parts, err := scratchParts(ctx, r.conn, scratch)
	if err != nil {
		return scrubKey(failure(res, "list the scratch parts", err), creds)
	}
	res.Outcome, res.Parts = OutcomeFetched, parts
	return res
}

// newestListing is the newest backup of the owner whose index lists a part of
// need's physical table with need's hash_of_all_files. Backup names start
// with their UTC start time, and objstore.ReadIndexes sorts by name. It
// returns the backup's name and its index Base.
func (r *RestoreSource) newestListing(indexes []objstore.BackupIndex, need Need) (string, string, bool) {
	for i := len(indexes) - 1; i >= 0; i-- {
		idx := indexes[i]
		if idx.OwnerIndexerID != r.owner {
			continue
		}
		for _, t := range idx.Tables {
			if t.PhysicalTable != need.Table {
				continue
			}
			for _, part := range t.Parts {
				if part.PhysHash == need.PartPhysHash {
					return idx.Name, idx.Base, true
				}
			}
		}
	}
	return "", "", false
}

func (r *RestoreSource) restoredPart(ctx context.Context, staged Scratch, physHash string) (string, uint64, bool, error) {
	rows, err := r.conn.Query(ctx, "SELECT name, rows FROM system.parts WHERE database = ? AND table = ? AND active AND hash_of_all_files = ? ORDER BY name LIMIT 1",
		staged.Database, staged.Table, physHash)
	if err != nil {
		return "", 0, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", 0, false, rows.Err()
	}
	var name string
	var count uint64
	if err := rows.Scan(&name, &count); err != nil {
		return "", 0, false, err
	}
	return name, count, true, rows.Err()
}

// credentialNeed is the need passed to RestoreConfig.Credentials.
func (r *RestoreSource) credentialNeed(ctx context.Context) time.Duration {
	if r.FetchTimeout > 0 {
		return r.FetchTimeout
	}
	if deadline, ok := ctx.Deadline(); ok {
		return time.Until(deadline)
	}
	return RestoreIndexTimeout
}

// scrubKey replaces each part of the restore key c (secret, session token
// and access key id, raw and SQL-escaped) in res.Err's text with
// [redacted]. The outcome was taken from the original error first; the
// scrubbed error does not unwrap to it, so no caller can print the original
// server message through errors.As.
func scrubKey(res Result, c objstore.Credentials) Result {
	if res.Err == nil {
		return res
	}
	text := res.Err.Error()
	for _, key := range []string{c.SecretAccessKey, c.SessionToken, c.AccessKeyID} {
		if key == "" {
			continue
		}
		text = strings.ReplaceAll(text, key, redacted)
		if escaped := chparts.EscapeSQLString(key); escaped != key {
			text = strings.ReplaceAll(text, escaped, redacted)
		}
	}
	if text != res.Err.Error() {
		res.Err = errors.New(text)
	}
	return res
}
