package objstore

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"
	"testing"
	"time"
)

// MinIO temporary credentials (STS AssumeRole of the mech-lockdelete,
// mech-nodelete and mech-readonly users) that scripts/ci/clickhouse-mechanics.sh
// mints for the session-token tests: the same rights as their parent keys,
// plus a session token, like the R2 temporary credentials of the si-backup
// broker.
const (
	envSTSLockDelete = "CH_S3_STS_LOCKDELETE"
	envSTSNoDelete   = "CH_S3_STS_NODELETE"
	envSTSReadOnly   = "CH_S3_STS_READONLY"
)

// requireSTS is the temporary credential of <prefix>_{ACCESS_KEY,SECRET_KEY,
// SESSION_TOKEN}; once ARBITER_CH_S3=1 (requireS3), a missing one fails.
func requireSTS(t *testing.T, prefix string) Credentials {
	t.Helper()
	c := Credentials{AccessKeyID: os.Getenv(prefix + "_ACCESS_KEY"), SecretAccessKey: os.Getenv(prefix + "_SECRET_KEY"), SessionToken: os.Getenv(prefix + "_SESSION_TOKEN")}
	if c.AccessKeyID == "" || c.SecretAccessKey == "" || c.SessionToken == "" {
		t.Fatalf("ARBITER_CH_S3=1 requires %s_ACCESS_KEY, %s_SECRET_KEY and %s_SESSION_TOKEN", prefix, prefix, prefix)
	}
	return c
}

// siBackupCollections lists the si_backup_* named collections.
func siBackupCollections(t *testing.T, env s3Env) []string {
	t.Helper()
	rows, err := env.conn.Query(context.Background(), "SELECT name FROM system.named_collections WHERE startsWith(name, ?) ORDER BY name", CollectionPrefix)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		names = append(names, name)
	}
	return names
}

// s3ServerAtLeast reports whether node A runs at least major.minor.
func s3ServerAtLeast(t *testing.T, env s3Env, major, minor int) bool {
	t.Helper()
	var version string
	if err := env.conn.QueryRow(context.Background(), "SELECT version()").Scan(&version); err != nil {
		t.Fatal(err)
	}
	var gotMajor, gotMinor int
	if _, err := fmt.Sscanf(version, "%d.%d.", &gotMajor, &gotMinor); err != nil {
		t.Fatalf("cannot read ClickHouse version %q: %v", version, err)
	}
	return gotMajor > major || gotMajor == major && gotMinor >= minor
}

// filesIn counts the objects named file directly in dir.
func filesIn(t *testing.T, env s3Env, dir, file string, c Credentials) uint64 {
	t.Helper()
	var n uint64
	query := "SELECT countIf(_file = " + literal(file) + ") FROM " + s3Function(dir+"*", c, "One") + " SETTINGS s3_throw_on_zero_files_match = 0"
	if err := env.conn.QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	return n
}

// TestS3_SessionTokenBackupAndDrillGoThroughNamedCollections runs the
// Backuper and the Drill with temporary credentials that carry a session
// token: index reads and writes use the five-argument s3(), BACKUP and
// RESTORE a temporary named collection that is dropped afterwards. With a
// key that may delete only */.lock a full backup and three differentials
// on it complete, and the last restores whole, with every row, after every
// collection of the chain was dropped; with a key that cannot delete,
// BACKUP fails only at removing its own .lock and the Backuper accepts it
// (D3), leaving the .lock, writing the index and logging a warning. The
// drill, with a read-only temporary key, verifies the last differential and
// the accepted backup. DropStaleCollections drops an old collection a
// killed run left.
func TestS3_SessionTokenBackupAndDrillGoThroughNamedCollections(t *testing.T) {
	// Given
	env := requireS3(t)
	write, noDelete, read := requireSTS(t, envSTSLockDelete), requireSTS(t, envSTSNoDelete), requireSTS(t, envSTSReadOnly)
	ctx := context.Background()
	before := siBackupCollections(t, env)
	tbl := createSafeTable(t, env, "t_session")
	insertPart(t, env, tbl, "p0", 0, 100)
	insertPart(t, env, tbl, "p1", 0, 7)
	s := publish(t, env, "", 1, tbl)
	src := newFakeManifests(s)
	ownerNoDelete := env.owner + 1
	sNoDelete := s
	sNoDelete.TrackFrontiers = append(sNoDelete.TrackFrontiers[:0:0], s.TrackFrontiers...)
	sNoDelete.TrackFrontiers[0].IndexerID = ownerNoDelete
	sNoDelete.SnapshotID = ""
	sNoDelete, err := sNoDelete.Seal()
	if err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	if !s3ServerAtLeast(t, env, 26, 8) {
		// Before 26.8 the S3 backup engine does not send a named
		// collection's session_token (the si-backup R2 design needs 26.8,
		// D4); the five-argument s3() works. Pin both, so a server that
		// changes either fails here.
		b := env.backuper(src, s3T0, 0)
		b.Credentials = write
		_, err := b.Run(ctx)
		if err == nil || !strings.Contains(err.Error(), "InvalidTokenId") {
			t.Fatalf("a session-token BACKUP before 26.8: %v; want the server's InvalidTokenId", err)
		}
		if _, err := ReadIndexes(ctx, env.conn, env.bucket, env.owner, read); err != nil {
			t.Fatalf("the five-argument s3() before 26.8: %v", err)
		}
		if after := siBackupCollections(t, env); fmt.Sprint(after) != fmt.Sprint(before) {
			t.Fatalf("named collections before %v, after %v: the failed run drops its own", before, after)
		}
		t.Logf("session-token BACKUP needs ClickHouse 26.8; the rest of the test runs there")
		return
	}
	backuper := func(owner uint64, m ManifestSource, at time.Time, c Credentials) *Backuper {
		b := env.backuper(m, at, 0)
		b.Owner, b.Credentials, b.Logger = owner, c, slog.New(slog.NewTextHandler(&logged, nil))
		return b
	}

	// When: a full backup and three differentials, each after one more part
	// and its own published manifest, each run dropping its collection; and
	// a full backup with the key that cannot delete.
	full, err := backuper(env.owner, src, s3T0, write).Run(ctx)
	if err != nil {
		t.Fatalf("full backup with a temporary key: %v", err)
	}
	rows := uint64(107)
	parent, seq := s, uint64(1)
	var diffs []BackupIndex
	for i := 1; i <= 3; i++ {
		insertPart(t, env, tbl, "p0", 1000*uint64(i), 5)
		rows += 5
		seq++
		parent = publish(t, env, parent.SnapshotID, seq, tbl)
		src.byID[parent.SnapshotID], src.latest = parent, parent.SnapshotID
		diff, err := backuper(env.owner, src, s3T0.Add(time.Duration(i)*time.Hour), write).Run(ctx)
		if err != nil {
			t.Fatalf("differential %d with a temporary key: %v", i, err)
		}
		diffs = append(diffs, diff)
	}
	accepted, errAccepted := backuper(ownerNoDelete, newFakeManifests(sNoDelete), s3T0, noDelete).Run(ctx)

	// Then: every differential is based on the full, in its index and in the
	// .backup ClickHouse wrote.
	for i, d := range diffs {
		if d.Kind != BackupIncremental || d.Base != full.Name || d.ChainLength != uint32(i+2) {
			t.Fatalf("differential %d: %+v; want based on %s, chain length %d", i+1, d, full.Name, i+2)
		}
		var descriptor string
		if err := env.conn.QueryRow(ctx, "SELECT * FROM "+s3Function(env.bucket.BackupURL(env.owner, d.Name)+".backup", read, "RawBLOB")).Scan(&descriptor); err != nil {
			t.Fatal(err)
		}
		if want := ", '" + OwnerPrefix(env.owner) + full.Name + "/')</base_backup>"; !strings.Contains(descriptor, want) {
			t.Fatalf("the .backup of differential %d does not name the full as its base (%s)", i+1, want)
		}
	}
	if errAccepted != nil {
		t.Fatalf("a refused .lock delete with the backup written must be accepted: %v", errAccepted)
	}
	if !strings.Contains(logged.String(), "accepting the backup") || strings.Count(logged.String(), "accepting the backup") != 1 {
		t.Fatalf("want exactly one acceptance warning, got: %s", logged.String())
	}
	for _, secret := range []string{write.SecretAccessKey, write.SessionToken, noDelete.SecretAccessKey, noDelete.SessionToken} {
		if strings.Contains(logged.String(), secret) {
			t.Fatal("a secret or a session token reached the log")
		}
	}
	type backupOf struct {
		owner     uint64
		name      string
		wantLocks uint64
	}
	all := []backupOf{{env.owner, full.Name, 0}, {ownerNoDelete, accepted.Name, 1}}
	for _, d := range diffs {
		all = append(all, backupOf{env.owner, d.Name, 0})
	}
	for _, x := range all {
		dir := env.bucket.BackupURL(x.owner, x.name)
		if n := filesIn(t, env, dir, ".lock", read); n != x.wantLocks {
			t.Errorf("backup %s of indexer %d holds %d .lock object(s), want %d", x.name, x.owner, n, x.wantLocks)
		}
		if n := filesIn(t, env, dir, ".backup", read); n != 1 {
			t.Errorf("backup %s of indexer %d holds %d .backup object(s), want 1", x.name, x.owner, n)
		}
	}
	if after := siBackupCollections(t, env); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("named collections before %v, after %v: every run drops its own", before, after)
	}

	// The last differential restores whole, through a fresh collection with
	// the read key and its full as base_backup, with every row.
	last := diffs[len(diffs)-1]
	endpoint, closeEndpoint, err := OpenEndpoint(ctx, env.conn, env.bucket, read)
	if err != nil {
		t.Fatal(err)
	}
	whole := Scratch{Database: StageDatabase, Table: "objstore_session_whole"}
	s3Exec(t, env.conn, "CREATE DATABASE IF NOT EXISTS "+StageDatabase)
	restoreErr := runAsync(ctx, env.conn, RestoreSQLAt(endpoint, env.owner, last.Name, last.Base, tbl.physical, "", whole), read, "RESTORED")
	if err := closeEndpoint(); err != nil {
		t.Fatal(err)
	}
	if restoreErr != nil {
		t.Fatalf("restore of the last differential: %v", restoreErr)
	}
	var restored uint64
	if err := env.conn.QueryRow(ctx, "SELECT count() FROM "+whole.Qualified()).Scan(&restored); err != nil {
		t.Fatal(err)
	}
	if restored != rows {
		t.Fatalf("the last differential restores %d rows, want %d", restored, rows)
	}

	// The drills gate the last differential (from its index alone: its Base
	// is the full) and the accepted full backup.
	for _, x := range []struct {
		owner uint64
		name  string
	}{{env.owner, last.Name}, {ownerNoDelete, accepted.Name}} {
		owner := x.owner
		m := ManifestSource(src)
		if owner == ownerNoDelete {
			m = newFakeManifests(sNoDelete)
		}
		namer, err := NewScratchNamer(fmt.Sprintf("objstore_session_%d_", owner%1000))
		if err != nil {
			t.Fatal(err)
		}
		d := &Drill{
			Conn: env.conn, Manifests: m, Schemas: fakeSchemas{tbl.schema.TableID: tbl.schema},
			Bucket: env.bucket, Owner: owner, NetworkID: s3NetworkID, Credentials: read, Scratch: namer, Backup: x.name,
		}
		v, err := d.Run(ctx)
		if err != nil || !v.Pass || v.Backup != x.name {
			t.Fatalf("drill of backup %s of indexer %d with a temporary read key: %+v, %v", x.name, owner, v, err)
		}
	}
	if after := siBackupCollections(t, env); fmt.Sprint(after) != fmt.Sprint(before) {
		t.Fatalf("named collections before %v, after the restores %v", before, after)
	}

	// A collection a killed run left 13 hours ago, and one of a run going on.
	stale := fmt.Sprintf("%s%d_0123456789abcdef", CollectionPrefix, time.Now().Add(-13*time.Hour).Unix())
	running := fmt.Sprintf("%s%d_fedcba9876543210", CollectionPrefix, time.Now().Unix())
	for _, name := range []string{stale, running} {
		s3Exec(t, env.conn, createCollectionSQL(name, env.bucket, read))
		t.Cleanup(func() { _ = env.conn.Exec(context.Background(), dropCollectionSQL(name)) })
	}
	dropped, err := DropStaleCollections(ctx, env.conn, 12*time.Hour)
	if err != nil || fmt.Sprint(dropped) != fmt.Sprint([]string{stale}) {
		t.Fatalf("DropStaleCollections dropped %v, %v; want [%s]", dropped, err, stale)
	}
	if after := siBackupCollections(t, env); !slices.Contains(after, running) || slices.Contains(after, stale) {
		t.Fatalf("collections after DropStaleCollections: %v", after)
	}
}
