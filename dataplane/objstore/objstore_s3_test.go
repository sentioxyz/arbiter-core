package objstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// The TestS3_ tests run against node A of the ClickHouse mechanics pair and
// its MinIO (scripts/ci/clickhouse-mechanics.sh; README § ClickHouse
// mechanics), one after another: each rebuilds hg_safe and hg_rstage and
// uses an owner prefix of its own.
const (
	envS3            = "ARBITER_CH_S3"
	envS3Addr        = "CH_ADDR"
	envS3Endpoint    = "CH_S3_ENDPOINT"
	envS3Bucket      = "CH_S3_BUCKET"
	envS3WriteKey    = "CH_S3_LOCKDELETE_ACCESS_KEY"
	envS3WriteSecret = "CH_S3_LOCKDELETE_SECRET_KEY"
	envS3ReadKey     = "CH_S3_READONLY_ACCESS_KEY"
	envS3ReadSecret  = "CH_S3_READONLY_SECRET_KEY"
	s3NetworkID      = "objstore-s3-test"
)

// s3Env is node A, the mechanics bucket and two of its keys: write may
// delete only */.lock (the most a backup key may hold, K24) and read only
// reads and lists.
type s3Env struct {
	conn   clickhouse.Conn
	bucket Bucket
	write  Credentials
	read   Credentials
	owner  uint64
}

// requireS3 skips unless ARBITER_CH_S3=1 and then fails on any missing
// prerequisite, like the mechanics suite.
func requireS3(t *testing.T) s3Env {
	t.Helper()
	if os.Getenv(envS3) != "1" {
		t.Skip("set ARBITER_CH_S3=1 and run scripts/ci/clickhouse-mechanics.sh (README § ClickHouse mechanics) to run the object-store tests")
	}
	var missing []string
	for _, name := range []string{envS3Addr, envS3Endpoint, envS3Bucket, envS3WriteKey, envS3WriteSecret, envS3ReadKey, envS3ReadSecret} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("ARBITER_CH_S3=1 requires %s", strings.Join(missing, ", "))
	}
	bucket, err := ParseBucket(os.Getenv(envS3Endpoint) + "/" + os.Getenv(envS3Bucket))
	if err != nil {
		t.Fatal(err)
	}
	env := s3Env{
		conn:   chtest.Open(t, os.Getenv(envS3Addr)),
		bucket: bucket,
		write:  Credentials{AccessKeyID: os.Getenv(envS3WriteKey), SecretAccessKey: os.Getenv(envS3WriteSecret)},
		read:   Credentials{AccessKeyID: os.Getenv(envS3ReadKey), SecretAccessKey: os.Getenv(envS3ReadSecret)},
		// A prefix no earlier test or run used: no key of the suite deletes.
		owner: uint64(time.Now().UnixMicro()),
	}
	reset := func() {
		for _, db := range []string{SafeDatabase, StageDatabase} {
			if err := env.conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC"); err != nil {
				t.Logf("drop %s: %v", db, err)
			}
		}
	}
	reset()
	t.Cleanup(reset)
	s3Exec(t, env.conn, "CREATE DATABASE "+SafeDatabase)
	return env
}

func s3Exec(t *testing.T, conn clickhouse.Conn, query string) {
	t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// s3Table is one hg_safe table of mechanicsSchema's shape.
type s3Table struct {
	schema   payloadexec.TableSchema
	physical string
}

// createSafeTable creates hg_safe.<s3db__name> with the protocol DDL
// (merges pinned off, so every INSERT stays its own part).
func createSafeTable(t *testing.T, env s3Env, name string) s3Table {
	t.Helper()
	sch := payloadexec.TableSchema{
		TableID:     "s3db." + name,
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
	_, safe, _, err := ddl.Intents(ddl.Pinned{UnsafeDB: "hg_unsafe", SafeDB: SafeDatabase, PromoteDB: "hg_promote", NodeID: "objstore-s3", KeeperShardID: 1}, sch)
	if err != nil {
		t.Fatal(err)
	}
	s3Exec(t, env.conn, safe.SQL())
	return s3Table{schema: sch, physical: safe.Table}
}

// insertPart writes n rows of partition into tbl as one part.
func insertPart(t *testing.T, env s3Env, tbl s3Table, partition string, from, n uint64) {
	t.Helper()
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"max_threads": 1, "max_insert_threads": 1}))
	query := fmt.Sprintf("INSERT INTO %s.%s (%s, `p`, `v`) SELECT SHA256(concat('s3:', %s, ':', toString(number))), %s, number FROM numbers(%d, %d)",
		SafeDatabase, ident(tbl.physical), ident(ddl.RowIDColumn), literal(partition), literal(partition), from, n)
	if err := env.conn.Exec(ctx, query); err != nil {
		t.Fatalf("insert into %s: %v", tbl.physical, err)
	}
}

// publish seals the manifest a publication would carry for tables as
// hg_safe holds them now, with the owner's track frontier at seq.
func publish(t *testing.T, env s3Env, parent string, seq uint64, tables ...s3Table) replay.SafeSnapshotManifest {
	t.Helper()
	ctx := context.Background()
	var manifests []replay.TableManifest
	var schemas []payloadexec.TableSchema
	for _, tbl := range tables {
		parts, err := chparts.ActiveParts(ctx, env.conn, SafeDatabase, tbl.physical)
		if err != nil {
			t.Fatal(err)
		}
		scans, err := chexec.ScanAllParts(ctx, env.conn, SafeDatabase+"."+tbl.physical, tbl.schema)
		if err != nil {
			t.Fatal(err)
		}
		rowHash := map[string]string{}
		for _, s := range scans {
			rowHash[s.PartName] = s.RowLtHash
		}
		tm := replay.TableManifest{TableID: tbl.schema.TableID, SchemaHash: payloadexec.TableSchemaHash(s3NetworkID, tbl.schema)}
		byPartition := map[string][]string{}
		for _, p := range parts {
			id := chparts.LogicalPartitionID(tbl.schema, p)
			byPartition[id] = append(byPartition[id], rowHash[p.Name])
			tm.ActiveParts = append(tm.ActiveParts, replay.PartManifestEntry{
				TableID: tbl.schema.TableID, PartitionID: id, PartName: p.Name, PartPhysHash: p.PhysHash,
				PartRowLtHash: rowHash[p.Name], RowCount: p.Rows, Bytes: p.Bytes,
			})
		}
		for id, hashes := range byPartition {
			root, err := payloadexec.SumPartRowLtHashes(hashes)
			if err != nil {
				t.Fatal(err)
			}
			tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{TableID: tbl.schema.TableID, PartitionID: id, Root: root})
		}
		manifests = append(manifests, tm)
		schemas = append(schemas, tbl.schema)
	}
	m, err := (replay.SafeSnapshotManifest{
		ParentSnapshotID: parent, SafeBlockSeq: seq,
		SchemaSnapshotID: "objstore-s3", SchemaRoot: payloadexec.SchemaRoot(s3NetworkID, schemas), ExecutorProfileID: "objstore-s3",
		Tables:         manifests,
		TrackFrontiers: []replay.TrackFrontier{{IndexerID: env.owner, SafeBlockSeq: seq, AppliedPromotionSeq: seq}},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckManifest(m.SnapshotID, m); err != nil {
		t.Fatalf("the test manifest: %v", err)
	}
	return m
}

// fakeManifests is a ManifestSource over published test manifests that
// records which manifests a caller asked for.
type fakeManifests struct {
	latest      string
	byID        map[string]replay.SafeSnapshotManifest
	asked       []string
	latestCalls int
}

func newFakeManifests(ms ...replay.SafeSnapshotManifest) *fakeManifests {
	f := &fakeManifests{byID: map[string]replay.SafeSnapshotManifest{}}
	for _, m := range ms {
		f.byID[m.SnapshotID] = m
		f.latest = m.SnapshotID
	}
	return f
}

func (f *fakeManifests) Latest(context.Context) (replay.SafeSnapshotManifest, error) {
	f.latestCalls++
	return f.byID[f.latest], nil
}

func (f *fakeManifests) Manifest(_ context.Context, id string) (replay.SafeSnapshotManifest, error) {
	f.asked = append(f.asked, id)
	m, ok := f.byID[id]
	if !ok {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("no manifest %s", id)
	}
	return m, nil
}

type fakeSchemas map[string]payloadexec.TableSchema

func (f fakeSchemas) TableSchema(_ context.Context, id string) (payloadexec.TableSchema, error) {
	s, ok := f[id]
	if !ok {
		return payloadexec.TableSchema{}, fmt.Errorf("table %s has no schema on its owner's host (status %q)", id, "")
	}
	return s, nil
}

var s3T0 = time.Date(2026, 10, 15, 3, 17, 0, 0, time.UTC)

func (env s3Env) backuper(m ManifestSource, at time.Time, links uint32) *Backuper {
	return &Backuper{
		Conn: env.conn, Manifests: m, Bucket: env.bucket, Owner: env.owner, NetworkID: s3NetworkID,
		Credentials: env.write, MaxChainLinks: links, Now: func() time.Time { return at },
	}
}

// createdBackups counts system.backups rows of this test's owner that
// finished with BACKUP_CREATED.
func createdBackups(t *testing.T, env s3Env) uint64 {
	t.Helper()
	var n uint64
	if err := env.conn.QueryRow(context.Background(),
		"SELECT count() FROM system.backups WHERE status = 'BACKUP_CREATED' AND position(name, ?) > 0", "/"+OwnerPrefix(env.owner)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// scratchParts lists a scratch table's active parts.
func scratchParts(t *testing.T, env s3Env, s Scratch) []chparts.Part {
	t.Helper()
	parts, err := chparts.ActiveParts(context.Background(), env.conn, s.Database, s.Table)
	if err != nil {
		t.Fatal(err)
	}
	return parts
}

func physHashesOf(parts []chparts.Part) []string {
	var out []string
	for _, p := range parts {
		out = append(out, p.PhysHash)
	}
	sort.Strings(out)
	return out
}

func manifestHashes(m replay.SafeSnapshotManifest, tableID, partitionID string) []string {
	var out []string
	for _, t := range m.Tables {
		for _, p := range t.ActiveParts {
			if t.TableID == tableID && p.PartitionID == partitionID {
				out = append(out, p.PartRowLtHash)
			}
		}
	}
	sort.Strings(out)
	return out
}

func stageTables(t *testing.T, env s3Env, prefix string) uint64 {
	t.Helper()
	var n uint64
	if err := env.conn.QueryRow(context.Background(),
		"SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, ?)", StageDatabase, prefix).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestS3_BackuperRunsAFullThenAnIncrementalBackup(t *testing.T) {
	// Given
	env := requireS3(t)
	tbl := createSafeTable(t, env, "t_chain")
	insertPart(t, env, tbl, "p0", 0, 1000)
	insertPart(t, env, tbl, "p1", 0, 10)
	s := publish(t, env, "", 1, tbl)
	src := newFakeManifests(s)
	ctx := context.Background()

	// When: a full backup, one more part, an incremental one, and with a
	// two-link chain a full one again.
	full, err := env.backuper(src, s3T0, 2).Run(ctx)
	if err != nil {
		t.Fatalf("full backup: %v", err)
	}
	insertPart(t, env, tbl, "p0", 1000, 5)
	incr, err := env.backuper(src, s3T0.Add(time.Hour), 2).Run(ctx)
	if err != nil {
		t.Fatalf("incremental backup: %v", err)
	}
	again, err := env.backuper(src, s3T0.Add(2*time.Hour), 2).Run(ctx)
	if err != nil {
		t.Fatalf("third backup: %v", err)
	}

	// Then
	if full.Kind != BackupFull || full.Base != "" || full.ChainLength != 1 || full.Name != BackupName(s3T0, BackupFull) {
		t.Errorf("full: %+v", full)
	}
	if incr.Kind != BackupIncremental || incr.Base != full.Name || incr.ChainLength != 2 || incr.Name != BackupName(s3T0.Add(time.Hour), BackupIncremental) {
		t.Errorf("incremental: %+v", incr)
	}
	if again.Kind != BackupFull || again.Base != "" || again.ChainLength != 1 {
		t.Errorf("a two-link chain starts a full backup after its incremental one: %+v", again)
	}
	entry := replay.TrackFrontier{IndexerID: env.owner, SafeBlockSeq: 1, AppliedPromotionSeq: 1}
	for _, idx := range []BackupIndex{full, incr, again} {
		if idx.Format != BackupIndexFormat || idx.NetworkID != s3NetworkID || idx.OwnerIndexerID != env.owner ||
			idx.SnapshotID != s.SnapshotID || idx.Entry != entry || idx.StartedAt == "" || idx.FinishedAt == "" {
			t.Errorf("index header: %+v", idx)
		}
		if len(idx.Tables) != 1 || idx.Tables[0].PhysicalTable != tbl.physical || idx.Tables[0].TableID != tbl.schema.TableID || idx.Tables[0].IncarnationSeq != 0 {
			t.Errorf("index tables: %+v", idx.Tables)
		}
	}
	live, err := chparts.ActiveParts(ctx, env.conn, SafeDatabase, tbl.physical)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Tables[0].Parts) != 2 || len(incr.Tables[0].Parts) != 3 || len(live) != 3 {
		t.Fatalf("parts: full %d, incremental %d, live %d", len(full.Tables[0].Parts), len(incr.Tables[0].Parts), len(live))
	}
	for i, p := range incr.Tables[0].Parts {
		want := BackupIndexPart{Name: live[i].Name, PartitionID: chparts.LogicalPartitionID(tbl.schema, live[i]), PhysHash: live[i].PhysHash, Rows: live[i].Rows}
		if p != want {
			t.Errorf("index part %d: %+v, want %+v", i, p, want)
		}
	}
	read, err := ReadIndexes(ctx, env.conn, env.bucket, env.owner, env.read)
	if err != nil {
		t.Fatalf("read indexes with the restore key: %v", err)
	}
	if len(read) != 3 || read[0].Name != full.Name || read[1].Name != incr.Name || read[2].Name != again.Name {
		t.Fatalf("indexes: %+v", read)
	}
	if n := createdBackups(t, env); n != 3 {
		t.Fatalf("system.backups holds %d created backups of the owner, want 3", n)
	}
}

func TestS3_BackuperNeverReusesABackupName(t *testing.T) {
	// Given
	env := requireS3(t)
	tbl := createSafeTable(t, env, "t_reuse")
	insertPart(t, env, tbl, "p0", 0, 10)
	src := newFakeManifests(publish(t, env, "", 1, tbl))
	ctx := context.Background()
	first, err := env.backuper(src, s3T0, 1).Run(ctx)
	if err != nil {
		t.Fatalf("first backup: %v", err)
	}
	orphan := BackupName(s3T0.Add(time.Hour), BackupFull)
	if err := runAsync(ctx, env.conn, BackupSQL(env.bucket, env.owner, orphan, "", env.write), env.write, "BACKUP_CREATED"); err != nil {
		t.Fatalf("a backup without an index: %v", err)
	}

	// The remains of a backup that failed: one object, no .backup file.
	stray := BackupName(s3T0.Add(2*time.Hour), BackupFull)
	s3Exec(t, env.conn, "INSERT INTO FUNCTION "+s3Function(env.bucket.BackupURL(env.owner, stray)+"data/stray.bin", env.write, "RawBLOB")+" SELECT 'stray'")

	// When: the same name again (indexed), the orphan's name (no index) and
	// the stray's name (no .backup file).
	_, errIndexed := env.backuper(src, s3T0, 1).Run(ctx)
	_, errOrphan := env.backuper(src, s3T0.Add(time.Hour), 1).Run(ctx)
	_, errStray := env.backuper(src, s3T0.Add(2*time.Hour), 1).Run(ctx)
	errIndex := writeIndex(ctx, env.conn, env.bucket, env.write, first)

	// Then
	for name, err := range map[string]error{first.Name: errIndexed, orphan: errOrphan, stray: errStray} {
		want := fmt.Sprintf("objstore: backup %s of indexer %d exists already; a backup name is never reused", name, env.owner)
		if err == nil || err.Error() != want {
			t.Errorf("got %v, want %s", err, want)
		}
	}
	var ex *clickhouse.Exception
	if !errors.As(errIndex, &ex) || ex.Code != 36 || !strings.Contains(ex.Message, "already exists") {
		t.Errorf("rewriting an index: %v, want ClickHouse's refusal (36) of an existing object", errIndex)
	}
	if n := createdBackups(t, env); n != 2 {
		t.Fatalf("system.backups holds %d created backups of the owner, want 2 (no BACKUP ran for a refused name)", n)
	}
	read, err := ReadIndexes(ctx, env.conn, env.bucket, env.owner, env.read)
	if err != nil || len(read) != 1 || !reflect.DeepEqual(read[0], first) {
		t.Fatalf("the index is unchanged: %+v, %v", read, err)
	}
}

func TestS3_RestoreAndGateKeepsOnlyTheManifestsPartsFromABackupAheadOfPublication(t *testing.T) {
	// Given: two parts of p0 and one of p1 published, then a third part of p0
	// written before the backup.
	env := requireS3(t)
	tbl := createSafeTable(t, env, "t_ahead")
	insertPart(t, env, tbl, "p0", 0, 100)
	insertPart(t, env, tbl, "p0", 100, 10)
	insertPart(t, env, tbl, "p1", 0, 5)
	s := publish(t, env, "", 1, tbl)
	insertPart(t, env, tbl, "p0", 110, 5)
	ctx := context.Background()
	idx, err := env.backuper(newFakeManifests(s), s3T0, 0).Run(ctx)
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	namer, err := NewScratchNamer("objstore_ahead_")
	if err != nil {
		t.Fatal(err)
	}
	scratch := namer.Next()

	// When
	res, err := RestoreAndGate(ctx, env.conn, GateInput{
		Backup:   BackupRef{Bucket: env.bucket, Owner: env.owner, Name: idx.Name, Credentials: env.read},
		Manifest: s, TableID: tbl.schema.TableID, PhysicalTable: tbl.physical, Schema: tbl.schema,
		PartitionID: "p_p0", Scratch: scratch,
	})

	// Then
	if err != nil {
		t.Fatalf("restore and gate: %v", err)
	}
	if !res.Complete || res.Reason != DrillVerified || res.Plan.Diverged || len(res.Plan.Need) != 2 {
		t.Fatalf("result: %+v", res)
	}
	if len(res.Classification.Keep) != 2 || len(res.Classification.Drop) != 1 {
		t.Fatalf("kept %v, dropped %v; want the two published parts kept and the later one dropped", res.Classification.Keep, res.Classification.Drop)
	}
	// RESTORE recreates the scratch from the backed-up CREATE, so it keeps
	// hg_safe's merge pin: no background merge changes the scratch's parts
	// between the restore, the scan and the DROP PART.
	var engine string
	if err := env.conn.QueryRow(ctx, "SELECT engine_full FROM system.tables WHERE database = ? AND name = ?", scratch.Database, scratch.Table).Scan(&engine); err != nil ||
		!strings.Contains(engine, "max_bytes_to_merge_at_max_space_in_pool = 0") {
		t.Fatalf("the scratch must keep hg_safe's merge pin (%v): %s", err, engine)
	}
	kept := slices.Clone(res.Classification.Kept)
	sort.Strings(kept)
	if !slices.Equal(kept, manifestHashes(s, tbl.schema.TableID, "p_p0")) {
		t.Fatal("the kept parts are not the manifest's parts")
	}
	parts := scratchParts(t, env, scratch)
	var publishedPhys []string
	for _, p := range s.Tables[0].ActiveParts {
		if p.PartitionID == "p_p0" {
			publishedPhys = append(publishedPhys, p.PartPhysHash)
		}
	}
	sort.Strings(publishedPhys)
	if !slices.Equal(physHashesOf(parts), publishedPhys) {
		t.Fatalf("the scratch holds %v, want the published parts' hash_of_all_files %v", physHashesOf(parts), publishedPhys)
	}
	var names []string
	for _, p := range parts {
		names = append(names, p.Name)
		if p.PartitionValue != "p0" {
			t.Errorf("the scratch holds a part of partition %s", p.PartitionValue)
		}
	}
	keptNames := slices.Clone(res.KeptParts)
	sort.Strings(keptNames)
	if !slices.Equal(names, keptNames) {
		t.Errorf("KeptParts %v, the scratch's parts %v", res.KeptParts, names)
	}
}

func TestS3_RestoreAndGateReportsANeedTheBackupCannotMeet(t *testing.T) {
	// Given: a backup of one part, then a manifest of two.
	env := requireS3(t)
	tbl := createSafeTable(t, env, "t_short")
	insertPart(t, env, tbl, "p0", 0, 50)
	s0 := publish(t, env, "", 1, tbl)
	ctx := context.Background()
	idx, err := env.backuper(newFakeManifests(s0), s3T0, 0).Run(ctx)
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	insertPart(t, env, tbl, "p0", 50, 5)
	s1 := publish(t, env, s0.SnapshotID, 2, tbl)
	namer, err := NewScratchNamer("objstore_short_")
	if err != nil {
		t.Fatal(err)
	}
	in := GateInput{
		Backup:   BackupRef{Bucket: env.bucket, Owner: env.owner, Name: idx.Name, Credentials: env.read},
		Manifest: s1, TableID: tbl.schema.TableID, PhysicalTable: tbl.physical, Schema: tbl.schema, PartitionID: "p_p0",
	}

	// When
	in.Scratch = namer.Next()
	short, errShort := RestoreAndGate(ctx, env.conn, in)
	diverged := in
	diverged.Scratch = namer.Next()
	diverged.Local = []string{elementHex("a row hash the manifest does not list")}
	div, errDiv := RestoreAndGate(ctx, env.conn, diverged)

	// Then
	if errShort != nil || short.Complete || short.Reason != DrillNeedUnsatisfied || len(short.Plan.Need) != 2 || len(short.Classification.Kept) != 1 {
		t.Fatalf("a backup missing a needed part: %+v, %v", short, errShort)
	}
	if n := len(scratchParts(t, env, in.Scratch)); n != 1 {
		t.Fatalf("the failed gate keeps its scratch with the one part it restored, holds %d", n)
	}
	if errDiv != nil || !div.Plan.Diverged || div.Complete || div.Reason != "" {
		t.Fatalf("a local copy that is not a sub-multiset: %+v, %v", div, errDiv)
	}
	if n := stageTables(t, env, diverged.Scratch.Table); n != 0 {
		t.Fatalf("a Diverged plan restores nothing, found %d scratch table(s)", n)
	}
}

func TestS3_DrillVerifiesABackupAgainstTheManifestItsIndexNames(t *testing.T) {
	// Given: S1 lists t1; t2 is created after it; the backup names S1; then
	// t1 grows and S2, the latest manifest, lists more than the backup holds.
	env := requireS3(t)
	t1 := createSafeTable(t, env, "t_drill_1")
	insertPart(t, env, t1, "p0", 0, 100)
	insertPart(t, env, t1, "p1", 0, 7)
	s1 := publish(t, env, "", 1, t1)
	t2 := createSafeTable(t, env, "t_drill_2")
	insertPart(t, env, t2, "p0", 0, 3)
	src := newFakeManifests(s1)
	ctx := context.Background()
	idx, err := env.backuper(src, s3T0, 0).Run(ctx)
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	insertPart(t, env, t1, "p0", 100, 9)
	s2 := publish(t, env, s1.SnapshotID, 2, t1, t2)
	src.byID[s2.SnapshotID], src.latest = s2, s2.SnapshotID
	src.latestCalls, src.asked = 0, nil
	namer, err := NewScratchNamer("objstore_drill_")
	if err != nil {
		t.Fatal(err)
	}
	d := &Drill{
		Conn: env.conn, Manifests: src, Schemas: fakeSchemas{t1.schema.TableID: t1.schema, t2.schema.TableID: t2.schema},
		Bucket: env.bucket, Owner: env.owner, NetworkID: s3NetworkID, Credentials: env.read, Scratch: namer,
	}

	// When
	v, err := d.Run(ctx)

	// Then
	if err != nil {
		t.Fatalf("drill: %v", err)
	}
	if !v.Pass || v.Format != DrillVerdictFormat || v.Backup != idx.Name || v.SnapshotID != s1.SnapshotID || v.OwnerIndexerID != env.owner || v.Entry != idx.Entry {
		t.Fatalf("verdict: %+v", v)
	}
	if src.latestCalls != 0 || !slices.Equal(src.asked, []string{s1.SnapshotID}) {
		t.Fatalf("the drill asked for the latest manifest %d time(s) and for %v; it must take only the index's %s", src.latestCalls, src.asked, s1.SnapshotID)
	}
	if len(v.Tables) != 2 || v.Tables[0].PhysicalTable != t1.physical || v.Tables[0].Reason != DrillVerified ||
		v.Tables[1].PhysicalTable != t2.physical || v.Tables[1].Reason != DrillNotInManifest || v.Tables[1].TableID != "" {
		t.Fatalf("tables: %+v", v.Tables)
	}
	if got := v.Tables[0].Partitions; len(got) != 2 || got[0] != (DrillPartition{PartitionID: "p_p0", Reason: DrillVerified, Need: 1, Kept: 1}) ||
		got[1] != (DrillPartition{PartitionID: "p_p1", Reason: DrillVerified, Need: 1, Kept: 1}) {
		t.Fatalf("partitions: %+v", got)
	}
	if n := stageTables(t, env, "objstore_drill_"); n != 0 {
		t.Fatalf("a verified partition's scratch is dropped, %d remain", n)
	}
	named := *d
	named.Backup = idx.Name
	if again, err := named.Run(ctx); err != nil || !again.Pass || again.Backup != idx.Name {
		t.Fatalf("a drill of the named backup: %+v, %v", again, err)
	}
}

func TestS3_DrillFailsWhenNoTableOfTheOwnerIsVerified(t *testing.T) {
	// Given: hg_safe holds a table that the manifest does not list.
	env := requireS3(t)
	tbl := createSafeTable(t, env, "t_unlisted")
	insertPart(t, env, tbl, "p0", 0, 4)
	s := publish(t, env, "", 1)
	ctx := context.Background()
	if _, err := env.backuper(newFakeManifests(s), s3T0, 0).Run(ctx); err != nil {
		t.Fatalf("backup: %v", err)
	}
	namer, err := NewScratchNamer("objstore_unlisted_")
	if err != nil {
		t.Fatal(err)
	}
	d := &Drill{
		Conn: env.conn, Manifests: newFakeManifests(s), Schemas: fakeSchemas{tbl.schema.TableID: tbl.schema},
		Bucket: env.bucket, Owner: env.owner, NetworkID: s3NetworkID, Credentials: env.read, Scratch: namer,
	}

	// When
	v, err := d.Run(ctx)
	_, errNone := (&Drill{Conn: env.conn, Manifests: newFakeManifests(s), Schemas: fakeSchemas{}, Bucket: env.bucket, Owner: env.owner + 1, NetworkID: s3NetworkID, Credentials: env.read, Scratch: namer}).Run(ctx)

	// Then
	if err != nil {
		t.Fatalf("drill: %v", err)
	}
	if v.Pass || len(v.Tables) != 1 || v.Tables[0].Reason != DrillNotInManifest {
		t.Fatalf("a drill that verified no table must not pass: %+v", v)
	}
	want := fmt.Sprintf("objstore: no backup of indexer %d under %s/%s", env.owner+1, env.bucket.URL(), OwnerPrefix(env.owner+1))
	if errNone == nil || errNone.Error() != want {
		t.Fatalf("an owner without a backup: %v, want %s", errNone, want)
	}
}

func TestS3_DrillRefusesASchemaWhoseHashIsNotTheManifests(t *testing.T) {
	// Given: the owner's host answers a schema whose hash is not the
	// manifest's schema_hash.
	env := requireS3(t)
	tbl := createSafeTable(t, env, "t_schema")
	insertPart(t, env, tbl, "p0", 0, 4)
	s := publish(t, env, "", 1, tbl)
	ctx := context.Background()
	if _, err := env.backuper(newFakeManifests(s), s3T0, 0).Run(ctx); err != nil {
		t.Fatalf("backup: %v", err)
	}
	wrong := tbl.schema
	wrong.Columns = []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt32"}}
	namer, err := NewScratchNamer("objstore_schema_")
	if err != nil {
		t.Fatal(err)
	}
	d := &Drill{
		Conn: env.conn, Manifests: newFakeManifests(s), Schemas: fakeSchemas{tbl.schema.TableID: wrong},
		Bucket: env.bucket, Owner: env.owner, NetworkID: s3NetworkID, Credentials: env.read, Scratch: namer,
	}

	// When
	v, err := d.Run(ctx)

	// Then
	if err != nil {
		t.Fatalf("drill: %v", err)
	}
	if v.Pass || len(v.Tables) != 1 || v.Tables[0].Reason != DrillSchemaHashMismatch || len(v.Tables[0].Partitions) != 0 {
		t.Fatalf("verdict: %+v", v)
	}
	if n := stageTables(t, env, "objstore_schema_"); n != 0 {
		t.Fatalf("nothing is restored for a table whose schema is refused, found %d scratch table(s)", n)
	}
}
