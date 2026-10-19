package chmechanics

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/objstore"
)

// The S3 mechanics (M12-M14, plan S3-C1 contract §17) run against the
// MinIO the script starts beside the pair (bucket si-mechanics, network
// alias arbiter-core-minio) with three keys: mech-nodelete may read, write
// and list objects and abort multipart uploads but delete nothing;
// mech-lockdelete may also delete objects whose name ends in /.lock;
// mech-readonly may only read and list.
const (
	envS3                 = "ARBITER_CH_S3"
	envS3Endpoint         = "CH_S3_ENDPOINT"
	envS3Bucket           = "CH_S3_BUCKET"
	envS3NoDeleteKey      = "CH_S3_NODELETE_ACCESS_KEY"
	envS3NoDeleteSecret   = "CH_S3_NODELETE_SECRET_KEY"
	envS3LockDeleteKey    = "CH_S3_LOCKDELETE_ACCESS_KEY"
	envS3LockDeleteSecret = "CH_S3_LOCKDELETE_SECRET_KEY"
	envS3ReadOnlyKey      = "CH_S3_READONLY_ACCESS_KEY"
	envS3ReadOnlySecret   = "CH_S3_READONLY_SECRET_KEY"
)

// siBackupGrants is SI_BACKUP_GRANTS and siRestoreGrants SI_RESTORE_GRANTS
// (plan S3-C1 contract §18), verbatim and in order. A measured difference
// changes these lists and production's test_render.py copies in one step.
var (
	siBackupGrants = []string{
		"GRANT BACKUP ON hg_safe.*",
		"GRANT READ, WRITE ON S3",
		"GRANT CREATE TEMPORARY TABLE ON *.*",
		"GRANT SELECT ON system.parts",
		"GRANT SELECT ON system.tables",
		"GRANT SELECT ON system.backups",
	}
	siRestoreGrants = []string{
		"GRANT CREATE DATABASE ON hg_rstage.*",
		"GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE ON hg_rstage.*",
		"GRANT READ ON S3",
		"GRANT CREATE TEMPORARY TABLE ON *.*",
		"GRANT SELECT ON system.parts",
		"GRANT SELECT ON system.tables",
		"GRANT SELECT ON system.backups",
	}
)

// s3Mechanics is the mechanics bucket and its three keys.
type s3Mechanics struct {
	bucket                         objstore.Bucket
	noDelete, lockDelete, readOnly objstore.Credentials
}

// requireS3 skips unless ARBITER_CH_S3=1 and then fails on any missing
// variable: like the restart tests, a second opt-in, here for a run with
// the script's MinIO.
func requireS3(t *testing.T) s3Mechanics {
	t.Helper()
	if os.Getenv(envS3) != "1" {
		t.Skip("set ARBITER_CH_S3=1 with the MinIO of scripts/ci/clickhouse-mechanics.sh to run the S3 mechanics")
	}
	names := []string{envS3Endpoint, envS3Bucket, envS3NoDeleteKey, envS3NoDeleteSecret, envS3LockDeleteKey, envS3LockDeleteSecret, envS3ReadOnlyKey, envS3ReadOnlySecret}
	var missing []string
	for _, name := range names {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("ARBITER_CH_S3=1 requires %s", strings.Join(missing, ", "))
	}
	bucket, err := objstore.ParseBucket(os.Getenv(envS3Endpoint) + "/" + os.Getenv(envS3Bucket))
	if err != nil {
		t.Fatal(err)
	}
	key := func(id, secret string) objstore.Credentials {
		return objstore.Credentials{AccessKeyID: os.Getenv(id), SecretAccessKey: os.Getenv(secret)}
	}
	return s3Mechanics{
		bucket:     bucket,
		noDelete:   key(envS3NoDeleteKey, envS3NoDeleteSecret),
		lockDelete: key(envS3LockDeleteKey, envS3LockDeleteSecret),
		readOnly:   key(envS3ReadOnlyKey, envS3ReadOnlySecret),
	}
}

// secrets are the three mechanics secrets M14 searches for.
func (s s3Mechanics) secrets() []string {
	return []string{s.noDelete.SecretAccessKey, s.lockDelete.SecretAccessKey, s.readOnly.SecretAccessKey}
}

// prefix is a directory of the mechanics bucket no other test or run uses:
// no key of the suite deletes, so names are never reused.
func (s s3Mechanics) prefix(t *testing.T, label string) string {
	return fmt.Sprintf("%s/mechanics/%s-%s-%d/", s.bucket.URL(), label, suffix(t), time.Now().UnixNano())
}

// s3Target is a BACKUP or RESTORE destination.
func s3Target(url string, c objstore.Credentials) string {
	return fmt.Sprintf("S3(%s, %s, %s)", literal(url), literal(c.AccessKeyID), literal(c.SecretAccessKey))
}

// s3Table is the s3() table function.
func s3Table(url string, c objstore.Credentials, format string) string {
	return fmt.Sprintf("s3(%s, %s, %s, %s)", literal(url), literal(c.AccessKeyID), literal(c.SecretAccessKey), literal(format))
}

// countObjects counts the objects whose name starts with url through s3()
// (_path; the One format reads none of them, and a glob that matches nothing
// counts 0).
func countObjects(t *testing.T, conn clickhouse.Conn, url string, c objstore.Credentials) uint64 {
	t.Helper()
	var n uint64
	query := "SELECT count(DISTINCT _path) FROM " + s3Table(url+"**", c, "One") + " SETTINGS s3_throw_on_zero_files_match = 0"
	if err := conn.QueryRow(context.Background(), query).Scan(&n); err != nil {
		t.Fatalf("list %s: %v", url, err)
	}
	return n
}

// TestMechanics_S3IncrementalBackupRestoreKeepsHashOfAllFiles (M12) re-pins
// M5 on an S3 endpoint, the transport of P1's backups (spec §2, §16 P1):
// BACKUP DATABASE to S3() with base_backup stores fewer objects than the full
// backup, and RESTORE TABLE ... AS <stage> PARTITIONS from the incremental
// backup with a read-only key restores every part of the partition with its
// hash_of_all_files, rows and row hashes, and no other partition.
func TestMechanics_S3IncrementalBackupRestoreKeepsHashOfAllFiles(t *testing.T) {
	env := requireMechanics(t)
	s3 := requireS3(t)
	p := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, _ := protocolTables(t, env.A, env.AAddr, p, sch)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 0, 100_000, nil)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p1", 0, 10, nil)
	dir := s3.prefix(t, "m12")
	full, inc := dir+"full/", dir+"inc/"
	fullStats := runBackup(t, env.A, fmt.Sprintf("BACKUP DATABASE %s TO %s", ident(p.SafeDB), s3Target(full, s3.lockDelete)), "BACKUP_CREATED")
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 100_000, 10, nil)
	incStats := runBackup(t, env.A, fmt.Sprintf("BACKUP DATABASE %s TO %s SETTINGS base_backup = %s",
		ident(p.SafeDB), s3Target(inc, s3.lockDelete), s3Target(full, s3.lockDelete)), "BACKUP_CREATED")
	fullObjects, incObjects := countObjects(t, env.A, full, s3.readOnly), countObjects(t, env.A, inc, s3.readOnly)
	t.Logf("full backup %+v in %d objects; incremental backup %+v in %d objects", fullStats, fullObjects, incStats, incObjects)
	if incObjects >= fullObjects {
		t.Fatalf("the incremental backup stores %d objects, not fewer than the full backup's %d: base_backup was not used", incObjects, fullObjects)
	}
	if incStats.NumEntries >= incStats.NumFiles {
		t.Fatalf("the incremental backup stores %d of the %d files it describes: base_backup was not used", incStats.NumEntries, incStats.NumFiles)
	}
	stage := "hg_rstage_m" + suffix(t)
	dropDatabasesAt(t, env.AAddr, stage)
	mustExec(t, env.A, "CREATE DATABASE IF NOT EXISTS "+ident(stage))
	runBackup(t, env.A, fmt.Sprintf("RESTORE TABLE %s AS %s PARTITIONS 'p0' FROM %s",
		qualified(p.SafeDB, safe.Table), qualified(stage, safe.Table), s3Target(inc, s3.readOnly)), "RESTORED")
	want := activeParts(t, env.A, p.SafeDB, safe.Table, "p0")
	got := activeParts(t, env.A, stage, safe.Table, "p0")
	if len(want) != 2 {
		t.Fatalf("source p0 holds %d parts, want 2: %v", len(want), names(want))
	}
	if gotHashes, wantHashes := physHashes(got), physHashes(want); !slices.Equal(gotHashes, wantHashes) {
		t.Fatalf("RESTORE from the incremental S3 backup: hash_of_all_files %v, source %v", gotHashes, wantHashes)
	}
	if rows := totalRows(got); rows != 100_010 {
		t.Fatalf("the staging p0 holds %d rows, want 100010", rows)
	}
	if !slices.Equal(rowHashList(t, env.A, stage, safe.Table, sch, got), rowHashList(t, env.A, p.SafeDB, safe.Table, sch, want)) {
		t.Fatal("the restored rows differ from the source's")
	}
	if other := activeParts(t, env.A, stage, safe.Table, "p1"); len(other) != 0 {
		t.Fatalf("RESTORE ... PARTITIONS 'p0' restored partition p1: %v", names(other))
	}
}

// mechSchemas is an objstore.SchemaSource over known schemas.
type mechSchemas map[string]payloadexec.TableSchema

func (s mechSchemas) TableSchema(_ context.Context, id string) (payloadexec.TableSchema, error) {
	sch, ok := s[id]
	if !ok {
		return payloadexec.TableSchema{}, fmt.Errorf("no schema for %s", id)
	}
	return sch, nil
}

// mechManifests is an objstore.ManifestSource of one manifest.
type mechManifests struct{ m replay.SafeSnapshotManifest }

func (s mechManifests) Latest(context.Context) (replay.SafeSnapshotManifest, error) { return s.m, nil }

func (s mechManifests) Manifest(_ context.Context, id string) (replay.SafeSnapshotManifest, error) {
	if id != s.m.SnapshotID {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("no manifest %s", id)
	}
	return s.m, nil
}

// mechManifest seals the manifest a publication would carry for db.table as
// it is now, with owner's track frontier.
func mechManifest(t *testing.T, conn clickhouse.Conn, db, table string, sch payloadexec.TableSchema, owner uint64) replay.SafeSnapshotManifest {
	t.Helper()
	var parts []mechPart
	for _, part := range partsOf(t, conn, db, table) {
		if part.Active {
			parts = append(parts, part)
		}
	}
	hashes := rowHashes(t, conn, db, table, sch, parts)
	tm := replay.TableManifest{TableID: sch.TableID, SchemaHash: payloadexec.TableSchemaHash("mechanics", sch)}
	byPartition := map[string][]string{}
	for _, part := range parts {
		id := "p_" + part.Partition
		byPartition[id] = append(byPartition[id], hashes[part.Name])
		tm.ActiveParts = append(tm.ActiveParts, replay.PartManifestEntry{
			TableID: sch.TableID, PartitionID: id, PartName: part.Name, PartPhysHash: part.PhysHash, PartRowLtHash: hashes[part.Name], RowCount: part.Rows,
		})
	}
	for id, list := range byPartition {
		tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{TableID: sch.TableID, PartitionID: id, Root: sumRowHashes(t, list...)})
	}
	m, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq: 1, SchemaSnapshotID: "mechanics", SchemaRoot: payloadexec.SchemaRoot("mechanics", []payloadexec.TableSchema{sch}), ExecutorProfileID: "mechanics",
		Tables:         []replay.TableManifest{tm},
		TrackFrontiers: []replay.TrackFrontier{{IndexerID: owner, SafeBlockSeq: 1, AppliedPromotionSeq: 1}},
	}).Seal()
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// ownProtocolDatabases gives the test hg_safe and hg_rstage on node A,
// empty, for its duration: SI_BACKUP_GRANTS and SI_RESTORE_GRANTS name
// them, and no other mechanics test uses them.
func ownProtocolDatabases(t *testing.T, env mechanicsEnv) {
	t.Helper()
	for _, db := range []string{objstore.SafeDatabase, objstore.StageDatabase} {
		mustExec(t, env.A, "DROP DATABASE IF EXISTS "+db+" SYNC")
	}
	dropDatabasesAt(t, env.AAddr, objstore.SafeDatabase, objstore.StageDatabase)
	mustExec(t, env.A, "CREATE DATABASE "+objstore.SafeDatabase)
}

// grantUser creates user with exactly grants (each "GRANT ... ON ...",
// completed with TO <user>), dropping a leftover of an earlier run first.
func grantUser(t *testing.T, env mechanicsEnv, user string, grants []string) clickhouse.Conn {
	t.Helper()
	mustExec(t, env.A, "DROP USER IF EXISTS "+ident(user))
	formats := make([]string, 0, len(grants))
	for _, g := range grants {
		formats = append(formats, g+" TO %s")
	}
	password := createUser(t, env.A, env.AAddr, user, formats...)
	return chtest.OpenAs(t, env.AAddr, user, password)
}

// TestMechanics_S3BackupWithAKeyThatCannotDelete (M13) measures what a
// backup key must be allowed to delete (plan S3-C1 clarification K24) and
// pins SI_BACKUP_GRANTS and SI_RESTORE_GRANTS: the backup job's
// objstore.Backuper, as si_backup holding exactly SI_BACKUP_GRANTS, backs
// up hg_safe with mech-nodelete and with mech-lockdelete, and the second
// must complete; the test logs exactly one line "M13 backup delete right:
// none" (the first completed too) or "M13 backup delete right: lock" (it
// failed removing its own .lock and nothing else). The index si_backup
// wrote lists the table and its parts. objstore.Drill and
// objstore.RestoreAndGate, as si_restore holding exactly SI_RESTORE_GRANTS,
// restore with mech-readonly and drop the part written after the manifest.
// Then every refusal of spec §8.2 (as amended, never ALL on hg_rstage): 497
// for si_backup's SELECT, INSERT and ALTER on hg_safe and CREATE TABLE in
// hg_rstage, and for si_restore's BACKUP, INSERT INTO FUNCTION s3(...),
// CREATE DICTIONARY, SYSTEM STOP MERGES and a view with another definer in
// hg_rstage.
func TestMechanics_S3BackupWithAKeyThatCannotDelete(t *testing.T) {
	env := requireMechanics(t)
	s3 := requireS3(t)
	ownProtocolDatabases(t, env)
	p := mechanicsPinned(t)
	p.SafeDB = objstore.SafeDatabase
	sch := mechanicsSchema(t)
	_, safe, _, err := ddl.Intents(p, sch)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, env.A, safe.SQL())
	mustInsert(t, env.A, safe.Database, safe.Table, "p0", 0, 1000, nil)
	mustInsert(t, env.A, safe.Database, safe.Table, "p0", 1000, 10, nil)
	mustInsert(t, env.A, safe.Database, safe.Table, "p1", 0, 5, nil)
	ownerNoDelete := uint64(time.Now().UnixNano())
	ownerLockDelete := ownerNoDelete + 1
	manifests := mechManifests{mechManifest(t, env.A, safe.Database, safe.Table, sch, ownerNoDelete)}
	lockManifests := mechManifests{mechManifest(t, env.A, safe.Database, safe.Table, sch, ownerLockDelete)}
	// A part written after the manifests: the backups are ahead of them.
	mustInsert(t, env.A, safe.Database, safe.Table, "p0", 1010, 3, nil)

	asBackup := grantUser(t, env, "si_backup", siBackupGrants)
	asRestore := grantUser(t, env, "si_restore", siRestoreGrants)
	if got, want := accessTypes(t, env.A, "si_backup"), []string{"BACKUP", "CREATE TEMPORARY TABLE", "READ", "SELECT", "SELECT", "SELECT", "WRITE"}; !slices.Equal(got, want) {
		t.Fatalf("system.grants of si_backup: %v, want %v", got, want)
	}
	if got, want := accessTypes(t, env.A, "si_restore"), []string{"ALTER", "CREATE DATABASE", "CREATE TABLE", "CREATE TEMPORARY TABLE", "DROP TABLE", "INSERT", "OPTIMIZE", "READ", "SELECT", "SELECT", "SELECT", "SELECT", "TRUNCATE"}; !slices.Equal(got, want) {
		t.Fatalf("system.grants of si_restore: %v, want %v", got, want)
	}

	ctx := context.Background()
	at := time.Now().UTC()
	backuper := func(owner uint64, m objstore.ManifestSource, key objstore.Credentials) *objstore.Backuper {
		return &objstore.Backuper{Conn: asBackup, Manifests: m, Bucket: s3.bucket, Owner: owner, NetworkID: "mechanics", Credentials: key, Now: func() time.Time { return at }}
	}
	_, errNoDelete := backuper(ownerNoDelete, manifests, s3.noDelete).Run(ctx)
	idx, errLockDelete := backuper(ownerLockDelete, lockManifests, s3.lockDelete).Run(ctx)
	if errLockDelete != nil {
		t.Fatalf("BACKUP as si_backup with mech-lockdelete, which may delete only its own .lock, failed: %v; K24: the plan stops until the user decides", errLockDelete)
	}
	right := "none"
	if errNoDelete != nil {
		msg := errNoDelete.Error()
		if !strings.Contains(msg, "/.lock") || !strings.Contains(strings.ToLower(msg), "access denied") {
			t.Fatalf("BACKUP with mech-nodelete failed for another reason than removing its .lock: %v", errNoDelete)
		}
		t.Logf("BACKUP with mech-nodelete: %v", errNoDelete)
		// The failed backup removed nothing: its .backup and .lock stay.
		dir := s3.bucket.BackupURL(ownerNoDelete, objstore.BackupName(at, objstore.BackupFull))
		for _, name := range []string{".backup", ".lock"} {
			if n := countObjects(t, env.A, dir+name, s3.readOnly); n != 1 {
				t.Fatalf("the failed mech-nodelete backup holds %d %s object(s), want 1: a key that cannot delete removed it", n, name)
			}
		}
		right = "lock"
	}
	t.Logf("M13 backup delete right: %s", right)
	lockLeft := countObjects(t, env.A, s3.bucket.BackupURL(ownerLockDelete, idx.Name)+".lock", s3.readOnly)
	if lockLeft != 0 {
		t.Fatalf("the mech-lockdelete backup %s left its .lock behind", idx.Name)
	}
	// SI_BACKUP_GRANTS list hg_safe's tables and parts: BACKUP ON hg_safe.*
	// shows the tables in system.tables and system.parts.
	if len(idx.Tables) != 1 || idx.Tables[0].PhysicalTable != safe.Table || idx.Tables[0].TableID != sch.TableID || len(idx.Tables[0].Parts) != 4 {
		t.Fatalf("the index si_backup wrote lists %+v; want %s (%s) with its 4 active parts", idx.Tables, safe.Table, sch.TableID)
	}

	// The drill as si_restore with mech-readonly: the index read, the
	// manifest the index names, a restore and gate of each partition and the
	// drop of each verified scratch (plan S3-C1 Task 25's restore-drill).
	drillNamer, err := objstore.NewScratchNamer("m13_drill_")
	if err != nil {
		t.Fatal(err)
	}
	drill := &objstore.Drill{
		Conn: asRestore, Manifests: lockManifests, Schemas: mechSchemas{sch.TableID: sch}, Bucket: s3.bucket,
		Owner: ownerLockDelete, NetworkID: "mechanics", Credentials: s3.readOnly, Scratch: drillNamer,
	}
	verdict, err := drill.Run(ctx)
	if err != nil || !verdict.Pass || verdict.Backup != idx.Name || len(verdict.Tables) != 1 || len(verdict.Tables[0].Partitions) != 2 {
		t.Fatalf("Drill as si_restore with mech-readonly: %+v, %v; want a pass over p_p0 and p_p1 of backup %s", verdict, err, idx.Name)
	}

	namer, err := objstore.NewScratchNamer("m13_")
	if err != nil {
		t.Fatal(err)
	}
	scratch := namer.Next()
	res, err := objstore.RestoreAndGate(ctx, asRestore, objstore.GateInput{
		Backup:   objstore.BackupRef{Bucket: s3.bucket, Owner: ownerLockDelete, Name: idx.Name, Credentials: s3.readOnly},
		Manifest: lockManifests.m, TableID: sch.TableID, PhysicalTable: safe.Table, Schema: sch, PartitionID: "p_p0", Scratch: scratch,
	})
	if err != nil || !res.Complete || len(res.Classification.Keep) != 2 || len(res.Classification.Drop) != 1 {
		t.Fatalf("RestoreAndGate as si_restore with mech-readonly: %+v, %v; want two parts kept and the later one dropped", res, err)
	}

	refused := func(who string, conn clickhouse.Conn, query string) {
		t.Helper()
		err := conn.Exec(ctx, query)
		if clickHouseCode(err) != codeAccessDenied {
			t.Errorf("%s: %s: err = %v, want ACCESS_DENIED (497)", who, query, err)
			return
		}
		t.Logf("%s refused: %v", who, err)
	}
	onSafe := qualified(safe.Database, safe.Table)
	refused("si_backup", asBackup, "SELECT count() FROM "+onSafe)
	refused("si_backup", asBackup, fmt.Sprintf("INSERT INTO %s (%s, `p`, `v`) VALUES (%s, 'p9', 1)", onSafe, ident(ddl.RowIDColumn), literal(strings.Repeat("0", 31)+"1")))
	refused("si_backup", asBackup, "ALTER TABLE "+onSafe+" DROP PARTITION 'p1'")
	refused("si_backup", asBackup, "CREATE TABLE hg_rstage.m13_refused (a UInt8) ENGINE = MergeTree ORDER BY tuple()")
	onScratch := scratch.Qualified()
	refused("si_restore", asRestore, "BACKUP TABLE "+onScratch+" TO "+s3Target(s3.prefix(t, "m13-refused"), s3.lockDelete))
	refused("si_restore", asRestore, "INSERT INTO FUNCTION "+s3Table(s3.prefix(t, "m13-refused")+"x.json", s3.noDelete, "RawBLOB")+" SELECT 'refused'")
	refused("si_restore", asRestore, "CREATE DICTIONARY hg_rstage.m13_dictionary (k UInt64, v String) PRIMARY KEY k SOURCE(CLICKHOUSE(TABLE "+literal(scratch.Table)+")) LAYOUT(FLAT()) LIFETIME(0)")
	refused("si_restore", asRestore, "SYSTEM STOP MERGES "+onScratch)
	refused("si_restore", asRestore, "CREATE VIEW hg_rstage.m13_definer_view DEFINER = default SQL SECURITY DEFINER AS SELECT * FROM "+onScratch)
}

// TestMechanics_S3SecretsAreMaskedInLogsAndSystemTables (M14) pins that a
// key travelling in a statement (K23) does not reach system.query_log,
// system.processes or system.backups: a BACKUP slowed by
// max_backup_bandwidth is sampled in system.processes and system.backups
// while it runs; then a RESTORE, an s3() read and an INSERT INTO FUNCTION
// s3(...); every row of the three tables on both servers is searched (in Go,
// never by a query that would itself carry a secret) for the three
// mechanics secrets, and each statement's own rows show the secret argument
// as [HIDDEN]. The script greps both servers' log files for the secrets.
func TestMechanics_S3SecretsAreMaskedInLogsAndSystemTables(t *testing.T) {
	env := requireMechanics(t)
	s3 := requireS3(t)
	p := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, _ := protocolTables(t, env.A, env.AAddr, p, sch)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 0, 200_000, nil)
	dir := s3.prefix(t, "m14")
	run := fmt.Sprintf("m14-%s-%d", suffix(t), time.Now().UnixNano())
	ids := map[string]string{"BACKUP": run + "-backup", "RESTORE": run + "-restore", "s3() read": run + "-read", "INSERT INTO FUNCTION s3": run + "-write"}
	statements := map[string]string{
		"BACKUP": fmt.Sprintf("BACKUP DATABASE %s TO %s", ident(p.SafeDB), s3Target(dir+"backup/", s3.lockDelete)),
	}
	stage := "hg_rstage_m" + suffix(t)
	dropDatabasesAt(t, env.AAddr, stage)
	mustExec(t, env.A, "CREATE DATABASE IF NOT EXISTS "+ident(stage))
	statements["RESTORE"] = fmt.Sprintf("RESTORE TABLE %s AS %s FROM %s", qualified(p.SafeDB, safe.Table), qualified(stage, safe.Table), s3Target(dir+"backup/", s3.readOnly))
	statements["s3() read"] = "SELECT count() FROM " + s3Table(dir+"backup/**", s3.readOnly, "One")
	statements["INSERT INTO FUNCTION s3"] = "INSERT INTO FUNCTION " + s3Table(dir+"probe.json", s3.noDelete, "RawBLOB") + " SELECT 'm14'"

	// The secret must not appear anywhere in text; the [HIDDEN] check is
	// on the statement's own rows.
	check := func(where, text string, wantHidden bool) {
		t.Helper()
		for i, secret := range s3.secrets() {
			if strings.Contains(text, secret) {
				t.Errorf("%s carries mechanics secret %d", where, i+1)
			}
		}
		if wantHidden && !strings.Contains(text, "[HIDDEN]") {
			t.Errorf("%s shows no [HIDDEN] secret argument: %s", where, text)
		}
	}
	rowsOf := func(conn clickhouse.Conn, query string, args ...any) [][]string {
		t.Helper()
		rows, err := conn.Query(context.Background(), query, args...)
		if err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		defer rows.Close()
		var out [][]string
		for rows.Next() {
			var a, b string
			if err := rows.Scan(&a, &b); err != nil {
				t.Fatalf("%s: %v", query, err)
			}
			out = append(out, []string{a, b})
		}
		if err := rows.Err(); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
		return out
	}

	// 1. The slowed BACKUP, sampled while it runs.
	done := make(chan error, 1)
	go func() {
		ctx := clickhouse.Context(context.Background(), clickhouse.WithQueryID(ids["BACKUP"]),
			clickhouse.WithSettings(clickhouse.Settings{"max_backup_bandwidth": 1_000_000}))
		var id, status string
		err := env.A.QueryRow(ctx, statements["BACKUP"]).Scan(&id, &status)
		if err == nil && status != "BACKUP_CREATED" {
			err = fmt.Errorf("status %s", status)
		}
		done <- err
	}()
	sampled := false
	eventually(t, settleTimeout, "the slowed BACKUP in system.processes", func() (bool, string) {
		for _, row := range rowsOf(env.A, "SELECT query_id, query FROM system.processes") {
			check("system.processes query "+row[0], row[1], row[0] == ids["BACKUP"])
			if row[0] == ids["BACKUP"] {
				sampled = true
			}
		}
		for _, row := range rowsOf(env.A, "SELECT toString(status), name FROM system.backups WHERE status = 'CREATING_BACKUP'") {
			check("system.backups (running) name", row[1], true)
		}
		return sampled, "no system.processes row for " + ids["BACKUP"] + " yet"
	})
	if err := <-done; err != nil {
		t.Fatalf("the slowed BACKUP: %v", err)
	}

	// 2. The RESTORE, the s3() read and the write through s3().
	for _, kind := range []string{"RESTORE", "s3() read", "INSERT INTO FUNCTION s3"} {
		ctx := clickhouse.Context(context.Background(), clickhouse.WithQueryID(ids[kind]))
		var err error
		if kind == "s3() read" {
			var n uint64
			err = env.A.QueryRow(ctx, statements[kind]).Scan(&n)
		} else {
			err = env.A.Exec(ctx, statements[kind])
		}
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
	}

	// 3. Every row of the three tables on both servers, and each
	// statement's own query_log rows.
	for _, server := range []struct {
		name string
		conn clickhouse.Conn
	}{{envAddr, env.A}, {envReplicaAddr, env.B}} {
		mustExec(t, server.conn, "SYSTEM FLUSH LOGS")
		for _, row := range rowsOf(server.conn, "SELECT query_id, concat(query, '\n', exception, '\n', stack_trace) FROM system.query_log") {
			check(server.name+" system.query_log row "+row[0], row[1], false)
		}
		for _, row := range rowsOf(server.conn, "SELECT query_id, query FROM system.processes") {
			check(server.name+" system.processes row "+row[0], row[1], false)
		}
		for _, row := range rowsOf(server.conn, "SELECT name, error FROM system.backups") {
			check(server.name+" system.backups row "+row[0], row[0]+"\n"+row[1], false)
		}
	}
	for kind, id := range ids {
		rows := rowsOf(env.A, "SELECT toString(type), query FROM system.query_log WHERE query_id = ?", id)
		if len(rows) == 0 {
			t.Errorf("%s: no system.query_log row for %s", kind, id)
		}
		for _, row := range rows {
			check(kind+" system.query_log "+row[0], row[1], true)
			if !strings.Contains(row[1], s3.readOnly.AccessKeyID) && !strings.Contains(row[1], s3.lockDelete.AccessKeyID) && !strings.Contains(row[1], s3.noDelete.AccessKeyID) {
				t.Errorf("%s: system.query_log shows no access key id: %s", kind, row[1])
			}
		}
	}
	for _, row := range rowsOf(env.A, "SELECT toString(status), name FROM system.backups WHERE position(name, ?) > 0", dir) {
		check("system.backups "+row[0], row[1], true)
	}
}
