package rebuild

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
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
	"github.com/sentioxyz/arbiter-core/wire"
)

// The rebuild's ClickHouse tests run on scripts/ci/clickhouse-mechanics.sh's
// pair: node A (CH_ADDR, the shared Keeper and the backups disk) is the
// rebuilt owner's server, node B (CH_REPLICA_ADDR) holds the pool verifier's
// hg_unsafe replica and, for the fence tests, the replicas of an old server.
// The lost owner's tables live on node A in databases of their own, so its
// Disk backup is readable where the rebuild restores.

const rebuildIndexer = 2

func requireRebuildCH(t *testing.T) (a, b clickhouse.Conn) {
	t.Helper()
	if os.Getenv("ARBITER_CH_REBUILD") != "1" {
		t.Skip("set ARBITER_CH_REBUILD=1 and run scripts/ci/clickhouse-mechanics.sh, which runs this package on its pair (README § ClickHouse mechanics)")
	}
	var missing []string
	if os.Getenv("ARBITER_CH_KEEPER") != "1" {
		missing = append(missing, "ARBITER_CH_KEEPER=1")
	}
	for _, name := range []string{"CH_ADDR", "CH_REPLICA_ADDR"} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("ARBITER_CH_REBUILD=1 requires %s", strings.Join(missing, ", "))
	}
	a, b = chtest.Open(t, os.Getenv("CH_ADDR")), chtest.Open(t, os.Getenv("CH_REPLICA_ADDR"))
	for name, conn := range map[string]clickhouse.Conn{"CH_ADDR": a, "CH_REPLICA_ADDR": b} {
		deadline := time.Now().Add(time.Minute)
		for {
			var n uint64
			err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n)
			if err == nil {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("the server at %s does not reach its Keeper: %v", name, err)
			}
			time.Sleep(500 * time.Millisecond)
		}
	}
	var disks uint64
	if err := a.QueryRow(context.Background(), "SELECT count() FROM system.disks WHERE name = 'backups'").Scan(&disks); err != nil || disks != 1 {
		t.Fatalf("ARBITER_CH_REBUILD=1 requires the backups disk on CH_ADDR (scripts/ci/clickhouse-mechanics-backups.xml): %d, %v", disks, err)
	}
	return a, b
}

func suffixOf(t *testing.T) (string, uint32) {
	sum := sha1.Sum([]byte(t.Name()))
	return hex.EncodeToString(sum[:])[:10], 3_000_000 + binary.BigEndian.Uint32(sum[10:14])%1_000_000
}

func mustExec(t *testing.T, conn clickhouse.Conn, query string) {
	t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

func dropDatabasesAtEnd(t *testing.T, conn clickhouse.Conn, databases ...string) {
	t.Helper()
	t.Cleanup(func() {
		for _, db := range databases {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+db+"` SYNC")
		}
	})
}

func rebuildSchema(s string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: "rb.t_" + s, PartitionBy: "p",
		Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}
}

// insertRows writes n rows of partition into db.table as one part, with
// _hg_row_id derived from the partition and v.
func insertRows(t *testing.T, conn clickhouse.Conn, db, table, partition string, from, n uint64) {
	t.Helper()
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"max_threads": 1, "max_insert_threads": 1}))
	q := fmt.Sprintf("INSERT INTO `%s`.`%s` (`%s`, `p`, `v`) SELECT SHA256(concat('rb:', '%s', ':', toString(number))), '%s', number FROM numbers(%d, %d)",
		db, table, ddl.RowIDColumn, partition, partition, from, n)
	if err := conn.Exec(ctx, q); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
}

// diskStore is an ObjectStore over a BACKUP ... TO Disk('backups', ...) of
// the lost owner's hg_safe table, as Plan S3-C1's index describes an S3
// backup.
type diskStore struct {
	backup, sourceDB, promoteDB string
	parts                       map[string][]BackupPart // logical partition -> parts
	restored                    []string
	mu                          sync.Mutex
}

func (s *diskStore) Parts(_ string, partitionID string) []BackupPart { return s.parts[partitionID] }

func (s *diskStore) RestorePartitions(ctx context.Context, conn clickhouse.Conn, tableID string, partitionIDs []string, scratch string) error {
	clause, err := partitionsClause(partitionIDs)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.restored = append(s.restored, partitionIDs...)
	s.mu.Unlock()
	return conn.Exec(ctx, fmt.Sprintf("RESTORE TABLE `%s`.`%s` AS `%s`.`%s` %s FROM Disk('backups', '%s')",
		s.sourceDB, ddl.CHTableName(tableID), s.promoteDB, scratch, clause, s.backup))
}

// rebuildFixture is one owner whose ClickHouse is lost and rebuilt on node A,
// with the pool verifier's replica on node B.
type rebuildFixture struct {
	t        *testing.T
	a, b     clickhouse.Conn
	schema   payloadexec.TableSchema
	old      ddl.Pinned // the lost ClickHouse's databases
	cfg      Config     // the rebuild of the new one
	verifier ddl.Pinned
	unsafe   map[string][]string // logical partition -> hg_unsafe part names, as inserted
	states   wire.PartitionStates
	store    *diskStore
}

// newRebuildFixture builds the owner before the loss: hg_unsafe with two
// parts in p_p0 and one in p_p1, replicated to the verifier; each partition
// promoted into hg_safe (ATTACH PARTITION FROM, as a promotion's REPLACE
// leaves it); the arbiter's SafeParts and base roots read from that hg_safe;
// a Disk backup of it; and the journal at the arbiter's PublishSeqs.
func newRebuildFixture(t *testing.T) *rebuildFixture {
	t.Helper()
	a, b := requireRebuildCH(t)
	s, shard := suffixOf(t)
	f := &rebuildFixture{t: t, a: a, b: b, schema: rebuildSchema(s), unsafe: map[string][]string{}}
	node := "snode-rb-" + s
	f.old = ddl.Pinned{UnsafeDB: "hg_unsafe_o" + s, SafeDB: "hg_safe_o" + s, PromoteDB: "hg_promote_o" + s, NodeID: node, KeeperShardID: shard}
	newDBs := ddl.Pinned{UnsafeDB: "hg_unsafe_n" + s, SafeDB: "hg_safe_n" + s, PromoteDB: "hg_promote_n" + s, NodeID: node, KeeperShardID: shard}
	f.verifier = ddl.Pinned{UnsafeDB: "hg_unsafe_v" + s, SafeDB: "hg_safe_v" + s, PromoteDB: "hg_promote_v" + s, NodeID: "verifier-" + s, KeeperShardID: shard}
	dropDatabasesAtEnd(t, b, f.verifier.UnsafeDB, f.verifier.SafeDB, f.verifier.PromoteDB)
	dropDatabasesAtEnd(t, a, f.old.UnsafeDB, f.old.SafeDB, f.old.PromoteDB, "hg_evil_"+s)
	// Cleanups run last-in first-out: the new databases go first.
	dropDatabasesAtEnd(t, a, newDBs.UnsafeDB, newDBs.SafeDB, newDBs.PromoteDB)
	ctx := context.Background()
	for conn, p := range map[clickhouse.Conn]ddl.Pinned{a: f.old, b: f.verifier} {
		if err := ddl.EnsureTable(ctx, conn, p, f.schema, 0, ddl.ModeCreateAndVerify); err != nil {
			t.Fatal(err)
		}
	}
	table := ddl.CHTableName(f.schema.TableID)
	for _, ins := range []struct {
		partition string
		from, n   uint64
	}{{"p0", 0, 100}, {"p0", 100, 100}, {"p1", 0, 10}} {
		before := f.unsafeParts(a, f.old.UnsafeDB)
		insertRows(t, a, f.old.UnsafeDB, table, ins.partition, ins.from, ins.n)
		for _, p := range f.unsafeParts(a, f.old.UnsafeDB) {
			if !slices.Contains(before, p) {
				f.unsafe["p_"+ins.partition] = append(f.unsafe["p_"+ins.partition], p)
			}
		}
	}
	mustExec(t, b, fmt.Sprintf("SYSTEM SYNC REPLICA `%s`.`%s`", f.verifier.UnsafeDB, table))
	for _, value := range []string{"p0", "p1"} {
		mustExec(t, a, fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION '%s' FROM `%s`.`%s`", f.old.SafeDB, table, value, f.old.UnsafeDB, table))
	}
	// A later promotion of p0 replaced the partition with itself through its
	// shadow, as REPLACE PARTITION does: the same parts under new names.
	mustExec(t, a, fmt.Sprintf("ALTER TABLE `%s`.`%s` ATTACH PARTITION 'p0' FROM `%s`.`%s`", f.old.PromoteDB, table, f.old.SafeDB, table))
	mustExec(t, a, fmt.Sprintf("ALTER TABLE `%s`.`%s` REPLACE PARTITION 'p0' FROM `%s`.`%s`", f.old.SafeDB, table, f.old.PromoteDB, table))
	mustExec(t, a, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PARTITION 'p0'", f.old.PromoteDB, table))
	publish := map[string]uint64{"p_p0": 3, "p_p1": 2}
	f.states = wire.PartitionStates{PromotionSeq: 3}
	f.store = &diskStore{backup: "rb_" + s + "_" + fmt.Sprint(time.Now().UnixNano()), sourceDB: f.old.SafeDB, promoteDB: newDBs.PromoteDB, parts: map[string][]BackupPart{}}
	for _, partitionID := range []string{"p_p0", "p_p1"} {
		parts, content := f.content(a, f.old.SafeDB, partitionID)
		state := wire.PartitionState{TableID: f.schema.TableID, PartitionID: partitionID, PublishSeq: publish[partitionID], BaseSafeSnapshotID: "0xsnap"}
		var rows []string
		for i, p := range parts {
			state.SafeParts = append(state.SafeParts, replay.PartManifestEntry{TableID: f.schema.TableID, PartitionID: partitionID,
				PartName: p.Name, PartPhysHash: p.PhysHash, PartRowLtHash: content[i].Row, RowCount: p.Rows})
			rows = append(rows, content[i].Row)
			f.store.parts[partitionID] = append(f.store.parts[partitionID], BackupPart{Name: p.Name, HashOfAllFiles: p.PhysHash, Rows: p.Rows})
		}
		root, err := payloadexec.SumPartRowLtHashes(rows)
		if err != nil {
			t.Fatal(err)
		}
		state.BasePartitionRoot = root
		f.states.Partitions = append(f.states.Partitions, state)
	}
	mustExec(t, a, fmt.Sprintf("BACKUP TABLE `%s`.`%s` TO Disk('backups', '%s')", f.old.SafeDB, table, f.store.backup))
	dir := t.TempDir()
	journal := map[string]any{"watermarks": map[string]uint64{f.schema.TableID + "\x00p_p0": 3, f.schema.TableID + "\x00p_p1": 2},
		"last_acks": map[string]any{}, "base_roots": map[string]string{}, "base_snapshot_ids": map[string]string{}, "unpromoted_sums": map[string]string{}}
	b2, err := json.Marshal(journal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "state.json"), b2, 0o600); err != nil {
		t.Fatal(err)
	}
	f.cfg = Config{NetworkID: "devnet2", IndexerID: rebuildIndexer, Pinned: newDBs, StateDir: dir,
		GenesisTables: []payloadexec.TableSchema{f.schema}, UnsafeSyncTimeout: 2 * time.Minute}
	return f
}

func (f *rebuildFixture) unsafeParts(conn clickhouse.Conn, db string) []string {
	parts, err := chparts.ActiveParts(context.Background(), conn, db, ddl.CHTableName(f.schema.TableID))
	if err != nil {
		f.t.Fatal(err)
	}
	var out []string
	for _, p := range parts {
		out = append(out, p.Name)
	}
	return out
}

func (f *rebuildFixture) content(conn clickhouse.Conn, db, partitionID string) ([]chparts.Part, []partContent) {
	parts, content, err := localContent(context.Background(), conn, db, ddl.CHTableName(f.schema.TableID), f.schema, partitionID)
	if err != nil {
		f.t.Fatal(err)
	}
	return parts, content
}

// cleanUnsafePart drops one promoted part from hg_unsafe on the live owner,
// as its cleanup does (the drop replicates to the verifier), so only the
// backup still holds it.
func (f *rebuildFixture) cleanUnsafePart(partitionID string, i int) {
	f.t.Helper()
	table := ddl.CHTableName(f.schema.TableID)
	mustExec(f.t, f.a, fmt.Sprintf("ALTER TABLE `%s`.`%s` DROP PART '%s'", f.old.UnsafeDB, table, f.unsafe[partitionID][i]))
	mustExec(f.t, f.b, fmt.Sprintf("SYSTEM SYNC REPLICA `%s`.`%s`", f.verifier.UnsafeDB, table))
}

// lose loses the owner's ClickHouse: hg_safe and hg_promote are gone and its
// hg_unsafe replica stays registered in Keeper, inactive, as a dead server's
// does.
func (f *rebuildFixture) lose() {
	f.t.Helper()
	mustExec(f.t, f.a, fmt.Sprintf("DETACH TABLE `%s`.`%s`", f.old.UnsafeDB, ddl.CHTableName(f.schema.TableID)))
	mustExec(f.t, f.a, fmt.Sprintf("DROP DATABASE `%s` SYNC", f.old.SafeDB))
	mustExec(f.t, f.a, fmt.Sprintf("DROP DATABASE `%s` SYNC", f.old.PromoteDB))
}

func (f *rebuildFixture) inputs() Inputs {
	owner := uint64(rebuildIndexer)
	db, table, _ := strings.Cut(f.schema.TableID, ".")
	return Inputs{
		Registry: wire.TableRegistrySnapshot{QuarantinedIndexers: []uint64{rebuildIndexer}, Incarnations: []wire.TableIncarnation{{
			Seq: 1, DatabaseID: db, TableID: table, Origin: wire.TableOriginGenesis, Status: wire.TableStatusActive, OwnerIndexerID: &owner}}},
		States: []wire.PartitionStates{f.states},
	}
}

// wantSafeParts requires the new hg_safe to equal the arbiter's SafeParts by
// (row LtHash, hash_of_all_files) in every partition and returns the names
// it holds them under.
func (f *rebuildFixture) wantSafeParts() map[string][]string {
	f.t.Helper()
	names := map[string][]string{}
	for _, state := range f.states.Partitions {
		parts, content := f.content(f.a, f.cfg.Pinned.SafeDB, state.PartitionID)
		left, ok := subtract(manifest(ownedPartition{state: state}), content)
		if !ok || len(left) > 0 {
			f.t.Fatalf("hg_safe partition %s is not the arbiter's SafeParts by content: %+v", state.PartitionID, content)
		}
		for _, p := range parts {
			names[state.PartitionID] = append(names[state.PartitionID], p.Name)
		}
	}
	return names
}

func (f *rebuildFixture) journal() string {
	f.t.Helper()
	b, err := os.ReadFile(filepath.Join(f.cfg.StateDir, "state.json"))
	if err != nil {
		f.t.Fatal(err)
	}
	return string(b)
}

// writeConn records every statement that is not a read.
type writeConn struct {
	clickhouse.Conn
	mu     sync.Mutex
	writes []string
}

func (c *writeConn) Exec(ctx context.Context, query string, args ...any) error {
	c.mu.Lock()
	c.writes = append(c.writes, query)
	c.mu.Unlock()
	return c.Conn.Exec(ctx, query, args...)
}

func TestFenceRefusesAnActiveReplica(t *testing.T) {
	a, b := requireRebuildCH(t)
	s, shard := suffixOf(t)
	p := ddl.Pinned{UnsafeDB: "hg_unsafe_f" + s, SafeDB: "hg_safe_f" + s, PromoteDB: "hg_promote_f" + s, NodeID: "snode-f-" + s, KeeperShardID: shard}
	dropDatabasesAtEnd(t, b, p.UnsafeDB, p.SafeDB, p.PromoteDB)
	sch := rebuildSchema(s)
	if err := ddl.EnsureTable(context.Background(), b, p, sch, 0, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}

	res, err := Fence(context.Background(), a, p, []string{sch.TableID})

	if !errors.Is(err, ErrFenceReplicaActive) || !strings.HasPrefix(err.Error(), "fence: replica "+p.NodeID+" of "+sch.TableID+" is active at ") ||
		!strings.HasSuffix(err.Error(), "; the old ClickHouse is not fenced") || strings.Contains(err.Error(), "an unknown host") {
		t.Fatalf("err = %v", err)
	}
	if len(res) != 0 {
		t.Fatalf("results = %+v", res)
	}
	replicas, err := ddl.KeeperReplicas(context.Background(), a, p, sch.TableID)
	if err != nil || !slices.Equal(replicas, []string{p.NodeID}) {
		t.Fatalf("the active replica must stay: %v, %v", replicas, err)
	}
}

func TestFenceDropsTheStaleReplicaOfEveryOwnedTable(t *testing.T) {
	a, b := requireRebuildCH(t)
	ctx := context.Background()
	s, shard := suffixOf(t)
	old := ddl.Pinned{UnsafeDB: "hg_unsafe_f" + s, SafeDB: "hg_safe_f" + s, PromoteDB: "hg_promote_f" + s, NodeID: "snode-f-" + s, KeeperShardID: shard}
	pool := old
	pool.UnsafeDB, pool.SafeDB, pool.PromoteDB, pool.NodeID = "hg_unsafe_g"+s, "hg_safe_g"+s, "hg_promote_g"+s, "verifier-f-"+s
	dropDatabasesAtEnd(t, b, old.UnsafeDB, old.SafeDB, old.PromoteDB, pool.UnsafeDB, pool.SafeDB, pool.PromoteDB)
	var ids []string
	for i, owned := range []bool{true, true, false} {
		sch := rebuildSchema(fmt.Sprintf("%s_%d", s, i))
		ids = append(ids, sch.TableID)
		if err := ddl.EnsureTable(ctx, b, pool, sch, 0, ddl.ModeCreateAndVerify); err != nil {
			t.Fatal(err)
		}
		if owned {
			if err := ddl.EnsureTable(ctx, b, old, sch, 0, ddl.ModeCreateAndVerify); err != nil {
				t.Fatal(err)
			}
			mustExec(t, b, fmt.Sprintf("DETACH TABLE `%s`.`%s`", old.UnsafeDB, ddl.CHTableName(sch.TableID)))
		}
	}

	res, err := Fence(ctx, a, old, ids)

	if err != nil {
		t.Fatal(err)
	}
	want := []FenceResult{
		{TableID: ids[0], Path: ddl.ZooKeeperPath(old, ids[0]), Present: true, Dropped: true},
		{TableID: ids[1], Path: ddl.ZooKeeperPath(old, ids[1]), Present: true, Dropped: true},
		{TableID: ids[2], Path: ddl.ZooKeeperPath(old, ids[2])},
	}
	if !slices.Equal(res, want) {
		t.Fatalf("results = %+v, want %+v", res, want)
	}
	for _, id := range ids {
		replicas, err := ddl.KeeperReplicas(ctx, a, old, id)
		if err != nil || !slices.Equal(replicas, []string{pool.NodeID}) {
			t.Fatalf("%s replicas = %v, %v; want only the pool's", id, replicas, err)
		}
	}
}

func TestFenceRefusesAScopedReplicaOfThisNode(t *testing.T) {
	a, b := requireRebuildCH(t)
	ctx := context.Background()
	s, shard := suffixOf(t)
	p := ddl.Pinned{UnsafeDB: "hg_unsafe_f" + s, SafeDB: "hg_safe_f" + s, PromoteDB: "hg_promote_f" + s, NodeID: "snode-f-" + s, KeeperShardID: shard}
	scoped := "hg_scoped_f" + s
	dropDatabasesAtEnd(t, b, p.UnsafeDB, p.SafeDB, p.PromoteDB, scoped)
	sch := rebuildSchema(s)
	if err := ddl.EnsureTable(ctx, b, p, sch, 0, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	mustExec(t, b, fmt.Sprintf("DETACH TABLE `%s`.`%s`", p.UnsafeDB, ddl.CHTableName(sch.TableID)))
	mustExec(t, b, "CREATE DATABASE `"+scoped+"`")
	mustExec(t, b, fmt.Sprintf("CREATE TABLE `%s`.t (v UInt64) ENGINE = ReplicatedMergeTree('/sentio/%d/unsafe_inc/rb__t__i3', '%s') ORDER BY v", scoped, shard, p.NodeID))

	res, err := Fence(ctx, a, p, []string{sch.TableID})

	want := fmt.Sprintf("fence: /sentio/%d/unsafe_inc has children [rb__t__i3]; the scoped root must be empty before the purge switch", shard)
	if !errors.Is(err, ErrFenceScopedReplica) || err.Error() != want || res != nil {
		t.Fatalf("Fence = %+v, %v; want %q", res, err, want)
	}
	if replicas, err := ddl.KeeperReplicas(ctx, a, p, sch.TableID); err != nil || !slices.Equal(replicas, []string{p.NodeID}) {
		t.Fatalf("nothing may be dropped: %v, %v", replicas, err)
	}
}

func TestRebuildRestoresHgSafeToTheArbitersSafeParts(t *testing.T) {
	f := newRebuildFixture(t)
	f.cleanUnsafePart("p_p1", 0)
	f.lose()

	report, err := Rebuild(context.Background(), f.cfg, f.a, f.inputs(), f.store)

	if err != nil {
		t.Fatal(err)
	}
	names := f.wantSafeParts()
	var acknowledged []string
	for _, e := range f.states.Partitions[0].SafeParts {
		acknowledged = append(acknowledged, e.PartName)
	}
	t.Logf("p_p0: rebuilt under %v, acknowledged under %v", names["p_p0"], acknowledged)
	if slices.Equal(slices.Sorted(slices.Values(acknowledged)), slices.Sorted(slices.Values(names["p_p0"]))) {
		t.Fatal("the fixture must give the rebuilt p_p0 other names than the acknowledged ones")
	}
	wantFence := []FenceResult{{TableID: f.schema.TableID, Path: ddl.ZooKeeperPath(f.cfg.Pinned, f.schema.TableID), Present: true, Dropped: true}}
	if !slices.Equal(report.Fence, wantFence) || report.AlreadyRebuilt || report.DryRun || len(report.Partitions) != 2 {
		t.Fatalf("report = %+v", report)
	}
	for _, p := range report.Partitions {
		if p.Parts != len(f.store.parts[p.PartitionID]) || p.FromOwnUnsafe+p.FromObjectStore != p.Parts || len(p.Missing) != 0 {
			t.Fatalf("partition report %+v", p)
		}
	}
	journal := f.journal()
	for partitionID, partNames := range names {
		for _, name := range partNames {
			if !strings.Contains(journal, `"safe_part_name":"`+name+`"`) {
				t.Fatalf("the journal does not record %s of %s: %s", name, partitionID, journal)
			}
		}
	}
	if strings.Count(journal, `"rebuilt":true`) != 2 {
		t.Fatalf("the journal must mark both partitions rebuilt: %s", journal)
	}
	if active, err := ddl.ReplicaIsActive(context.Background(), f.a, f.cfg.Pinned, f.schema.TableID, f.cfg.Pinned.NodeID); err != nil || !active {
		t.Fatalf("the new hg_unsafe replica is not active: %v, %v", active, err)
	}
	if left, err := scratchTables(f.a, f.cfg.Pinned.PromoteDB); err != nil || len(left) != 0 {
		t.Fatalf("scratch tables left: %v, %v", left, err)
	}
}

func scratchTables(conn clickhouse.Conn, db string) ([]string, error) {
	rows, err := conn.Query(context.Background(), "SELECT name FROM system.tables WHERE database = ? AND position(name, '__rebuild_') > 0", db)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

func TestRebuildPrefersTheOwnUnsafeReplicaThenTheObjectStore(t *testing.T) {
	f := newRebuildFixture(t)
	f.cleanUnsafePart("p_p0", 1)
	f.lose()

	report, err := Rebuild(context.Background(), f.cfg, f.a, f.inputs(), f.store)

	if err != nil {
		t.Fatal(err)
	}
	f.wantSafeParts()
	got := map[string][2]int{}
	for _, p := range report.Partitions {
		got[p.PartitionID] = [2]int{p.FromOwnUnsafe, p.FromObjectStore}
	}
	if want := map[string][2]int{"p_p0": {1, 1}, "p_p1": {1, 0}}; fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("sources (own_unsafe, object_store) = %v, want %v", got, want)
	}
	if !slices.Equal(f.store.restored, []string{"p_p0"}) {
		t.Fatalf("the object store restored %v, want only p_p0", f.store.restored)
	}
}

func TestRebuildDryRunWritesNothingAndReportsCoverage(t *testing.T) {
	f := newRebuildFixture(t)
	f.cleanUnsafePart("p_p0", 1)
	live := f.cfg
	live.Pinned = f.old
	live.DryRun = true
	before := f.journal()
	w := &writeConn{Conn: f.a}

	report, err := Rebuild(context.Background(), live, w, f.inputs(), f.store)

	if err != nil {
		t.Fatal(err)
	}
	if len(w.writes) != 0 {
		t.Fatalf("the dry run ran %v", w.writes)
	}
	if f.journal() != before {
		t.Fatal("the dry run changed the journal")
	}
	if !report.DryRun || report.Fence != nil || len(report.Partitions) != 2 ||
		report.Partitions[0].FromOwnUnsafe != 1 || report.Partitions[0].FromObjectStore != 1 || report.Partitions[1].FromOwnUnsafe != 1 {
		t.Fatalf("report = %+v", report)
	}
	for _, line := range report.JSONLines() {
		if !strings.HasPrefix(line, `{"phase":"plan"`) && !strings.HasPrefix(line, `{"phase":"done"`) {
			t.Fatalf("dry-run line %s", line)
		}
	}
	empty := &diskStore{parts: map[string][]BackupPart{}}
	report, err = Rebuild(context.Background(), live, w, f.inputs(), empty)
	missing := f.states.Partitions[0].SafeParts[1]
	if !errors.Is(err, ErrRebuildSourceMissing) || !strings.HasPrefix(err.Error(), "rebuild: no source holds part "+missing.PartName+" (row hash ") ||
		!strings.HasSuffix(err.Error(), ", hash_of_all_files "+missing.PartPhysHash+") of "+f.schema.TableID+" partition p_p0") {
		t.Fatalf("err = %v", err)
	}
	if len(report.Partitions) != 2 || !slices.Equal(report.Partitions[0].Missing, []string{missing.PartName}) || len(w.writes) != 0 {
		t.Fatalf("report = %+v, writes %v", report, w.writes)
	}
}

func TestRebuildRefusesAPhysHashMismatch(t *testing.T) {
	f := newRebuildFixture(t)
	s, _ := suffixOf(t)
	table := ddl.CHTableName(f.schema.TableID)
	// The same rows of p1 in wide parts: equal row LtHash, other files.
	evil := "hg_evil_" + s
	mustExec(t, f.a, "CREATE DATABASE `"+evil+"`")
	mustExec(t, f.a, fmt.Sprintf("CREATE TABLE `%s`.`%s` AS `%s`.`%s`", evil, table, f.old.SafeDB, table))
	mustExec(t, f.a, fmt.Sprintf("ALTER TABLE `%s`.`%s` MODIFY SETTING min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0", evil, table))
	mustExec(t, f.a, fmt.Sprintf("INSERT INTO `%s`.`%s` SELECT * FROM `%s`.`%s` WHERE p = 'p1' SETTINGS max_insert_threads = 1", evil, table, f.old.SafeDB, table))
	evilBackup := f.store.backup + "_evil"
	mustExec(t, f.a, fmt.Sprintf("BACKUP TABLE `%s`.`%s` TO Disk('backups', '%s')", evil, table, evilBackup))
	f.store.backup, f.store.sourceDB = evilBackup, evil
	f.cleanUnsafePart("p_p1", 0)
	f.lose()

	_, err := Rebuild(context.Background(), f.cfg, f.a, f.inputs(), f.store)

	recorded := f.states.Partitions[1].SafeParts[0].PartPhysHash
	if !errors.Is(err, ErrRebuildPhysHashMismatch) || !strings.Contains(err.Error(), " of "+f.schema.TableID+" partition p_p1 came back with hash_of_all_files ") ||
		!strings.HasSuffix(err.Error(), "; the arbiter records "+recorded) {
		t.Fatalf("err = %v", err)
	}
	if parts, _ := f.content(f.a, f.cfg.Pinned.SafeDB, "p_p1"); len(parts) != 0 {
		t.Fatalf("a mismatching part reached hg_safe: %+v", parts)
	}
	if strings.Contains(f.journal(), `"rebuilt":true`) {
		t.Fatal("a refused rebuild must not write the journal")
	}
}

func TestRebuildIsIdempotent(t *testing.T) {
	f := newRebuildFixture(t)
	f.cleanUnsafePart("p_p0", 0)
	f.lose()
	if _, err := Rebuild(context.Background(), f.cfg, f.a, f.inputs(), f.store); err != nil {
		t.Fatal(err)
	}
	names := f.wantSafeParts()
	journal := f.journal()
	w := &writeConn{Conn: f.a}

	report, err := Rebuild(context.Background(), f.cfg, w, f.inputs(), f.store)

	if err != nil {
		t.Fatal(err)
	}
	if !report.AlreadyRebuilt || report.Fence != nil || len(report.Partitions) != 2 || len(w.writes) != 0 {
		t.Fatalf("second run: report %+v, writes %v", report, w.writes)
	}
	if fmt.Sprint(f.wantSafeParts()) != fmt.Sprint(names) || f.journal() != journal {
		t.Fatal("the second run changed hg_safe or the journal")
	}
	lines := report.JSONLines()
	if !strings.Contains(lines[len(lines)-1], `"already_rebuilt":true`) {
		t.Fatalf("done line %s", lines[len(lines)-1])
	}

	// A journal that holds the rebuilt inventories at the right seqs but
	// under other names (a later rebuild restored hg_safe again and stopped
	// before its journal write) is not done: the run refreshes the names.
	stateFile := filepath.Join(f.cfg.StateDir, "state.json")
	stale := strings.ReplaceAll(journal, `"safe_part_name":"`, `"safe_part_name":"stale_`)
	if stale == journal {
		t.Fatal("the journal holds no safe_part_name to make stale")
	}
	if err := os.WriteFile(stateFile, []byte(stale), 0o600); err != nil {
		t.Fatal(err)
	}
	report, err = Rebuild(context.Background(), f.cfg, f.a, f.inputs(), f.store)
	if err != nil {
		t.Fatal(err)
	}
	if report.AlreadyRebuilt {
		t.Fatal("a journal with another run's part names was taken as already rebuilt")
	}
	if strings.Contains(f.journal(), "stale_") || fmt.Sprint(f.wantSafeParts()) != fmt.Sprint(names) {
		t.Fatalf("the run did not refresh the journaled names, or changed hg_safe: %s", f.journal())
	}
	if report, err = Rebuild(context.Background(), f.cfg, f.a, f.inputs(), f.store); err != nil || !report.AlreadyRebuilt {
		t.Fatalf("the run after the refresh: %+v, %v; want already rebuilt", report, err)
	}
}

func TestRebuildRefusesForeignSafeParts(t *testing.T) {
	f := newRebuildFixture(t)
	f.lose()
	if _, err := Rebuild(context.Background(), f.cfg, f.a, f.inputs(), f.store); err != nil {
		t.Fatal(err)
	}
	table := ddl.CHTableName(f.schema.TableID)
	insertRows(t, f.a, f.cfg.Pinned.SafeDB, table, "p0", 5000, 1)
	before, _ := f.content(f.a, f.cfg.Pinned.SafeDB, "p_p0")
	journal := f.journal()

	_, err := Rebuild(context.Background(), f.cfg, f.a, f.inputs(), f.store)

	want := "rebuild: hg_safe." + table + " partition p_p0 holds parts outside the arbiter's SafeParts; only an empty or already rebuilt table is rebuilt"
	if !errors.Is(err, ErrRebuildForeignSafeParts) || err.Error() != want {
		t.Fatalf("err = %v, want %q", err, want)
	}
	if after, _ := f.content(f.a, f.cfg.Pinned.SafeDB, "p_p0"); !slices.Equal(after, before) || f.journal() != journal {
		t.Fatal("the refused rebuild changed hg_safe or the journal")
	}
	scans, err := chexec.ScanAllParts(context.Background(), f.a, f.cfg.Pinned.SafeDB+"."+table, f.schema)
	if err != nil || len(scans) != 4 {
		t.Fatalf("hg_safe holds %d parts (%v), want the three rebuilt and the foreign one", len(scans), err)
	}
}
