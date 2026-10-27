package ddl

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// testMemberPinned gives each test its own member databases and node id, on
// the server's "default" storage policy named explicitly, so the policy
// setting is rendered, created and verified on any ClickHouse.
func testMemberPinned(t *testing.T) MemberPinned {
	t.Helper()
	s := uniqueSuffix(t)
	return MemberPinned{
		MemberUnsafeDB: "hg_member_unsafe_" + s, ReplicaDB: "hg_replica_" + s, StageDB: "hg_rstage_" + s,
		NodeID: "member-" + s, StoragePolicy: "default",
	}
}

func dropMemberDatabasesSync(t *testing.T, conn clickhouse.Conn, m MemberPinned) {
	t.Helper()
	t.Cleanup(func() {
		for _, db := range []string{m.MemberUnsafeDB, m.ReplicaDB, m.StageDB} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+quoteIdent(db)+" SYNC")
		}
	})
}

// execRecorder passes every call through and records each Exec statement.
type execRecorder struct {
	clickhouse.Conn
	mu    sync.Mutex
	execs []string
}

func (c *execRecorder) Exec(ctx context.Context, query string, args ...any) error {
	c.mu.Lock()
	c.execs = append(c.execs, query)
	c.mu.Unlock()
	return c.Conn.Exec(ctx, query, args...)
}

func (c *execRecorder) statements() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.execs)
}

func ensureMemberDatabases(t *testing.T, conn clickhouse.Conn, m MemberPinned) {
	t.Helper()
	dropMemberDatabasesSync(t, conn, m)
	if err := EnsureMemberDatabases(context.Background(), conn, m); err != nil {
		t.Fatalf("EnsureMemberDatabases: %v", err)
	}
}

// TestEnsureMemberDatabasesCreatesNoEngineClause: the member creates only
// hg_replica and hg_rstage, without ENGINE (26.3 checks a database ENGINE as
// TABLE ENGINE ON <engine>, which si_member is never granted; housegate spec
// 2026-10-14 §8.2), idempotently; hg_member_unsafe is not its to create.
func TestEnsureMemberDatabasesCreatesNoEngineClause(t *testing.T) {
	conn := requireCH(t)
	m := testMemberPinned(t)
	dropMemberDatabasesSync(t, conn, m)
	rec := &execRecorder{Conn: conn}
	for range 2 {
		if err := EnsureMemberDatabases(context.Background(), rec, m); err != nil {
			t.Fatalf("EnsureMemberDatabases: %v", err)
		}
	}
	one := []string{"CREATE DATABASE IF NOT EXISTS `" + m.ReplicaDB + "`", "CREATE DATABASE IF NOT EXISTS `" + m.StageDB + "`"}
	if got, want := rec.statements(), append(slices.Clone(one), one...); !slices.Equal(got, want) {
		t.Fatalf("statements = %q, want %q", got, want)
	}
	rows, err := conn.Query(context.Background(), `SELECT name FROM system.databases WHERE name IN (?, ?, ?) ORDER BY name`, m.MemberUnsafeDB, m.ReplicaDB, m.StageDB)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		got = append(got, name)
	}
	if want := []string{m.ReplicaDB, m.StageDB}; !slices.Equal(got, want) {
		t.Fatalf("databases = %v, want %v (and no %s)", got, want, m.MemberUnsafeDB)
	}
}

// TestEnsureCopyCreatesAndVerifiesTheCopy: EnsureCopy creates the copy once,
// verifies it on every call, and reports a copy whose live shape drifted
// (here its incarnation comment) instead of adopting it.
func TestEnsureCopyCreatesAndVerifiesTheCopy(t *testing.T) {
	conn := requireCH(t)
	m := testMemberPinned(t)
	ensureMemberDatabases(t, conn, m)
	ctx := context.Background()
	for range 2 {
		if err := EnsureCopy(ctx, conn, m, goldenSchema(), 7); err != nil {
			t.Fatalf("EnsureCopy: %v", err)
		}
	}
	var engine, engineFull, comment string
	if err := conn.QueryRow(ctx, `SELECT engine, engine_full, comment FROM system.tables WHERE database = ? AND name = ?`, m.ReplicaDB, "db__t__i7").
		Scan(&engine, &engineFull, &comment); err != nil {
		t.Fatalf("read the copy: %v", err)
	}
	if engine != EngineMergeTree || comment != "hg_incarnation=7" ||
		!strings.Contains(engineFull, "SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, storage_policy = 'default'") {
		t.Fatalf("copy engine %q comment %q engine_full %q", engine, comment, engineFull)
	}
	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.`db__t__i7` MODIFY COMMENT 'hg_incarnation=8'", quoteIdent(m.ReplicaDB))); err != nil {
		t.Fatal(err)
	}
	if err := EnsureCopy(ctx, conn, m, goldenSchema(), 7); !errors.Is(err, ErrProtocolTableDrift) || !strings.Contains(err.Error(), "comment") {
		t.Fatalf("EnsureCopy of a copy whose comment drifted = %v, want comment drift", err)
	}
}

// TestDropCopyIsIdempotent: dropping a copy, again, or one never created
// succeeds; only hg_replica is ever named.
func TestDropCopyIsIdempotent(t *testing.T) {
	conn := requireCH(t)
	m := testMemberPinned(t)
	ensureMemberDatabases(t, conn, m)
	ctx := context.Background()
	if err := EnsureCopy(ctx, conn, m, goldenSchema(), 7); err != nil {
		t.Fatal(err)
	}
	rec := &execRecorder{Conn: conn}
	for range 2 {
		if err := DropCopy(ctx, rec, m, "db.t", 7); err != nil {
			t.Fatalf("DropCopy: %v", err)
		}
	}
	if err := DropCopy(ctx, rec, m, "db.never", 3); err != nil {
		t.Fatalf("DropCopy of a copy never created: %v", err)
	}
	want := []string{
		"DROP TABLE IF EXISTS `" + m.ReplicaDB + "`.`db__t__i7` SYNC",
		"DROP TABLE IF EXISTS `" + m.ReplicaDB + "`.`db__t__i7` SYNC",
		"DROP TABLE IF EXISTS `" + m.ReplicaDB + "`.`db__never__i3` SYNC",
	}
	if got := rec.statements(); !slices.Equal(got, want) {
		t.Fatalf("statements = %q, want %q", got, want)
	}
	tables, err := ListMemberTables(ctx, conn, m)
	if err != nil {
		t.Fatal(err)
	}
	if len(tables) != 0 {
		t.Fatalf("member tables after DropCopy = %+v, want none", tables)
	}
	if err := DropCopyTable(ctx, conn, m, ""); err == nil || err.Error() != "ddl: DropCopyTable needs a table name" {
		t.Fatalf("DropCopyTable without a name = %v", err)
	}
}

// TestListMemberTablesNeverListsTheStageDatabase: the member listing holds
// hg_member_unsafe and hg_replica only, sorted by table then database; the
// stage listing holds only the given node prefix's scratch.
func TestListMemberTablesNeverListsTheStageDatabase(t *testing.T) {
	conn := requireCH(t)
	m := testMemberPinned(t)
	ensureMemberDatabases(t, conn, m)
	ctx := context.Background()
	if err := EnsureCopy(ctx, conn, m, goldenSchema(), 7); err != nil {
		t.Fatal(err)
	}
	// The admin-made stand-in for Plan S3-E1's replica (plain MergeTree: no
	// Keeper needed), and scratch tables of this node and of another one.
	for _, sql := range []string{
		"CREATE DATABASE IF NOT EXISTS " + quoteIdent(m.MemberUnsafeDB),
		fmt.Sprintf("CREATE TABLE %s.`db__t` (x UInt8) ENGINE = MergeTree ORDER BY x", quoteIdent(m.MemberUnsafeDB)),
	} {
		if err := conn.Exec(ctx, sql); err != nil {
			t.Fatal(err)
		}
	}
	for _, name := range []string{m.NodeID + "_0a1b2c3d_2", m.NodeID + "_0a1b2c3d_1", m.NodeID + "_0a1b2c3d_r", "other-node_0a1b2c3d_1"} {
		scratch, err := ScratchIntent(m, goldenSchema(), name)
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, scratch.SQL()); err != nil {
			t.Fatalf("create scratch %s: %v", name, err)
		}
	}
	tables, err := ListMemberTables(ctx, conn, m)
	if err != nil {
		t.Fatalf("ListMemberTables: %v", err)
	}
	want := []LocalTable{
		{Database: m.MemberUnsafeDB, Table: "db__t"},
		{Database: m.ReplicaDB, Table: "db__t__i7", Comment: "hg_incarnation=7"},
	}
	if !reflect.DeepEqual(tables, want) {
		t.Fatalf("ListMemberTables = %+v, want %+v", tables, want)
	}
	stage, err := ListStageTables(ctx, conn, m, m.NodeID+"_")
	if err != nil {
		t.Fatalf("ListStageTables: %v", err)
	}
	if want := []string{m.NodeID + "_0a1b2c3d_1", m.NodeID + "_0a1b2c3d_2", m.NodeID + "_0a1b2c3d_r"}; !slices.Equal(stage, want) {
		t.Fatalf("ListStageTables = %v, want %v", stage, want)
	}
	if _, err := ListStageTables(ctx, conn, m, ""); err == nil || err.Error() != "ddl: ListStageTables needs a prefix" {
		t.Fatalf("ListStageTables without a prefix = %v", err)
	}
}

// TestAttachPartitionFromAScratchIntoACopySucceeds: a scratch made with
// ScratchIntent is accepted by ATTACH PARTITION ... FROM into a copy made with
// EnsureCopy; each attach appends the scratch's part, retires nothing already
// in the copy and keeps the part's hash_of_all_files (housegate spec
// 2026-10-14 §8.5 step 5).
func TestAttachPartitionFromAScratchIntoACopySucceeds(t *testing.T) {
	conn := requireCH(t)
	m := testMemberPinned(t)
	ensureMemberDatabases(t, conn, m)
	ctx := context.Background()
	if err := EnsureCopy(ctx, conn, m, goldenSchema(), 7); err != nil {
		t.Fatal(err)
	}
	copyTable := quoteIdent(m.ReplicaDB) + ".`db__t__i7`"
	var want []string
	for n, rows := range []string{
		"('" + strings.Repeat("a", 32) + "', 'a', 1), ('" + strings.Repeat("b", 32) + "', 'a', 2)",
		"('" + strings.Repeat("c", 32) + "', 'a', 3)",
	} {
		scratch, err := ScratchIntent(m, goldenSchema(), fmt.Sprintf("%s_0a1b2c3d_%d", m.NodeID, n+1))
		if err != nil {
			t.Fatal(err)
		}
		if err := conn.Exec(ctx, scratch.SQL()); err != nil {
			t.Fatalf("create scratch %s: %v", scratch.Table, err)
		}
		qualifiedScratch := quoteIdent(m.StageDB) + "." + quoteIdent(scratch.Table)
		if err := conn.Exec(ctx, "INSERT INTO "+qualifiedScratch+" (`_hg_row_id`, `p`, `v`) VALUES "+rows); err != nil {
			t.Fatal(err)
		}
		hashes := activePartHashes(t, conn, m.StageDB, scratch.Table)
		if len(hashes) != 1 {
			t.Fatalf("scratch %s holds parts %v, want one", scratch.Table, hashes)
		}
		if err := conn.Exec(ctx, "ALTER TABLE "+copyTable+" ATTACH PARTITION 'a' FROM "+qualifiedScratch); err != nil {
			t.Fatalf("ATTACH PARTITION FROM scratch %s: %v", scratch.Table, err)
		}
		want = append(want, hashes...)
		slices.Sort(want)
		if got := activePartHashes(t, conn, m.ReplicaDB, "db__t__i7"); !slices.Equal(got, want) {
			t.Fatalf("after attaching %s the copy's parts have hash_of_all_files %v, want %v", scratch.Table, got, want)
		}
	}
	var rows uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+copyTable).Scan(&rows); err != nil || rows != 3 {
		t.Fatalf("copy rows = %d, %v; want 3", rows, err)
	}
}

// activePartHashes returns the sorted hash_of_all_files of every active part
// of database.table.
func activePartHashes(t *testing.T, conn clickhouse.Conn, database, table string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT hash_of_all_files FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY hash_of_all_files`, database, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var hash string
		if err := rows.Scan(&hash); err != nil {
			t.Fatal(err)
		}
		out = append(out, hash)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}
