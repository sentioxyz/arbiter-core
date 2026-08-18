package ddl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func ensureSchema(t *testing.T) payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t_" + uniqueSuffix(t),
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
}

func TestEnsureProtocolTables_CreateVerifyTamperDrift(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	tables := []payloadexec.TableSchema{sch}

	if err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("first ensure: %v", err)
	}
	if err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("second ensure: %v", err)
	}
	table := CHTableName(sch.TableID)
	var engine, zk, replica string
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", p.UnsafeDB, table).Scan(&engine); err != nil || engine != "ReplicatedMergeTree" {
		t.Fatalf("unsafe engine = %q err=%v", engine, err)
	}
	if err := conn.QueryRow(ctx, "SELECT zookeeper_path, replica_name FROM system.replicas WHERE database = ? AND table = ?", p.UnsafeDB, table).Scan(&zk, &replica); err != nil {
		t.Fatalf("system.replicas: %v", err)
	}
	if zk != ZooKeeperPath(p, sch.TableID) || replica != p.NodeID {
		t.Fatalf("zk=%q replica=%q", zk, replica)
	}
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", p.SafeDB, table).Scan(&engine); err != nil || engine != "MergeTree" {
		t.Fatalf("safe engine = %q err=%v", engine, err)
	}

	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s MODIFY SETTING parts_to_throw_insert = 2999", p.UnsafeDB, table)); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default())
	if !errors.Is(err, ErrProtocolTableDrift) {
		t.Fatalf("err = %v, want ErrProtocolTableDrift", err)
	}
	if !strings.Contains(err.Error(), p.UnsafeDB+"."+table) || !strings.Contains(err.Error(), "parts_to_throw_insert") {
		t.Fatalf("drift error must name table and setting: %v", err)
	}
}

func TestEnsureProtocolTables_VerifyOnlyNeverCreates(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	tables := []payloadexec.TableSchema{ensureSchema(t)}

	err := EnsureProtocolTables(ctx, conn, p, tables, ModeVerifyOnly, slog.Default())
	if !errors.Is(err, ErrProtocolTableMissing) {
		t.Fatalf("err = %v, want ErrProtocolTableMissing", err)
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.databases WHERE name IN (?, ?, ?)", p.UnsafeDB, p.SafeDB, p.PromoteDB).Scan(&n); err != nil {
		t.Fatalf("count databases: %v", err)
	}
	if n != 0 {
		t.Fatalf("verify-only created %d databases", n)
	}
}

func TestEnsureProtocolTables_DetectsEngineAndColumnDrift(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	table := CHTableName(sch.TableID)
	for _, q := range []string{
		"CREATE DATABASE IF NOT EXISTS " + p.UnsafeDB,
		"CREATE DATABASE IF NOT EXISTS " + p.SafeDB,
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY tuple()", p.UnsafeDB, table),
		fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), p String) ENGINE = MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0", p.SafeDB, table),
	} {
		if err := conn.Exec(ctx, q); err != nil {
			t.Fatalf("%s: %v", q, err)
		}
	}
	err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{sch}, ModeVerifyOnly, slog.Default())
	if !errors.Is(err, ErrProtocolTableDrift) {
		t.Fatalf("err = %v, want ErrProtocolTableDrift", err)
	}
	msg := err.Error()
	for _, want := range []string{"engine", "columns", p.UnsafeDB + "." + table, p.SafeDB + "." + table} {
		if !strings.Contains(msg, want) {
			t.Fatalf("drift error missing %q: %s", want, msg)
		}
	}
}

func TestEnsureProtocolTables_SkipsFreezeViolationWithWarning(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	good := ensureSchema(t)
	bad := payloadexec.TableSchema{TableID: "db.bad_" + uniqueSuffix(t), PartitionBy: "n", Columns: []lthash.Column{{Name: "n", Type: "UInt64"}}}

	if err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{bad, good}, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("one bad declaration must not stop the ensure: %v", err)
	}
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", p.UnsafeDB, CHTableName(bad.TableID)).Scan(&n); err != nil || n != 0 {
		t.Fatalf("freeze-violating table must be skipped, count=%d err=%v", n, err)
	}
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", p.UnsafeDB, CHTableName(good.TableID)).Scan(&n); err != nil || n != 1 {
		t.Fatalf("good table must be created, count=%d err=%v", n, err)
	}
}

func TestEnsureProtocolTables_TwoReplicasSameZKPathReplicate(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	a := testPinned(t)
	dropDatabasesSync(t, conn, a)
	b := Pinned{UnsafeDB: a.UnsafeDB + "_b", SafeDB: a.SafeDB + "_b", PromoteDB: a.PromoteDB + "_b", NodeID: a.NodeID + "-b"}
	dropDatabasesSync(t, conn, b)
	sch := ensureSchema(t)
	tables := []payloadexec.TableSchema{sch}
	for _, p := range []Pinned{a, b} {
		if err := EnsureProtocolTables(ctx, conn, p, tables, ModeCreateAndVerify, slog.Default()); err != nil {
			t.Fatalf("ensure %s: %v", p.NodeID, err)
		}
	}
	table := CHTableName(sch.TableID)
	if err := conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p0', 1)", a.UnsafeDB, table, 1)); err != nil {
		t.Fatalf("insert into replica a: %v", err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf("SYSTEM SYNC REPLICA %s.%s", b.UnsafeDB, table)); err != nil {
		t.Fatalf("sync replica b: %v", err)
	}
	var rows uint64
	if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s", b.UnsafeDB, table)).Scan(&rows); err != nil || rows != 1 {
		t.Fatalf("replica b rows = %d err=%v, want 1 (same zk path %s)", rows, err, ZooKeeperPath(a, sch.TableID))
	}
}

func TestParseMode(t *testing.T) {
	for in, want := range map[string]Mode{"off": ModeOff, "verify": ModeVerifyOnly, "create": ModeCreateAndVerify} {
		got, err := ParseMode(in)
		if err != nil || got != want {
			t.Fatalf("ParseMode(%q) = %v, %v", in, got, err)
		}
		if got.String() != in {
			t.Fatalf("String() = %q want %q", got.String(), in)
		}
	}
	if _, err := ParseMode("maybe"); err == nil {
		t.Fatal("unknown mode must error")
	}
}
