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

func TestEnsureProtocolTables_VerifiesQuotedPartitionIdentifier(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	partition := "part\n\t\r,`key"
	sch := payloadexec.TableSchema{
		TableID:     "db.quoted_" + uniqueSuffix(t),
		PartitionBy: partition,
		Columns: []lthash.Column{
			{Name: partition, Type: "String"},
			{Name: "value with space", Type: "UInt64"},
		},
	}

	if err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{sch}, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("create and verify quoted identifier: %v", err)
	}
	if err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{sch}, ModeVerifyOnly, slog.Default()); err != nil {
		t.Fatalf("verify-only quoted identifier: %v", err)
	}
}

func TestEnsureProtocolTables_VerifiesLiteralBackslashesInPartitionIdentifier(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)

	for name, partition := range map[string]string{
		"backslash-n":             "part\\nkey",
		"backslash-hex":           "part\\x41key",
		"backslash-with-backtick": "part\\n`key\\x41",
	} {
		t.Run(name, func(t *testing.T) {
			sch := payloadexec.TableSchema{
				TableID:     "db.literal_backslash_" + uniqueSuffix(t),
				PartitionBy: partition,
				Columns: []lthash.Column{
					{Name: partition, Type: "String"},
					{Name: "value", Type: "UInt64"},
				},
			}

			if err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{sch}, ModeCreateAndVerify, slog.Default()); err != nil {
				t.Fatalf("create and verify literal-backslash identifier %q: %v", partition, err)
			}
			if err := EnsureProtocolTables(ctx, conn, p, []payloadexec.TableSchema{sch}, ModeVerifyOnly, slog.Default()); err != nil {
				t.Fatalf("verify-only literal-backslash identifier %q: %v", partition, err)
			}
		})
	}
}

func TestVerifyProtocolTable_RejectsArrayExpressionMatchingQuotedColumnName(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := payloadexec.TableSchema{
		TableID:     "db.expression_" + uniqueSuffix(t),
		PartitionBy: "arr[1]",
		Columns: []lthash.Column{
			{Name: "arr[1]", Type: "String"},
			{Name: "arr", Type: "Array(String)"},
		},
	}
	_, want, err := Intents(p, sch)
	if err != nil {
		t.Fatalf("build intent: %v", err)
	}
	if err := conn.Exec(ctx, "CREATE DATABASE "+p.SafeDB); err != nil {
		t.Fatalf("create database: %v", err)
	}
	query := fmt.Sprintf(
		"CREATE TABLE %s.%s (`_hg_row_id` FixedString(32), `arr[1]` String, `arr` Array(String)) ENGINE = MergeTree PARTITION BY arr[1] ORDER BY (arr[1], `_hg_row_id`) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0",
		p.SafeDB,
		want.Table,
	)
	if err := conn.Exec(ctx, query); err != nil {
		t.Fatalf("create expression-drifted table: %v", err)
	}

	err = VerifyProtocolTable(ctx, conn, want)
	if !errors.Is(err, ErrProtocolTableDrift) {
		t.Fatalf("array expression must drift from quoted column intent: %v", err)
	}
	for _, field := range []string{"partition_key", "sorting_key"} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("expression drift must name %s: %v", field, err)
		}
	}
}

func TestVerifyProtocolTable_RequiresExplicitPinnedSettingOverride(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	if err := conn.Exec(ctx, "CREATE DATABASE "+p.SafeDB); err != nil {
		t.Fatalf("create database: %v", err)
	}
	table := "missing_pin_" + uniqueSuffix(t)
	if err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id FixedString(32), v UInt64) ENGINE = MergeTree ORDER BY (_hg_row_id)", p.SafeDB, table)); err != nil {
		t.Fatalf("create table without per-table pin: %v", err)
	}
	var global string
	if err := conn.QueryRow(ctx, "SELECT value FROM system.merge_tree_settings WHERE name = 'parts_to_throw_insert'").Scan(&global); err != nil {
		t.Fatalf("read matching global: %v", err)
	}
	want := TableIntent{
		Database: p.SafeDB,
		Table:    table,
		Engine:   EngineMergeTree,
		Columns: []lthash.Column{
			{Name: RowIDColumn, Type: RowIDType},
			{Name: "v", Type: "UInt64"},
		},
		SortingKey: []string{RowIDColumn},
		Settings:   []PinnedSetting{{Name: "parts_to_throw_insert", Value: global}},
	}

	err := VerifyProtocolTable(ctx, conn, want)
	if !errors.Is(err, ErrProtocolTableDrift) {
		t.Fatalf("missing explicit setting override = %v, want ErrProtocolTableDrift", err)
	}
	if !strings.Contains(err.Error(), "parts_to_throw_insert") || !strings.Contains(err.Error(), "missing") {
		t.Fatalf("missing override drift must name setting and absence: %v", err)
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
	connA := requireCH(t)
	connB := requireReplicaCH(t)
	requireKeeper(t, connA)
	requireKeeper(t, connB)
	a := testPinned(t)
	dropDatabasesSync(t, connA, a)
	b := a
	b.NodeID += "-b"
	dropDatabasesSync(t, connB, b)
	sch := ensureSchema(t)
	tables := []payloadexec.TableSchema{sch}
	if err := EnsureProtocolTables(ctx, connA, a, tables, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("ensure node A %s: %v", a.NodeID, err)
	}
	if err := EnsureProtocolTables(ctx, connB, b, tables, ModeCreateAndVerify, slog.Default()); err != nil {
		t.Fatalf("ensure node B %s: %v", b.NodeID, err)
	}
	table := CHTableName(sch.TableID)
	if err := connA.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s VALUES (unhex('%064x'), 'p0', 1)", a.UnsafeDB, table, 1)); err != nil {
		t.Fatalf("insert into replica a: %v", err)
	}
	if err := connB.Exec(ctx, fmt.Sprintf("SYSTEM SYNC REPLICA %s.%s", b.UnsafeDB, table)); err != nil {
		t.Fatalf("sync replica b: %v", err)
	}
	var rows uint64
	if err := connB.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.%s", b.UnsafeDB, table)).Scan(&rows); err != nil || rows != 1 {
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
