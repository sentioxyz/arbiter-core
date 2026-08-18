//go:build !skip

package verifier

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

var scannerFixtureSequence atomic.Uint64

func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("clickhouse ping: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestCHScanner_MatchesSourceHashing(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	database, tableID := scannerFixtureNames(t)
	table := ddl.CHTableName(tableID)
	qualified := database + "." + table
	schema := scanTableSchema()
	schema.TableID = tableID

	if err := runScannerFixtureSetup(
		t.Cleanup,
		func() { dropScannerFixtureSync(t, conn, database, qualified) },
		func(query string) error { return conn.Exec(context.Background(), query) },
		database,
		qualified,
	); err != nil {
		t.Fatalf("setup scanner fixture: %v", err)
	}

	rid0 := payloadexec.RowID("testnet", tableID, "acct:1:n", 0)
	rid1 := payloadexec.RowID("testnet", tableID, "acct:1:n", 1)
	insertScanRows(t, conn, qualified, scanRow{rid: rid0, p: "p0", v: 1}, scanRow{rid: rid1, p: "p0", v: 2})
	partNames := activeScanPartNames(t, ctx, conn, database, table)
	if len(partNames) != 1 {
		t.Fatalf("active parts = %d, want 1 (%v)", len(partNames), partNames)
	}

	cfg := testConfigV()
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.UnsafeDatabase = database
	scanner := NewScanner(cfg, conn)
	got, err := scanner.Scan(ctx, []arbiter.PartRef{{
		TableID: tableID, PartitionID: "p0", PartRowLtHash: "0xLIE", PartName: partNames[0],
	}})
	if err != nil {
		t.Fatalf("scan: %v", err)
	}

	want := expectedScanHash(t, schema, rid0, rid1)
	if len(got) != 1 || got[0].ClaimedPartRowLtHash != "0xLIE" ||
		got[0].ScannedPartRowLtHash != want || got[0].LivePartName != partNames[0] {
		t.Fatalf("scan result: %+v want hash %s part %s", got, want, partNames[0])
	}
	if _, err := scanner.Scan(ctx, []arbiter.PartRef{{
		TableID: tableID, PartitionID: "p0", PartRowLtHash: "0xLIE", PartName: "absent_0_0_0",
	}}); err == nil {
		t.Fatal("missing part must make scanner refuse to attest")
	}
}

func scannerFixtureNames(t *testing.T) (database, tableID string) {
	t.Helper()
	seed := fmt.Sprintf("verifier/%s/%d/%d", t.Name(), os.Getpid(), scannerFixtureSequence.Add(1))
	sum := sha1.Sum([]byte(seed))
	suffix := hex.EncodeToString(sum[:])[:12]
	return "hg_unsafe_scan_" + suffix, "db.t_" + suffix
}

func runScannerFixtureSetup(registerCleanup func(func()), cleanup func(), exec func(string) error, database, qualified string) error {
	registerCleanup(cleanup)
	queries := []string{
		"DROP DATABASE IF EXISTS " + database + " SYNC",
		"CREATE DATABASE IF NOT EXISTS " + database,
		"DROP TABLE IF EXISTS " + qualified + " SYNC",
		fmt.Sprintf(`
			CREATE TABLE %s (
				_hg_row_id FixedString(32),
				p String,
				v UInt64
			) ENGINE = MergeTree
			PARTITION BY p
			ORDER BY tuple()`, qualified),
		"SYSTEM STOP MERGES " + qualified,
	}
	for _, query := range queries {
		if err := exec(query); err != nil {
			return fmt.Errorf("exec %q: %w", query, err)
		}
	}
	return nil
}

func dropScannerFixtureSync(t *testing.T, conn clickhouse.Conn, database, qualified string) {
	t.Helper()
	for _, query := range []string{
		"DROP TABLE IF EXISTS " + qualified + " SYNC",
		"DROP DATABASE IF EXISTS " + database + " SYNC",
	} {
		if err := conn.Exec(context.Background(), query); err != nil {
			t.Errorf("cleanup %q: %v", query, err)
		}
	}
}

func TestScannerFixtureNamesAreUniquePerInvocation(t *testing.T) {
	databaseA, tableA := scannerFixtureNames(t)
	databaseB, tableB := scannerFixtureNames(t)
	if databaseA == databaseB || tableA == tableB {
		t.Fatalf("fixture names reused: %s/%s and %s/%s", databaseA, tableA, databaseB, tableB)
	}
}

func TestScannerFixtureRegistersCleanupBeforeSetupFailure(t *testing.T) {
	var events []string
	var cleanup func()
	errInjected := errors.New("injected setup failure")

	err := runScannerFixtureSetup(
		func(fn func()) {
			events = append(events, "register-cleanup")
			cleanup = fn
		},
		func() { events = append(events, "cleanup") },
		func(query string) error {
			events = append(events, "setup:"+query)
			return errInjected
		},
		"hg_unsafe_scan_test",
		"hg_unsafe_scan_test.db__t_test",
	)
	if !errors.Is(err, errInjected) {
		t.Fatalf("setup error = %v, want injected failure", err)
	}
	if cleanup == nil {
		t.Fatal("cleanup was not registered before setup")
	}
	cleanup()
	if len(events) != 3 || events[0] != "register-cleanup" || events[2] != "cleanup" {
		t.Fatalf("lifecycle events = %v, want cleanup registration before setup and cleanup after failure", events)
	}
}

type scanRow struct {
	rid []byte
	p   string
	v   uint64
}

func scanTableSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
}

func expectedScanHash(t *testing.T, schema payloadexec.TableSchema, rid0, rid1 []byte) string {
	t.Helper()
	want := lthash.New()
	for _, row := range []struct {
		rid []byte
		p   string
		v   uint64
	}{{rid0, "p0", 1}, {rid1, "p0", 2}} {
		h, err := payloadexec.RowElementHash(schema, row.rid, []any{row.p, row.v})
		if err != nil {
			t.Fatalf("row hash: %v", err)
		}
		want.AddHash(h)
	}
	return "0x" + hex.EncodeToString(want.Bytes())
}

func insertScanRows(t *testing.T, conn clickhouse.Conn, table string, rows ...scanRow) {
	t.Helper()
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO "+table)
	if err != nil {
		t.Fatalf("PrepareBatch: %v", err)
	}
	for _, row := range rows {
		if err := batch.Append(row.rid, row.p, row.v); err != nil {
			t.Fatalf("batch.Append: %v", err)
		}
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("batch.Send: %v", err)
	}
}

func activeScanPartNames(t *testing.T, ctx context.Context, conn clickhouse.Conn, database, table string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT name FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY name", database, table)
	if err != nil {
		t.Fatalf("query active parts: %v", err)
	}
	defer rows.Close()
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan part name: %v", err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("active part rows: %v", err)
	}
	return names
}
