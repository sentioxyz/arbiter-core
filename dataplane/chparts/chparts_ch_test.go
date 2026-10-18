package chparts

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// requireCH mirrors snode/ch_test.go: opt-in via ARBITER_CH_INTEGRATION=1.
func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	return chtest.Open(t, addr)
}

// safeTable creates sch's hg_safe intent (ddl.Intents: the protocol columns,
// MergeTree, ddl.SafeSettings) in a database of its own, named after the test
// and the table id, stops its merges and drops the database when the test
// ends. It returns the database and the physical table name.
func safeTable(t *testing.T, conn clickhouse.Conn, sch payloadexec.TableSchema) (string, string) {
	t.Helper()
	sum := sha1.Sum([]byte(t.Name() + "\x00" + sch.TableID))
	db := "chparts_" + hex.EncodeToString(sum[:])[:10]
	ctx := context.Background()
	mustExec(t, conn, "DROP DATABASE IF EXISTS "+db+" SYNC")
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC") })
	mustExec(t, conn, "CREATE DATABASE "+db)
	_, safe, _, err := ddl.Intents(ddl.Pinned{UnsafeDB: db + "_unsafe", SafeDB: db, PromoteDB: db + "_promote", NodeID: "chparts"}, sch)
	if err != nil {
		t.Fatalf("intents: %v", err)
	}
	if err := conn.Exec(ctx, safe.SQL()); err != nil {
		t.Fatalf("create %s.%s: %v", db, safe.Table, err)
	}
	mustExec(t, conn, "SYSTEM STOP MERGES "+db+"."+safe.Table)
	return db, safe.Table
}

func mustExec(t *testing.T, conn clickhouse.Conn, query string) {
	t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// short is the head of a row LtHash for a failure message.
func short(h string) string {
	if len(h) <= 18 {
		return h
	}
	return h[:18]
}

// rowID is the 32-byte _hg_row_id of test row n.
func rowID(n uint64) string { return fmt.Sprintf("%032d", n) }

// insertRows writes rows first..last of partition value p in one INSERT,
// which ClickHouse stores as one new part (merges are stopped).
func insertRows(t *testing.T, conn clickhouse.Conn, db, table, p string, first, last uint64) {
	t.Helper()
	mustExec(t, conn, fmt.Sprintf(
		"INSERT INTO %s.%s (_hg_row_id, p, v) SELECT toFixedString(leftPad(toString(number), 32, '0'), 32), '%s', number FROM numbers(%d, %d)",
		db, table, EscapeSQLString(p), first, last-first+1))
}

// rowsRoot is the LtHash sum of rows first..last of partition value p, from
// the executor's own row element hash (payloadexec.RowElementHash): the value
// a scan of exactly those rows must give.
func rowsRoot(t *testing.T, sch payloadexec.TableSchema, p string, first, last uint64) string {
	t.Helper()
	var hashes []string
	for n := first; n <= last; n++ {
		h, err := payloadexec.RowElementHash(sch, []byte(rowID(n)), []any{p, n})
		if err != nil {
			t.Fatalf("row element hash %d: %v", n, err)
		}
		hashes = append(hashes, "0x"+hex.EncodeToString(h.Bytes()))
	}
	root, err := payloadexec.SumPartRowLtHashes(hashes)
	if err != nil {
		t.Fatalf("sum rows %d..%d: %v", first, last, err)
	}
	return root
}

func TestActivePartsAndPartitionContentScan(t *testing.T) {
	conn := requireCH(t)
	ctx := context.Background()
	sch := partitionedSchema()
	db, table := safeTable(t, conn, sch)

	if parts, err := ActiveParts(ctx, conn, db, table); err != nil || parts != nil {
		t.Fatalf("ActiveParts of an empty table = %#v, %v; want nil, nil", parts, err)
	}
	if parts, err := ActiveParts(ctx, conn, db, "missing"); err != nil || parts != nil {
		t.Fatalf("ActiveParts of a missing table = %#v, %v; want nil, nil", parts, err)
	}

	insertRows(t, conn, db, table, "a", 1, 3)
	insertRows(t, conn, db, table, "a", 4, 5)
	insertRows(t, conn, db, table, "b", 6, 6)

	parts, err := ActiveParts(ctx, conn, db, table)
	if err != nil {
		t.Fatalf("ActiveParts: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("ActiveParts = %+v, want 3 parts", parts)
	}
	if !slices.IsSortedFunc(parts, func(a, b Part) int { return strings.Compare(a.Name, b.Name) }) {
		t.Fatalf("ActiveParts not ordered by name: %+v", parts)
	}
	wantRows := map[string][]uint64{"p_a": {}, "p_b": {}}
	for _, p := range parts {
		id := LogicalPartitionID(sch, p)
		if _, ok := wantRows[id]; !ok {
			t.Fatalf("part %s has logical partition %q (value %q), want p_a or p_b", p.Name, id, p.PartitionValue)
		}
		wantRows[id] = append(wantRows[id], p.Rows)
		if p.PartitionID == "" || len(p.PhysHash) != 32 || p.Path == "" || p.Bytes == 0 {
			t.Fatalf("part %+v lacks a partition_id, a 32-hex hash_of_all_files, a path or its bytes", p)
		}
	}
	if !slices.Equal(wantRows["p_a"], []uint64{3, 2}) || !slices.Equal(wantRows["p_b"], []uint64{1}) {
		t.Fatalf("rows per logical partition = %v, want p_a [3 2] and p_b [1]", wantRows)
	}

	root, scanned, err := PartitionContentScan(ctx, conn, db, table, sch, "p_a")
	if err != nil {
		t.Fatalf("PartitionContentScan p_a: %v", err)
	}
	if want := rowsRoot(t, sch, "a", 1, 5); root != want {
		t.Fatalf("p_a root %s, want the sum of rows 1..5 %s", short(root), short(want))
	}
	inA := PartsInLogicalPartition(parts, sch, "p_a")
	if len(scanned) != 2 || scanned[0].Name != inA[0].Name || scanned[1].Name != inA[1].Name {
		t.Fatalf("scanned %+v, want the two p_a parts %s and %s by name", scanned, inA[0].Name, inA[1].Name)
	}
	if scanned[0].Rows != 3 || scanned[0].RowLtHash != rowsRoot(t, sch, "a", 1, 3) ||
		scanned[1].Rows != 2 || scanned[1].RowLtHash != rowsRoot(t, sch, "a", 4, 5) {
		t.Fatalf("scanned parts carry the wrong rows or row LtHash: %+v", scanned)
	}
	direct, err := chexec.ScanParts(ctx, conn, db+"."+table, sch, []string{inA[0].Name, inA[1].Name})
	if err != nil {
		t.Fatalf("chexec.ScanParts: %v", err)
	}
	for i, d := range direct {
		if d.PartName != scanned[i].Name || d.RowLtHash != scanned[i].RowLtHash || d.RowCount != scanned[i].Rows {
			t.Fatalf("scanned[%d] = %+v, chexec.ScanParts says %+v", i, scanned[i], d)
		}
	}

	root, scanned, err = PartitionContentScan(ctx, conn, db, table, sch, "p_b")
	if err != nil || root != rowsRoot(t, sch, "b", 6, 6) || len(scanned) != 1 || scanned[0].Rows != 1 {
		t.Fatalf("PartitionContentScan p_b = %s, %+v, %v; want row 6 alone", short(root), scanned, err)
	}

	zero, err := payloadexec.SumPartRowLtHashes(nil)
	if err != nil {
		t.Fatalf("empty sum: %v", err)
	}
	root, scanned, err = PartitionContentScan(ctx, conn, db, table, sch, "p_c")
	if err != nil || root != zero || scanned != nil {
		t.Fatalf("PartitionContentScan of a partition with no part = %s, %#v, %v; want the zero root and no part", short(root), scanned, err)
	}
}

func TestDropPartitionIfPresent(t *testing.T) {
	conn := requireCH(t)
	ctx := context.Background()
	sch := partitionedSchema()
	db, table := safeTable(t, conn, sch)
	insertRows(t, conn, db, table, "a", 1, 2)
	insertRows(t, conn, db, table, "b", 3, 3)

	dropped, err := DropPartitionIfPresent(ctx, conn, db, table, sch, "p_a")
	if err != nil || !dropped {
		t.Fatalf("DropPartitionIfPresent p_a = %v, %v; want true, nil", dropped, err)
	}
	parts, err := ActiveParts(ctx, conn, db, table)
	if err != nil || len(parts) != 1 || LogicalPartitionID(sch, parts[0]) != "p_b" {
		t.Fatalf("after dropping p_a: %+v, %v; want only the p_b part", parts, err)
	}
	for _, id := range []string{"p_a", "p_c", "all", "q_x"} {
		dropped, err := DropPartitionIfPresent(ctx, conn, db, table, sch, id)
		if err != nil || dropped {
			t.Fatalf("DropPartitionIfPresent %s with no such part = %v, %v; want false, nil (nothing issued)", id, dropped, err)
		}
	}
	if parts, err := ActiveParts(ctx, conn, db, table); err != nil || len(parts) != 1 {
		t.Fatalf("p_b must survive: %+v, %v", parts, err)
	}

	usch := unpartitionedSchema()
	udb, utable := safeTable(t, conn, usch)
	mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s (_hg_row_id, p, v) VALUES ('%s', 'x', 7)", udb, utable, rowID(7)))
	dropped, err = DropPartitionIfPresent(ctx, conn, udb, utable, usch, "all")
	if err != nil || !dropped {
		t.Fatalf("DropPartitionIfPresent all of an unpartitioned table = %v, %v; want true, nil", dropped, err)
	}
	if parts, err := ActiveParts(ctx, conn, udb, utable); err != nil || parts != nil {
		t.Fatalf("after dropping all: %+v, %v; want no part", parts, err)
	}

	missing := "missing_table"
	if dropped, err := DropPartitionIfPresent(ctx, conn, db, missing, sch, "p_b"); err != nil || dropped {
		t.Fatalf("DropPartitionIfPresent on a missing table = %v, %v; want false, nil", dropped, err)
	}
}
