package shadow

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

// requireCH is the dataplane/ddl opt-in: ARBITER_CH_INTEGRATION=1 and a
// ClickHouse on CH_ADDR (default 127.0.0.1:9000), opened through chtest.
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

// recordingConn records every statement the shadow sends.
type recordingConn struct {
	clickhouse.Conn
	mu   sync.Mutex
	sent []string
}

func (c *recordingConn) record(q string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, q)
}

func (c *recordingConn) statements() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.sent...)
}

func (c *recordingConn) Exec(ctx context.Context, q string, args ...any) error {
	c.record(q)
	return c.Conn.Exec(ctx, q, args...)
}

func (c *recordingConn) Query(ctx context.Context, q string, args ...any) (driver.Rows, error) {
	c.record(q)
	return c.Conn.Query(ctx, q, args...)
}

func (c *recordingConn) QueryRow(ctx context.Context, q string, args ...any) driver.Row {
	c.record(q)
	return c.Conn.QueryRow(ctx, q, args...)
}

func (c *recordingConn) PrepareBatch(ctx context.Context, q string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	c.record(q)
	return c.Conn.PrepareBatch(ctx, q, opts...)
}

// localPull stands in for the owner unsafe pull on one server: it copies the
// named part from a local owner table with the pull's statement shape
// (INSERT … SELECT * … WHERE _part = …), not through remote().
type localPull struct {
	conn clickhouse.Conn
	db   string
}

func (p localPull) Name() string { return "owner_unsafe_pull" }

func (p localPull) Fetch(ctx context.Context, need sources.Need, _ payloadexec.TableSchema, scratch sources.Scratch) sources.Result {
	q := fmt.Sprintf("INSERT INTO %s.%s SELECT * FROM %s.%s WHERE _part = '%s'",
		quoteIdent(scratch.Database), quoteIdent(scratch.Table), quoteIdent(p.db), quoteIdent(need.Table), need.PartName)
	if err := p.conn.Exec(ctx, q); err != nil {
		return sources.Result{Source: p.Name(), Outcome: sources.OutcomeRefused, Err: err}
	}
	return sources.Result{Source: p.Name(), Outcome: sources.OutcomeFetched}
}

// TestShadowReplaysInTheStageDatabaseUnderItsPrefix: against a real
// ClickHouse, the shadow pulls the candidate into a scratch, scans it,
// replays the block through the ClickHouse materializer, reproduces the
// network verifiers' receipt and scan, creates every table of its own in the
// stage database under ReplayPrefix, and leaves none behind.
func TestShadowReplaysInTheStageDatabaseUnderItsPrefix(t *testing.T) {
	admin := requireCH(t)
	ctx := context.Background()
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	stageDB, ownerDB := "hg_rstage_"+suffix, "hg_unsafe_"+suffix
	for _, db := range []string{stageDB, ownerDB} {
		mustExec(t, admin, "DROP DATABASE IF EXISTS "+db+" SYNC")
		mustExec(t, admin, "CREATE DATABASE "+db)
		t.Cleanup(func() { _ = admin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC") })
	}

	fx := newFixture(t)
	b := fx.block(7, ownerA, "p0", 101)
	schema := testSchema()
	_, ownerTable, _, err := ddl.Intents(ddl.Pinned{UnsafeDB: "unused_unsafe", SafeDB: ownerDB, PromoteDB: "unused_promote", NodeID: "owner"}, schema)
	if err != nil {
		t.Fatalf("owner table intent: %v", err)
	}
	mustExec(t, admin, ownerTable.SQL())
	batch, err := admin.PrepareBatch(ctx, "INSERT INTO "+quoteIdent(ownerDB)+".`db__t`")
	if err != nil {
		t.Fatalf("prepare owner insert: %v", err)
	}
	statementID := b.BV.ReplayJob.Statements[0].StatementID
	if err := batch.Append(payloadexec.RowID(testNetwork, "db.t", statementID, 0), "p0", uint64(101)); err != nil {
		t.Fatalf("append owner row: %v", err)
	}
	if err := batch.Send(); err != nil {
		t.Fatalf("insert owner row: %v", err)
	}
	owned, err := chexec.ScanAllParts(ctx, admin, ownerDB+".db__t", schema)
	if err != nil || len(owned) != 1 {
		t.Fatalf("owner parts %+v (%v), want one", owned, err)
	}
	if owned[0].RowLtHash != b.Part.PartRowLtHash {
		t.Fatalf("the owner's part scans to %s, the replay computes %s", owned[0].RowLtHash, b.Part.PartRowLtHash)
	}
	name := owned[0].PartName
	b.BV.Statements[0].CandidateParts[0].PartName = name
	b.BV.ScanParts[0].PartName = name
	for i := range b.BV.Evidence {
		b.BV.Evidence[i].ByteSideScan.Parts[0].LivePartName = name
	}

	conn := &recordingConn{Conn: admin}
	cfg := testConfig(t.TempDir())
	cfg.Pinned.StageDB = stageDB
	arb := newFakeArbiter(6, b.BV)
	v, err := New(cfg, Deps{Blocks: arb, Watermark: arb, Snapshots: fx.snapshots, Payloads: fx.payloads, Conn: conn,
		UnsafePull: func(uint64) sources.Source { return localPull{conn: admin, db: ownerDB} }})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	stepAll(t, v, 1)

	wantCounts(t, v.Stats(), ownerA, 1, 0, 0, 0, 0, 0)
	ours := "`" + stageDB + "`.`" + ReplayPrefix(testNodeID, testBootNonce)
	var creates []string
	for _, q := range conn.statements() {
		if strings.Contains(q, ownerDB) || strings.Contains(q, "hg_safe") || strings.Contains(q, "hg_replica") {
			t.Fatalf("the shadow addressed another database: %q", q)
		}
		head := strings.ToUpper(strings.TrimSpace(q))
		if strings.HasPrefix(head, "CREATE") || strings.HasPrefix(head, "INSERT") || strings.HasPrefix(head, "DROP") {
			if !strings.Contains(q, ours) {
				t.Fatalf("statement outside %s…: %q", ours, q)
			}
		}
		if strings.HasPrefix(head, "CREATE") {
			if strings.Contains(q, ours+"replay_") && !strings.Contains(q, "SETTINGS storage_policy = '"+cfg.Pinned.StoragePolicy+"'") {
				t.Fatalf("replay scratch omitted the configured storage policy: %q", q)
			}
			creates = append(creates, q[:strings.Index(q, "(")])
		}
	}
	if len(creates) != 2 || !strings.Contains(creates[0], ours+"scan_1`") || !strings.Contains(creates[1], ours+"replay_1`") {
		t.Fatalf("created %q, want the candidate scratch then the replay scratch", creates)
	}
	var left uint64
	if err := admin.QueryRow(ctx, "SELECT count() FROM system.tables WHERE database = ?", stageDB).Scan(&left); err != nil || left != 0 {
		t.Fatalf("%d tables left in %s (%v), want 0", left, stageDB, err)
	}
}

func mustExec(t *testing.T, conn clickhouse.Conn, q string) {
	t.Helper()
	if err := conn.Exec(context.Background(), q); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}
