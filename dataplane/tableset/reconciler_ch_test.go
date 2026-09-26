package tableset

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

const testNetwork = "net-tableset"

func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn := openCH(t, addr)
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but ClickHouse has no Keeper: %v", err)
		}
		t.Skipf("ClickHouse has no Keeper: %v", err)
	}
	return conn
}

func requireReplicaCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_REPLICA") != "1" {
		t.Skip("set ARBITER_CH_REPLICA=1 and CH_REPLICA_ADDR to run the two-node tests")
	}
	addr := os.Getenv("CH_REPLICA_ADDR")
	if addr == "" {
		t.Fatal("ARBITER_CH_REPLICA=1 requires CH_REPLICA_ADDR")
	}
	return openCH(t, addr)
}

func openCH(t *testing.T, addr string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func suffix(t *testing.T) string {
	sum := sha1.Sum([]byte(t.Name()))
	return hex.EncodeToString(sum[:])[:10]
}

func testPinned(t *testing.T, conns ...clickhouse.Conn) ddl.Pinned {
	t.Helper()
	s := suffix(t)
	p := ddl.Pinned{UnsafeDB: "hg_unsafe_" + s, SafeDB: "hg_safe_" + s, PromoteDB: "hg_promote_" + s, NodeID: "node-" + s}
	t.Cleanup(func() {
		for _, conn := range conns {
			for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
				_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
			}
		}
	})
	return p
}

func schemaFor(t *testing.T, name string, extra ...lthash.Column) payloadexec.TableSchema {
	cols := []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}
	return payloadexec.TableSchema{TableID: "db." + name + "_" + suffix(t), PartitionBy: "p", Columns: append(cols, extra...)}
}

func chainInc(t *testing.T, seq uint64, schema payloadexec.TableSchema, st wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: st,
		SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash(testNetwork, schema), SchemaJSON: string(js)}
}

func genesisInc(t *testing.T, seq uint64, schema payloadexec.TableSchema, st wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginGenesis, Status: st,
		SchemaHash: payloadexec.TableSchemaHash(testNetwork, schema)}
}

// fakeView is a settable RegistryView.
type fakeView struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	changed chan struct{}
	ready   chan struct{}
}

func newFakeView() *fakeView {
	v := &fakeView{changed: make(chan struct{}), ready: make(chan struct{})}
	close(v.ready)
	return v
}

func (v *fakeView) View() (wire.TableRegistrySnapshot, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snap, v.enabled
}

func (v *fakeView) Changed() <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.changed
}

func (v *fakeView) Ready() <-chan struct{} { return v.ready }

// set installs the incarnations, in the caller's order and with the caller's
// seqs, as the next version.
func (v *fakeView) set(incs ...wire.TableIncarnation) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snap = wire.TableRegistrySnapshot{Version: v.snap.Version + 1, Seeded: true, Incarnations: incs}
	v.enabled = true
	close(v.changed)
	v.changed = make(chan struct{})
}

type fakeArbiter struct {
	mu      sync.Mutex
	purges  []string
	err     error
	nodeSet []string
}

func (a *fakeArbiter) SubmitTablePurged(_ context.Context, nodeID string, seq uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.purges = append(a.purges, fmt.Sprintf("%s/%d", nodeID, seq))
	return a.err
}

func (a *fakeArbiter) PurgeNodeSet(context.Context) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.nodeSet), nil
}

func (a *fakeArbiter) reported() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.purges)
}

func newReconciler(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, view *fakeView, arb *fakeArbiter, mutate func(*Config, *Deps)) *Reconciler {
	t.Helper()
	cfg := Config{Pinned: p}
	deps := Deps{Conn: conn, Registry: view, Arbiter: arb, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if mutate != nil {
		mutate(&cfg, &deps)
	}
	r, err := New(cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// pinClock fixes the reconciler's clock so a per-table backoff never expires
// on its own during the test. The returned clock only moves when advanced.
func pinClock(r *Reconciler) *testClock {
	c := &testClock{t: time.Now()}
	r.now = c.Now
	return c
}

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func stateCount(st Stats) int {
	n := 0
	for _, c := range st.States {
		n += c
	}
	return n
}

func localComments(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, tableID string) map[string]string {
	t.Helper()
	tables, err := ddl.ListProtocolTables(context.Background(), conn, p)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, lt := range tables {
		if lt.Table == ddl.CHTableName(tableID) {
			out[lt.Database] = lt.Comment
		}
	}
	return out
}

// createWithComment creates schema's three tables carrying exactly comment
// (which EnsureTable cannot produce for a non-canonical marker).
func createWithComment(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, schema payloadexec.TableSchema, comment string) {
	t.Helper()
	ctx := context.Background()
	unsafe, safe, promote, err := ddl.IncarnationIntents(p, schema, 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
		if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+db); err != nil {
			t.Fatal(err)
		}
	}
	for _, intent := range []ddl.TableIntent{unsafe, safe, promote} {
		intent.Comment = comment
		if err := conn.Exec(ctx, intent.SQL()); err != nil {
			t.Fatal(err)
		}
	}
}

func keeperPathExists(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, tableID string) bool {
	t.Helper()
	paths, err := ddl.KeeperUnsafeTables(context.Background(), conn, p)
	if err != nil {
		t.Fatal(err)
	}
	return slices.Contains(paths, ddl.CHTableName(tableID))
}

func mustReconcile(t *testing.T, r *Reconciler) {
	t.Helper()
	if err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
}

func TestReconciler_CreatesPendingThenKeepsActiveReady(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusPending))
	if r.Ready(s.TableID) {
		t.Fatal("ready before any pass")
	}
	mustReconcile(t, r)
	if !r.Ready(s.TableID) {
		t.Fatal("pending table not ready after a pass")
	}
	for db, c := range localComments(t, conn, p, s.TableID) {
		if c != "hg_incarnation=1" {
			t.Fatalf("%s comment = %q", db, c)
		}
	}
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	if !r.Ready(s.TableID) || r.Stats().States[StateReady] != 1 {
		t.Fatalf("active table not ready: %+v", r.Stats())
	}
}

func TestReconciler_PurgingDropsTablesAndKeeperPathThenReports(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if left := localComments(t, conn, p, s.TableID); len(left) != 0 {
		t.Fatalf("tables left: %v", left)
	}
	if keeperPathExists(t, conn, p, s.TableID) {
		t.Fatal("the last replica's drop must remove the Keeper table path")
	}
	if got := arb.reported(); !slices.Equal(got, []string{p.NodeID + "/1"}) {
		t.Fatalf("reports = %v", got)
	}
	if r.Ready(s.TableID) {
		t.Fatal("a purged table is not ready")
	}
	inc := chainInc(t, 1, s, wire.TableStatusPurging)
	inc.PurgedBy = []string{p.NodeID}
	view.set(inc)
	mustReconcile(t, r)
	if got := arb.reported(); len(got) != 1 {
		t.Fatalf("a recorded purge must not be resubmitted: %v", got)
	}
}

func TestReconciler_PurgeWaitsForQuiescence(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	quiet := false
	r := newReconciler(t, conn, p, view, arb, func(_ *Config, d *Deps) {
		d.Quiescent = func(string) (bool, error) { return quiet, nil }
	})
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if len(localComments(t, conn, p, s.TableID)) != 3 || len(arb.reported()) != 0 {
		t.Fatal("a non-quiescent table must be neither dropped nor reported")
	}
	if r.Stats().States[StateWaitingQuiescence] != 1 {
		t.Fatalf("stats = %+v", r.Stats())
	}
	quiet = true
	mustReconcile(t, r)
	if len(localComments(t, conn, p, s.TableID)) != 0 || len(arb.reported()) != 1 {
		t.Fatal("a quiescent Purging table must be dropped and reported")
	}
}

func TestReconciler_SweepsDecommissionedReplicas(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t, conn, replicaConn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned = true })
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	// Two more replicas on the second server, in their own databases (the
	// Keeper path depends on the table only): verifier-live is a current node
	// that has not purged yet; verifier-gone is decommissioned (detached).
	live, gone := p, p
	live.UnsafeDB, live.SafeDB, live.PromoteDB, live.NodeID = p.UnsafeDB+"_live", p.SafeDB+"_live", p.PromoteDB+"_live", "verifier-live"
	gone.UnsafeDB, gone.SafeDB, gone.PromoteDB, gone.NodeID = p.UnsafeDB+"_gone", p.SafeDB+"_gone", p.PromoteDB+"_gone", "verifier-gone"
	t.Cleanup(func() {
		for _, q := range []ddl.Pinned{live, gone} {
			for _, db := range []string{q.UnsafeDB, q.SafeDB, q.PromoteDB} {
				_ = replicaConn.Exec(ctx, "DROP DATABASE IF EXISTS "+db+" SYNC")
			}
		}
	})
	for _, q := range []ddl.Pinned{live, gone} {
		if err := ddl.EnsureTable(ctx, replicaConn, q, s, 1, ddl.ModeCreateAndVerify); err != nil {
			t.Fatal(err)
		}
	}
	if err := replicaConn.Exec(ctx, fmt.Sprintf("DETACH TABLE %s.%s", gone.UnsafeDB, ddl.CHTableName(s.TableID))); err != nil {
		t.Fatal(err)
	}
	arb.nodeSet = []string{p.NodeID, "verifier-live"}
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	replicas, err := ddl.KeeperReplicas(ctx, conn, p, s.TableID)
	if err != nil || !slices.Equal(replicas, []string{"verifier-live"}) {
		t.Fatalf("replicas = %v, %v; want only the current verifier-live", replicas, err)
	}
	if got := arb.reported(); !slices.Equal(got, []string{p.NodeID + "/1"}) {
		t.Fatalf("reports = %v", got)
	}
	// verifier-live purges its own tables: the last replica removes the path.
	if err := ddl.DropTable(ctx, replicaConn, live, s.TableID); err != nil {
		t.Fatal(err)
	}
	if keeperPathExists(t, conn, p, s.TableID) {
		t.Fatal("keeper path must be gone once every current node dropped")
	}
}

func TestReconciler_ConvergesAfterCrashMidCreate(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	unsafe, _, _, err := ddl.IncarnationIntents(p, s, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(context.Background(), "CREATE DATABASE IF NOT EXISTS "+p.UnsafeDB); err != nil {
		t.Fatal(err)
	}
	if err := conn.Exec(context.Background(), unsafe.SQL()); err != nil {
		t.Fatal(err)
	}
	view.set(chainInc(t, 1, s, wire.TableStatusPending))
	mustReconcile(t, r)
	if got := localComments(t, conn, p, s.TableID); len(got) != 3 || !r.Ready(s.TableID) {
		t.Fatalf("after a crash mid-create: tables %v ready %v", got, r.Ready(s.TableID))
	}
}

func TestReconciler_ConvergesAfterCrashMidPurge(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	for _, db := range []string{p.PromoteDB, p.SafeDB} {
		if err := conn.Exec(context.Background(), fmt.Sprintf("DROP TABLE %s.%s SYNC", db, ddl.CHTableName(s.TableID))); err != nil {
			t.Fatal(err)
		}
	}
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if len(localComments(t, conn, p, s.TableID)) != 0 || keeperPathExists(t, conn, p, s.TableID) || len(arb.reported()) != 1 {
		t.Fatal("a purge interrupted after hg_safe must converge")
	}
}

func TestReconciler_DropsLeftoversOfRetiredKeysAndEarlierIncarnations(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	gone, reused := schemaFor(t, "gone"), schemaFor(t, "reused")
	for _, s := range []payloadexec.TableSchema{gone, reused} {
		if err := ddl.EnsureTable(context.Background(), conn, p, s, 1, ddl.ModeCreateAndVerify); err != nil {
			t.Fatal(err)
		}
	}
	// gone: its only incarnation is Purged. reused: incarnation 2 is Purged
	// and a new Pending incarnation 3 exists (this node missed the purge).
	g := chainInc(t, 1, gone, wire.TableStatusPurged)
	old := chainInc(t, 2, reused, wire.TableStatusPurged)
	next := chainInc(t, 3, reused, wire.TableStatusPending)
	// The tables were created as incarnation 1; renumber reused's marker to 2.
	if err := ddl.DropTable(context.Background(), conn, p, reused.TableID); err != nil {
		t.Fatal(err)
	}
	if err := ddl.EnsureTable(context.Background(), conn, p, reused, 2, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view.set(g, old, next)
	mustReconcile(t, r)
	if left := localComments(t, conn, p, gone.TableID); len(left) != 0 {
		t.Fatalf("leftover of a Purged key survives: %v", left)
	}
	for db, c := range localComments(t, conn, p, reused.TableID) {
		if c != "hg_incarnation=3" {
			t.Fatalf("%s comment = %q, want the new incarnation", db, c)
		}
	}
	if st := r.Stats(); st.LeftoverDrops != 2 || !r.Ready(reused.TableID) {
		t.Fatalf("stats = %+v ready=%v", st, r.Ready(reused.TableID))
	}
}

func TestReconciler_ForeignMarkerIsDriftNotALeftover(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	if err := ddl.EnsureTable(context.Background(), conn, p, s, 9, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view.set(chainInc(t, 1, s, wire.TableStatusPending))
	err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly)
	if !errors.Is(err, ddl.ErrProtocolTableDrift) {
		t.Fatalf("err = %v, want drift", err)
	}
	if got := localComments(t, conn, p, s.TableID); len(got) != 3 {
		t.Fatalf("an unattributable table must never be dropped: %v", got)
	}
}

// R-F1: under a retired key (live incarnation Purged, Refused or Legacy) only
// tables the marker attributes to an incarnation of that same key, or
// unmarked tables of a key with a genesis incarnation, are dropped. Anything
// else is reported as unknown and never dropped, and the pass is not fatal:
// nothing needs creating there. The source SNode's Keeper sweep of retired
// keys must not drop them either.
func TestReconciler_UnattributableLeftoverUnderARetiredKeyIsReportedNotDropped(t *testing.T) {
	for _, sweep := range []bool{false, true} {
		t.Run(fmt.Sprintf("sweep=%v", sweep), func(t *testing.T) {
			conn := requireCH(t)
			p := testPinned(t, conn)
			view, arb := newFakeView(), &fakeArbiter{}
			r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned = sweep })
			foreign := schemaFor(t, "foreign")       // marked 2: another key's incarnation
			noncanonical := schemaFor(t, "noncanon") // marked "02": not a canonical marker
			unmarked := schemaFor(t, "unmarked")     // unmarked, and its key has no genesis incarnation
			own := schemaFor(t, "own")               // marked 4: its own Purged incarnation
			genesis := schemaFor(t, "genesis")       // unmarked, its key's genesis incarnation is Purged
			createWithComment(t, conn, p, foreign, "hg_incarnation=2")
			createWithComment(t, conn, p, noncanonical, "hg_incarnation=02")
			createWithComment(t, conn, p, unmarked, "")
			createWithComment(t, conn, p, own, "hg_incarnation=4")
			createWithComment(t, conn, p, genesis, "")
			view.set(
				chainInc(t, 1, foreign, wire.TableStatusPurged),
				chainInc(t, 2, noncanonical, wire.TableStatusPurged),
				chainInc(t, 3, unmarked, wire.TableStatusRefused),
				chainInc(t, 4, own, wire.TableStatusPurged),
				genesisInc(t, 5, genesis, wire.TableStatusPurged),
			)
			if err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
				t.Fatalf("an unattributable table under a retired key must not be fatal: %v", err)
			}
			var wantUnknown []string
			for _, s := range []payloadexec.TableSchema{foreign, noncanonical, unmarked} {
				if got := localComments(t, conn, p, s.TableID); len(got) != 3 {
					t.Fatalf("%s: an unattributable table must never be dropped: %v", s.TableID, got)
				}
				for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
					wantUnknown = append(wantUnknown, db+"."+ddl.CHTableName(s.TableID))
				}
			}
			slices.Sort(wantUnknown)
			for _, s := range []payloadexec.TableSchema{own, genesis} {
				if got := localComments(t, conn, p, s.TableID); len(got) != 0 {
					t.Fatalf("%s: an attributable leftover must be dropped: %v", s.TableID, got)
				}
			}
			st := r.Stats()
			if st.LeftoverDrops != 2 || !slices.Equal(st.Unknown, wantUnknown) {
				t.Fatalf("stats = %+v, want 2 leftover drops and unknown %v", st, wantUnknown)
			}
			// A later pass leaves them in place too.
			mustReconcile(t, r)
			if got := localComments(t, conn, p, foreign.TableID); len(got) != 3 {
				t.Fatalf("second pass dropped a foreign table: %v", got)
			}
		})
	}
}

func TestReconciler_UnknownTablesAreOnlyReported(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	stray := schemaFor(t, "stray")
	if err := ddl.EnsureTable(context.Background(), conn, p, stray, 0, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view.set()
	mustReconcile(t, r)
	if got := localComments(t, conn, p, stray.TableID); len(got) != 3 {
		t.Fatalf("unknown tables must survive: %v", got)
	}
	if st := r.Stats(); len(st.Unknown) != 3 {
		t.Fatalf("unknown = %v", st.Unknown)
	}
}

func TestReconciler_IsolatesPerTableFailures(t *testing.T) {
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t, conn, replicaConn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	// R-F2: with the clock pinned the first 1 s backoff cannot expire between
	// the passes, so "not retried by the next pass" is deterministic.
	pinClock(r)
	blocked, fine := schemaFor(t, "blocked"), schemaFor(t, "fine")
	// Another server actively holds this node's replica name for blocked.
	if err := ddl.EnsureTable(context.Background(), replicaConn, p, blocked, 1, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view.set(chainInc(t, 1, blocked, wire.TableStatusPending), chainInc(t, 2, fine, wire.TableStatusPending))
	mustReconcile(t, r)
	if r.Ready(blocked.TableID) || !r.Ready(fine.TableID) {
		t.Fatalf("ready blocked=%v fine=%v", r.Ready(blocked.TableID), r.Ready(fine.TableID))
	}
	mustReconcile(t, r)
	if st := r.Stats(); st.Failures[blocked.TableID] != 1 {
		t.Fatalf("a backed-off table must not be retried by the next pass: %+v", st)
	}
	r.Trigger()
	mustReconcile(t, r)
	if st := r.Stats(); st.Failures[blocked.TableID] != 2 {
		t.Fatalf("Trigger clears the backoff: %+v", st)
	}
}

// R-F4: WaitReady wakes the reconcile loop but keeps every per-table backoff
// (P9); only Trigger clears it.
func TestReconciler_WaitReadyWakesWithoutClearingBackoff(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view := newFakeView()
	arb := &fakeArbiter{err: status.Error(codes.Unavailable, "arbiter down")}
	r := newReconciler(t, conn, p, view, arb, nil)
	pinClock(r)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if st := r.Stats(); st.Failures[s.TableID] != 1 {
		t.Fatalf("stats = %+v", st)
	}
	_, wake := r.Wake()
	if r.WaitReady(context.Background(), []string{s.TableID}, 20*time.Millisecond) {
		t.Fatal("a Purging table is never ready")
	}
	select {
	case <-wake:
	default:
		t.Fatal("WaitReady must wake the reconcile loop")
	}
	mustReconcile(t, r)
	if st := r.Stats(); st.Failures[s.TableID] != 1 || len(arb.reported()) != 1 {
		t.Fatalf("WaitReady cleared the backoff: %+v, reports %v", st, arb.reported())
	}
	r.Trigger()
	select {
	case <-wake:
	default:
		t.Fatal("Trigger must wake the reconcile loop")
	}
	mustReconcile(t, r)
	if st := r.Stats(); st.Failures[s.TableID] != 2 || len(arb.reported()) != 2 {
		t.Fatalf("Trigger must clear the backoff: %+v, reports %v", st, arb.reported())
	}
}

func TestReconciler_UnimplementedPurgeIsRetriedNotFatal(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view := newFakeView()
	arb := &fakeArbiter{err: status.Error(codes.Unimplemented, "unknown method SubmitTablePurged")}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, r)
	if st := r.Stats(); st.States[StatePurging] != 1 || st.Failures[s.TableID] != 1 {
		t.Fatalf("stats = %+v", st)
	}
}

func TestReconciler_ReadyNeverCountsAnEarlierIncarnation(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, r)
	view.set(chainInc(t, 1, s, wire.TableStatusPurged), chainInc(t, 2, s, wire.TableStatusPending))
	if r.Ready(s.TableID) {
		t.Fatal("incarnation 1's tables must not make incarnation 2 ready")
	}
	mustReconcile(t, r)
	if !r.Ready(s.TableID) {
		t.Fatal("incarnation 2 not ready after its pass")
	}
}

func TestReconciler_RegistryDisabledReconcilesTheGenesisSetOnly(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	g, stray := schemaFor(t, "g"), schemaFor(t, "stray")
	if err := ddl.EnsureTable(context.Background(), conn, p, stray, 4, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.Genesis = []payloadexec.TableSchema{g} })
	if err := r.Reconcile(context.Background(), ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	if !r.Ready(g.TableID) || len(localComments(t, conn, p, stray.TableID)) != 3 {
		t.Fatal("disabled registry: genesis created, nothing dropped")
	}
	for db, c := range localComments(t, conn, p, g.TableID) {
		if c != "" {
			t.Fatalf("genesis %s carries comment %q", db, c)
		}
	}
	// Enabled: the genesis table is a genesis-origin Active incarnation and
	// keeps its unmarked tables and its configured mode.
	view.set(wire.TableIncarnation{Seq: 1, DatabaseID: "db", TableID: g.TableID[3:], Origin: wire.TableOriginGenesis,
		Status: wire.TableStatusActive, SchemaHash: payloadexec.TableSchemaHash(testNetwork, g)})
	mustReconcile(t, r)
	if !r.Ready(g.TableID) {
		t.Fatal("genesis table not ready under the enabled registry")
	}
}

// R-F7b: tables verified while the registry was disabled are recorded as the
// unmarked genesis incarnation (seq 0). Enabling the registry with that key's
// genesis incarnation live must not make them not-Ready until the next pass,
// while a later chain recreation of the key never counts them.
func TestReconciler_GenesisStaysReadyWhenTheRegistryIsEnabled(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	g := schemaFor(t, "g")
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.Genesis = []payloadexec.TableSchema{g} })
	if err := r.Reconcile(context.Background(), ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	if !r.Ready(g.TableID) {
		t.Fatal("genesis table not ready while the registry is disabled")
	}
	view.set(genesisInc(t, 1, g, wire.TableStatusActive))
	if !r.Ready(g.TableID) {
		t.Fatal("enabling the registry made a verified genesis table not ready before the next pass")
	}
	if !r.WaitReady(context.Background(), []string{g.TableID}, time.Second) {
		t.Fatal("WaitReady must see the genesis table ready without a pass")
	}
	view.set(genesisInc(t, 1, g, wire.TableStatusRetiring))
	if !r.Ready(g.TableID) {
		t.Fatal("a Retiring genesis incarnation still has its tables")
	}
	view.set(genesisInc(t, 1, g, wire.TableStatusPurging))
	if r.Ready(g.TableID) {
		t.Fatal("a Purging genesis incarnation is not ready")
	}
	view.set(genesisInc(t, 1, g, wire.TableStatusPurged), chainInc(t, 2, g, wire.TableStatusPending))
	if r.Ready(g.TableID) {
		t.Fatal("the genesis tables must not make a chain recreation of the key ready")
	}
}

// D2 naming is not injective across every key the registry ever saw: a Legacy
// key "x__y.z" and a live key "x.y__z" share the physical name x__y__z. The
// live key's tables must never be dropped as the Legacy key's leftovers.
func TestReconciler_NeverDropsATableALiveKeyOwns(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := suffix(t)
	live := payloadexec.TableSchema{TableID: "x.y__z_" + s, Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	legacy := wire.TableIncarnation{Seq: 1, DatabaseID: "x__y", TableID: "z_" + s, Origin: wire.TableOriginLegacy, Status: wire.TableStatusLegacy}
	if ddl.CHTableName(legacy.Key()) != ddl.CHTableName(live.TableID) {
		t.Fatal("fixture: the two keys must share a physical name")
	}
	view.set(legacy, chainInc(t, 2, live, wire.TableStatusActive))
	mustReconcile(t, r)
	mustReconcile(t, r)
	if got := localComments(t, conn, p, live.TableID); len(got) != 3 || !r.Ready(live.TableID) {
		t.Fatalf("tables %v ready %v: the live key's tables must survive", got, r.Ready(live.TableID))
	}
	if st := r.Stats(); st.LeftoverDrops != 0 {
		t.Fatalf("stats = %+v", st)
	}
}

// Fix round 1, item 1: a backoff that has expired must not survive a
// non-failing transition, or NextDelay stays 0 and the loop spins.
func TestReconciler_ExpiredBackoffDoesNotPinNextDelay(t *testing.T) {
	t.Run("quiescence error then not quiescent", func(t *testing.T) {
		conn := requireCH(t)
		p := testPinned(t, conn)
		view, arb := newFakeView(), &fakeArbiter{}
		var quietErr error = errors.New("promotion state unavailable")
		r := newReconciler(t, conn, p, view, arb, func(_ *Config, d *Deps) {
			d.Quiescent = func(string) (bool, error) { return false, quietErr }
		})
		clock := pinClock(r)
		s := schemaFor(t, "t")
		view.set(chainInc(t, 1, s, wire.TableStatusPurging))
		mustReconcile(t, r)
		if st := r.Stats(); st.Failures[s.TableID] != 1 || st.States[StateWaitingQuiescence] != 1 {
			t.Fatalf("stats = %+v", st)
		}
		quietErr = nil
		clock.advance(time.Minute)
		mustReconcile(t, r)
		if st := r.Stats(); st.States[StateWaitingQuiescence] != 1 || st.Failures[s.TableID] != 1 {
			t.Fatalf("stats = %+v", st)
		}
		clock.advance(time.Minute)
		if got := r.NextDelay(); got != ddl.DefaultReconcileInterval {
			t.Fatalf("NextDelay = %v, want the interval %v", got, ddl.DefaultReconcileInterval)
		}
	})
	t.Run("failed purge report then purged", func(t *testing.T) {
		conn := requireCH(t)
		p := testPinned(t, conn)
		view := newFakeView()
		// The report committed but the client saw an error.
		arb := &fakeArbiter{err: status.Error(codes.Unavailable, "connection reset")}
		r := newReconciler(t, conn, p, view, arb, nil)
		clock := pinClock(r)
		s := schemaFor(t, "t")
		view.set(chainInc(t, 1, s, wire.TableStatusPurging))
		mustReconcile(t, r)
		if st := r.Stats(); st.Failures[s.TableID] != 1 {
			t.Fatalf("stats = %+v", st)
		}
		view.set(chainInc(t, 1, s, wire.TableStatusPurged))
		mustReconcile(t, r)
		clock.advance(time.Minute)
		if got := r.NextDelay(); got != ddl.DefaultReconcileInterval {
			t.Fatalf("NextDelay = %v, want the interval %v", got, ddl.DefaultReconcileInterval)
		}
		if st := r.Stats(); stateCount(st) != 0 {
			t.Fatalf("a retired key must not count in States: %+v", st)
		}
	})
}

// Fix round 1, item 2: a Purging key never drops a physical name that a
// present key desires (D2 collision), and does not report the purge.
func TestReconciler_PurgeNeverDropsAPhysicalNameAPresentKeyDesires(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	s := suffix(t)
	live := payloadexec.TableSchema{TableID: "x.y__z_" + s, Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	colliding := payloadexec.TableSchema{TableID: "x__y.z_" + s, Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	if ddl.CHTableName(live.TableID) != ddl.CHTableName(colliding.TableID) {
		t.Fatal("fixture: the two keys must share a physical name")
	}
	view.set(chainInc(t, 1, colliding, wire.TableStatusPurging), chainInc(t, 2, live, wire.TableStatusActive))
	mustReconcile(t, r)
	mustReconcile(t, r)
	got := localComments(t, conn, p, live.TableID)
	if len(got) != 3 || !r.Ready(live.TableID) {
		t.Fatalf("tables %v ready %v: the present key's tables must survive", got, r.Ready(live.TableID))
	}
	for db, c := range got {
		if c != "hg_incarnation=2" {
			t.Fatalf("%s comment = %q", db, c)
		}
	}
	if reports := arb.reported(); len(reports) != 0 {
		t.Fatalf("a purge whose drop was skipped must not be reported: %v", reports)
	}
	if !keeperPathExists(t, conn, p, live.TableID) {
		t.Fatal("the present key's Keeper path must survive")
	}
}

// Fix round 1, item 3: a chain key whose schema cannot be decoded backs off
// instead of failing on every pass.
func TestReconciler_UndecodableChainSchemaBacksOff(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	clock := pinClock(r)
	inc := chainInc(t, 1, schemaFor(t, "t"), wire.TableStatusPending)
	inc.SchemaJSON = "{not json"
	view.set(inc)
	for range 3 {
		mustReconcile(t, r)
	}
	if st := r.Stats(); st.Failures[inc.Key()] != 1 {
		t.Fatalf("a backed-off undecodable schema must not fail on every pass: %+v", st)
	}
	clock.advance(time.Minute)
	mustReconcile(t, r)
	if st := r.Stats(); st.Failures[inc.Key()] != 2 {
		t.Fatalf("an expired backoff retries: %+v", st)
	}
}

// Fix round 1, item 4: a local table whose marker names a retired (Purged or
// Refused) incarnation of another key with the same D2 physical name is a
// leftover of that key (a node evicted before the purge that rejoined), so
// it is dropped before the create instead of being fatal drift. A marker of
// a live incarnation of another key stays drift and is never dropped.
func TestReconciler_DropsALeftoverOfARetiredCollidingKey(t *testing.T) {
	for _, st := range []wire.TableIncarnationStatus{wire.TableStatusPurged, wire.TableStatusRefused} {
		t.Run(string(st), func(t *testing.T) {
			conn := requireCH(t)
			p := testPinned(t, conn)
			view, arb := newFakeView(), &fakeArbiter{}
			r := newReconciler(t, conn, p, view, arb, nil)
			s := suffix(t)
			old := payloadexec.TableSchema{TableID: "a__b.c_" + s, Columns: []lthash.Column{{Name: "w", Type: "String"}}}
			next := payloadexec.TableSchema{TableID: "a.b__c_" + s, Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
			if ddl.CHTableName(old.TableID) != ddl.CHTableName(next.TableID) {
				t.Fatal("fixture: the two keys must share a physical name")
			}
			if err := ddl.EnsureTable(context.Background(), conn, p, old, 1, ddl.ModeCreateAndVerify); err != nil {
				t.Fatal(err)
			}
			view.set(chainInc(t, 1, old, st), chainInc(t, 2, next, wire.TableStatusPending))
			mustReconcile(t, r)
			for db, c := range localComments(t, conn, p, next.TableID) {
				if c != "hg_incarnation=2" {
					t.Fatalf("%s comment = %q, want the new key's incarnation", db, c)
				}
			}
			if stats := r.Stats(); stats.LeftoverDrops != 1 || !r.Ready(next.TableID) {
				t.Fatalf("stats = %+v ready=%v", stats, r.Ready(next.TableID))
			}
		})
	}
	t.Run("live colliding incarnation is drift", func(t *testing.T) {
		conn := requireCH(t)
		p := testPinned(t, conn)
		view, arb := newFakeView(), &fakeArbiter{}
		r := newReconciler(t, conn, p, view, arb, nil)
		s := suffix(t)
		next := payloadexec.TableSchema{TableID: "a.b__c_" + s, Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
		legacy := wire.TableIncarnation{Seq: 1, DatabaseID: "a__b", TableID: "c_" + s, Origin: wire.TableOriginLegacy, Status: wire.TableStatusLegacy}
		if err := ddl.EnsureTable(context.Background(), conn, p, next, 1, ddl.ModeCreateAndVerify); err != nil {
			t.Fatal(err)
		}
		view.set(legacy, chainInc(t, 2, next, wire.TableStatusPending))
		if err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly); !errors.Is(err, ddl.ErrProtocolTableDrift) {
			t.Fatalf("err = %v, want drift", err)
		}
		for db, c := range localComments(t, conn, p, next.TableID) {
			if c != "hg_incarnation=1" {
				t.Fatalf("%s comment = %q: a live incarnation's table must never be dropped", db, c)
			}
		}
	})
}
