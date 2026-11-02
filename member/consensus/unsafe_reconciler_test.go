package consensus

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

var poolsSchema = payloadexec.TableSchema{TableID: "dex.pools", Columns: []lthash.Column{{Name: "id", Type: "String"}}}

const createDatabase = "CREATE DATABASE IF NOT EXISTS `hg_member_unsafe`"

type reconcilerHarness struct {
	r     *UnsafeReconciler
	conn  *chFake
	view  *viewFake
	arb   *arbiterFake
	dir   string
	logs  *syncBuffer
	shard uint32
}

// newReconciler is member-b's reconciler over the fakes; the purge node set
// lists member-b.
func newReconciler(t *testing.T, create bool, shard uint32, snap wire.TableRegistrySnapshot) *reconcilerHarness {
	t.Helper()
	h := &reconcilerHarness{conn: newCHFake(), view: newViewFake(snap), arb: &arbiterFake{nodes: []string{"snode-1", testNodeID}},
		dir: t.TempDir(), logs: &syncBuffer{}, shard: shard}
	r, err := NewUnsafeReconciler(ReconcilerConfig{NodeID: testNodeID, StateDir: h.dir, KeeperShardID: shard, CreateReplicas: create, NetworkID: testNetworkID},
		ReconcilerDeps{DDLConn: h.conn, Registry: h.view, Arbiter: h.arb, Logger: textLogger(h.logs)})
	if err != nil {
		t.Fatal(err)
	}
	h.r = r
	return h
}

// createSQL is the CREATE TABLE of member-b's replica of inc at path.
func createSQL(t *testing.T, schema payloadexec.TableSchema, seq uint64, path string) string {
	t.Helper()
	intent, err := ddl.MemberUnsafeIntent(ddl.DefaultMemberPinned(testNodeID, 0, ""), schema, seq, path)
	if err != nil {
		t.Fatal(err)
	}
	return intent.SQL()
}

func (h *reconcilerHarness) pass(t *testing.T) error {
	t.Helper()
	return h.r.reconcile(context.Background())
}

func (h *reconcilerHarness) mustPass(t *testing.T) {
	t.Helper()
	if err := h.pass(t); err != nil {
		t.Fatal(err)
	}
}

// noSystemStatement fails if any statement the reconciler issued is a
// SYSTEM statement (SYSTEM DROP REPLICA above all).
func (h *reconcilerHarness) noSystemStatement(t *testing.T) {
	t.Helper()
	for _, s := range h.conn.statements() {
		if strings.HasPrefix(strings.ToUpper(strings.TrimSpace(s)), "SYSTEM") {
			t.Fatalf("the reconciler issued %q", s)
		}
	}
}

func writeLedger(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, QuiescenceFile), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestUnsafeReconcilerCreatesReplicasOnlyWhenRegisteredPurgeOnAndPlaced(t *testing.T) {
	path := "/sentio/0/unsafe_inc/dex__swaps/3"
	for _, tc := range []struct {
		name     string
		create   bool
		purge    bool
		nodes    []string
		members  []string
		leaving  []wire.LeavingMember
		status   wire.TableIncarnationStatus
		want     bool
		askedSet bool
	}{
		{"registered, purge on, placed, active", true, true, []string{testNodeID}, []string{testNodeID}, nil, wire.TableStatusActive, true, true},
		{"pending is created too", true, true, []string{testNodeID}, []string{testNodeID}, nil, wire.TableStatusPending, true, true},
		{"an unreleased leaving record still places it", true, true, []string{testNodeID}, []string{"member-c"},
			[]wire.LeavingMember{{NodeID: testNodeID, RemovedGeneration: 2}}, wire.TableStatusActive, true, true},
		{"CreateReplicas off", false, true, []string{testNodeID}, []string{testNodeID}, nil, wire.TableStatusActive, false, false},
		{"not registered", true, true, []string{"snode-1"}, []string{testNodeID}, nil, wire.TableStatusActive, false, true},
		{"purge off", true, false, []string{testNodeID}, []string{testNodeID}, nil, wire.TableStatusActive, false, false},
		{"not placed", true, true, []string{testNodeID}, []string{"member-c"}, nil, wire.TableStatusActive, false, true},
		{"no group", true, true, []string{testNodeID}, nil, nil, wire.TableStatusActive, false, true},
		{"retiring is never created", true, true, []string{testNodeID}, []string{testNodeID}, nil, wire.TableStatusRetiring, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newReconciler(t, tc.create, 0, groupSnapshot(tc.purge, tc.members, tc.leaving, chainIncarnation(t, 3, swapsSchema, tc.status, true)))
			h.arb.nodes = tc.nodes
			h.mustPass(t)
			writes := h.conn.writes()
			if tc.want {
				if !slices.Equal(writes, []string{createDatabase, createSQL(t, swapsSchema, 3, path)}) {
					t.Fatalf("writes = %q, want the database and member-b's replica at %s", writes, path)
				}
				if got := h.r.Stats().Replicas; got != 1 {
					t.Fatalf("Replicas = %d, want 1", got)
				}
			} else if len(writes) != 0 || h.r.Stats().Replicas != 0 {
				t.Fatalf("writes = %q, Replicas %d: want none", writes, h.r.Stats().Replicas)
			}
			if asked := h.arb.calls > 0; asked != tc.askedSet {
				t.Fatalf("purge node set asked %v, want %v", asked, tc.askedSet)
			}
			if h.arb.reports != 0 {
				t.Fatal("the reconciler reported a purge")
			}
		})
	}
}

func TestUnsafeReconcilerUsesTheIncarnationKeeperPath(t *testing.T) {
	scoped := chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true)
	unscoped := chainIncarnation(t, 2, poolsSchema, wire.TableStatusActive, false)
	h := newReconciler(t, true, 4, groupSnapshot(true, []string{testNodeID}, nil, unscoped, scoped))
	h.mustPass(t)
	poolsIntent, err := ddl.MemberUnsafeIntent(ddl.DefaultMemberPinned(testNodeID, 4, ""), poolsSchema, 2, "/sentio/4/unsafe/dex__pools")
	if err != nil {
		t.Fatal(err)
	}
	swapsIntent, err := ddl.MemberUnsafeIntent(ddl.DefaultMemberPinned(testNodeID, 4, ""), swapsSchema, 3, "/sentio/4/unsafe_inc/dex__swaps/3")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{createDatabase, poolsIntent.SQL(), createDatabase, swapsIntent.SQL()}
	if got := h.conn.writes(); !slices.Equal(got, want) {
		t.Fatalf("writes = %q, want %q", got, want)
	}
	if want := ddl.ZooKeeperPathFor(4, "dex.pools", ddl.KeeperScope{IncarnationSeq: 2}); want != "/sentio/4/unsafe/dex__pools" {
		t.Fatalf("the unscoped path is %s", want)
	}
	// A second pass finds both replicas at their paths and changes nothing.
	h.mustPass(t)
	if got := h.conn.writes(); len(got) != 4 || h.r.Stats().Replicas != 2 {
		t.Fatalf("a second pass wrote %q (Replicas %d)", got[4:], h.r.Stats().Replicas)
	}
}

func TestUnsafeReconcilerStopsOnReplicaAlreadyExistsAndNeverDropsAReplica(t *testing.T) {
	path := "/sentio/0/unsafe_inc/dex__swaps/3"
	h := newReconciler(t, true, 0, groupSnapshot(true, []string{testNodeID}, nil,
		chainIncarnation(t, 2, poolsSchema, wire.TableStatusActive, true), chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true)))
	h.conn.createErr["dex__swaps"] = &clickhouse.Exception{Code: 253, Name: "DB::Exception",
		Message: "Replica /sentio/0/unsafe_inc/dex__swaps/3/replicas/member-b already exists. (REPLICA_ALREADY_EXISTS)"}
	const text = "member consensus: hg_member_unsafe.dex__swaps: replica member-b already exists at /sentio/0/unsafe_inc/dex__swaps/3; not dropping it (si_member_ddl holds no SYSTEM DROP REPLICA); the owner's sweep removes it after an eviction"
	err := h.pass(t)
	if !errors.Is(err, ErrReplicaAlreadyExists) || err.Error() != text {
		t.Fatalf("pass = %v, want ErrReplicaAlreadyExists with %q", err, text)
	}
	st := h.r.Stats()
	if st.ReplicaAlreadyExists != 1 || st.Failures["dex.swaps"] != 1 || st.Replicas != 1 || !h.conn.has(ddl.MemberUnsafeDatabase, "dex__pools") {
		t.Fatalf("stats %+v: want one stop, one failure of dex.swaps and dex.pools created", st)
	}
	if !strings.Contains(h.logs.String(), "level=ERROR msg=\""+text+"\"") {
		t.Fatalf("logs = %s", h.logs.String())
	}
	// The table stays stopped: no second CREATE, no drop of anything.
	creates := 0
	if err := h.pass(t); err != nil {
		t.Fatalf("a second pass = %v", err)
	}
	for _, s := range h.conn.writes() {
		if s == createSQL(t, swapsSchema, 3, path) {
			creates++
		}
		if strings.HasPrefix(s, "DROP") {
			t.Fatalf("the reconciler dropped %q", s)
		}
	}
	if creates != 1 || h.r.Stats().ReplicaAlreadyExists != 1 {
		t.Fatalf("%d CREATEs of dex__swaps after a stop, %d stops; want 1 and 1", creates, h.r.Stats().ReplicaAlreadyExists)
	}
	h.noSystemStatement(t)
}

func TestUnsafeReconcilerKeepsTheReplicaThroughRetiringAndDropsItAtPurgingWhenQuiet(t *testing.T) {
	inc := chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true)
	h := newReconciler(t, true, 0, groupSnapshot(true, []string{testNodeID}, nil, inc))
	h.mustPass(t)
	if !h.conn.has(ddl.MemberUnsafeDatabase, "dex__swaps") {
		t.Fatal("the active replica was not created")
	}
	written := len(h.conn.writes())
	inc.Status = wire.TableStatusRetiring
	h.view.set(groupSnapshot(true, []string{testNodeID}, nil, inc))
	h.mustPass(t)
	if got := h.conn.writes(); len(got) != written || h.r.Stats().Replicas != 1 {
		t.Fatalf("a retiring incarnation's replica was touched: %q", got[written:])
	}
	inc.Status = wire.TableStatusPurging
	h.view.set(groupSnapshot(true, []string{testNodeID}, nil, inc))
	// An unresolved block still names the table: keep it.
	writeLedger(t, h.dir, `{"version":1,"blocks":[{"block_seq":9,"tables":[{"table_id":"dex.swaps","incarnation_seq":3}]}]}`)
	h.mustPass(t)
	if got := h.conn.writes(); len(got) != written || h.r.Stats().Replicas != 1 {
		t.Fatalf("a purging replica named by an unresolved block was dropped: %q", got[written:])
	}
	// A ledger that cannot be read keeps it too.
	writeLedger(t, h.dir, `{`)
	if err := h.pass(t); err == nil || !strings.Contains(err.Error(), "quiescence.json is corrupt") {
		t.Fatalf("a corrupt ledger gave %v", err)
	}
	if !h.conn.has(ddl.MemberUnsafeDatabase, "dex__swaps") {
		t.Fatal("a purging replica was dropped without a readable ledger")
	}
	// Quiet: drop it, with DROP TABLE SYNC and nothing else.
	writeLedger(t, h.dir, `{"version":1,"blocks":[]}`)
	h.mustPass(t)
	want := "DROP TABLE IF EXISTS `hg_member_unsafe`.`dex__swaps` SYNC"
	if got := h.conn.writes(); len(got) != written+1 || got[written] != want || h.r.Stats().Replicas != 0 {
		t.Fatalf("writes after quiescence = %q, want only %q", got[written:], want)
	}
	h.mustPass(t)
	if got := h.conn.writes(); len(got) != written+1 {
		t.Fatalf("a purging incarnation was created again: %q", got[written+1:])
	}
	h.noSystemStatement(t)
}

func TestUnsafeReconcilerDropsAReleasedLeavingMembersReplicas(t *testing.T) {
	swaps := chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true)
	pools := chainIncarnation(t, 2, poolsSchema, wire.TableStatusActive, true)
	leaving := []wire.LeavingMember{{NodeID: testNodeID, RemovedGeneration: 2}}
	h := newReconciler(t, true, 0, groupSnapshot(true, []string{"member-c"}, leaving, pools, swaps))
	h.mustPass(t)
	if h.r.Stats().Replicas != 2 {
		t.Fatalf("an unreleased Leaving member holds %d replicas, want 2", h.r.Stats().Replicas)
	}
	written := len(h.conn.writes())
	leaving[0].Released = true
	h.view.set(groupSnapshot(true, []string{"member-c"}, leaving, pools, swaps))
	writeLedger(t, h.dir, `{"version":1,"blocks":[{"block_seq":9,"tables":[{"table_id":"dex.swaps","incarnation_seq":3}]}]}`)
	h.mustPass(t)
	want := []string{"DROP TABLE IF EXISTS `hg_member_unsafe`.`dex__pools` SYNC"}
	if got := h.conn.writes()[written:]; !slices.Equal(got, want) || h.r.Stats().Replicas != 1 {
		t.Fatalf("released with swaps unresolved: writes %q, want %q", got, want)
	}
	writeLedger(t, h.dir, `{"version":1,"blocks":[]}`)
	h.mustPass(t)
	want = append(want, "DROP TABLE IF EXISTS `hg_member_unsafe`.`dex__swaps` SYNC")
	if got := h.conn.writes()[written:]; !slices.Equal(got, want) || h.r.Stats().Replicas != 0 {
		t.Fatalf("released and quiet: writes %q, want %q", got, want)
	}
	h.mustPass(t)
	if got := h.conn.writes()[written:]; len(got) != 2 {
		t.Fatalf("a released member created a replica again: %q", got[2:])
	}
	h.noSystemStatement(t)
}

func TestUnsafeReconcilerLeavesATableItCannotAttribute(t *testing.T) {
	inc := chainIncarnation(t, 3, swapsSchema, wire.TableStatusPurging, true)
	h := newReconciler(t, true, 0, groupSnapshot(true, []string{testNodeID}, nil, inc))
	// A replicated root above the incarnation path, a replica named after
	// another node, and a table without an incarnation comment.
	h.conn.addRaw(ddl.MemberUnsafeDatabase, "dex__swaps", "hg_incarnation=3", "ReplicatedMergeTree('/sentio/0/unsafe_inc/dex__swaps', 'member-b') ORDER BY _hg_row_id")
	h.conn.addRaw(ddl.MemberUnsafeDatabase, "dex__trades", "", "ReplicatedMergeTree('/sentio/0/unsafe/dex__trades', 'member-b') ORDER BY _hg_row_id")
	h.mustPass(t)
	h.mustPass(t)
	if got := h.conn.writes(); len(got) != 0 {
		t.Fatalf("the reconciler touched tables it cannot attribute: %q", got)
	}
	if n := strings.Count(h.logs.String(), "hg_member_unsafe table is not this member's replica of a known incarnation at its Keeper path; leaving it for an operator"); n != 2 {
		t.Fatalf("%d warnings over two passes, want one per table: %s", n, h.logs.String())
	}
}

func TestUnsafeReconcilerRecreatesASameNameTableAfterThePurge(t *testing.T) {
	old := chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, false)
	h := newReconciler(t, true, 0, groupSnapshot(true, []string{testNodeID}, nil, old))
	h.mustPass(t)
	old.Status = wire.TableStatusPurged
	recreated := chainIncarnation(t, 5, swapsSchema, wire.TableStatusPending, true)
	h.view.set(groupSnapshot(true, []string{testNodeID}, nil, old, recreated))
	h.mustPass(t)
	want := []string{createDatabase, createSQL(t, swapsSchema, 3, "/sentio/0/unsafe/dex__swaps"),
		"DROP TABLE IF EXISTS `hg_member_unsafe`.`dex__swaps` SYNC", createDatabase, createSQL(t, swapsSchema, 5, "/sentio/0/unsafe_inc/dex__swaps/5")}
	if got := h.conn.writes(); !slices.Equal(got, want) || h.r.Stats().Replicas != 1 {
		t.Fatalf("writes = %q, want %q", got, want)
	}
}

func TestUnsafeReconcilerRunsAtEveryRegistryChange(t *testing.T) {
	inc := chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true)
	h := newReconciler(t, true, 0, groupSnapshot(false, []string{testNodeID}, nil, inc))
	h.r.cfg.Interval = time.Hour
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- h.r.Run(ctx) }()
	time.Sleep(50 * time.Millisecond)
	if h.conn.has(ddl.MemberUnsafeDatabase, "dex__swaps") {
		t.Fatal("a replica was created with the purge switch off")
	}
	h.view.set(groupSnapshot(true, []string{testNodeID}, nil, inc))
	deadline := time.Now().Add(5 * time.Second)
	for !h.conn.has(ddl.MemberUnsafeDatabase, "dex__swaps") {
		if time.Now().After(deadline) {
			t.Fatal("the purge switch-on did not wake the reconciler")
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v", err)
	}
}

func TestNewUnsafeReconcilerRefusesAnIncompleteConfig(t *testing.T) {
	good := ReconcilerConfig{NodeID: testNodeID, StateDir: t.TempDir(), NetworkID: testNetworkID}
	deps := ReconcilerDeps{DDLConn: newCHFake(), Registry: newViewFake(wire.TableRegistrySnapshot{}), Arbiter: &arbiterFake{}}
	for _, tc := range []struct {
		name string
		edit func(*ReconcilerConfig, *ReconcilerDeps)
		want string
	}{
		{"no network id", func(c *ReconcilerConfig, _ *ReconcilerDeps) { c.NetworkID = "" }, "member consensus: network id is required"},
		{"no state dir", func(c *ReconcilerConfig, _ *ReconcilerDeps) { c.StateDir = "" }, "member consensus: state dir is required"},
		{"a bad storage policy", func(c *ReconcilerConfig, _ *ReconcilerDeps) { c.StoragePolicy = "member disk" },
			`member consensus: ddl: MemberPinned.StoragePolicy "member disk" must be a ClickHouse storage policy name ([A-Za-z0-9_]+)`},
		{"a negative interval", func(c *ReconcilerConfig, _ *ReconcilerDeps) { c.Interval = -time.Second }, "member consensus: reconcile interval must not be negative"},
		{"no arbiter", func(_ *ReconcilerConfig, d *ReconcilerDeps) { d.Arbiter = nil },
			"member consensus: the reconciler needs its si_member_ddl connection, the registry and the arbiter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, d := good, deps
			tc.edit(&cfg, &d)
			if _, err := NewUnsafeReconciler(cfg, d); err == nil || err.Error() != tc.want {
				t.Fatalf("NewUnsafeReconciler = %v, want %q", err, tc.want)
			}
		})
	}
	r, err := NewUnsafeReconciler(good, deps)
	if err != nil || r.cfg.Interval != ddl.DefaultReconcileInterval {
		t.Fatalf("NewUnsafeReconciler = %v with interval %v, want the default", err, r.cfg.Interval)
	}
}
