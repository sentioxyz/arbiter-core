package tableset

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// syncBuffer is a log sink the reconciler may write from any goroutine.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// auditedOwner is the owner SNode's reconciler (SweepDecommissioned) with a
// pinned clock and its log captured.
func auditedOwner(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, view *fakeView, arb *fakeArbiter) (*Reconciler, *testClock, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	r := newReconciler(t, conn, p, view, arb, func(c *Config, d *Deps) {
		c.SweepDecommissioned = true
		d.Logger = slog.New(slog.NewTextHandler(logs, nil))
	})
	return r, pinClock(r), logs
}

// strayRoot creates a replicated table at path on conn, in a database of the
// test's own.
func strayRoot(t *testing.T, conn clickhouse.Conn, database, table, path string) {
	t.Helper()
	ctx := context.Background()
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+database); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" SYNC") })
	if err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s.%s (x UInt64) ENGINE = ReplicatedMergeTree('%s', 'stray') ORDER BY x", database, table, path)); err != nil {
		t.Fatal(err)
	}
}

func TestOwnerKeeperAuditReportsAnUnknownReplica(t *testing.T) {
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := isolatedPinned(t, conn, replicaConn)
	legacy, scoped := schemaFor(t, "legacy"), schemaFor(t, "scoped")
	scopedScope := ddl.KeeperScope{IncarnationSeq: 2, Scoped: true}
	owner := uint64(0)
	legacyInc := chainInc(t, 1, legacy, wire.TableStatusActive)
	scopedIncarnation := scopedInc(t, 2, scoped, wire.TableStatusActive)
	legacyInc.OwnerIndexerID, scopedIncarnation.OwnerIndexerID = &owner, &owner
	scopedIncarnation.PurgeMembers = []string{"member-c"}
	// The purge node set lists every registered node: the owner, the pool
	// verifier and each registered member, member-x of another database's
	// group among them.
	view, arb := newFakeView(), &fakeArbiter{nodeSet: []string{p.NodeID, "member-b", "member-c", "member-old", "member-x", "verifier-1"}}
	view.setSnap(wire.TableRegistrySnapshot{
		SIIndexers:   []arbiter.SIIndexerEntry{{IndexerID: 0, SNodeNodeID: p.NodeID}},
		Incarnations: []wire.TableIncarnation{legacyInc, scopedIncarnation},
		Groups: []wire.DatabaseGroup{
			{OwnerIndexerID: 0, DatabaseID: "db", Members: []wire.GroupMember{{NodeID: "member-b"}}, PastMembers: []string{"member-old"}},
			{OwnerIndexerID: 0, DatabaseID: "other", Members: []wire.GroupMember{{NodeID: "member-x"}}},
		},
	})
	source, clock, logs := auditedOwner(t, conn, p, view, arb)
	mustReconcile(t, source)
	if st := source.Stats(); st.KeeperAuditRuns != 1 || len(st.KeeperFindings) != 0 || !source.Ready(legacy.TableID) || !source.Ready(scoped.TableID) {
		t.Fatalf("the first pass audits only the owner's own replicas: %+v", st)
	}
	// A pool verifier, a group member, a past member, the scoped
	// incarnation's purge member, a registered member of another database's
	// group and an intruder each hold a replica of both tables on the second
	// server.
	for _, name := range []string{"verifier-1", "member-b", "member-old", "member-c", "member-x", "intruder"} {
		q := peer(t, replicaConn, p, name)
		replicaOf(t, replicaConn, q, legacy, ddl.KeeperScope{IncarnationSeq: 1}, false)
		replicaOf(t, replicaConn, q, scoped, scopedScope, false)
	}
	mustReconcile(t, source)
	if runs := source.Stats().KeeperAuditRuns; runs != 1 {
		t.Fatalf("audit runs = %d before the interval passed, want 1", runs)
	}
	clock.advance(DefaultKeeperAuditInterval)
	mustReconcile(t, source)
	legacyPath := ddl.ZooKeeperPathFor(p.KeeperShardID, legacy.TableID, ddl.KeeperScope{})
	scopedPath := ddl.ZooKeeperPathFor(p.KeeperShardID, scoped.TableID, scopedScope)
	want := []KeeperFinding{
		{Path: legacyPath, Replica: "intruder", Reason: KeeperFindingUnknownReplica},
		{Path: legacyPath, Replica: "member-c", Reason: KeeperFindingUnknownReplica},
		{Path: legacyPath, Replica: "member-x", Reason: KeeperFindingUnknownReplica},
		{Path: scopedPath, Replica: "intruder", Reason: KeeperFindingUnknownReplica},
		{Path: scopedPath, Replica: "member-x", Reason: KeeperFindingUnknownReplica},
	}
	st := source.Stats()
	if st.KeeperAuditRuns != 2 || !reflect.DeepEqual(st.KeeperFindings, want) {
		t.Fatalf("audit runs %d, findings %+v; want 2 and %+v", st.KeeperAuditRuns, st.KeeperFindings, want)
	}
	line := "keeper replica on an owner path is not the owner, a pool verifier or a current or past member"
	if n := strings.Count(logs.String(), line); n != 5 {
		t.Fatalf("logged %d unknown-replica warnings, want 5:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "table="+scoped.TableID+" path="+scopedPath+" replica=intruder") {
		t.Fatalf("the warning names the table, path and replica:\n%s", logs.String())
	}
	clock.advance(DefaultKeeperAuditInterval)
	mustReconcile(t, source)
	if st := source.Stats(); st.KeeperAuditRuns != 3 || !reflect.DeepEqual(st.KeeperFindings, want) || strings.Count(logs.String(), line) != 5 {
		t.Fatalf("a repeated finding is kept and not logged again: %+v\n%s", st, logs.String())
	}
	nonOwner := newReconciler(t, conn, p, view, arb, nil)
	mustReconcile(t, nonOwner)
	if st := nonOwner.Stats(); st.KeeperAuditRuns != 0 || st.KeeperFindings != nil {
		t.Fatalf("only the owner (SweepDecommissioned) audits: %+v", st)
	}
}

func TestOwnerKeeperAuditReportsANonIncarnationRoot(t *testing.T) {
	conn := requireCH(t)
	p := isolatedPinned(t, conn)
	scoped, legacy := schemaFor(t, "scoped"), schemaFor(t, "legacy")
	view, arb := newFakeView(), &fakeArbiter{nodeSet: []string{p.NodeID}}
	view.set(scopedInc(t, 1, scoped, wire.TableStatusActive), chainInc(t, 2, legacy, wire.TableStatusActive))
	root := fmt.Sprintf("/sentio/%d/unsafe_inc", p.KeeperShardID)
	stray := "hg_stray_" + suffix(t)
	direct := "db__direct_" + suffix(t)
	strayRoot(t, conn, stray, "unknown_seq", root+"/"+ddl.CHTableName(scoped.TableID)+"/9")
	strayRoot(t, conn, stray, "unscoped_seq", root+"/"+ddl.CHTableName(legacy.TableID)+"/2")
	strayRoot(t, conn, stray, "bogus", root+"/"+ddl.CHTableName(scoped.TableID)+"/bogus")
	strayRoot(t, conn, stray, "direct", root+"/"+direct)
	source, _, logs := auditedOwner(t, conn, p, view, arb)
	mustReconcile(t, source)
	if !source.Ready(scoped.TableID) || !source.Ready(legacy.TableID) {
		t.Fatalf("not ready: %+v", source.Stats())
	}
	st := source.Stats()
	var paths []string
	for _, f := range st.KeeperFindings {
		if f.Reason != KeeperFindingNonIncarnationRoot || f.Replica != "" {
			t.Fatalf("finding %+v; every finding here is a non-incarnation root", f)
		}
		paths = append(paths, f.Path)
	}
	for _, want := range []string{
		root + "/" + ddl.CHTableName(scoped.TableID) + "/9",     // a canonical seq that is no incarnation
		root + "/" + ddl.CHTableName(scoped.TableID) + "/bogus", // not a seq
		root + "/" + ddl.CHTableName(legacy.TableID) + "/2",     // the seq of an incarnation that is not scoped
		root + "/" + direct, // a table created directly under unsafe_inc
		root + "/" + direct + "/replicas",
	} {
		if !slices.Contains(paths, want) {
			t.Fatalf("findings %v lack %s", paths, want)
		}
	}
	for _, path := range paths {
		if strings.HasPrefix(path, ddl.ZooKeeperPathFor(p.KeeperShardID, scoped.TableID, KeeperScopeOf(scopedInc(t, 1, scoped, wire.TableStatusActive)))) {
			t.Fatalf("the live incarnation's own path %s is reported", path)
		}
	}
	if !slices.IsSorted(paths) {
		t.Fatalf("findings are not sorted by path: %v", paths)
	}
	if n := strings.Count(logs.String(), "replicated-table root under the SI keeper roots is not an incarnation path"); n != len(paths) {
		t.Fatalf("logged %d non-incarnation-root warnings, want %d:\n%s", n, len(paths), logs.String())
	}
}

func TestOwnerKeeperAuditDropsNothing(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := isolatedPinned(t, conn, replicaConn)
	scoped, legacy := schemaFor(t, "scoped"), schemaFor(t, "legacy")
	scopedScope := ddl.KeeperScope{IncarnationSeq: 1, Scoped: true}
	view, arb := newFakeView(), &fakeArbiter{nodeSet: []string{p.NodeID}}
	view.set(scopedInc(t, 1, scoped, wire.TableStatusActive), chainInc(t, 2, legacy, wire.TableStatusActive))
	source, clock, _ := auditedOwner(t, conn, p, view, arb)
	mustReconcile(t, source)
	// An attached and a detached intruder on both live paths, and a stray
	// root under unsafe_inc.
	for _, tc := range []struct {
		name   string
		detach bool
	}{{"intruder-live", false}, {"intruder-gone", true}} {
		q := peer(t, replicaConn, p, tc.name)
		replicaOf(t, replicaConn, q, scoped, scopedScope, tc.detach)
		replicaOf(t, replicaConn, q, legacy, ddl.KeeperScope{IncarnationSeq: 2}, tc.detach)
	}
	strayRoot(t, conn, "hg_stray_"+suffix(t), "bogus", fmt.Sprintf("/sentio/%d/unsafe_inc/%s/bogus", p.KeeperShardID, ddl.CHTableName(scoped.TableID)))
	before := keeperRoots(t, conn, p)
	scopedBefore := replicasAt(t, conn, p, scoped.TableID, scopedScope)
	legacyBefore := replicasAt(t, conn, p, legacy.TableID, ddl.KeeperScope{})
	for i := 0; i < 3; i++ {
		clock.advance(DefaultKeeperAuditInterval)
		mustReconcile(t, source)
	}
	st := source.Stats()
	if st.KeeperAuditRuns != 4 || len(st.KeeperFindings) != 5 || st.LeftoverDrops != 0 {
		t.Fatalf("stats = %+v; want 4 audits, 5 findings (two intruders on two paths and the stray root), no drop", st)
	}
	if after := keeperRoots(t, conn, p); !reflect.DeepEqual(after, before) {
		t.Fatalf("roots changed under the audit:\n%+v\nwas\n%+v", after, before)
	}
	if got := replicasAt(t, conn, p, scoped.TableID, scopedScope); !slices.Equal(got, scopedBefore) || len(got) != 3 {
		t.Fatalf("incarnation-path replicas = %v, were %v", got, scopedBefore)
	}
	if got := replicasAt(t, conn, p, legacy.TableID, ddl.KeeperScope{}); !slices.Equal(got, legacyBefore) || len(got) != 3 {
		t.Fatalf("legacy replicas = %v, were %v", got, legacyBefore)
	}
	var active uint64
	if err := replicaConn.QueryRow(ctx, `SELECT count() FROM system.replicas WHERE replica_name = 'intruder-live' AND is_readonly = 0`).Scan(&active); err != nil || active != 2 {
		t.Fatalf("intruder-live's writable replicas = %d, %v; want both", active, err)
	}
}
