package tableset

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"slices"
	"sort"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// isolatedShard is a Keeper shard of the test's own. The ddl, tableset, snode
// and verifier targets share one Keeper in CI, and these tests create
// incarnation paths and read whole roots, so they never use shard 0.
func isolatedShard(t *testing.T) uint32 {
	t.Helper()
	sum := sha1.Sum([]byte("keeper-shard/" + t.Name()))
	return 1_000_000 + binary.BigEndian.Uint32(sum[:4])%1_000_000
}

// isolatedPinned is testPinned on the test's own Keeper shard.
func isolatedPinned(t *testing.T, conns ...clickhouse.Conn) ddl.Pinned {
	t.Helper()
	p := testPinned(t, conns...)
	p.KeeperShardID = isolatedShard(t)
	return p
}

// scopedInc is chainInc for an incarnation the registry marks
// scoped_keeper_path.
func scopedInc(t *testing.T, seq uint64, schema payloadexec.TableSchema, st wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	inc := chainInc(t, seq, schema, st)
	inc.ScopedKeeperPath = true
	return inc
}

// setSnap installs snap (its Version bumped, Seeded) as the next version.
func (v *fakeView) setSnap(snap wire.TableRegistrySnapshot) {
	v.mu.Lock()
	defer v.mu.Unlock()
	snap.Version = v.snap.Version + 1
	snap.Seeded = true
	v.snap = snap
	v.enabled = true
	close(v.changed)
	v.changed = make(chan struct{})
}

// peer is another node's Pinned on the second server: p's databases with a
// suffix, and node id name; its databases are dropped when the test ends.
func peer(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, name string) ddl.Pinned {
	t.Helper()
	q := p
	tag := "_" + fmt.Sprintf("%x", sha1.Sum([]byte(name)))[:6]
	q.UnsafeDB, q.SafeDB, q.PromoteDB, q.NodeID = p.UnsafeDB+tag, p.SafeDB+tag, p.PromoteDB+tag, name
	t.Cleanup(func() {
		for _, db := range []string{q.UnsafeDB, q.SafeDB, q.PromoteDB} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	return q
}

// replicaOf creates q's tables of schema in scope s on conn: an attached,
// active replica on that path. With detach the hg_unsafe table is detached,
// the inactive replica a dead or evicted node leaves behind.
func replicaOf(t *testing.T, conn clickhouse.Conn, q ddl.Pinned, schema payloadexec.TableSchema, s ddl.KeeperScope, detach bool) {
	t.Helper()
	ctx := context.Background()
	if err := ddl.EnsureTableFor(ctx, conn, q, schema, s, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	if detach {
		if err := conn.Exec(ctx, fmt.Sprintf("DETACH TABLE %s.%s", q.UnsafeDB, ddl.CHTableName(schema.TableID))); err != nil {
			t.Fatal(err)
		}
	}
}

func replicasAt(t *testing.T, conn clickhouse.Conn, p ddl.Pinned, tableID string, s ddl.KeeperScope) []string {
	t.Helper()
	replicas, err := ddl.KeeperReplicasAt(context.Background(), conn, p.KeeperShardID, tableID, s)
	if err != nil {
		t.Fatal(err)
	}
	return replicas
}

func keeperRoots(t *testing.T, conn clickhouse.Conn, p ddl.Pinned) ddl.KeeperRoots {
	t.Helper()
	roots, err := ddl.ListKeeperRoots(context.Background(), conn, p.KeeperShardID)
	if err != nil {
		t.Fatal(err)
	}
	return roots
}

func hasScoped(roots ddl.KeeperRoots, tableID string, seq uint64) bool {
	return slices.Contains(roots.Scoped, ddl.ScopedKeeperPath{Table: ddl.CHTableName(tableID), IncarnationSeq: seq})
}

// insertUnsafe inserts n rows into q's hg_unsafe table of schema (whose
// columns are schemaFor's p and v, plus any String extra).
func insertUnsafe(t *testing.T, conn clickhouse.Conn, q ddl.Pinned, schema payloadexec.TableSchema, n int) {
	t.Helper()
	cols, values := "`_hg_row_id`, `p`, `v`", "toFixedString(hex(MD5(toString(rand64()))), 32), 'a', number"
	for _, c := range schema.Columns[2:] {
		cols, values = cols+", `"+c.Name+"`", values+", ''"
	}
	sql := fmt.Sprintf("INSERT INTO %s.%s (%s) SELECT %s FROM numbers(%d)", q.UnsafeDB, ddl.CHTableName(schema.TableID), cols, values, n)
	if err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatal(err)
	}
}

func unsafeRows(t *testing.T, conn clickhouse.Conn, q ddl.Pinned, schema payloadexec.TableSchema) uint64 {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count() FROM %s.%s", q.UnsafeDB, ddl.CHTableName(schema.TableID))).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func syncReplica(t *testing.T, conn clickhouse.Conn, q ddl.Pinned, schema payloadexec.TableSchema) {
	t.Helper()
	if err := conn.Exec(context.Background(), fmt.Sprintf("SYSTEM SYNC REPLICA %s.%s", q.UnsafeDB, ddl.CHTableName(schema.TableID))); err != nil {
		t.Fatal(err)
	}
}

func TestReconcilerCreatesAScopedIncarnationOnItsIncarnationPath(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	p := isolatedPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	r := newReconciler(t, conn, p, view, arb, nil)
	scoped, legacy := schemaFor(t, "scoped"), schemaFor(t, "legacy")
	view.set(chainInc(t, 1, legacy, wire.TableStatusActive), scopedInc(t, 3, scoped, wire.TableStatusPending))
	mustReconcile(t, r)
	if !r.Ready(scoped.TableID) || !r.Ready(legacy.TableID) {
		t.Fatalf("not ready: %+v", r.Stats())
	}
	scope := ddl.KeeperScope{IncarnationSeq: 3, Scoped: true}
	if got := replicasAt(t, conn, p, scoped.TableID, scope); !slices.Equal(got, []string{p.NodeID}) {
		t.Fatalf("incarnation-path replicas = %v, want only %s", got, p.NodeID)
	}
	var zk string
	if err := conn.QueryRow(ctx, `SELECT zookeeper_path FROM system.replicas WHERE database = ? AND table = ?`, p.UnsafeDB, ddl.CHTableName(scoped.TableID)).Scan(&zk); err != nil {
		t.Fatal(err)
	}
	if want := ddl.ZooKeeperPathFor(p.KeeperShardID, scoped.TableID, scope); zk != want {
		t.Fatalf("zookeeper_path = %q, want %q", zk, want)
	}
	roots := keeperRoots(t, conn, p)
	if !slices.Equal(roots.Legacy, []string{ddl.CHTableName(legacy.TableID)}) || !hasScoped(roots, scoped.TableID, 3) || len(roots.Scoped) != 1 {
		t.Fatalf("roots = %+v; want the legacy table on the legacy root and the scoped one at its incarnation path only", roots)
	}
	for db, c := range localComments(t, conn, p, scoped.TableID) {
		if c != "hg_incarnation=3" {
			t.Fatalf("%s comment = %q", db, c)
		}
	}
	view.set(chainInc(t, 1, legacy, wire.TableStatusActive), scopedInc(t, 3, scoped, wire.TableStatusActive))
	mustReconcile(t, r)
	if !r.Ready(scoped.TableID) || r.Stats().States[StateReady] != 2 {
		t.Fatalf("the scoped table must verify on its incarnation path: %+v", r.Stats())
	}
}

func TestReconcilerPurgesAndSweepsBothPathKinds(t *testing.T) {
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := isolatedPinned(t, conn, replicaConn)
	view, arb := newFakeView(), &fakeArbiter{}
	source := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned = true })
	legacy, scoped := schemaFor(t, "legacy"), schemaFor(t, "scoped")
	legacyScope, scopedScope := ddl.KeeperScope{}, ddl.KeeperScope{IncarnationSeq: 2, Scoped: true}
	view.set(chainInc(t, 1, legacy, wire.TableStatusActive), scopedInc(t, 2, scoped, wire.TableStatusActive))
	mustReconcile(t, source)
	// On each path, verifier-live is a current node that has not purged yet;
	// verifier-gone is decommissioned (detached).
	live, gone := peer(t, replicaConn, p, "verifier-live"), peer(t, replicaConn, p, "verifier-gone")
	for _, q := range []ddl.Pinned{live, gone} {
		replicaOf(t, replicaConn, q, legacy, ddl.KeeperScope{IncarnationSeq: 1}, q.NodeID == gone.NodeID)
		replicaOf(t, replicaConn, q, scoped, scopedScope, q.NodeID == gone.NodeID)
	}
	arb.nodeSet = []string{p.NodeID, "verifier-live"}
	view.set(chainInc(t, 1, legacy, wire.TableStatusPurging), scopedInc(t, 2, scoped, wire.TableStatusPurging))
	mustReconcile(t, source)
	for _, tc := range []struct {
		schema payloadexec.TableSchema
		scope  ddl.KeeperScope
	}{{legacy, legacyScope}, {scoped, scopedScope}} {
		if got := replicasAt(t, conn, p, tc.schema.TableID, tc.scope); !slices.Equal(got, []string{"verifier-live"}) {
			t.Fatalf("%s replicas = %v; want only the current verifier-live", tc.schema.TableID, got)
		}
		if left := localComments(t, conn, p, tc.schema.TableID); len(left) != 0 {
			t.Fatalf("%s tables left: %v", tc.schema.TableID, left)
		}
	}
	if got := arb.reported(); !slices.Equal(got, []string{p.NodeID + "/1", p.NodeID + "/2"}) {
		t.Fatalf("reports = %v", got)
	}
	// verifier-live purges its own tables: each last replica removes its path.
	for _, tc := range []struct {
		schema payloadexec.TableSchema
		scope  ddl.KeeperScope
	}{{legacy, legacyScope}, {scoped, scopedScope}} {
		if err := ddl.DropTableFor(context.Background(), replicaConn, live, tc.schema.TableID, tc.scope); err != nil {
			t.Fatal(err)
		}
	}
	roots := keeperRoots(t, conn, p)
	if slices.Contains(roots.Legacy, ddl.CHTableName(legacy.TableID)) || hasScoped(roots, scoped.TableID, 2) {
		t.Fatalf("roots = %+v; both paths must be gone once every current node dropped", roots)
	}
}

func TestClearDecommissionedCoversTheScopedRoot(t *testing.T) {
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := isolatedPinned(t, conn, replicaConn)
	v := p
	v.NodeID = "verifier-" + suffix(t)
	s := schemaFor(t, "t")
	scope := ddl.KeeperScope{IncarnationSeq: 2, Scoped: true}
	// An evicted node's replica of incarnation 2 on its incarnation path, and
	// an evicted node's replica of incarnation 1 on the legacy path.
	replicaOf(t, replicaConn, peer(t, replicaConn, p, "verifier-gone"), s, scope, true)
	replicaOf(t, replicaConn, peer(t, replicaConn, p, "verifier-gone-legacy"), s, ddl.KeeperScope{IncarnationSeq: 1}, true)
	view, arb := newFakeView(), &fakeArbiter{nodeSet: []string{p.NodeID, v.NodeID}}
	view.set(chainInc(t, 1, s, wire.TableStatusPurged), scopedInc(t, 2, s, wire.TableStatusPending))
	verifier := newReconciler(t, replicaConn, v, view, arb, nil)
	source := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned = true })

	mustReconcile(t, verifier)
	if verifier.Ready(s.TableID) || len(localComments(t, replicaConn, v, s.TableID)) != 0 {
		t.Fatal("a non-source node must not create over a decommissioned replica on the incarnation path")
	}
	if st := verifier.Stats(); st.Failures[s.TableID] != 1 {
		t.Fatalf("the refusal is a per-table failure: %+v", st)
	}
	if got := replicasAt(t, conn, p, s.TableID, scope); !slices.Equal(got, []string{"verifier-gone"}) {
		t.Fatalf("incarnation-path replicas = %v; a non-source node must not remove a replica", got)
	}

	mustReconcile(t, source)
	if !source.Ready(s.TableID) {
		t.Fatalf("the source must remove the decommissioned replica and create: %+v", source.Stats())
	}
	if got := replicasAt(t, conn, p, s.TableID, scope); !slices.Equal(got, []string{p.NodeID}) {
		t.Fatalf("incarnation-path replicas = %v; want only the source", got)
	}
	// The live incarnation is scoped, so the legacy path holds only a retired
	// incarnation's replicas: the source's sweep removes the evicted one there
	// too, without touching its own tables.
	if got := replicasAt(t, conn, p, s.TableID, ddl.KeeperScope{}); len(got) != 0 {
		t.Fatalf("legacy replicas = %v; the source's sweep must remove the evicted replica", got)
	}

	verifier.Trigger()
	mustReconcile(t, verifier)
	if !verifier.Ready(s.TableID) {
		t.Fatalf("the verifier creates once the source removed the replica: %+v", verifier.Stats())
	}
	want := []string{p.NodeID, v.NodeID}
	sort.Strings(want)
	if got := replicasAt(t, conn, p, s.TableID, scope); !slices.Equal(got, want) {
		t.Fatalf("incarnation-path replicas = %v, want %v", got, want)
	}
}

func TestSweepPurgedKeeperPathsCoversBothRoots(t *testing.T) {
	testSweepPurgedKeeperPaths(t, func(inc wire.TableIncarnation) wire.TableIncarnation { return inc })
}

// TestSweepPurgedKeeperPathsOnACompactedRegistry: arbiter#123. The arbiter
// serves Purged incarnations without schema_json, schema_ref, created,
// deleted and purged_by; the same sweep runs on that registry, since it
// needs only key, seq, status, owner and scoped_keeper_path.
func TestSweepPurgedKeeperPathsOnACompactedRegistry(t *testing.T) {
	testSweepPurgedKeeperPaths(t, compactedAsServed)
}

// compactedAsServed is inc as the arbiter's read API serves it.
func compactedAsServed(inc wire.TableIncarnation) wire.TableIncarnation {
	switch inc.Status {
	case wire.TableStatusPurged:
		inc.SchemaJSON, inc.SchemaRef, inc.Created, inc.Deleted, inc.PurgedBy = "", nil, nil, nil, nil
	case wire.TableStatusRefused:
		inc.SchemaJSON = ""
	}
	return inc
}

func testSweepPurgedKeeperPaths(t *testing.T, served func(wire.TableIncarnation) wire.TableIncarnation) {
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := isolatedPinned(t, conn, replicaConn)
	gone := peer(t, replicaConn, p, "verifier-gone")
	legacy, scoped, recreated := schemaFor(t, "legacy"), schemaFor(t, "scoped"), schemaFor(t, "recreated")
	// Three paths that survived their purge because verifier-gone was
	// evicted after the source's sweep: a legacy path, an incarnation path,
	// and the incarnation path of a key re-created since (incarnation 4).
	replicaOf(t, replicaConn, gone, legacy, ddl.KeeperScope{IncarnationSeq: 1}, true)
	replicaOf(t, replicaConn, gone, scoped, ddl.KeeperScope{IncarnationSeq: 2, Scoped: true}, true)
	replicaOf(t, replicaConn, gone, recreated, ddl.KeeperScope{IncarnationSeq: 3, Scoped: true}, true)
	v := peer(t, replicaConn, p, "verifier-"+suffix(t))
	view, arb := newFakeView(), &fakeArbiter{nodeSet: []string{p.NodeID, v.NodeID}}
	purged := func(inc wire.TableIncarnation) wire.TableIncarnation {
		inc.Created = &arbiter.L2EventRef{BlockNumber: inc.Seq, BlockHash: "0xcreated", TxHash: "0xtx"}
		inc.Deleted = &arbiter.L2EventRef{BlockNumber: inc.Seq + 1, BlockHash: "0xdeleted", TxHash: "0xtx"}
		inc.PurgedBy = []string{p.NodeID}
		return served(inc)
	}
	view.set(purged(chainInc(t, 1, legacy, wire.TableStatusPurged)), purged(scopedInc(t, 2, scoped, wire.TableStatusPurged)),
		purged(scopedInc(t, 3, recreated, wire.TableStatusPurged)), served(scopedInc(t, 4, recreated, wire.TableStatusActive)))
	source := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned = true })
	verifier := newReconciler(t, replicaConn, v, view, arb, nil)

	mustReconcile(t, verifier)
	if roots := keeperRoots(t, conn, p); !slices.Contains(roots.Legacy, ddl.CHTableName(legacy.TableID)) || !hasScoped(roots, scoped.TableID, 2) || !hasScoped(roots, recreated.TableID, 3) {
		t.Fatalf("roots = %+v; only the source sweeps", roots)
	}
	mustReconcile(t, source)
	roots := keeperRoots(t, conn, p)
	if slices.Contains(roots.Legacy, ddl.CHTableName(legacy.TableID)) {
		t.Fatalf("legacy roots = %v; the surviving legacy path must be swept", roots.Legacy)
	}
	if hasScoped(roots, scoped.TableID, 2) || hasScoped(roots, recreated.TableID, 3) {
		t.Fatalf("scoped roots = %v; the surviving incarnation paths must be swept", roots.Scoped)
	}
	if !hasScoped(roots, recreated.TableID, 4) || !source.Ready(recreated.TableID) {
		t.Fatalf("the re-created key's live incarnation must stay: roots %v, stats %+v", roots.Scoped, source.Stats())
	}
	if c := localComments(t, conn, p, recreated.TableID); len(c) != 3 || c[p.UnsafeDB] != "hg_incarnation=4" {
		t.Fatalf("the live incarnation's tables = %v", c)
	}
	if got := arb.reported(); len(got) != 0 {
		t.Fatalf("a sweep reports nothing: %v", got)
	}
}

func TestSameNameRecreateAfterAPurgeOnBothPathKinds(t *testing.T) {
	for _, kind := range []struct {
		name   string
		scoped bool
	}{{"legacy", false}, {"scoped", true}} {
		t.Run(kind.name, func(t *testing.T) {
			conn := requireCH(t)
			replicaConn := requireReplicaCH(t)
			p := isolatedPinned(t, conn, replicaConn)
			incOf := func(seq uint64, schema payloadexec.TableSchema, st wire.TableIncarnationStatus) wire.TableIncarnation {
				inc := chainInc(t, seq, schema, st)
				inc.ScopedKeeperPath = kind.scoped
				return inc
			}
			view, arb := newFakeView(), &fakeArbiter{}
			source := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned = true })
			s := schemaFor(t, "t")
			first := incOf(1, s, wire.TableStatusActive)
			view.set(first)
			mustReconcile(t, source)
			// A member holds a replica of incarnation 1 and its rows, then dies:
			// its table is detached and its Keeper replica stays.
			member := peer(t, replicaConn, p, "member-dead")
			replicaOf(t, replicaConn, member, s, KeeperScopeOf(first), false)
			insertUnsafe(t, conn, p, s, 3)
			syncReplica(t, replicaConn, member, s)
			if n := unsafeRows(t, replicaConn, member, s); n != 3 {
				t.Fatalf("member rows = %d, want 3", n)
			}
			if err := replicaConn.Exec(context.Background(), fmt.Sprintf("DETACH TABLE %s.%s", member.UnsafeDB, ddl.CHTableName(s.TableID))); err != nil {
				t.Fatal(err)
			}
			// The purge runs while the dead member is still registered: the
			// owner drops its own replica, keeps the member's and reports.
			arb.nodeSet = []string{p.NodeID, "member-dead"}
			view.set(incOf(1, s, wire.TableStatusPurging))
			mustReconcile(t, source)
			if got := replicasAt(t, conn, p, s.TableID, KeeperScopeOf(first)); !slices.Equal(got, []string{"member-dead"}) {
				t.Fatalf("replicas after the owner's purge = %v, want only the dead member's", got)
			}
			if got := arb.reported(); !slices.Equal(got, []string{p.NodeID + "/1"}) {
				t.Fatalf("reports = %v", got)
			}
			// EvictNode completes the purge; the name is re-created with a new
			// structure.
			arb.nodeSet = []string{p.NodeID}
			again := schemaFor(t, "t", lthash.Column{Name: "w", Type: "String"})
			second := incOf(2, again, wire.TableStatusPending)
			view.set(incOf(1, s, wire.TableStatusPurged), second)
			mustReconcile(t, source)
			if !source.Ready(again.TableID) {
				t.Fatalf("the re-created incarnation must be created: %+v", source.Stats())
			}
			if got := replicasAt(t, conn, p, again.TableID, KeeperScopeOf(second)); !slices.Equal(got, []string{p.NodeID}) {
				t.Fatalf("re-created replicas = %v, want only the owner", got)
			}
			if got := replicasAt(t, conn, p, s.TableID, KeeperScopeOf(first)); slices.Contains(got, "member-dead") || (kind.scoped && len(got) != 0) {
				t.Fatalf("incarnation 1's path still holds %v; the owner must remove the evicted member's replica", got)
			}
			if n := unsafeRows(t, conn, p, again); n != 0 {
				t.Fatalf("the re-created table holds %d rows of the purged incarnation", n)
			}
			insertUnsafe(t, conn, p, again, 1)
			if n := unsafeRows(t, conn, p, again); n != 1 {
				t.Fatalf("re-created rows = %d, want 1", n)
			}
		})
	}
}

func TestScopedRecreateSurvivesDroppingTheStaleLastUnscopedReplica(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := isolatedPinned(t, conn, replicaConn)
	view, arb := newFakeView(), &fakeArbiter{}
	source := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned = true })
	s := schemaFor(t, "t")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	mustReconcile(t, source)
	// verifier-gone's replica of incarnation 1, left inactive by a node that
	// died before the purge and stays registered through it.
	replicaOf(t, replicaConn, peer(t, replicaConn, p, "verifier-gone"), s, ddl.KeeperScope{IncarnationSeq: 1}, true)
	arb.nodeSet = []string{p.NodeID, "verifier-gone"}
	view.set(chainInc(t, 1, s, wire.TableStatusPurging))
	mustReconcile(t, source)
	legacyPath := ddl.ZooKeeperPathFor(p.KeeperShardID, s.TableID, ddl.KeeperScope{})
	if got := replicasAt(t, conn, p, s.TableID, ddl.KeeperScope{}); !slices.Equal(got, []string{"verifier-gone"}) {
		t.Fatalf("%s replicas = %v, want only the stale verifier-gone", legacyPath, got)
	}
	// The scoped re-create while the stale replica is the last one on the
	// legacy path: nothing waits for it.
	scope := ddl.KeeperScope{IncarnationSeq: 2, Scoped: true}
	view.set(chainInc(t, 1, s, wire.TableStatusPurged), scopedInc(t, 2, s, wire.TableStatusActive))
	mustReconcile(t, source)
	if !source.Ready(s.TableID) {
		t.Fatalf("the scoped re-create must not wait for the stale legacy replica: %+v", source.Stats())
	}
	insertUnsafe(t, conn, p, s, 5)
	if got := replicasAt(t, conn, p, s.TableID, ddl.KeeperScope{}); !slices.Equal(got, []string{"verifier-gone"}) {
		t.Fatalf("legacy replicas = %v; a registered node's replica is not swept", got)
	}
	// verifier-gone is evicted: the owner's sweep drops the stale last
	// replica, which removes the legacy path and every child of it.
	arb.nodeSet = []string{p.NodeID}
	source.Trigger()
	mustReconcile(t, source)
	if roots := keeperRoots(t, conn, p); slices.Contains(roots.Legacy, ddl.CHTableName(s.TableID)) || !hasScoped(roots, s.TableID, 2) {
		t.Fatalf("roots = %+v; want the legacy path gone and the incarnation path kept", roots)
	}
	if got := replicasAt(t, conn, p, s.TableID, scope); !slices.Equal(got, []string{p.NodeID}) {
		t.Fatalf("incarnation-path replicas = %v, want only the owner", got)
	}
	var zk string
	var readonly uint8
	if err := conn.QueryRow(ctx, `SELECT zookeeper_path, is_readonly FROM system.replicas WHERE database = ? AND table = ?`, p.UnsafeDB, ddl.CHTableName(s.TableID)).Scan(&zk, &readonly); err != nil {
		t.Fatal(err)
	}
	if zk != ddl.ZooKeeperPathFor(p.KeeperShardID, s.TableID, scope) || readonly != 0 {
		t.Fatalf("replica at %q readonly %d; want a writable replica at its incarnation path", zk, readonly)
	}
	if n := unsafeRows(t, conn, p, s); n != 5 {
		t.Fatalf("rows = %d, want the 5 written before the drop", n)
	}
	insertUnsafe(t, conn, p, s, 2)
	if n := unsafeRows(t, conn, p, s); n != 7 {
		t.Fatalf("rows = %d, want 7 after a write that follows the drop", n)
	}
	mustReconcile(t, source)
	if !source.Ready(s.TableID) {
		t.Fatalf("the scoped table must still verify: %+v", source.Stats())
	}
}
