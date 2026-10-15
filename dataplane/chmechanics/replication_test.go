package chmechanics

import (
	"context"
	"fmt"
	"slices"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// unsafeIntent is p's hg_unsafe ReplicatedMergeTree intent for sch: replica
// p.NodeID at ddl.ZooKeeperPath(p, sch.TableID), with ddl.UnsafeSettings.
func unsafeIntent(t *testing.T, p ddl.Pinned, sch payloadexec.TableSchema) ddl.TableIntent {
	t.Helper()
	unsafe, _, _, err := ddl.Intents(p, sch)
	if err != nil {
		t.Fatalf("protocol intents for %s: %v", sch.TableID, err)
	}
	return unsafe
}

// keeperNames lists the children of a Keeper path through system.zookeeper.
func keeperNames(t *testing.T, conn clickhouse.Conn, path string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT name FROM system.zookeeper WHERE path = ? ORDER BY name", path)
	if err != nil {
		t.Fatalf("system.zookeeper %s: %v", path, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan system.zookeeper %s: %v", path, err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("system.zookeeper %s: %v", path, err)
	}
	return out
}

// TestMechanics_OptimizeOnANonOwnerReplicaMergesTheOwnersParts (M9) pins the
// merge guard's residual gap (spec §2, §18; housegate CLAUDE.md merge-guard
// note): an OPTIMIZE ... FINAL issued on a peer replica of an hg_unsafe table
// merges the owner's parts on the owner although the table pins merges off
// (ddl.UnsafeSettings); the merge keeps the row content, and both replicas
// end with one byte-identical part.
func TestMechanics_OptimizeOnANonOwnerReplicaMergesTheOwnersParts(t *testing.T) {
	env := requireMechanics(t)
	sch := mechanicsSchema(t)
	owner := mechanicsPinned(t)
	owner.NodeID = "owner-" + suffix(t)
	peer := owner
	peer.NodeID = "peer-" + suffix(t)
	ownerUnsafe := unsafeIntent(t, owner, sch)
	peerUnsafe := unsafeIntent(t, peer, sch)
	if ownerUnsafe.ZooKeeperPath != peerUnsafe.ZooKeeperPath {
		t.Fatalf("replicas on different Keeper paths: %s, %s", ownerUnsafe.ZooKeeperPath, peerUnsafe.ZooKeeperPath)
	}
	createTables(t, env.A, env.AAddr, ownerUnsafe)
	createTables(t, env.B, env.BAddr, peerUnsafe)
	db, table := ownerUnsafe.Database, ownerUnsafe.Table
	replicated := qualified(db, table)
	mustInsert(t, env.A, db, table, "p0", 0, 1, nil)
	mustInsert(t, env.A, db, table, "p0", 1, 2, nil)
	before := activeParts(t, env.A, db, table, "p0")
	if len(before) != 2 {
		t.Fatalf("owner holds %d parts, want 2: %v", len(before), names(before))
	}
	beforeHashes := rowHashes(t, env.A, db, table, sch, before)
	want := sumRowHashes(t, beforeHashes[before[0].Name], beforeHashes[before[1].Name])
	mustExec(t, env.B, "SYSTEM SYNC REPLICA "+replicated)
	if got := activeParts(t, env.B, db, table, "p0"); !slices.Equal(names(got), names(before)) {
		t.Fatalf("the peer holds %v, want the owner's parts %v", names(got), names(before))
	}
	mustExec(t, env.B, fmt.Sprintf("OPTIMIZE TABLE %s PARTITION 'p0' FINAL", replicated))
	var merged []mechPart
	eventually(t, settleTimeout, "the owner holding one merged part", func() (bool, string) {
		mustExec(t, env.A, "SYSTEM SYNC REPLICA "+replicated)
		merged = activeParts(t, env.A, db, table, "p0")
		return len(merged) == 1, fmt.Sprintf("owner parts %v", names(merged))
	})
	if merged[0].Rows != 3 {
		t.Fatalf("the merged part holds %d rows, want 3", merged[0].Rows)
	}
	if got := rowHashes(t, env.A, db, table, sch, merged)[merged[0].Name]; got != want {
		t.Fatalf("the merge changed the row content: %s, want %s", abbreviate(got), abbreviate(want))
	}
	mustExec(t, env.B, "SYSTEM SYNC REPLICA "+replicated)
	peerParts := activeParts(t, env.B, db, table, "p0")
	if !slices.Equal(names(peerParts), names(merged)) || !slices.Equal(physHashes(peerParts), physHashes(merged)) {
		t.Fatalf("the peer holds %+v, the owner %+v: replicas must hold byte-identical parts", peerParts, merged)
	}
}

// TestMechanics_DroppingTheLastReplicaRemovesTheKeeperSubtree (M10) pins why
// the purge switch never nests an incarnation-scoped Keeper path under a
// legacy one (spec §8.8): with a table nested at P/scoped/1 below a legacy
// table's path P, dropping one legacy replica keeps P and the nested table's
// metadata, and dropping the last legacy replica removes P and everything
// below it.
func TestMechanics_DroppingTheLastReplicaRemovesTheKeeperSubtree(t *testing.T) {
	env := requireMechanics(t)
	ctx := context.Background()
	sch := mechanicsSchema(t)
	a := mechanicsPinned(t)
	a.NodeID = "a-" + suffix(t)
	b := a
	b.NodeID = "b-" + suffix(t)
	legacyA := unsafeIntent(t, a, sch)
	legacyB := unsafeIntent(t, b, sch)
	nested := legacyA
	nested.Table = legacyA.Table + "_nested"
	nested.ZooKeeperPath = legacyA.ZooKeeperPath + "/scoped/1"
	createTables(t, env.A, env.AAddr, legacyA, nested)
	createTables(t, env.B, env.BAddr, legacyB)
	listed := func() []string {
		t.Helper()
		got, err := ddl.KeeperUnsafeTables(ctx, env.A, a)
		if err != nil {
			t.Fatalf("Keeper tables under /sentio/%d/unsafe: %v", a.KeeperShardID, err)
		}
		return got
	}
	replicas := func() []string {
		t.Helper()
		got, err := ddl.KeeperReplicas(ctx, env.A, a, sch.TableID)
		if err != nil {
			t.Fatalf("Keeper replicas of %s: %v", sch.TableID, err)
		}
		return got
	}
	if got := listed(); !slices.Equal(got, []string{legacyA.Table}) {
		t.Fatalf("Keeper lists %v, want only %s", got, legacyA.Table)
	}
	if got := replicas(); !slices.Equal(got, []string{a.NodeID, b.NodeID}) {
		t.Fatalf("legacy replicas %v, want %s and %s", got, a.NodeID, b.NodeID)
	}
	if got := keeperNames(t, env.A, nested.ZooKeeperPath+"/replicas"); !slices.Equal(got, []string{a.NodeID}) {
		t.Fatalf("the nested table's replicas %v, want %s under the legacy path", got, a.NodeID)
	}
	mustExec(t, env.A, "DROP TABLE "+qualified(legacyA.Database, legacyA.Table)+" SYNC")
	if got := listed(); !slices.Equal(got, []string{legacyA.Table}) {
		t.Fatalf("after dropping one replica Keeper lists %v, want %s", got, legacyA.Table)
	}
	if got := replicas(); !slices.Equal(got, []string{b.NodeID}) {
		t.Fatalf("after dropping one replica: %v, want %s", got, b.NodeID)
	}
	if got := keeperNames(t, env.A, nested.ZooKeeperPath+"/replicas"); !slices.Equal(got, []string{a.NodeID}) {
		t.Fatalf("after dropping one legacy replica the nested table's replicas are %v", got)
	}
	mustExec(t, env.B, "DROP TABLE "+qualified(legacyB.Database, legacyB.Table)+" SYNC")
	if got := listed(); len(got) != 0 {
		t.Fatalf("Keeper still lists %v under /sentio/%d/unsafe after the last legacy replica was dropped; the nested table's metadata lived below it", got, a.KeeperShardID)
	}
}
