package consensus

import (
	"reflect"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// readinessSnapshot: member-b waits for readiness in two groups of owner 0
// (dex at generation 3, nft at generation 2) and in one of owner 2 (dex at
// generation 1, no physical table yet), and is ready in owner 2's lp group.
// dex.pools of owner 0 was re-created: incarnation 3 is Purging on the
// unscoped path, incarnation 7 is the live, scoped one. nft.mints is
// Retiring, nft.drops Pending. member-c's groups never count.
func readinessSnapshot() wire.TableRegistrySnapshot {
	pools7 := fixtureInc(7, "dex", "pools", 0, wire.TableStatusActive)
	pools7.ScopedKeeperPath = true
	return fixtureSnapshot(fixtureStage3("purge", "placement"), []wire.DatabaseGroup{
		fixtureGroup(0, "dex", 3, fixtureMember(fixtureNode, 3, 0, wire.MemberClassBootstrapping), fixtureMember("member-c", 1, 1, wire.MemberClassCaughtUp)),
		fixtureGroup(0, "nft", 2, fixtureMember(fixtureNode, 2, 0, wire.MemberClassBootstrapping)),
		fixtureGroup(2, "dex", 1, fixtureMember(fixtureNode, 1, 0, wire.MemberClassBootstrapping)),
		fixtureGroup(2, "lp", 4, fixtureMember(fixtureNode, 2, 3, wire.MemberClassCaughtUp)),
		fixtureGroup(0, "other", 1, fixtureMember("member-c", 1, 0, wire.MemberClassBootstrapping)),
	},
		fixtureInc(3, "dex", "pools", 0, wire.TableStatusPurging),
		fixtureInc(4, "dex", "swaps", 0, wire.TableStatusActive),
		fixtureInc(5, "nft", "mints", 0, wire.TableStatusRetiring),
		fixtureInc(6, "nft", "drops", 0, wire.TableStatusPending),
		pools7,
		fixtureInc(8, "lp", "pairs", 2, wire.TableStatusActive),
		fixtureInc(9, "other", "t", 0, wire.TableStatusActive),
		fixtureInc(10, "dex", "gone", 0, wire.TableStatusPurged),
	)
}

// TestPlanReadinessAnnouncesOnlySyncedGroups: a group is announced, at its
// current generation, once every Pending or Active live table of its
// database has a synced local replica at that incarnation's Keeper path; a
// ready member, another member's group and a group without such a table are
// never announced. A Retiring table is never waited for: the reconciler
// creates no hg_member_unsafe replica from Retiring on (spec §8.8).
func TestPlanReadinessAnnouncesOnlySyncedGroups(t *testing.T) {
	snap := readinessSnapshot()
	paths := ReplicaPaths(snap, fixtureNode, 0)
	wantPaths := map[string]string{
		"dex__pools": "/sentio/0/unsafe_inc/dex__pools/7",
		"dex__swaps": "/sentio/0/unsafe/dex__swaps",
		"nft__drops": "/sentio/0/unsafe/nft__drops",
	}
	if !reflect.DeepEqual(paths, wantPaths) {
		t.Fatalf("ReplicaPaths = %v, want %v", paths, wantPaths)
	}
	if got := PlanReadiness(snap, fixtureNode, LocalReplicas{"dex__pools": true, "dex__swaps": false, "nft__mints": true}); len(got) != 0 {
		t.Fatalf("with dex__swaps not synced and nft__drops missing, PlanReadiness = %v, want nothing", got)
	}
	got := PlanReadiness(snap, fixtureNode, LocalReplicas{"dex__pools": true, "dex__swaps": true, "nft__mints": true})
	want := []arbiter.MemberReadyBody{{NodeID: fixtureNode, OwnerIndexerID: 0, DatabaseID: "dex", Generation: 3}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("with dex synced, PlanReadiness = %v, want %v", got, want)
	}
	got = PlanReadiness(snap, fixtureNode, LocalReplicas{"dex__pools": true, "dex__swaps": true, "nft__mints": true, "nft__drops": true})
	want = append(want, arbiter.MemberReadyBody{NodeID: fixtureNode, OwnerIndexerID: 0, DatabaseID: "nft", Generation: 2})
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("with dex and nft synced, PlanReadiness = %v, want %v", got, want)
	}
	if got := PlanReadiness(snap, "member-c", LocalReplicas{"other__t": true}); !reflect.DeepEqual(got, []arbiter.MemberReadyBody{{NodeID: "member-c", OwnerIndexerID: 0, DatabaseID: "other", Generation: 1}}) {
		t.Fatalf("member-c's plan = %v, want only its own waiting group", got)
	}
}

// TestPlanReadinessDoesNotWaitForARetiringTable: a member that joins while a
// table of its group's database is Retiring holds no replica of it (the
// reconciler never creates one from Retiring on, spec §8.8), so readiness
// waits only for the Pending and Active tables: nft is announced with
// nft.mints (Retiring) absent once nft.drops (Pending) is synced, and the
// Retiring table has no replica path to check.
func TestPlanReadinessDoesNotWaitForARetiringTable(t *testing.T) {
	snap := readinessSnapshot()
	if _, ok := ReplicaPaths(snap, fixtureNode, 0)["nft__mints"]; ok {
		t.Fatalf("ReplicaPaths lists the Retiring nft__mints; want only Pending and Active tables")
	}
	got := PlanReadiness(snap, fixtureNode, LocalReplicas{"dex__pools": true, "dex__swaps": true, "nft__drops": true})
	want := []arbiter.MemberReadyBody{
		{NodeID: fixtureNode, OwnerIndexerID: 0, DatabaseID: "dex", Generation: 3},
		{NodeID: fixtureNode, OwnerIndexerID: 0, DatabaseID: "nft", Generation: 2},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("with the Retiring nft__mints absent, PlanReadiness = %v, want %v", got, want)
	}
	if got := PlanReadiness(snap, fixtureNode, LocalReplicas{"dex__pools": true, "dex__swaps": true, "nft__mints": true}); len(got) != 1 || got[0].DatabaseID != "dex" {
		t.Fatalf("with only the Retiring nft__mints synced, PlanReadiness = %v, want dex alone (nft.drops is not synced)", got)
	}
}
