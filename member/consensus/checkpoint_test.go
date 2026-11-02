package consensus

import (
	"reflect"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/wire"
)

// checkpointSnapshot: member-b is a current member of owner 0's dex and nft
// groups and of owner 2's lp group, and Leaving in owner 0's old group;
// member-c alone is in owner 0's other group.
func checkpointSnapshot() wire.TableRegistrySnapshot {
	old := fixtureGroup(0, "old", 5, fixtureMember("member-c", 1, 1, wire.MemberClassCaughtUp))
	old.Leaving = []wire.LeavingMember{{NodeID: fixtureNode, RemovedGeneration: 5}}
	return fixtureSnapshot(fixtureStage3("purge", "placement", "checkpoints"), []wire.DatabaseGroup{
		fixtureGroup(0, "nft", 2, fixtureMember(fixtureNode, 2, 2, wire.MemberClassBehind)),
		fixtureGroup(0, "dex", 3, fixtureMember(fixtureNode, 1, 1, wire.MemberClassCaughtUp)),
		old,
		fixtureGroup(0, "other", 1, fixtureMember("member-c", 1, 1, wire.MemberClassCaughtUp)),
		fixtureGroup(2, "lp", 4, fixtureMember(fixtureNode, 2, 3, wire.MemberClassCaughtUp)),
	},
		fixtureInc(1, "dex", "pools", 0, wire.TableStatusActive),
		fixtureInc(2, "nft", "mints", 0, wire.TableStatusActive),
		fixtureInc(3, "old", "t", 0, wire.TableStatusActive),
		fixtureInc(4, "other", "t", 0, wire.TableStatusActive),
		fixtureInc(5, "lp", "pairs", 2, wire.TableStatusActive),
	)
}

func installed(owner uint64, snapshot string, keys ...string) OwnerInstallState {
	st := OwnerInstallState{SnapshotID: snapshot, Entry: fixtureEntry(owner, 90, 40)}
	for _, k := range keys {
		st.Tables = append(st.Tables, TableInstallState{TableID: k})
	}
	return st
}

// TestPlanCheckpointListsOnlyTheMembersGroupsOfOneOwner: the checkpoint of
// owner 0 lists exactly owner 0's groups that name member-b as a current
// member, sorted by database, at each group's generation; a Leaving record,
// another member's group and owner 2's group are left out, and there is no
// checkpoint without a journaled manifest, for an entry of another owner, or
// for an owner of whose groups the node is in none.
func TestPlanCheckpointListsOnlyTheMembersGroupsOfOneOwner(t *testing.T) {
	snap := checkpointSnapshot()
	cp, ok := PlanCheckpoint(snap, fixtureNode, 0, installed(0, "0xsnap", "dex.pools", "nft.mints"))
	want := arbiter.MemberCheckpoint{NodeID: fixtureNode, OwnerIndexerID: 0, SnapshotID: "0xsnap", Entry: fixtureEntry(0, 90, 40),
		Groups: []arbiter.CheckpointGroup{{DatabaseID: "dex", GroupGeneration: 3}, {DatabaseID: "nft", GroupGeneration: 2}}}
	if !ok || !reflect.DeepEqual(cp, want) {
		t.Fatalf("PlanCheckpoint(owner 0) = %+v, %v; want %+v", cp, ok, want)
	}
	if cp, ok := PlanCheckpoint(snap, fixtureNode, 2, installed(2, "0xsnap", "lp.pairs")); !ok || len(cp.Groups) != 1 || cp.Groups[0].DatabaseID != "lp" || cp.Groups[0].GroupGeneration != 4 {
		t.Fatalf("PlanCheckpoint(owner 2) = %+v, %v; want the lp group alone at generation 4", cp, ok)
	}
	if _, ok := PlanCheckpoint(snap, fixtureNode, 0, installed(0, "", "dex.pools")); ok {
		t.Fatal("a checkpoint was planned without a journaled manifest")
	}
	if _, ok := PlanCheckpoint(snap, fixtureNode, 0, installed(2, "0xsnap", "dex.pools")); ok {
		t.Fatal("a checkpoint of owner 0 was planned at owner 2's entry")
	}
	if _, ok := PlanCheckpoint(snap, "member-d", 0, installed(0, "0xsnap")); ok {
		t.Fatal("a checkpoint was planned for a node that is in no group of the owner")
	}
}

// TestPlanCheckpointExcludesTablesWithClosedReasons: every table of a group's
// database that the member has not installed at the manifest is excluded
// with a reason of the closed set, sorted by table id: foreign and diverged
// from the journal, audit_pending while the audit is not clean, not_installed
// or disk_threshold for a copy behind the manifest or none at all, retiring
// for a Retiring or Purging table; a copy of an earlier incarnation of a
// re-created table installs nothing of the live one.
func TestPlanCheckpointExcludesTablesWithClosedReasons(t *testing.T) {
	snap := fixtureSnapshot(fixtureStage3("checkpoints"), []wire.DatabaseGroup{
		fixtureGroup(0, "dex", 3, fixtureMember(fixtureNode, 1, 1, wire.MemberClassCaughtUp)),
	},
		fixtureInc(1, "dex", "a", 0, wire.TableStatusActive),
		fixtureInc(2, "dex", "b", 0, wire.TableStatusActive),
		fixtureInc(3, "dex", "c", 0, wire.TableStatusActive),
		fixtureInc(4, "dex", "d", 0, wire.TableStatusActive),
		fixtureInc(5, "dex", "e", 0, wire.TableStatusPending),
		fixtureInc(6, "dex", "f", 0, wire.TableStatusRetiring),
		fixtureInc(7, "dex", "g", 0, wire.TableStatusPurging),
		fixtureInc(8, "dex", "h", 0, wire.TableStatusRefused),
		fixtureInc(9, "dex", "i", 0, wire.TableStatusPurged),
		fixtureInc(10, "dex", "j", 0, wire.TableStatusRetiring),
		fixtureInc(11, "dex", "j", 0, wire.TableStatusActive),
		fixtureInc(12, "dex", "k", 2, wire.TableStatusActive),
	)
	journal := install.OwnerJournal{SnapshotID: "0xsnap", Entry: fixtureEntry(0, 90, 40)}
	at := func(key string, seq uint64) install.TableState {
		return install.TableState{Owner: 0, TableKey: key, IncarnationSeq: seq, SnapshotID: "0xsnap", Entry: journal.Entry}
	}
	behind := at("dex.c", 3)
	behind.SnapshotID, behind.Entry = "0xolder", fixtureEntry(0, 80, 39)
	foreign, diverged := at("dex.a", 1), at("dex.b", 2)
	foreign.Foreign, diverged.Diverged = true, true
	tables := []install.TableState{foreign, diverged, behind, at("dex.d", 4), at("dex.e", 5), at("dex.j", 10)}
	plan := func(stats install.Stats) []arbiter.ExcludedTable {
		t.Helper()
		cp, ok := PlanCheckpoint(snap, fixtureNode, 0, InstallStateOf(snap, 0, journal, tables, stats))
		if !ok || len(cp.Groups) != 1 {
			t.Fatalf("PlanCheckpoint = %+v, %v; want one group", cp, ok)
		}
		return cp.Groups[0].Excluded
	}
	ex := func(key string, reason arbiter.CheckpointExclusionReason) arbiter.ExcludedTable {
		return arbiter.ExcludedTable{TableID: key, Reason: reason}
	}
	want := []arbiter.ExcludedTable{ex("dex.a", arbiter.ExclusionForeign), ex("dex.b", arbiter.ExclusionDiverged), ex("dex.c", arbiter.ExclusionNotInstalled),
		ex("dex.f", arbiter.ExclusionRetiring), ex("dex.g", arbiter.ExclusionRetiring), ex("dex.j", arbiter.ExclusionNotInstalled)}
	if got := plan(install.Stats{AuditClean: true}); !reflect.DeepEqual(got, want) {
		t.Fatalf("excluded = %v, want %v", got, want)
	}
	want[2] = ex("dex.c", arbiter.ExclusionDiskThreshold)
	if got := plan(install.Stats{AuditClean: true, InstallsPaused: true}); !reflect.DeepEqual(got, want) {
		t.Fatalf("with installs paused, excluded = %v, want %v", got, want)
	}
	want = []arbiter.ExcludedTable{ex("dex.a", arbiter.ExclusionForeign), ex("dex.b", arbiter.ExclusionDiverged), ex("dex.c", arbiter.ExclusionAuditPending),
		ex("dex.d", arbiter.ExclusionAuditPending), ex("dex.e", arbiter.ExclusionAuditPending), ex("dex.f", arbiter.ExclusionRetiring),
		ex("dex.g", arbiter.ExclusionRetiring), ex("dex.j", arbiter.ExclusionNotInstalled)}
	if got := plan(install.Stats{AuditClean: false}); !reflect.DeepEqual(got, want) {
		t.Fatalf("with the audit pending, excluded = %v, want %v", got, want)
	}
	known := map[arbiter.CheckpointExclusionReason]bool{arbiter.ExclusionNotInstalled: true, arbiter.ExclusionDiverged: true, arbiter.ExclusionForeign: true,
		arbiter.ExclusionDiskThreshold: true, arbiter.ExclusionAuditPending: true, arbiter.ExclusionRetiring: true}
	for _, e := range want {
		if !known[e.Reason] {
			t.Fatalf("%s is excluded with %q, outside the closed set", e.TableID, e.Reason)
		}
	}
}
