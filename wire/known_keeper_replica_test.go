package wire

import (
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestKnownKeeperReplicaCoversOwnerPoolMembersAndPurgeMembers: a replica on
// an owner's Keeper path is known when it is the owner's SNode, a node of the
// network pool, a current, Leaving or past member of the incarnation's group,
// or one of its purge members (housegate spec 2026-10-14 §8.2, §8.8);
// anything else is the Keeper audit's unknown_replica, another owner's SNode
// and a registered member of another group included. The purge node set
// lists every registered member, so the pool is that set without the SNodes
// and without every node the registry names as a member.
func TestKnownKeeperReplicaCoversOwnerPoolMembersAndPurgeMembers(t *testing.T) {
	owner := uint64(1)
	snap := TableRegistrySnapshot{
		Params: arbiter.TableRegistryParams{SIIndexerID: 0},
		SIIndexers: []arbiter.SIIndexerEntry{
			{IndexerID: 0, SNodeNodeID: "snode-1"},
			{IndexerID: 1, SNodeNodeID: "snode-2"},
		},
		Groups: []DatabaseGroup{
			{OwnerIndexerID: 0, DatabaseID: "dex", Members: []GroupMember{{NodeID: "member-x"}}},
			{OwnerIndexerID: 1, DatabaseID: "dex", Members: []GroupMember{{NodeID: "member-a"}},
				PastMembers: []string{"member-old"}, Leaving: []LeavingMember{{NodeID: "member-leaving"}}},
			{OwnerIndexerID: 1, DatabaseID: "nft", Members: []GroupMember{{NodeID: "member-nft"}}},
		},
		MemberCheckpointSeqs: []MemberCheckpointSeq{{NodeID: "member-seq", OwnerIndexerID: 0, CheckpointSeq: 3}},
	}
	inc := TableIncarnation{Seq: 4, DatabaseID: "dex", TableID: "swaps", OwnerIndexerID: &owner, ScopedKeeperPath: true,
		PurgeMembers: []string{"member-c"}}
	mints := TableIncarnation{Seq: 5, DatabaseID: "nft", TableID: "mints", OwnerIndexerID: &owner, ScopedKeeperPath: true,
		PurgeMembers: []string{"member-p"}}
	snap.Incarnations = []TableIncarnation{inc, mints}
	// GetPurgeNodeSet: every registered, non-evicted SNode and verifier, so
	// every registered member of every group as well as the pool.
	purgeNodeSet := []string{"member-a", "member-c", "member-leaving", "member-nft", "member-old", "member-p", "member-seq",
		"member-x", "snode-1", "snode-2", "verifier-2", "verifier-1", "verifier-1"}
	pool := snap.NetworkPool(purgeNodeSet)
	if !slices.Equal(pool, []string{"verifier-1", "verifier-2"}) {
		t.Fatalf("NetworkPool = %v, want [verifier-1 verifier-2]: no SNode and no node the registry names as a member", pool)
	}
	for _, tc := range []struct {
		replica string
		want    bool
	}{
		{"snode-2", true},        // the owner's SNode
		{"verifier-1", true},     // a pool verifier
		{"member-a", true},       // a current member of G(1, dex)
		{"member-old", true},     // a past member
		{"member-leaving", true}, // a Leaving member keeps its replicas until released
		{"member-c", true},       // a purge member of the incarnation
		{"snode-1", false},       // another owner's SNode, registered
		{"member-x", false},      // a registered member of another owner's group of the same database
		{"member-nft", false},    // a registered member of another database's group
		{"member-p", false},      // a registered purge member of another incarnation
		{"member-seq", false},    // a registered member the view names only by its checkpoint sequence
		{"intruder", false},
		{"", false},
	} {
		if got := snap.KnownKeeperReplica(inc, pool, tc.replica); got != tc.want {
			t.Errorf("KnownKeeperReplica(%q) = %v, want %v", tc.replica, got, tc.want)
		}
	}

	// Without a group or si_indexers (before placement and the signed-claims
	// activation) the pool is the whole purge node set, and only it and the
	// purge members are known; the owner then is the founding indexer, and
	// its SNode is a registered node.
	bare := TableRegistrySnapshot{}
	legacy := TableIncarnation{Seq: 1, DatabaseID: "dex", TableID: "swaps"}
	if p := bare.NetworkPool([]string{"snode-1"}); !slices.Equal(p, []string{"snode-1"}) ||
		!bare.KnownKeeperReplica(legacy, p, "snode-1") || bare.KnownKeeperReplica(legacy, nil, "snode-1") {
		t.Fatal("a bare snapshot must know exactly the purge node set")
	}
	if snap.NetworkPool(nil) != nil {
		t.Fatal("an empty purge node set has no pool")
	}
	if !snap.KnownKeeperReplica(legacy, nil, "snode-1") || snap.KnownKeeperReplica(legacy, nil, "snode-2") {
		t.Fatal("an incarnation that names no owner belongs to the founding indexer, whose SNode is snode-1")
	}
	if !snap.KnownKeeperReplica(legacy, nil, "member-x") {
		t.Fatal("a member of the founding indexer's group is known on its incarnation's path")
	}
}
