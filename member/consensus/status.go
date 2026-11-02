package consensus

import (
	"slices"

	"github.com/sentioxyz/arbiter-core/wire"
)

// The modes and the extra class a member's status reports (plan S3-E1
// contract §42.3).
const (
	// ModeOff: the member runs without member.Config.Consensus (P2/P3).
	ModeOff = "off"
	// ModeConsensus: the member consensus mode is configured.
	ModeConsensus = "consensus"
	// ClassLeaving is the class of a group whose unreleased Leaving record
	// names the member: a Leaving member is in no group's members (spec
	// §9.2), so it has no member class of its own.
	ClassLeaving = "leaving"
)

// Status is the member consensus mode's part of the serving view
// (serve.ServingView.Consensus, GET /v1/serving).
type Status struct {
	Mode       string        `json:"mode"`
	Registered bool          `json:"registered"`
	Groups     []GroupStatus `json:"groups"`
}

// GroupStatus is the member's standing in one database group as the registry
// publishes it: ready at the group's generation, its class, and the
// checkpoint_seq of its stored checkpoint for the group (0 before the
// first).
type GroupStatus struct {
	OwnerIndexerID uint64 `json:"owner_indexer_id"`
	DatabaseID     string `json:"database_id"`
	Generation     uint64 `json:"generation"`
	Ready          bool   `json:"ready"`
	Class          string `json:"class"`
	CheckpointSeq  uint64 `json:"checkpoint_seq"`
}

// OffStatus is the status of a member without the consensus mode.
func OffStatus() Status { return Status{Mode: ModeOff, Groups: []GroupStatus{}} }

// statusOf is the consensus status of nodeID in snap: every group that names
// it as a current member or in an unreleased Leaving record, in the
// registry's group order.
func statusOf(snap wire.TableRegistrySnapshot, nodeID string, registered bool) Status {
	st := Status{Mode: ModeConsensus, Registered: registered, Groups: []GroupStatus{}}
	for _, g := range snap.GroupsWithMember(nodeID) {
		gs := GroupStatus{OwnerIndexerID: g.OwnerIndexerID, DatabaseID: g.DatabaseID, Generation: g.Generation, Class: ClassLeaving}
		if i := slices.IndexFunc(g.Members, func(m wire.GroupMember) bool { return m.NodeID == nodeID }); i >= 0 {
			m := g.Members[i]
			gs.Ready, gs.Class = m.Ready(), string(m.Class)
			if m.Checkpoint != nil {
				gs.CheckpointSeq = m.Checkpoint.Checkpoint.CheckpointSeq
			}
		}
		st.Groups = append(st.Groups, gs)
	}
	return st
}

// readyGroupCount counts the groups that name nodeID as a ready current
// member.
func readyGroupCount(snap wire.TableRegistrySnapshot, nodeID string) int {
	n := 0
	for _, g := range snap.Groups {
		if i := slices.IndexFunc(g.Members, func(m wire.GroupMember) bool { return m.NodeID == nodeID }); i >= 0 && g.Members[i].Ready() {
			n++
		}
	}
	return n
}
