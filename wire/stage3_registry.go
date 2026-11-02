package wire

import (
	"cmp"
	"fmt"
	"slices"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// The registry view of database groups (housegate spec 2026-10-14 §6.2, §9.1,
// §9.2; plan S3-E1 contract §39, §41, K64). These are read replies: no Raft
// command reaches them, so they have no canonical signing form and no JSON
// tags. Every list decodes empty to nil; an absent message is a nil pointer
// (Checkpoint, ObservedEntry, HostIndexerID) or the zero value (ReleaseEntry).

// MemberClass mirrors pb.MemberClass (spec §9.2). A Leaving member is not in
// DatabaseGroup.Members; its record is in Leaving.
type MemberClass string

const (
	MemberClassBootstrapping MemberClass = "bootstrapping"
	MemberClassCaughtUp      MemberClass = "caught_up"
	MemberClassBehind        MemberClass = "behind"
	MemberClassLagging       MemberClass = "lagging"
)

var memberClassFromPB = map[pb.MemberClass]MemberClass{
	pb.MemberClass_MEMBER_CLASS_BOOTSTRAPPING: MemberClassBootstrapping,
	pb.MemberClass_MEMBER_CLASS_CAUGHT_UP:     MemberClassCaughtUp,
	pb.MemberClass_MEMBER_CLASS_BEHIND:        MemberClassBehind,
	pb.MemberClass_MEMBER_CLASS_LAGGING:       MemberClassLagging,
}

func memberClassToPB(c MemberClass) pb.MemberClass {
	for k, v := range memberClassFromPB {
		if v == c {
			return k
		}
	}
	return pb.MemberClass_MEMBER_CLASS_UNSPECIFIED
}

// MemberCheckpointRecord mirrors pb.MemberCheckpointRecord: a stored
// checkpoint, its replica_signer token and the epoch whose replica_signer
// verified it.
type MemberCheckpointRecord struct {
	Checkpoint arbiter.MemberCheckpoint
	ReplicaJWS string
	Epoch      uint64
}

// GroupMember mirrors pb.GroupMember: one current member of a group.
type GroupMember struct {
	NodeID           string
	JoinedGeneration uint64
	ReadyGeneration  uint64
	Class            MemberClass
	Checkpoint       *MemberCheckpointRecord
	ObservedEntry    *replay.TrackFrontier
	LagAdvances      uint64
	HostIndexerID    *uint64 // the served host_indexer_id (D27); nil while the member is delisted, indexer 0 kept
}

// Ready reports whether the member has reported its replicas at or after the
// generation it joined at (spec §6.4): ReadyGeneration is 0 until a
// MemberReady is recorded.
func (m GroupMember) Ready() bool {
	return m.ReadyGeneration != 0 && m.ReadyGeneration >= m.JoinedGeneration
}

// LeavingMember mirrors pb.LeavingMember: a member a RebalanceGroup removed.
type LeavingMember struct {
	NodeID            string
	RemovedGeneration uint64
	ReleaseEntry      replay.TrackFrontier
	FrozenMembers     []string
	FrozenDomains     uint32
	Released          bool
}

// DatabaseGroup mirrors pb.DatabaseGroup: G(owner, database) (spec §6.2).
type DatabaseGroup struct {
	OwnerIndexerID uint64
	DatabaseID     string
	Members        []GroupMember
	NetworkFill    bool
	Generation     uint64
	PastMembers    []string
	Leaving        []LeavingMember
	GroupKey       string
	PlacementSalt  string
}

// MemberCheckpointSeq mirrors pb.MemberCheckpointSeq: the highest accepted
// checkpoint_seq of one member for one owner under ReplicaSigner (spec §9.1).
type MemberCheckpointSeq struct {
	NodeID         string
	OwnerIndexerID uint64
	CheckpointSeq  uint64
	ReplicaSigner  string
}

// Group returns G(owner, databaseID) when the registry holds it. The group
// shares its lists with the snapshot.
func (s TableRegistrySnapshot) Group(owner uint64, databaseID string) (DatabaseGroup, bool) {
	for _, g := range s.Groups {
		if g.OwnerIndexerID == owner && g.DatabaseID == databaseID {
			return g, true
		}
	}
	return DatabaseGroup{}, false
}

// GroupsWithMember returns, in registry order, the groups nodeID is a current
// member of or is Leaving and not yet released from: the groups whose tables
// the node must keep. The groups share their lists with the snapshot.
func (s TableRegistrySnapshot) GroupsWithMember(nodeID string) []DatabaseGroup {
	var out []DatabaseGroup
	for _, g := range s.Groups {
		current := slices.ContainsFunc(g.Members, func(m GroupMember) bool { return m.NodeID == nodeID })
		leaving := slices.ContainsFunc(g.Leaving, func(l LeavingMember) bool { return l.NodeID == nodeID && !l.Released })
		if current || leaving {
			out = append(out, g)
		}
	}
	return out
}

// PublishedCheckpointSeq returns the highest checkpoint_seq the arbiter
// accepted from nodeID for owner; a member signs its next checkpoint above
// both its own journal and this value.
func (s TableRegistrySnapshot) PublishedCheckpointSeq(nodeID string, owner uint64) (MemberCheckpointSeq, bool) {
	for _, seq := range s.MemberCheckpointSeqs {
		if seq.NodeID == nodeID && seq.OwnerIndexerID == owner {
			return seq, true
		}
	}
	return MemberCheckpointSeq{}, false
}

func nodeIDs[T any](items []T, id func(T) string) []string {
	out := make([]string, len(items))
	for i, item := range items {
		out[i] = id(item)
	}
	return out
}

// groupsFromPB decodes the groups. The arbiter keeps them sorted by (owner,
// database), each owned by an enrolled indexer, with sorted member, past
// member and leaving lists and a known class per member; anything else is a
// corrupt view, refused like an unknown status.
func groupsFromPB(m *pb.TableRegistrySnapshot, enrolled func(uint64) bool) ([]DatabaseGroup, error) {
	var out []DatabaseGroup
	for i, g := range m.GetGroups() {
		owner, db := g.GetOwnerIndexerId(), g.GetDatabaseId()
		if i > 0 {
			prev := out[i-1]
			if cmp.Or(cmp.Compare(prev.OwnerIndexerID, owner), cmp.Compare(prev.DatabaseID, db)) >= 0 {
				return nil, fmt.Errorf("table registry snapshot: groups must be sorted by (owner_indexer_id, database_id) without duplicates")
			}
		}
		if !enrolled(owner) {
			return nil, fmt.Errorf("table registry snapshot: group %d/%s is owned by indexer %d, which has no si_indexers entry", owner, db, owner)
		}
		group := DatabaseGroup{OwnerIndexerID: owner, DatabaseID: db, NetworkFill: g.GetNetworkFill(), Generation: g.GetGeneration(),
			PastMembers: mapSlice(g.GetPastMembers(), func(id string) string { return id }),
			GroupKey:    g.GetGroupKey(), PlacementSalt: g.GetPlacementSalt()}
		for _, gm := range g.GetMembers() {
			member, err := groupMemberFromPB(gm)
			if err != nil {
				return nil, fmt.Errorf("table registry snapshot: group %d/%s: %w", owner, db, err)
			}
			group.Members = append(group.Members, member)
		}
		for _, l := range g.GetLeaving() {
			group.Leaving = append(group.Leaving, LeavingMember{NodeID: l.GetNodeId(), RemovedGeneration: l.GetRemovedGeneration(),
				ReleaseEntry:  TrackFrontierFromPB(l.GetReleaseEntry()),
				FrozenMembers: mapSlice(l.GetFrozenMembers(), func(id string) string { return id }),
				FrozenDomains: l.GetFrozenDomains(), Released: l.GetReleased()})
		}
		if !ascendingUnique(nodeIDs(group.Members, func(m GroupMember) string { return m.NodeID })) ||
			!ascendingUnique(group.PastMembers) ||
			!ascendingUnique(nodeIDs(group.Leaving, func(l LeavingMember) string { return l.NodeID })) {
			return nil, fmt.Errorf("table registry snapshot: group %d/%s: members, past_members and leaving must each be sorted by node id without duplicates", owner, db)
		}
		out = append(out, group)
	}
	return out, nil
}

func groupMemberFromPB(m *pb.GroupMember) (GroupMember, error) {
	class, ok := memberClassFromPB[m.GetMemberClass()]
	if !ok {
		return GroupMember{}, fmt.Errorf("member %s has unknown class %v", m.GetNodeId(), m.GetMemberClass())
	}
	out := GroupMember{NodeID: m.GetNodeId(), JoinedGeneration: m.GetJoinedGeneration(), ReadyGeneration: m.GetReadyGeneration(),
		Class: class, LagAdvances: m.GetLagAdvances(), HostIndexerID: cloneUint64(m.HostIndexerId)}
	if r := m.GetCheckpoint(); r != nil {
		cp, err := MemberCheckpointBodyFromPB(r.GetCheckpoint())
		if err != nil {
			return GroupMember{}, fmt.Errorf("member %s: %w", m.GetNodeId(), err)
		}
		out.Checkpoint = &MemberCheckpointRecord{Checkpoint: cp, ReplicaJWS: r.GetReplicaJws(), Epoch: r.GetEpoch()}
	}
	if e := m.GetObservedEntry(); e != nil {
		entry := TrackFrontierFromPB(e)
		out.ObservedEntry = &entry
	}
	return out, nil
}

func groupsToPB(groups []DatabaseGroup) []*pb.DatabaseGroup {
	return mapSlice(groups, func(g DatabaseGroup) *pb.DatabaseGroup {
		return &pb.DatabaseGroup{OwnerIndexerId: g.OwnerIndexerID, DatabaseId: g.DatabaseID,
			Members:     mapSlice(g.Members, groupMemberToPB),
			NetworkFill: g.NetworkFill, Generation: g.Generation,
			PastMembers: mapSlice(g.PastMembers, func(id string) string { return id }),
			Leaving: mapSlice(g.Leaving, func(l LeavingMember) *pb.LeavingMember {
				return &pb.LeavingMember{NodeId: l.NodeID, RemovedGeneration: l.RemovedGeneration,
					ReleaseEntry:  TrackFrontierToPB(l.ReleaseEntry),
					FrozenMembers: mapSlice(l.FrozenMembers, func(id string) string { return id }),
					FrozenDomains: l.FrozenDomains, Released: l.Released}
			}),
			GroupKey: g.GroupKey, PlacementSalt: g.PlacementSalt}
	})
}

func groupMemberToPB(m GroupMember) *pb.GroupMember {
	out := &pb.GroupMember{NodeId: m.NodeID, JoinedGeneration: m.JoinedGeneration, ReadyGeneration: m.ReadyGeneration,
		MemberClass: memberClassToPB(m.Class), LagAdvances: m.LagAdvances, HostIndexerId: cloneUint64(m.HostIndexerID)}
	if m.Checkpoint != nil {
		out.Checkpoint = &pb.MemberCheckpointRecord{Checkpoint: MemberCheckpointBodyToPB(m.Checkpoint.Checkpoint),
			ReplicaJws: m.Checkpoint.ReplicaJWS, Epoch: m.Checkpoint.Epoch}
	}
	if m.ObservedEntry != nil {
		out.ObservedEntry = TrackFrontierToPB(*m.ObservedEntry)
	}
	return out
}

// memberCheckpointSeqsFromPB decodes member_checkpoint_seqs, sorted by
// (node_id, owner_indexer_id) without duplicates.
func memberCheckpointSeqsFromPB(m *pb.TableRegistrySnapshot) ([]MemberCheckpointSeq, error) {
	out := mapSlice(m.GetMemberCheckpointSeqs(), func(s *pb.MemberCheckpointSeq) MemberCheckpointSeq {
		return MemberCheckpointSeq{NodeID: s.GetNodeId(), OwnerIndexerID: s.GetOwnerIndexerId(),
			CheckpointSeq: s.GetCheckpointSeq(), ReplicaSigner: s.GetReplicaSigner()}
	})
	for i := 1; i < len(out); i++ {
		if cmp.Or(cmp.Compare(out[i-1].NodeID, out[i].NodeID), cmp.Compare(out[i-1].OwnerIndexerID, out[i].OwnerIndexerID)) >= 0 {
			return nil, fmt.Errorf("table registry snapshot: member_checkpoint_seqs must be sorted by (node_id, owner_indexer_id) without duplicates")
		}
	}
	return out, nil
}

func memberCheckpointSeqsToPB(seqs []MemberCheckpointSeq) []*pb.MemberCheckpointSeq {
	return mapSlice(seqs, func(s MemberCheckpointSeq) *pb.MemberCheckpointSeq {
		return &pb.MemberCheckpointSeq{NodeId: s.NodeID, OwnerIndexerId: s.OwnerIndexerID,
			CheckpointSeq: s.CheckpointSeq, ReplicaSigner: s.ReplicaSigner}
	})
}
