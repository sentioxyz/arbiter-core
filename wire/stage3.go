package wire

import (
	"fmt"
	"slices"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// The stage-3 commands (housegate spec 2026-10-14 §6.4, §7.4, §9.1-§9.2; plan
// S3-E1 contract §39, §41), Raft tags 38-42. Each request message is the Raft
// command itself (the RecordTablePurgedCmd precedent), so the request
// converters in requests.go are exact inverses that keep the signature, and
// Canonical (or Body) is the form authority hashes and signs. arbiter
// proposes these tags only after the v21 activation and the command's switch
// (contract §38): a voter whose decoder lacks them refuses them.

// RebalanceGroup mirrors pb.RebalanceGroupCmd (tag 38), both the
// ConsensusAdmin.RebalanceGroup request and the Raft command. Add and Remove
// are kept exactly as proposed; Canonical normalizes them.
type RebalanceGroup struct {
	OwnerIndexerID     uint64
	DatabaseID         string
	ExpectedGeneration uint64
	Add, Remove        []string
	NetworkFill        bool
	AuthorityJWS       string
}

// Canonical is the command's signing form (authority.RebalanceGroupHash): Add
// and Remove each sorted ascending without duplicates in a fresh slice, nil
// when empty.
func (c RebalanceGroup) Canonical() arbiter.RebalanceGroupCommand {
	return arbiter.RebalanceGroupCommand{OwnerIndexerID: c.OwnerIndexerID, DatabaseID: c.DatabaseID,
		ExpectedGeneration: c.ExpectedGeneration, Add: sortedUnique(c.Add), Remove: sortedUnique(c.Remove), NetworkFill: c.NetworkFill}
}

// ReselectVerifiers mirrors pb.ReselectVerifiersCmd (tag 39). The leader
// proposes it; there is no RPC.
type ReselectVerifiers struct {
	L3BlockSeq   uint64
	ExpectedSet  []string
	AuthorityJWS string
}

// Canonical is the command's signing form (authority.ReselectVerifiersHash):
// ExpectedSet sorted ascending without duplicates in a fresh slice.
func (c ReselectVerifiers) Canonical() arbiter.ReselectVerifiersCommand {
	return arbiter.ReselectVerifiersCommand{L3BlockSeq: c.L3BlockSeq, ExpectedSet: sortedUnique(c.ExpectedSet)}
}

// MarkMemberLagging mirrors pb.MarkMemberLaggingCmd (tag 42), both the
// ConsensusAdmin.MarkMemberLagging request and the Raft command.
type MarkMemberLagging struct {
	NodeID         string
	OwnerIndexerID uint64
	DatabaseID     string
	ObservedEntry  replay.TrackFrontier
	AuthorityJWS   string
}

// Canonical is the command's signing form (authority.MarkMemberLaggingHash).
func (c MarkMemberLagging) Canonical() arbiter.MarkMemberLaggingCommand {
	return arbiter.MarkMemberLaggingCommand{NodeID: c.NodeID, OwnerIndexerID: c.OwnerIndexerID,
		DatabaseID: c.DatabaseID, ObservedEntry: c.ObservedEntry}
}

// MemberReady mirrors pb.MemberReadyCmd (tag 40), both the
// VerifierGateway.SubmitMemberReady request and the Raft command.
// Ed25519Signature is hex, by the member's verifiers-entry ed25519 key.
type MemberReady struct {
	NodeID           string
	OwnerIndexerID   uint64
	DatabaseID       string
	Generation       uint64
	Ed25519Signature string
}

// Body is the signed message (authority.MemberReadyHash).
func (c MemberReady) Body() arbiter.MemberReadyBody {
	return arbiter.MemberReadyBody{NodeID: c.NodeID, OwnerIndexerID: c.OwnerIndexerID, DatabaseID: c.DatabaseID, Generation: c.Generation}
}

// MemberCheckpoint mirrors pb.MemberCheckpointCmd (tag 41), both the
// VerifierGateway.SubmitMemberCheckpoint request and the Raft command:
// Checkpoint is the signed body, ReplicaJWS the replica_signer's token.
type MemberCheckpoint struct {
	Checkpoint arbiter.MemberCheckpoint
	ReplicaJWS string
}

// sortedUnique returns ids sorted ascending without duplicates in a fresh
// slice, nil when there are none (the canonical empty form).
func sortedUnique(ids []string) []string {
	if len(ids) == 0 {
		return nil
	}
	return slices.Compact(slices.Sorted(slices.Values(ids)))
}

// exclusionReasonFromPB is the closed CheckpointExclusionReason set (plan
// S3-E1 K63); UNSPECIFIED is not in it.
var exclusionReasonFromPB = map[pb.CheckpointExclusionReason]arbiter.CheckpointExclusionReason{
	pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_NOT_INSTALLED:  arbiter.ExclusionNotInstalled,
	pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_DIVERGED:       arbiter.ExclusionDiverged,
	pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_FOREIGN:        arbiter.ExclusionForeign,
	pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_DISK_THRESHOLD: arbiter.ExclusionDiskThreshold,
	pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_AUDIT_PENDING:  arbiter.ExclusionAuditPending,
	pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_RETIRING:       arbiter.ExclusionRetiring,
}

// exclusionReasonToPB maps a reason outside the closed set to UNSPECIFIED,
// which every decoder refuses, so a bad value never becomes a known one.
func exclusionReasonToPB(r arbiter.CheckpointExclusionReason) pb.CheckpointExclusionReason {
	for k, v := range exclusionReasonFromPB {
		if v == r {
			return k
		}
	}
	return pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_UNSPECIFIED
}

// MemberCheckpointBodyFromPB converts a checkpoint body. A reason outside the
// closed set (UNSPECIFIED included) is refused here, on every voter alike,
// never applied. Repeated fields decode empty to nil. Body and entry must
// be present: owner 0 can legitimately carry a present zero frontier, so
// canonical value equality cannot detect an absent protobuf entry.
func MemberCheckpointBodyFromPB(m *pb.MemberCheckpoint) (arbiter.MemberCheckpoint, error) {
	if m == nil {
		return arbiter.MemberCheckpoint{}, fmt.Errorf("wire: member checkpoint body is required")
	}
	if m.Entry == nil {
		return arbiter.MemberCheckpoint{}, fmt.Errorf("wire: member checkpoint entry is required")
	}
	out := arbiter.MemberCheckpoint{NodeID: m.GetNodeId(), OwnerIndexerID: m.GetOwnerIndexerId(), CheckpointSeq: m.GetCheckpointSeq(),
		SnapshotID: m.GetSnapshotId(), Entry: TrackFrontierFromPB(m.GetEntry())}
	for _, g := range m.GetGroups() {
		group := arbiter.CheckpointGroup{DatabaseID: g.GetDatabaseId(), GroupGeneration: g.GetGroupGeneration()}
		for _, e := range g.GetExcluded() {
			reason, ok := exclusionReasonFromPB[e.GetReason()]
			if !ok {
				return arbiter.MemberCheckpoint{}, fmt.Errorf("wire: member checkpoint excludes %s with reason %v, which is not a known CheckpointExclusionReason", e.GetTableId(), e.GetReason())
			}
			group.Excluded = append(group.Excluded, arbiter.ExcludedTable{TableID: e.GetTableId(), Reason: reason})
		}
		out.Groups = append(out.Groups, group)
	}
	return out, nil
}

// MemberCheckpointBodyToPB returns an independent transport message.
func MemberCheckpointBodyToPB(v arbiter.MemberCheckpoint) *pb.MemberCheckpoint {
	return &pb.MemberCheckpoint{NodeId: v.NodeID, OwnerIndexerId: v.OwnerIndexerID, CheckpointSeq: v.CheckpointSeq,
		Groups: mapSlice(v.Groups, func(g arbiter.CheckpointGroup) *pb.CheckpointGroup {
			return &pb.CheckpointGroup{DatabaseId: g.DatabaseID, GroupGeneration: g.GroupGeneration,
				Excluded: mapSlice(g.Excluded, func(e arbiter.ExcludedTable) *pb.ExcludedTable {
					return &pb.ExcludedTable{TableId: e.TableID, Reason: exclusionReasonToPB(e.Reason)}
				})}
		}),
		SnapshotId: v.SnapshotID, Entry: TrackFrontierToPB(v.Entry)}
}
