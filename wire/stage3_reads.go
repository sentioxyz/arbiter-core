package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// The stage-3 SafeState reads (housegate spec 2026-10-14 §14, §15; plan S3-A
// contract §1, §3): GetPartitionStates and GetPromotionOutcomes. The arbiter
// answers each behind one leader read barrier on its data-plane listener.
// Neither read is a Raft command or reaches one, so these types have no
// canonical signing form and no JSON tags. The converters copy every field;
// an empty list becomes nil (mapSlice), an absent track or ack a nil pointer,
// and an absent watermark or command the zero value.

// PartitionStatesQuery selects GetPartitionStates' answer: every partition of
// TableID the FSM holds promotion state for, or only PartitionID. A partition
// has state once an applied acknowledgement reached it; one whose promotions
// are all unacknowledged or acknowledged with Applied:false is not listed,
// even when PartitionID names it.
type PartitionStatesQuery struct {
	TableID     string
	PartitionID string
}

// PartitionState is the FSM's promotion state of one partition: its
// fsm.PartitionState (the promotion base, PublishSeq = the newest applied
// acknowledgement, the diagnostic InventoryUnrefreshed), the cumulative
// SafeParts inventory in FSM order, the row LtHashes of its promoted unsafe
// parts still awaiting cleanup (ascending), its newest issued but
// unacknowledged promotion (0 = none) and its newest acknowledged one,
// applied or not (0 = none).
type PartitionState struct {
	TableID                       string
	PartitionID                   string
	BaseSafeSnapshotID            string
	BasePartitionRoot             string
	PublishSeq                    uint64
	SafeParts                     []replay.PartManifestEntry
	PromotedUnsafePartRowLtHashes []string
	UnackedPromotionSeq           uint64
	LastAckedPromotionSeq         uint64
	InventoryUnrefreshed          bool
}

// PartitionStates is GetPartitionStates' answer: the FSM's PromotionSeq and
// safe watermark at the read, and the selected partitions.
type PartitionStates struct {
	PromotionSeq  uint64
	SafeWatermark arbiter.SafeWatermark
	Partitions    []PartitionState
}

// PromotionOutcomesQuery selects GetPromotionOutcomes' answer: the promotions
// after AfterPromotionSeq (an exclusive cursor), of TableID (and PartitionID,
// which needs TableID) when set, at most Limit of them (0 = the arbiter's
// default).
type PromotionOutcomesQuery struct {
	TableID           string
	PartitionID       string
	AfterPromotionSeq uint64
	Limit             uint32
}

// PromotionOutcome is one issued promotion as the FSM keeps it: the command,
// its statements, the block that finishes it (the max block of StatementSeqs),
// its track (nil before the tracks activation; indexer 0 is a value), its
// source SNode, the authority token and epoch it was issued under, whether it
// is acknowledged and, for an acknowledgement recorded after the tracks
// activation (applied or not), the stored source acknowledgement with its
// signature and epoch, and whether a scheduled cleanup still awaits its
// acknowledgement (false also before the cleanup is scheduled, so false is
// not "cleaned up").
type PromotionOutcome struct {
	PromotionSeq   uint64
	Promote        arbiter.PromoteSafePartition
	StatementSeqs  []uint64
	BlockSeq       uint64
	TrackIndexerID *uint64
	SourceNode     string
	AuthorityJWS   string
	AuthorityEpoch uint64
	Acked          bool
	Ack            *arbiter.PromotionAck
	AckJWS         string
	AckEpoch       uint64
	CleanupPending bool
}

// PromotionOutcomes is GetPromotionOutcomes' answer: the FSM's PromotionSeq at
// the read, the outcomes ascending by promotion seq, and the cursor of the
// next page.
type PromotionOutcomes struct {
	PromotionSeq          uint64
	Outcomes              []PromotionOutcome
	NextAfterPromotionSeq uint64
}

func PartitionStatesQueryToPB(q PartitionStatesQuery) *pb.GetPartitionStatesRequest {
	return &pb.GetPartitionStatesRequest{TableId: q.TableID, PartitionId: q.PartitionID}
}

func PartitionStatesQueryFromPB(m *pb.GetPartitionStatesRequest) PartitionStatesQuery {
	return PartitionStatesQuery{TableID: m.GetTableId(), PartitionID: m.GetPartitionId()}
}

func safeWatermarkFromPB(m *pb.SafeWatermark) arbiter.SafeWatermark {
	return arbiter.SafeWatermark{SnapshotID: m.GetSnapshotId(), SafeBlockSeq: m.GetSafeBlockSeq(), ManifestRoot: m.GetManifestRoot()}
}

func safeWatermarkToPB(v arbiter.SafeWatermark) *pb.SafeWatermark {
	return &pb.SafeWatermark{SnapshotId: v.SnapshotID, SafeBlockSeq: v.SafeBlockSeq, ManifestRoot: v.ManifestRoot}
}

func partitionStateFromPB(m *pb.PartitionStateEntry) PartitionState {
	return PartitionState{
		TableID:                       m.GetTableId(),
		PartitionID:                   m.GetPartitionId(),
		BaseSafeSnapshotID:            m.GetBaseSafeSnapshotId(),
		BasePartitionRoot:             m.GetBasePartitionRoot(),
		PublishSeq:                    m.GetPublishSeq(),
		SafeParts:                     mapSlice(m.GetSafeParts(), partManifestEntryFromPB),
		PromotedUnsafePartRowLtHashes: mapSlice(m.GetPromotedUnsafePartRowLthashes(), func(h string) string { return h }),
		UnackedPromotionSeq:           m.GetUnackedPromotionSeq(),
		LastAckedPromotionSeq:         m.GetLastAckedPromotionSeq(),
		InventoryUnrefreshed:          m.GetInventoryUnrefreshed(),
	}
}

func partitionStateToPB(v PartitionState) *pb.PartitionStateEntry {
	return &pb.PartitionStateEntry{
		TableId:                       v.TableID,
		PartitionId:                   v.PartitionID,
		BaseSafeSnapshotId:            v.BaseSafeSnapshotID,
		BasePartitionRoot:             v.BasePartitionRoot,
		PublishSeq:                    v.PublishSeq,
		SafeParts:                     mapSlice(v.SafeParts, partManifestEntryToPB),
		PromotedUnsafePartRowLthashes: mapSlice(v.PromotedUnsafePartRowLtHashes, func(h string) string { return h }),
		UnackedPromotionSeq:           v.UnackedPromotionSeq,
		LastAckedPromotionSeq:         v.LastAckedPromotionSeq,
		InventoryUnrefreshed:          v.InventoryUnrefreshed,
	}
}

func PartitionStatesFromPB(m *pb.PartitionStates) PartitionStates {
	return PartitionStates{
		PromotionSeq:  m.GetPromotionSeq(),
		SafeWatermark: safeWatermarkFromPB(m.GetSafeWatermark()),
		Partitions:    mapSlice(m.GetPartitions(), partitionStateFromPB),
	}
}

func PartitionStatesToPB(v PartitionStates) *pb.PartitionStates {
	return &pb.PartitionStates{
		PromotionSeq:  v.PromotionSeq,
		SafeWatermark: safeWatermarkToPB(v.SafeWatermark),
		Partitions:    mapSlice(v.Partitions, partitionStateToPB),
	}
}

func PromotionOutcomesQueryToPB(q PromotionOutcomesQuery) *pb.GetPromotionOutcomesRequest {
	return &pb.GetPromotionOutcomesRequest{TableId: q.TableID, PartitionId: q.PartitionID, AfterPromotionSeq: q.AfterPromotionSeq, Limit: q.Limit}
}

func PromotionOutcomesQueryFromPB(m *pb.GetPromotionOutcomesRequest) PromotionOutcomesQuery {
	return PromotionOutcomesQuery{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), AfterPromotionSeq: m.GetAfterPromotionSeq(), Limit: m.GetLimit()}
}

func promotionOutcomeFromPB(m *pb.PromotionOutcome) PromotionOutcome {
	out := PromotionOutcome{
		PromotionSeq:   m.GetPromotionSeq(),
		Promote:        PromoteFromPB(m.GetPromote()),
		StatementSeqs:  mapSlice(m.GetStatementSeqs(), func(seq uint64) uint64 { return seq }),
		BlockSeq:       m.GetBlockSeq(),
		SourceNode:     m.GetSourceNode(),
		AuthorityJWS:   m.GetAuthorityJws(),
		AuthorityEpoch: m.GetAuthorityEpoch(),
		Acked:          m.GetAcked(),
		AckJWS:         m.GetAckJws(),
		AckEpoch:       m.GetAckEpoch(),
		CleanupPending: m.GetCleanupPending(),
	}
	if track := m.GetTrack(); track != nil {
		id := track.GetIndexerId()
		out.TrackIndexerID = &id
	}
	if ack := m.GetAck(); ack != nil {
		stored := PromotionAckFromPB(ack)
		out.Ack = &stored
	}
	return out
}

func promotionOutcomeToPB(v PromotionOutcome) *pb.PromotionOutcome {
	m := &pb.PromotionOutcome{
		PromotionSeq:   v.PromotionSeq,
		Promote:        PromoteToPB(v.Promote),
		StatementSeqs:  mapSlice(v.StatementSeqs, func(seq uint64) uint64 { return seq }),
		BlockSeq:       v.BlockSeq,
		SourceNode:     v.SourceNode,
		AuthorityJws:   v.AuthorityJWS,
		AuthorityEpoch: v.AuthorityEpoch,
		Acked:          v.Acked,
		AckJws:         v.AckJWS,
		AckEpoch:       v.AckEpoch,
		CleanupPending: v.CleanupPending,
	}
	if v.TrackIndexerID != nil {
		m.Track = &pb.L3BlockTrack{IndexerId: *v.TrackIndexerID}
	}
	if v.Ack != nil {
		m.Ack = PromotionAckToPB(*v.Ack)
	}
	return m
}

func PromotionOutcomesFromPB(m *pb.PromotionOutcomes) PromotionOutcomes {
	return PromotionOutcomes{
		PromotionSeq:          m.GetPromotionSeq(),
		Outcomes:              mapSlice(m.GetOutcomes(), promotionOutcomeFromPB),
		NextAfterPromotionSeq: m.GetNextAfterPromotionSeq(),
	}
}

func PromotionOutcomesToPB(v PromotionOutcomes) *pb.PromotionOutcomes {
	return &pb.PromotionOutcomes{
		PromotionSeq:          v.PromotionSeq,
		Outcomes:              mapSlice(v.Outcomes, promotionOutcomeToPB),
		NextAfterPromotionSeq: v.NextAfterPromotionSeq,
	}
}
