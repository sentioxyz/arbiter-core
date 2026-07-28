// Package wire is the Arbiter's ONLY pb ⇄ Go boundary (design §2). The fsm
// package consumes the decoded Command union and never imports gen/pb —
// canonical hashing runs over the mirror types by construction (§4.3/§13).
//
// Frozen nil/empty rule: the canonical Go form uses nil for an empty
// repeated field and a nil pointer for an absent message. proto3 repeated
// fields have no presence, so decoding yields nil naturally; a producer
// that hashes a non-nil empty slice ([] vs null in canonical JSON) fails
// its own hash recomputation and is rejected — a protocol conformance
// rule, not a lenient normalization.
package wire

import (
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// ChallengeVerdict mirrors pb.ChallengeVerdict.
type ChallengeVerdict int32

const (
	ChallengeVerdictUnspecified ChallengeVerdict = 0
	ChallengeVerdictSafe        ChallengeVerdict = 1
	ChallengeVerdictRejected    ChallengeVerdict = 2
)

type SubmitStatement struct {
	Envelope           arbiter.StatementEnvelope
	NonMembershipProof []byte
}
type SealL3Block struct{}
type MarkReplaying struct{ BlockSeq uint64 }
type RegisterRC struct{ RC arbiter.RCRecord }
type RecordAttestation struct{ Attestation replay.ReplayAttestation }
type RecordByteSideScan struct{ Scan arbiter.ByteSideScanMsg }
type RecordAnchorFinality struct {
	L3BlockSeq           uint64
	Anchor               arbiter.AnchorRef
	FinalityReached      bool
	LastMergeableReached bool
}
type RecordPromotionIssued struct {
	Promote      arbiter.PromoteSafePartition
	AuthorityJWS string
}
type RecordPromotionAck struct{ Ack arbiter.PromotionAck }
type PublishSafeSnapshot struct{ Manifest replay.SafeSnapshotManifest }
type ScheduleUnsafeCleanup struct {
	Cleanup      arbiter.UnsafeCleanup
	AuthorityJWS string
}
type RecordCleanupAck struct{ Ack arbiter.CleanupAck }
type OpenChallenge struct {
	BlockSeq uint64
	Reason   string
	OpenedBy string
}
type ResolveChallenge struct {
	BlockSeq uint64
	Verdict  ChallengeVerdict
}
type RegisterNode struct{ Registration arbiter.NodeRegistration }
type MarkActive struct{ NodeID string }
type EvictNode struct {
	NodeID string
	Reason string
}

// Command is the decoded RaftCommand: exactly one field is non-nil.
type Command struct {
	SubmitStatement       *SubmitStatement
	SealL3Block           *SealL3Block
	MarkReplaying         *MarkReplaying
	RegisterRC            *RegisterRC
	RecordAttestation     *RecordAttestation
	RecordByteSideScan    *RecordByteSideScan
	RecordAnchorFinality  *RecordAnchorFinality
	RecordPromotionIssued *RecordPromotionIssued
	RecordPromotionAck    *RecordPromotionAck
	PublishSafeSnapshot   *PublishSafeSnapshot
	ScheduleUnsafeCleanup *ScheduleUnsafeCleanup
	RecordCleanupAck      *RecordCleanupAck
	OpenChallenge         *OpenChallenge
	ResolveChallenge      *ResolveChallenge
	RegisterNode          *RegisterNode
	MarkActive            *MarkActive
	EvictNode             *EvictNode
}

// Encode marshals a Command into RaftCommand log-entry bytes.
func Encode(c Command) ([]byte, error) {
	out := &pb.RaftCommand{}
	set := 0
	if c.SubmitStatement != nil {
		set++
		out.Cmd = &pb.RaftCommand_SubmitStatement{SubmitStatement: &pb.SubmitStatementCmd{
			Envelope: EnvelopeToPB(c.SubmitStatement.Envelope), NonMembershipProof: c.SubmitStatement.NonMembershipProof}}
	}
	if c.SealL3Block != nil {
		set++
		out.Cmd = &pb.RaftCommand_SealL3Block{SealL3Block: &pb.SealL3BlockCmd{}}
	}
	if c.MarkReplaying != nil {
		set++
		out.Cmd = &pb.RaftCommand_MarkReplaying{MarkReplaying: &pb.MarkReplayingCmd{BlockSeq: c.MarkReplaying.BlockSeq}}
	}
	if c.RegisterRC != nil {
		set++
		out.Cmd = &pb.RaftCommand_RegisterRc{RegisterRc: &pb.RegisterRCCmd{Rc: RCToPB(c.RegisterRC.RC)}}
	}
	if c.RecordAttestation != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordAttestation{RecordAttestation: &pb.RecordAttestationCmd{Attestation: AttestationToPB(c.RecordAttestation.Attestation)}}
	}
	if c.RecordByteSideScan != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordByteSideScan{RecordByteSideScan: &pb.RecordByteSideScanCmd{Scan: ScanToPB(c.RecordByteSideScan.Scan)}}
	}
	if c.RecordAnchorFinality != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordAnchorFinality{RecordAnchorFinality: &pb.RecordAnchorFinalityCmd{
			L3BlockSeq: c.RecordAnchorFinality.L3BlockSeq, Anchor: AnchorRefToPB(c.RecordAnchorFinality.Anchor),
			FinalityReached: c.RecordAnchorFinality.FinalityReached, LastMergeableReached: c.RecordAnchorFinality.LastMergeableReached}}
	}
	if c.RecordPromotionIssued != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordPromotionIssued{RecordPromotionIssued: &pb.RecordPromotionIssuedCmd{
			Promote: PromoteToPB(c.RecordPromotionIssued.Promote), AuthorityJws: c.RecordPromotionIssued.AuthorityJWS}}
	}
	if c.RecordPromotionAck != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordPromotionAck{RecordPromotionAck: &pb.RecordPromotionAckCmd{Ack: PromotionAckToPB(c.RecordPromotionAck.Ack)}}
	}
	if c.PublishSafeSnapshot != nil {
		set++
		out.Cmd = &pb.RaftCommand_PublishSafeSnapshot{PublishSafeSnapshot: &pb.PublishSafeSnapshotCmd{Manifest: ManifestToPB(c.PublishSafeSnapshot.Manifest)}}
	}
	if c.ScheduleUnsafeCleanup != nil {
		set++
		out.Cmd = &pb.RaftCommand_ScheduleUnsafeCleanup{ScheduleUnsafeCleanup: &pb.ScheduleUnsafeCleanupCmd{
			Cleanup: CleanupToPB(c.ScheduleUnsafeCleanup.Cleanup), AuthorityJws: c.ScheduleUnsafeCleanup.AuthorityJWS}}
	}
	if c.RecordCleanupAck != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordCleanupAck{RecordCleanupAck: &pb.RecordCleanupAckCmd{Ack: CleanupAckToPB(c.RecordCleanupAck.Ack)}}
	}
	if c.OpenChallenge != nil {
		set++
		out.Cmd = &pb.RaftCommand_OpenChallenge{OpenChallenge: &pb.OpenChallengeCmd{
			BlockSeq: c.OpenChallenge.BlockSeq, Reason: c.OpenChallenge.Reason, OpenedBy: c.OpenChallenge.OpenedBy}}
	}
	if c.ResolveChallenge != nil {
		set++
		out.Cmd = &pb.RaftCommand_ResolveChallenge{ResolveChallenge: &pb.ResolveChallengeCmd{
			BlockSeq: c.ResolveChallenge.BlockSeq, Verdict: pb.ChallengeVerdict(c.ResolveChallenge.Verdict)}}
	}
	if c.RegisterNode != nil {
		set++
		out.Cmd = &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{Registration: RegistrationToPB(c.RegisterNode.Registration)}}
	}
	if c.MarkActive != nil {
		set++
		out.Cmd = &pb.RaftCommand_MarkActive{MarkActive: &pb.MarkActiveCmd{NodeId: c.MarkActive.NodeID}}
	}
	if c.EvictNode != nil {
		set++
		out.Cmd = &pb.RaftCommand_EvictNode{EvictNode: &pb.EvictNodeCmd{NodeId: c.EvictNode.NodeID, Reason: c.EvictNode.Reason}}
	}
	if set != 1 {
		return nil, fmt.Errorf("wire: exactly one command must be set, got %d", set)
	}
	return proto.Marshal(out)
}

// Decode parses RaftCommand log-entry bytes into the Go union.
func Decode(b []byte) (Command, error) {
	var in pb.RaftCommand
	if err := proto.Unmarshal(b, &in); err != nil {
		return Command{}, fmt.Errorf("wire: unmarshal RaftCommand: %w", err)
	}
	switch cmd := in.GetCmd().(type) {
	case *pb.RaftCommand_SubmitStatement:
		return Command{SubmitStatement: &SubmitStatement{
			Envelope: EnvelopeFromPB(cmd.SubmitStatement.GetEnvelope()), NonMembershipProof: cmd.SubmitStatement.GetNonMembershipProof()}}, nil
	case *pb.RaftCommand_SealL3Block:
		return Command{SealL3Block: &SealL3Block{}}, nil
	case *pb.RaftCommand_MarkReplaying:
		return Command{MarkReplaying: &MarkReplaying{BlockSeq: cmd.MarkReplaying.GetBlockSeq()}}, nil
	case *pb.RaftCommand_RegisterRc:
		return Command{RegisterRC: &RegisterRC{RC: RCFromPB(cmd.RegisterRc.GetRc())}}, nil
	case *pb.RaftCommand_RecordAttestation:
		return Command{RecordAttestation: &RecordAttestation{Attestation: AttestationFromPB(cmd.RecordAttestation.GetAttestation())}}, nil
	case *pb.RaftCommand_RecordByteSideScan:
		return Command{RecordByteSideScan: &RecordByteSideScan{Scan: ScanFromPB(cmd.RecordByteSideScan.GetScan())}}, nil
	case *pb.RaftCommand_RecordAnchorFinality:
		return Command{RecordAnchorFinality: &RecordAnchorFinality{
			L3BlockSeq: cmd.RecordAnchorFinality.GetL3BlockSeq(), Anchor: AnchorRefFromPB(cmd.RecordAnchorFinality.GetAnchor()),
			FinalityReached: cmd.RecordAnchorFinality.GetFinalityReached(), LastMergeableReached: cmd.RecordAnchorFinality.GetLastMergeableReached()}}, nil
	case *pb.RaftCommand_RecordPromotionIssued:
		return Command{RecordPromotionIssued: &RecordPromotionIssued{
			Promote: PromoteFromPB(cmd.RecordPromotionIssued.GetPromote()), AuthorityJWS: cmd.RecordPromotionIssued.GetAuthorityJws()}}, nil
	case *pb.RaftCommand_RecordPromotionAck:
		return Command{RecordPromotionAck: &RecordPromotionAck{Ack: PromotionAckFromPB(cmd.RecordPromotionAck.GetAck())}}, nil
	case *pb.RaftCommand_PublishSafeSnapshot:
		return Command{PublishSafeSnapshot: &PublishSafeSnapshot{Manifest: ManifestFromPB(cmd.PublishSafeSnapshot.GetManifest())}}, nil
	case *pb.RaftCommand_ScheduleUnsafeCleanup:
		return Command{ScheduleUnsafeCleanup: &ScheduleUnsafeCleanup{
			Cleanup: CleanupFromPB(cmd.ScheduleUnsafeCleanup.GetCleanup()), AuthorityJWS: cmd.ScheduleUnsafeCleanup.GetAuthorityJws()}}, nil
	case *pb.RaftCommand_RecordCleanupAck:
		return Command{RecordCleanupAck: &RecordCleanupAck{Ack: CleanupAckFromPB(cmd.RecordCleanupAck.GetAck())}}, nil
	case *pb.RaftCommand_OpenChallenge:
		return Command{OpenChallenge: &OpenChallenge{
			BlockSeq: cmd.OpenChallenge.GetBlockSeq(), Reason: cmd.OpenChallenge.GetReason(), OpenedBy: cmd.OpenChallenge.GetOpenedBy()}}, nil
	case *pb.RaftCommand_ResolveChallenge:
		return Command{ResolveChallenge: &ResolveChallenge{
			BlockSeq: cmd.ResolveChallenge.GetBlockSeq(), Verdict: ChallengeVerdict(cmd.ResolveChallenge.GetVerdict())}}, nil
	case *pb.RaftCommand_RegisterNode:
		return Command{RegisterNode: &RegisterNode{Registration: RegistrationFromPB(cmd.RegisterNode.GetRegistration())}}, nil
	case *pb.RaftCommand_MarkActive:
		return Command{MarkActive: &MarkActive{NodeID: cmd.MarkActive.GetNodeId()}}, nil
	case *pb.RaftCommand_EvictNode:
		return Command{EvictNode: &EvictNode{NodeID: cmd.EvictNode.GetNodeId(), Reason: cmd.EvictNode.GetReason()}}, nil
	default:
		return Command{}, fmt.Errorf("wire: RaftCommand has no command set")
	}
}
