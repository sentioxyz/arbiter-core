// Package wire is the Arbiter's ONLY pb ⇄ Go boundary (design §2). The fsm
// package consumes the decoded Command union and never imports gen/pb —
// canonical hashing runs over the mirror types by construction (§4.3/§13).
//
// Frozen legacy nil/empty rule: the canonical Go form uses nil for an empty
// repeated field and a nil pointer for an absent message. proto3 repeated
// fields have no presence, so decoding yields nil naturally; a producer
// that hashes a non-nil empty slice ([] vs null in canonical JSON) fails
// its own hash recomputation and is rejected — a protocol conformance
// rule, not a lenient normalization. Separate snapshot-query records use []
// for new canonical arrays through snapshotMap, without changing legacy hashes.
package wire

import (
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/replay"

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

// RegisterRC mirrors pb.RegisterRCCmd. SourceJWS is the bound source's
// ES256K signature over RC (authority.SNodeMessageResultClaim); empty before
// the signed-claims activation.
type RegisterRC struct {
	RC        arbiter.RCRecord
	SourceJWS string
}
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

// RecordPromotionAck mirrors pb.RecordPromotionAckCmd.
type RecordPromotionAck struct {
	Ack       arbiter.PromotionAck
	SourceJWS string
}
type PublishSafeSnapshot struct{ Manifest replay.SafeSnapshotManifest }
type ScheduleUnsafeCleanup struct {
	Cleanup      arbiter.UnsafeCleanup
	AuthorityJWS string
}

// RecordCleanupAck mirrors pb.RecordCleanupAckCmd.
type RecordCleanupAck struct {
	Ack       arbiter.CleanupAck
	SourceJWS string
}
type OpenChallenge struct {
	BlockSeq uint64
	Reason   string
	OpenedBy string
}
type ResolveChallenge struct {
	BlockSeq uint64
	Verdict  ChallengeVerdict
}

// RegisterNode mirrors pb.RegisterNodeCmd. An SNODE registration carries
// SignerJWS, a VERIFIER registration Ed25519Signature; Registration.
// RegistrationSeq rides inside the registration. All three stay empty before
// the signed-claims activation.
type RegisterNode struct {
	Registration     arbiter.NodeRegistration
	SignerJWS        string
	Ed25519Signature string
}

// MarkActive mirrors pb.MarkActiveCmd.
type MarkActive struct {
	NodeID           string
	RegistrationSeq  uint64
	SignerJWS        string
	Ed25519Signature string
}

// EvictNode mirrors pb.EvictNodeCmd. ExpectedRegistrationSeq and AuthorityJWS
// are set only by the authority-signed EvictNode RPC after the activation.
type EvictNode struct {
	NodeID                  string
	Reason                  string
	ExpectedRegistrationSeq uint64
	AuthorityJWS            string
}

// Canonical is the eviction's signing form (authority.EvictNodeHash).
func (c EvictNode) Canonical() arbiter.EvictNodeCommand {
	return arbiter.EvictNodeCommand{NodeID: c.NodeID, ExpectedRegistrationSeq: c.ExpectedRegistrationSeq, Reason: c.Reason}
}

type UpdateConsensusParams struct {
	Update       arbiter.ConsensusParamsUpdate
	AuthorityJWS string
}

// Command is the decoded RaftCommand: exactly one field is non-nil.
type Command struct {
	SubmitStatement                  *SubmitStatement
	SealL3Block                      *SealL3Block
	MarkReplaying                    *MarkReplaying
	RegisterRC                       *RegisterRC
	RecordAttestation                *RecordAttestation
	RecordByteSideScan               *RecordByteSideScan
	RecordAnchorFinality             *RecordAnchorFinality
	RecordPromotionIssued            *RecordPromotionIssued
	RecordPromotionAck               *RecordPromotionAck
	PublishSafeSnapshot              *PublishSafeSnapshot
	ScheduleUnsafeCleanup            *ScheduleUnsafeCleanup
	RecordCleanupAck                 *RecordCleanupAck
	OpenChallenge                    *OpenChallenge
	ResolveChallenge                 *ResolveChallenge
	RegisterNode                     *RegisterNode
	MarkActive                       *MarkActive
	EvictNode                        *EvictNode
	UpdateConsensusParams            *UpdateConsensusParams            `json:",omitempty"`
	BeginSnapshotQuery               *BeginSnapshotQuery               `json:",omitempty"`
	GrantSnapshotQuery               *GrantSnapshotQuery               `json:",omitempty"`
	ReleaseSnapshotQuery             *ReleaseSnapshotQuery             `json:",omitempty"`
	SubmitSnapshotQuery              *SubmitSnapshotQuery              `json:",omitempty"`
	AbortSnapshotQuery               *AbortSnapshotQuery               `json:",omitempty"`
	ActivateQueryProfile             *ActivateQueryProfile             `json:",omitempty"`
	RecordSnapshotQueryClaim         *RecordSnapshotQueryClaim         `json:",omitempty"`
	RecordSnapshotQueryAttestation   *RecordSnapshotQueryAttestation   `json:",omitempty"`
	PublishExecutorProfileTransition *PublishExecutorProfileTransition `json:",omitempty"`
	RecordSnapshotArtifactReady      *RecordSnapshotArtifactReady      `json:",omitempty"`
	ArtifactDisposition              *ArtifactDispositionCmd           `json:",omitempty"`
	SeedLegacyTables                 *SeedLegacyTables                 `json:",omitempty"`
	AddTable                         *AddTable                         `json:",omitempty"`
	RetireTables                     *RetireTables                     `json:",omitempty"`
	AdvanceL2Cursor                  *AdvanceL2Cursor                  `json:",omitempty"`
	RecordTablePurged                *RecordTablePurged                `json:",omitempty"`
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
		out.Cmd = &pb.RaftCommand_RegisterRc{RegisterRc: &pb.RegisterRCCmd{Rc: RCToPB(c.RegisterRC.RC), SourceJws: c.RegisterRC.SourceJWS}}
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
		out.Cmd = &pb.RaftCommand_RecordPromotionAck{RecordPromotionAck: &pb.RecordPromotionAckCmd{
			Ack: PromotionAckToPB(c.RecordPromotionAck.Ack), SourceJws: c.RecordPromotionAck.SourceJWS}}
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
		out.Cmd = &pb.RaftCommand_RecordCleanupAck{RecordCleanupAck: &pb.RecordCleanupAckCmd{
			Ack: CleanupAckToPB(c.RecordCleanupAck.Ack), SourceJws: c.RecordCleanupAck.SourceJWS}}
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
		out.Cmd = &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{
			Registration: RegistrationToPB(c.RegisterNode.Registration),
			SignerJws:    c.RegisterNode.SignerJWS, Ed25519Signature: c.RegisterNode.Ed25519Signature}}
	}
	if c.MarkActive != nil {
		set++
		m := c.MarkActive
		out.Cmd = &pb.RaftCommand_MarkActive{MarkActive: &pb.MarkActiveCmd{
			NodeId: m.NodeID, RegistrationSeq: m.RegistrationSeq, SignerJws: m.SignerJWS, Ed25519Signature: m.Ed25519Signature}}
	}
	if c.EvictNode != nil {
		set++
		e := c.EvictNode
		out.Cmd = &pb.RaftCommand_EvictNode{EvictNode: &pb.EvictNodeCmd{NodeId: e.NodeID, Reason: e.Reason,
			ExpectedRegistrationSeq: e.ExpectedRegistrationSeq, AuthorityJws: e.AuthorityJWS}}
	}
	if c.UpdateConsensusParams != nil {
		set++
		out.Cmd = &pb.RaftCommand_UpdateConsensusParams{UpdateConsensusParams: &pb.UpdateConsensusParamsCmd{
			Update: ConsensusParamsUpdateToPB(c.UpdateConsensusParams.Update), AuthorityJws: c.UpdateConsensusParams.AuthorityJWS}}
	}
	if c.BeginSnapshotQuery != nil {
		set++
		out.Cmd = &pb.RaftCommand_BeginSnapshotQuery{BeginSnapshotQuery: BeginSnapshotQueryToPB(*c.BeginSnapshotQuery)}
	}
	if c.GrantSnapshotQuery != nil {
		set++
		out.Cmd = &pb.RaftCommand_GrantSnapshotQuery{GrantSnapshotQuery: GrantSnapshotQueryToPB(*c.GrantSnapshotQuery)}
	}
	if c.ReleaseSnapshotQuery != nil {
		set++
		out.Cmd = &pb.RaftCommand_ReleaseSnapshotQuery{ReleaseSnapshotQuery: ReleaseSnapshotQueryToPB(*c.ReleaseSnapshotQuery)}
	}
	if c.SubmitSnapshotQuery != nil {
		set++
		out.Cmd = &pb.RaftCommand_SubmitSnapshotQuery{SubmitSnapshotQuery: SubmitSnapshotQueryToPB(*c.SubmitSnapshotQuery)}
	}
	if c.AbortSnapshotQuery != nil {
		set++
		out.Cmd = &pb.RaftCommand_AbortSnapshotQuery{AbortSnapshotQuery: AbortSnapshotQueryToPB(*c.AbortSnapshotQuery)}
	}
	if c.ActivateQueryProfile != nil {
		set++
		out.Cmd = &pb.RaftCommand_ActivateQueryProfile{ActivateQueryProfile: ActivateQueryProfileToPB(*c.ActivateQueryProfile)}
	}
	if c.RecordSnapshotQueryClaim != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordSnapshotQueryClaim{RecordSnapshotQueryClaim: RecordSnapshotQueryClaimToPB(*c.RecordSnapshotQueryClaim)}
	}
	if c.RecordSnapshotQueryAttestation != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordSnapshotQueryAttestation{RecordSnapshotQueryAttestation: RecordSnapshotQueryAttestationToPB(*c.RecordSnapshotQueryAttestation)}
	}
	if c.PublishExecutorProfileTransition != nil {
		set++
		out.Cmd = &pb.RaftCommand_PublishExecutorProfileTransition{PublishExecutorProfileTransition: PublishExecutorProfileTransitionToPB(*c.PublishExecutorProfileTransition)}
	}
	if c.RecordSnapshotArtifactReady != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordSnapshotArtifactReady{RecordSnapshotArtifactReady: RecordSnapshotArtifactReadyToPB(*c.RecordSnapshotArtifactReady)}
	}
	if c.ArtifactDisposition != nil {
		set++
		if err := requireSingleDispositionAction(c.ArtifactDisposition.Command.Action); err != nil {
			return nil, err
		}
		out.Cmd = &pb.RaftCommand_ArtifactDisposition{ArtifactDisposition: ArtifactDispositionCmdToPB(*c.ArtifactDisposition)}
	}
	if c.SeedLegacyTables != nil {
		set++
		out.Cmd = &pb.RaftCommand_SeedLegacyTables{SeedLegacyTables: &pb.SeedLegacyTablesCmd{
			AtBlock: L2BlockRefToPB(&c.SeedLegacyTables.AtBlock), Tables: legacyTablesToPB(c.SeedLegacyTables.Tables),
			IndexerId: cloneUint64(c.SeedLegacyTables.IndexerID)}}
	}
	if c.AddTable != nil {
		set++
		a := c.AddTable
		out.Cmd = &pb.RaftCommand_AddTable{AddTable: &pb.AddTableCmd{DatabaseId: a.DatabaseID, TableId: a.TableID,
			Created: L2EventRefToPB(&a.Created), Schema: L2EventRefToPB(&a.Schema), SchemaVersion: a.SchemaVersion,
			SchemaHash: a.SchemaHash, SchemaJson: a.SchemaJSON, OwnerIndexerId: cloneUint64(a.OwnerIndexerID)}}
	}
	if c.RetireTables != nil {
		set++
		r := c.RetireTables
		out.Cmd = &pb.RaftCommand_RetireTables{RetireTables: &pb.RetireTablesCmd{DatabaseId: r.DatabaseID,
			TableIds: mapSlice(r.TableIDs, func(s string) string { return s }), Deleted: L2EventRefToPB(&r.Deleted),
			Reason: pb.TableRetireReason(r.Reason)}}
	}
	if c.AdvanceL2Cursor != nil {
		set++
		out.Cmd = &pb.RaftCommand_AdvanceL2Cursor{AdvanceL2Cursor: &pb.AdvanceL2CursorCmd{To: L2BlockRefToPB(&c.AdvanceL2Cursor.To)}}
	}
	if c.RecordTablePurged != nil {
		set++
		out.Cmd = &pb.RaftCommand_RecordTablePurged{RecordTablePurged: RecordTablePurgedToRequest(*c.RecordTablePurged)}
	}
	if set != 1 {
		return nil, fmt.Errorf("wire: exactly one command must be set, got %d", set)
	}
	return proto.Marshal(out)
}

// Decode parses RaftCommand log-entry bytes into the Go union.
func Decode(b []byte) (Command, error) {
	var in pb.RaftCommand
	if err := validateCommandBytes(b, in.ProtoReflect().Descriptor(), true); err != nil {
		return Command{}, err
	}
	if err := proto.Unmarshal(b, &in); err != nil {
		return Command{}, fmt.Errorf("wire: unmarshal RaftCommand: %w", err)
	}
	switch cmd := in.GetCmd().(type) {
	case *pb.RaftCommand_UpdateConsensusParams:
		return Command{UpdateConsensusParams: &UpdateConsensusParams{
			Update: ConsensusParamsUpdateFromPB(cmd.UpdateConsensusParams.GetUpdate()), AuthorityJWS: cmd.UpdateConsensusParams.GetAuthorityJws()}}, nil
	case *pb.RaftCommand_BeginSnapshotQuery:
		v := BeginSnapshotQueryFromPB(cmd.BeginSnapshotQuery)
		return Command{BeginSnapshotQuery: &v}, nil
	case *pb.RaftCommand_GrantSnapshotQuery:
		v := GrantSnapshotQueryFromPB(cmd.GrantSnapshotQuery)
		return Command{GrantSnapshotQuery: &v}, nil
	case *pb.RaftCommand_ReleaseSnapshotQuery:
		v := ReleaseSnapshotQueryFromPB(cmd.ReleaseSnapshotQuery)
		return Command{ReleaseSnapshotQuery: &v}, nil
	case *pb.RaftCommand_SubmitSnapshotQuery:
		v := SubmitSnapshotQueryFromPB(cmd.SubmitSnapshotQuery)
		return Command{SubmitSnapshotQuery: &v}, nil
	case *pb.RaftCommand_AbortSnapshotQuery:
		v := AbortSnapshotQueryFromPB(cmd.AbortSnapshotQuery)
		return Command{AbortSnapshotQuery: &v}, nil
	case *pb.RaftCommand_ActivateQueryProfile:
		v := ActivateQueryProfileFromPB(cmd.ActivateQueryProfile)
		return Command{ActivateQueryProfile: &v}, nil
	case *pb.RaftCommand_RecordSnapshotQueryClaim:
		v := RecordSnapshotQueryClaimFromPB(cmd.RecordSnapshotQueryClaim)
		return Command{RecordSnapshotQueryClaim: &v}, nil
	case *pb.RaftCommand_RecordSnapshotQueryAttestation:
		v := RecordSnapshotQueryAttestationFromPB(cmd.RecordSnapshotQueryAttestation)
		return Command{RecordSnapshotQueryAttestation: &v}, nil
	case *pb.RaftCommand_PublishExecutorProfileTransition:
		v := PublishExecutorProfileTransitionFromPB(cmd.PublishExecutorProfileTransition)
		return Command{PublishExecutorProfileTransition: &v}, nil
	case *pb.RaftCommand_RecordSnapshotArtifactReady:
		v := RecordSnapshotArtifactReadyFromPB(cmd.RecordSnapshotArtifactReady)
		return Command{RecordSnapshotArtifactReady: &v}, nil
	case *pb.RaftCommand_ArtifactDisposition:
		v := ArtifactDispositionCmdFromPB(cmd.ArtifactDisposition)
		if err := requireSingleDispositionAction(v.Command.Action); err != nil {
			return Command{}, err
		}
		return Command{ArtifactDisposition: &v}, nil

	case *pb.RaftCommand_SeedLegacyTables:
		m := cmd.SeedLegacyTables
		return Command{SeedLegacyTables: &SeedLegacyTables{AtBlock: l2BlockRefValue(m.GetAtBlock()),
			Tables: legacyTablesFromPB(m.GetTables()), IndexerID: cloneUint64(m.IndexerId)}}, nil
	case *pb.RaftCommand_AddTable:
		m := cmd.AddTable
		return Command{AddTable: &AddTable{DatabaseID: m.GetDatabaseId(), TableID: m.GetTableId(),
			Created: l2EventRefValue(m.GetCreated()), Schema: l2EventRefValue(m.GetSchema()),
			SchemaVersion: m.GetSchemaVersion(), SchemaHash: m.GetSchemaHash(), SchemaJSON: m.GetSchemaJson(),
			OwnerIndexerID: cloneUint64(m.OwnerIndexerId)}}, nil
	case *pb.RaftCommand_RetireTables:
		m := cmd.RetireTables
		return Command{RetireTables: &RetireTables{DatabaseID: m.GetDatabaseId(),
			TableIDs: mapSlice(m.GetTableIds(), func(s string) string { return s }),
			Deleted:  l2EventRefValue(m.GetDeleted()), Reason: TableRetireReason(m.GetReason())}}, nil
	case *pb.RaftCommand_AdvanceL2Cursor:
		return Command{AdvanceL2Cursor: &AdvanceL2Cursor{To: l2BlockRefValue(cmd.AdvanceL2Cursor.GetTo())}}, nil
	case *pb.RaftCommand_RecordTablePurged:
		v := RecordTablePurgedFromRequest(cmd.RecordTablePurged)
		return Command{RecordTablePurged: &v}, nil

	case *pb.RaftCommand_SubmitStatement:
		return Command{SubmitStatement: &SubmitStatement{
			Envelope: EnvelopeFromPB(cmd.SubmitStatement.GetEnvelope()), NonMembershipProof: cmd.SubmitStatement.GetNonMembershipProof()}}, nil
	case *pb.RaftCommand_SealL3Block:
		return Command{SealL3Block: &SealL3Block{}}, nil
	case *pb.RaftCommand_MarkReplaying:
		return Command{MarkReplaying: &MarkReplaying{BlockSeq: cmd.MarkReplaying.GetBlockSeq()}}, nil
	case *pb.RaftCommand_RegisterRc:
		rc := cmd.RegisterRc.GetRc()
		if rc.GetSourceJws() != "" {
			return Command{}, errRequestOnly("RCRecord.source_jws", "RegisterRCCmd.source_jws")
		}
		return Command{RegisterRC: &RegisterRC{RC: RCFromPB(rc), SourceJWS: cmd.RegisterRc.GetSourceJws()}}, nil
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
		ack := cmd.RecordPromotionAck.GetAck()
		if ack.GetSourceJws() != "" {
			return Command{}, errRequestOnly("PromotionAck.source_jws", "RecordPromotionAckCmd.source_jws")
		}
		return Command{RecordPromotionAck: &RecordPromotionAck{Ack: PromotionAckFromPB(ack), SourceJWS: cmd.RecordPromotionAck.GetSourceJws()}}, nil
	case *pb.RaftCommand_PublishSafeSnapshot:
		return Command{PublishSafeSnapshot: &PublishSafeSnapshot{Manifest: ManifestFromPB(cmd.PublishSafeSnapshot.GetManifest())}}, nil
	case *pb.RaftCommand_ScheduleUnsafeCleanup:
		return Command{ScheduleUnsafeCleanup: &ScheduleUnsafeCleanup{
			Cleanup: CleanupFromPB(cmd.ScheduleUnsafeCleanup.GetCleanup()), AuthorityJWS: cmd.ScheduleUnsafeCleanup.GetAuthorityJws()}}, nil
	case *pb.RaftCommand_RecordCleanupAck:
		ack := cmd.RecordCleanupAck.GetAck()
		if ack.GetSourceJws() != "" {
			return Command{}, errRequestOnly("CleanupAck.source_jws", "RecordCleanupAckCmd.source_jws")
		}
		return Command{RecordCleanupAck: &RecordCleanupAck{Ack: CleanupAckFromPB(ack), SourceJWS: cmd.RecordCleanupAck.GetSourceJws()}}, nil
	case *pb.RaftCommand_OpenChallenge:
		return Command{OpenChallenge: &OpenChallenge{
			BlockSeq: cmd.OpenChallenge.GetBlockSeq(), Reason: cmd.OpenChallenge.GetReason(), OpenedBy: cmd.OpenChallenge.GetOpenedBy()}}, nil
	case *pb.RaftCommand_ResolveChallenge:
		return Command{ResolveChallenge: &ResolveChallenge{
			BlockSeq: cmd.ResolveChallenge.GetBlockSeq(), Verdict: ChallengeVerdict(cmd.ResolveChallenge.GetVerdict())}}, nil
	case *pb.RaftCommand_RegisterNode:
		// NodeRegistration.features is request-only (housegate spec 2026-10-09
		// §5.6): refuse it here so a buggy encoder fails on every voter alike.
		reg := cmd.RegisterNode.GetRegistration()
		if len(reg.GetFeatures()) != 0 {
			return Command{}, fmt.Errorf("wire: NodeRegistration.features is request-only and never part of a RaftCommand")
		}
		if reg.GetSignerJws() != "" || reg.GetEd25519Signature() != "" {
			return Command{}, errRequestOnly("NodeRegistration.signer_jws / ed25519_signature", "RegisterNodeCmd.signer_jws / ed25519_signature")
		}
		return Command{RegisterNode: &RegisterNode{Registration: RegistrationFromPB(reg),
			SignerJWS: cmd.RegisterNode.GetSignerJws(), Ed25519Signature: cmd.RegisterNode.GetEd25519Signature()}}, nil
	case *pb.RaftCommand_MarkActive:
		m := cmd.MarkActive
		return Command{MarkActive: &MarkActive{NodeID: m.GetNodeId(), RegistrationSeq: m.GetRegistrationSeq(),
			SignerJWS: m.GetSignerJws(), Ed25519Signature: m.GetEd25519Signature()}}, nil
	case *pb.RaftCommand_EvictNode:
		e := cmd.EvictNode
		return Command{EvictNode: &EvictNode{NodeID: e.GetNodeId(), Reason: e.GetReason(),
			ExpectedRegistrationSeq: e.GetExpectedRegistrationSeq(), AuthorityJWS: e.GetAuthorityJws()}}, nil
	default:
		return Command{}, fmt.Errorf("wire: RaftCommand has no command set")
	}
}

// errRequestOnly refuses a signature inside the request-only copy of a
// command's payload: the Raft command carries it in its own field, so a buggy
// encoder fails on every voter instead of being applied by some.
func errRequestOnly(field, carrier string) error {
	return fmt.Errorf("wire: %s is request-only; the Raft command carries it in %s", field, carrier)
}
