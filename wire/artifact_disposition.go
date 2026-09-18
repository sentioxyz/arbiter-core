package wire

import (
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"
)

// ArtifactDispositionCmd is the C1 Raft payload. Default-off: encoding it does
// not enable query admission.
type ArtifactDispositionCmd struct {
	Command          ArtifactDispositionCommandV1
	AdministratorJWS string
	Validation       ArtifactDispositionValidationV1
}

type ArtifactDispositionCommandV1 struct {
	Version          uint32
	NetworkID        string
	KeeperShardID    uint32
	ActorID          string
	RequestID        string
	ExpectedRevision uint64
	Action           ArtifactDispositionActionV1
}

type ArtifactDispositionActionV1 struct {
	BindPolicy        *ArtifactDispositionBindPolicyV1
	RegisterCandidate *ArtifactDispositionRegisterCandidateV1
	RecordReady       *ArtifactDispositionRecordReadyV1
	PublishCandidate  *ArtifactDispositionPublishCandidateV1
	CancelCandidate   *ArtifactDispositionCancelCandidateV1
	BeginRetirement   *ArtifactDispositionRetirementTargetV1
	FinishRetirement  *ArtifactDispositionRetirementTargetV1
	AdmitUse          *ArtifactDispositionAdmitUseV1
	CloseUse          *ArtifactDispositionCloseUseV1
	OpenChallenge     *ArtifactDispositionOpenChallengeV1
	ResolveObligation *ArtifactDispositionResolveObligationV1
}

type ArtifactDispositionPolicyV1 struct {
	PolicyID               string
	Kind                   string
	AdministratorAddresses []string
}

type ArtifactDispositionBindPolicyV1 struct {
	Policy ArtifactDispositionPolicyV1
}

type ArtifactDispositionOriginV1 struct {
	Kind              string
	ParentSnapshotID  string
	SafeBlockSeq      uint64
	ActivationID      string
	TransitionRoot    string
	ClientAccount     string
	StatementID       string
	RequestID         string
	ReservationID     string
	FencingGeneration uint64
	BlockSeq          uint64
	StatementSeq      uint64
	StatementRoot     string
	InputRoot         string
	UserJWSHash       string
	ExecutionOutcome  string
	CandidateSeq      uint64
}

type ArtifactDispositionUseV1 struct {
	ReferenceID               string
	Pin                       replay.SnapshotPin
	PrincipalID               string
	Origin                    ArtifactDispositionOriginV1
	ContinuationObligationSeq uint64
}

type ArtifactDispositionRegisterCandidateV1 struct {
	Pin                    replay.SnapshotPin
	Manifest               replay.SafeSnapshotManifest
	PublisherID            string
	RetentionPolicyID      string
	PublicationReferenceID string
	Origin                 ArtifactDispositionOriginV1
}

type ArtifactDispositionRecordReadyV1 struct {
	CandidateSeq uint64
	Submission   replay.SnapshotArtifactReadySubmission
}

type ArtifactDispositionPublishCandidateV1 struct {
	CandidateSeq       uint64
	Manifest           replay.SafeSnapshotManifest
	Transition         replay.ExecutorProfileTransition
	TransitionReceipts []replay.ExecutorProfileTransitionReceipt
}

type ArtifactDispositionCancelCandidateV1 struct {
	CandidateSeq uint64
	ReasonCode   string
}

type ArtifactDispositionRetirementTargetV1 struct {
	Pin               replay.SnapshotPin
	RetentionPolicyID string
}

type ArtifactDispositionAdmitUseV1 struct {
	Use ArtifactDispositionUseV1
}

type ArtifactDispositionCloseUseV1 struct {
	Use                      ArtifactDispositionUseV1
	ExpectedRegistryRevision uint64
}

type ArtifactDispositionOpenChallengeV1 struct {
	Pin         replay.SnapshotPin
	Origin      ArtifactDispositionOriginV1
	Attestation replay.SnapshotQueryAttestation
	ReplayUse   ArtifactDispositionUseV1
}

type ArtifactDispositionResolveObligationV1 struct {
	ObligationSeq uint64
}

type ArtifactDispositionRegistryObservationV1 struct {
	ReferenceID      string
	Pin              replay.SnapshotPin
	PrincipalID      string
	RegistryRevision uint64
	Observation      string
	RecordDigest     string
}

type ArtifactDispositionSourceObservationV1 struct {
	ObligationSeq             uint64
	SourceKind                string
	SourceIndex               uint64
	SourceIdentity            ArtifactDispositionOriginV1
	ReplacementObligationSeqs []uint64
}

type ArtifactDispositionValidationV1 struct {
	Version                  uint32
	CommandRoot              string
	ActorID                  string
	ActorRole                string
	AdministratorJWSHash     string
	RegistryObservations     []ArtifactDispositionRegistryObservationV1
	SourceObservations       []ArtifactDispositionSourceObservationV1
	CapacityAllowanceOrdinal uint64
}

func ArtifactDispositionCmdToPB(v ArtifactDispositionCmd) *pb.ArtifactDispositionCmd {
	return &pb.ArtifactDispositionCmd{
		Command:          ArtifactDispositionCommandToPB(v.Command),
		AdministratorJws: v.AdministratorJWS,
		Validation:       ArtifactDispositionValidationToPB(v.Validation),
	}
}

func ArtifactDispositionCmdFromPB(m *pb.ArtifactDispositionCmd) ArtifactDispositionCmd {
	if m == nil {
		return ArtifactDispositionCmd{}
	}
	return ArtifactDispositionCmd{
		Command:          ArtifactDispositionCommandFromPB(m.GetCommand()),
		AdministratorJWS: m.GetAdministratorJws(),
		Validation:       ArtifactDispositionValidationFromPB(m.GetValidation()),
	}
}

func ArtifactDispositionCommandToPB(v ArtifactDispositionCommandV1) *pb.ArtifactDispositionCommandV1 {
	return &pb.ArtifactDispositionCommandV1{
		Version:          v.Version,
		NetworkId:        v.NetworkID,
		KeeperShardId:    v.KeeperShardID,
		ActorId:          v.ActorID,
		RequestId:        v.RequestID,
		ExpectedRevision: v.ExpectedRevision,
		Action:           ArtifactDispositionActionToPB(v.Action),
	}
}

func ArtifactDispositionCommandFromPB(m *pb.ArtifactDispositionCommandV1) ArtifactDispositionCommandV1 {
	if m == nil {
		return ArtifactDispositionCommandV1{}
	}
	return ArtifactDispositionCommandV1{
		Version:          m.GetVersion(),
		NetworkID:        m.GetNetworkId(),
		KeeperShardID:    m.GetKeeperShardId(),
		ActorID:          m.GetActorId(),
		RequestID:        m.GetRequestId(),
		ExpectedRevision: m.GetExpectedRevision(),
		Action:           ArtifactDispositionActionFromPB(m.GetAction()),
	}
}

func ArtifactDispositionActionToPB(v ArtifactDispositionActionV1) *pb.ArtifactDispositionActionV1 {
	out := &pb.ArtifactDispositionActionV1{}
	switch {
	case v.BindPolicy != nil:
		out.Action = &pb.ArtifactDispositionActionV1_BindPolicy{BindPolicy: &pb.ArtifactDispositionBindPolicyV1{Policy: ArtifactDispositionPolicyToPB(v.BindPolicy.Policy)}}
	case v.RegisterCandidate != nil:
		r := v.RegisterCandidate
		out.Action = &pb.ArtifactDispositionActionV1_RegisterCandidate{RegisterCandidate: &pb.ArtifactDispositionRegisterCandidateV1{
			Pin: SnapshotPinToPB(r.Pin), Manifest: ManifestToPB(r.Manifest), PublisherId: r.PublisherID,
			RetentionPolicyId: r.RetentionPolicyID, PublicationReferenceId: r.PublicationReferenceID, Origin: ArtifactDispositionOriginToPB(r.Origin)}}
	case v.RecordReady != nil:
		out.Action = &pb.ArtifactDispositionActionV1_RecordReady{RecordReady: &pb.ArtifactDispositionRecordReadyV1{
			CandidateSeq: v.RecordReady.CandidateSeq, Submission: SnapshotArtifactReadySubmissionToPB(v.RecordReady.Submission)}}
	case v.PublishCandidate != nil:
		p := v.PublishCandidate
		out.Action = &pb.ArtifactDispositionActionV1_PublishCandidate{PublishCandidate: &pb.ArtifactDispositionPublishCandidateV1{
			CandidateSeq: p.CandidateSeq, Manifest: ManifestToPB(p.Manifest), Transition: ExecutorProfileTransitionToPB(p.Transition),
			TransitionReceipts: snapshotMap(p.TransitionReceipts, ExecutorProfileTransitionReceiptToPB)}}
	case v.CancelCandidate != nil:
		out.Action = &pb.ArtifactDispositionActionV1_CancelCandidate{CancelCandidate: &pb.ArtifactDispositionCancelCandidateV1{
			CandidateSeq: v.CancelCandidate.CandidateSeq, ReasonCode: v.CancelCandidate.ReasonCode}}
	case v.BeginRetirement != nil:
		out.Action = &pb.ArtifactDispositionActionV1_BeginRetirement{BeginRetirement: ArtifactDispositionRetirementTargetToPB(*v.BeginRetirement)}
	case v.FinishRetirement != nil:
		out.Action = &pb.ArtifactDispositionActionV1_FinishRetirement{FinishRetirement: ArtifactDispositionRetirementTargetToPB(*v.FinishRetirement)}
	case v.AdmitUse != nil:
		out.Action = &pb.ArtifactDispositionActionV1_AdmitUse{AdmitUse: &pb.ArtifactDispositionAdmitUseV1{Use: ArtifactDispositionUseToPB(v.AdmitUse.Use)}}
	case v.CloseUse != nil:
		out.Action = &pb.ArtifactDispositionActionV1_CloseUse{CloseUse: &pb.ArtifactDispositionCloseUseV1{
			Use: ArtifactDispositionUseToPB(v.CloseUse.Use), ExpectedRegistryRevision: v.CloseUse.ExpectedRegistryRevision}}
	case v.OpenChallenge != nil:
		o := v.OpenChallenge
		out.Action = &pb.ArtifactDispositionActionV1_OpenChallenge{OpenChallenge: &pb.ArtifactDispositionOpenChallengeV1{
			Pin: SnapshotPinToPB(o.Pin), Origin: ArtifactDispositionOriginToPB(o.Origin),
			Attestation: SnapshotQueryAttestationToPB(o.Attestation), ReplayUse: ArtifactDispositionUseToPB(o.ReplayUse)}}
	case v.ResolveObligation != nil:
		out.Action = &pb.ArtifactDispositionActionV1_ResolveObligation{ResolveObligation: &pb.ArtifactDispositionResolveObligationV1{ObligationSeq: v.ResolveObligation.ObligationSeq}}
	}
	return out
}

func ArtifactDispositionActionFromPB(m *pb.ArtifactDispositionActionV1) ArtifactDispositionActionV1 {
	if m == nil {
		return ArtifactDispositionActionV1{}
	}
	switch a := m.GetAction().(type) {
	case *pb.ArtifactDispositionActionV1_BindPolicy:
		return ArtifactDispositionActionV1{BindPolicy: &ArtifactDispositionBindPolicyV1{Policy: ArtifactDispositionPolicyFromPB(a.BindPolicy.GetPolicy())}}
	case *pb.ArtifactDispositionActionV1_RegisterCandidate:
		r := a.RegisterCandidate
		return ArtifactDispositionActionV1{RegisterCandidate: &ArtifactDispositionRegisterCandidateV1{
			Pin: SnapshotPinFromPB(r.GetPin()), Manifest: ManifestFromPB(r.GetManifest()), PublisherID: r.GetPublisherId(),
			RetentionPolicyID: r.GetRetentionPolicyId(), PublicationReferenceID: r.GetPublicationReferenceId(), Origin: ArtifactDispositionOriginFromPB(r.GetOrigin())}}
	case *pb.ArtifactDispositionActionV1_RecordReady:
		return ArtifactDispositionActionV1{RecordReady: &ArtifactDispositionRecordReadyV1{
			CandidateSeq: a.RecordReady.GetCandidateSeq(), Submission: SnapshotArtifactReadySubmissionFromPB(a.RecordReady.GetSubmission())}}
	case *pb.ArtifactDispositionActionV1_PublishCandidate:
		p := a.PublishCandidate
		return ArtifactDispositionActionV1{PublishCandidate: &ArtifactDispositionPublishCandidateV1{
			CandidateSeq: p.GetCandidateSeq(), Manifest: ManifestFromPB(p.GetManifest()), Transition: ExecutorProfileTransitionFromPB(p.GetTransition()),
			TransitionReceipts: snapshotMap(p.GetTransitionReceipts(), ExecutorProfileTransitionReceiptFromPB)}}
	case *pb.ArtifactDispositionActionV1_CancelCandidate:
		return ArtifactDispositionActionV1{CancelCandidate: &ArtifactDispositionCancelCandidateV1{
			CandidateSeq: a.CancelCandidate.GetCandidateSeq(), ReasonCode: a.CancelCandidate.GetReasonCode()}}
	case *pb.ArtifactDispositionActionV1_BeginRetirement:
		t := ArtifactDispositionRetirementTargetFromPB(a.BeginRetirement)
		return ArtifactDispositionActionV1{BeginRetirement: &t}
	case *pb.ArtifactDispositionActionV1_FinishRetirement:
		t := ArtifactDispositionRetirementTargetFromPB(a.FinishRetirement)
		return ArtifactDispositionActionV1{FinishRetirement: &t}
	case *pb.ArtifactDispositionActionV1_AdmitUse:
		return ArtifactDispositionActionV1{AdmitUse: &ArtifactDispositionAdmitUseV1{Use: ArtifactDispositionUseFromPB(a.AdmitUse.GetUse())}}
	case *pb.ArtifactDispositionActionV1_CloseUse:
		return ArtifactDispositionActionV1{CloseUse: &ArtifactDispositionCloseUseV1{
			Use: ArtifactDispositionUseFromPB(a.CloseUse.GetUse()), ExpectedRegistryRevision: a.CloseUse.GetExpectedRegistryRevision()}}
	case *pb.ArtifactDispositionActionV1_OpenChallenge:
		o := a.OpenChallenge
		return ArtifactDispositionActionV1{OpenChallenge: &ArtifactDispositionOpenChallengeV1{
			Pin: SnapshotPinFromPB(o.GetPin()), Origin: ArtifactDispositionOriginFromPB(o.GetOrigin()),
			Attestation: SnapshotQueryAttestationFromPB(o.GetAttestation()), ReplayUse: ArtifactDispositionUseFromPB(o.GetReplayUse())}}
	case *pb.ArtifactDispositionActionV1_ResolveObligation:
		return ArtifactDispositionActionV1{ResolveObligation: &ArtifactDispositionResolveObligationV1{ObligationSeq: a.ResolveObligation.GetObligationSeq()}}
	default:
		return ArtifactDispositionActionV1{}
	}
}

func ArtifactDispositionPolicyToPB(v ArtifactDispositionPolicyV1) *pb.ArtifactDispositionPolicyV1 {
	return &pb.ArtifactDispositionPolicyV1{PolicyId: v.PolicyID, Kind: v.Kind, AdministratorAddresses: snapshotClone(v.AdministratorAddresses)}
}
func ArtifactDispositionPolicyFromPB(m *pb.ArtifactDispositionPolicyV1) ArtifactDispositionPolicyV1 {
	if m == nil {
		return ArtifactDispositionPolicyV1{}
	}
	return ArtifactDispositionPolicyV1{PolicyID: m.GetPolicyId(), Kind: m.GetKind(), AdministratorAddresses: snapshotClone(m.GetAdministratorAddresses())}
}

func ArtifactDispositionOriginToPB(v ArtifactDispositionOriginV1) *pb.ArtifactDispositionOriginV1 {
	return &pb.ArtifactDispositionOriginV1{
		Kind: v.Kind, ParentSnapshotId: v.ParentSnapshotID, SafeBlockSeq: v.SafeBlockSeq, ActivationId: v.ActivationID,
		TransitionRoot: v.TransitionRoot, ClientAccount: v.ClientAccount, StatementId: v.StatementID, RequestId: v.RequestID,
		ReservationId: v.ReservationID, FencingGeneration: v.FencingGeneration, BlockSeq: v.BlockSeq, StatementSeq: v.StatementSeq,
		StatementRoot: v.StatementRoot, InputRoot: v.InputRoot, UserJwsHash: v.UserJWSHash, ExecutionOutcome: v.ExecutionOutcome,
		CandidateSeq: v.CandidateSeq,
	}
}
func ArtifactDispositionOriginFromPB(m *pb.ArtifactDispositionOriginV1) ArtifactDispositionOriginV1 {
	if m == nil {
		return ArtifactDispositionOriginV1{}
	}
	return ArtifactDispositionOriginV1{
		Kind: m.GetKind(), ParentSnapshotID: m.GetParentSnapshotId(), SafeBlockSeq: m.GetSafeBlockSeq(), ActivationID: m.GetActivationId(),
		TransitionRoot: m.GetTransitionRoot(), ClientAccount: m.GetClientAccount(), StatementID: m.GetStatementId(), RequestID: m.GetRequestId(),
		ReservationID: m.GetReservationId(), FencingGeneration: m.GetFencingGeneration(), BlockSeq: m.GetBlockSeq(), StatementSeq: m.GetStatementSeq(),
		StatementRoot: m.GetStatementRoot(), InputRoot: m.GetInputRoot(), UserJWSHash: m.GetUserJwsHash(), ExecutionOutcome: m.GetExecutionOutcome(),
		CandidateSeq: m.GetCandidateSeq(),
	}
}

func ArtifactDispositionUseToPB(v ArtifactDispositionUseV1) *pb.ArtifactDispositionUseV1 {
	return &pb.ArtifactDispositionUseV1{
		ReferenceId: v.ReferenceID, Pin: SnapshotPinToPB(v.Pin), PrincipalId: v.PrincipalID,
		Origin: ArtifactDispositionOriginToPB(v.Origin), ContinuationObligationSeq: v.ContinuationObligationSeq,
	}
}
func ArtifactDispositionUseFromPB(m *pb.ArtifactDispositionUseV1) ArtifactDispositionUseV1 {
	if m == nil {
		return ArtifactDispositionUseV1{}
	}
	return ArtifactDispositionUseV1{
		ReferenceID: m.GetReferenceId(), Pin: SnapshotPinFromPB(m.GetPin()), PrincipalID: m.GetPrincipalId(),
		Origin: ArtifactDispositionOriginFromPB(m.GetOrigin()), ContinuationObligationSeq: m.GetContinuationObligationSeq(),
	}
}

func ArtifactDispositionRetirementTargetToPB(v ArtifactDispositionRetirementTargetV1) *pb.ArtifactDispositionRetirementTargetV1 {
	return &pb.ArtifactDispositionRetirementTargetV1{Pin: SnapshotPinToPB(v.Pin), RetentionPolicyId: v.RetentionPolicyID}
}
func ArtifactDispositionRetirementTargetFromPB(m *pb.ArtifactDispositionRetirementTargetV1) ArtifactDispositionRetirementTargetV1 {
	if m == nil {
		return ArtifactDispositionRetirementTargetV1{}
	}
	return ArtifactDispositionRetirementTargetV1{Pin: SnapshotPinFromPB(m.GetPin()), RetentionPolicyID: m.GetRetentionPolicyId()}
}

func ArtifactDispositionValidationToPB(v ArtifactDispositionValidationV1) *pb.ArtifactDispositionValidationV1 {
	return &pb.ArtifactDispositionValidationV1{
		Version: v.Version, CommandRoot: v.CommandRoot, ActorId: v.ActorID, ActorRole: v.ActorRole,
		AdministratorJwsHash:     v.AdministratorJWSHash,
		RegistryObservations:     snapshotMap(v.RegistryObservations, ArtifactDispositionRegistryObservationToPB),
		SourceObservations:       snapshotMap(v.SourceObservations, ArtifactDispositionSourceObservationToPB),
		CapacityAllowanceOrdinal: v.CapacityAllowanceOrdinal,
	}
}
func ArtifactDispositionValidationFromPB(m *pb.ArtifactDispositionValidationV1) ArtifactDispositionValidationV1 {
	if m == nil {
		return ArtifactDispositionValidationV1{}
	}
	return ArtifactDispositionValidationV1{
		Version: m.GetVersion(), CommandRoot: m.GetCommandRoot(), ActorID: m.GetActorId(), ActorRole: m.GetActorRole(),
		AdministratorJWSHash:     m.GetAdministratorJwsHash(),
		RegistryObservations:     snapshotMap(m.GetRegistryObservations(), ArtifactDispositionRegistryObservationFromPB),
		SourceObservations:       snapshotMap(m.GetSourceObservations(), ArtifactDispositionSourceObservationFromPB),
		CapacityAllowanceOrdinal: m.GetCapacityAllowanceOrdinal(),
	}
}

func ArtifactDispositionRegistryObservationToPB(v ArtifactDispositionRegistryObservationV1) *pb.ArtifactDispositionRegistryObservationV1 {
	return &pb.ArtifactDispositionRegistryObservationV1{
		ReferenceId: v.ReferenceID, Pin: SnapshotPinToPB(v.Pin), PrincipalId: v.PrincipalID,
		RegistryRevision: v.RegistryRevision, Observation: v.Observation, RecordDigest: v.RecordDigest,
	}
}
func ArtifactDispositionRegistryObservationFromPB(m *pb.ArtifactDispositionRegistryObservationV1) ArtifactDispositionRegistryObservationV1 {
	if m == nil {
		return ArtifactDispositionRegistryObservationV1{}
	}
	return ArtifactDispositionRegistryObservationV1{
		ReferenceID: m.GetReferenceId(), Pin: SnapshotPinFromPB(m.GetPin()), PrincipalID: m.GetPrincipalId(),
		RegistryRevision: m.GetRegistryRevision(), Observation: m.GetObservation(), RecordDigest: m.GetRecordDigest(),
	}
}

func ArtifactDispositionSourceObservationToPB(v ArtifactDispositionSourceObservationV1) *pb.ArtifactDispositionSourceObservationV1 {
	return &pb.ArtifactDispositionSourceObservationV1{
		ObligationSeq: v.ObligationSeq, SourceKind: v.SourceKind, SourceIndex: v.SourceIndex,
		SourceIdentity: ArtifactDispositionOriginToPB(v.SourceIdentity), ReplacementObligationSeqs: snapshotClone(v.ReplacementObligationSeqs),
	}
}
func ArtifactDispositionSourceObservationFromPB(m *pb.ArtifactDispositionSourceObservationV1) ArtifactDispositionSourceObservationV1 {
	if m == nil {
		return ArtifactDispositionSourceObservationV1{}
	}
	return ArtifactDispositionSourceObservationV1{
		ObligationSeq: m.GetObligationSeq(), SourceKind: m.GetSourceKind(), SourceIndex: m.GetSourceIndex(),
		SourceIdentity: ArtifactDispositionOriginFromPB(m.GetSourceIdentity()), ReplacementObligationSeqs: snapshotClone(m.GetReplacementObligationSeqs()),
	}
}

func countArtifactDispositionActions(v ArtifactDispositionActionV1) int {
	n := 0
	if v.BindPolicy != nil {
		n++
	}
	if v.RegisterCandidate != nil {
		n++
	}
	if v.RecordReady != nil {
		n++
	}
	if v.PublishCandidate != nil {
		n++
	}
	if v.CancelCandidate != nil {
		n++
	}
	if v.BeginRetirement != nil {
		n++
	}
	if v.FinishRetirement != nil {
		n++
	}
	if v.AdmitUse != nil {
		n++
	}
	if v.CloseUse != nil {
		n++
	}
	if v.OpenChallenge != nil {
		n++
	}
	if v.ResolveObligation != nil {
		n++
	}
	return n
}

func requireSingleDispositionAction(v ArtifactDispositionActionV1) error {
	if n := countArtifactDispositionActions(v); n != 1 {
		return fmt.Errorf("wire: artifact disposition action must set exactly one variant, got %d", n)
	}
	return nil
}
