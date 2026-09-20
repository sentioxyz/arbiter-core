package wire

import (
	"encoding/json"
	"fmt"
	"reflect"

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
	Version          uint32                      `json:"version"`
	NetworkID        string                      `json:"network_id"`
	KeeperShardID    uint32                      `json:"keeper_shard_id"`
	ActorID          string                      `json:"actor_id"`
	RequestID        string                      `json:"request_id"`
	ExpectedRevision uint64                      `json:"expected_revision"`
	Action           ArtifactDispositionActionV1 `json:"action"`
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
	GrantReservation  *ArtifactDispositionGrantReservationV1
}

// canonicalSafeSnapshotManifest deliberately does not reuse replay's JSON
// encoding. The replay record predates C1 and uses omitempty for two ordinary
// fields; a C1 command must retain those zero values and empty arrays in its
// public root. Part storage locations are the one deliberate omission: design
// D5 makes StorageRefs fetch hints rather than identity, so relocating an
// artifact that still serves the same authenticated bytes must not change the
// command root of an otherwise identical registration.
type canonicalSafeSnapshotManifest struct {
	SnapshotID        string                   `json:"snapshot_id"`
	ParentSnapshotID  string                   `json:"parent_snapshot_id"`
	SafeBlockSeq      uint64                   `json:"safe_block_seq"`
	StateRoot         string                   `json:"state_root"`
	SchemaSnapshotID  string                   `json:"schema_snapshot_id"`
	SchemaRoot        string                   `json:"schema_root"`
	ExecutorProfileID string                   `json:"executor_profile_id"`
	DataRoot          string                   `json:"data_root"`
	ManifestRoot      string                   `json:"manifest_root"`
	Tables            []canonicalTableManifest `json:"tables"`
}

type canonicalTableManifest struct {
	TableID        string                       `json:"table_id"`
	SchemaHash     string                       `json:"schema_hash"`
	PartitionRoots []replay.PartitionCommitment `json:"partition_roots"`
	ActiveParts    []canonicalPartManifestEntry `json:"active_parts"`
}

type canonicalPartManifestEntry struct {
	TableID       string `json:"table_id"`
	PartitionID   string `json:"partition_id"`
	PartName      string `json:"part_name"`
	PartPhysHash  string `json:"part_phys_hash"`
	PartRowLtHash string `json:"part_row_lthash"`
	RowCount      uint64 `json:"row_count"`
	Bytes         uint64 `json:"bytes"`
}

func canonicalManifest(v replay.SafeSnapshotManifest) canonicalSafeSnapshotManifest {
	out := canonicalSafeSnapshotManifest{
		SnapshotID: v.SnapshotID, ParentSnapshotID: v.ParentSnapshotID, SafeBlockSeq: v.SafeBlockSeq,
		StateRoot: v.StateRoot, SchemaSnapshotID: v.SchemaSnapshotID, SchemaRoot: v.SchemaRoot,
		ExecutorProfileID: v.ExecutorProfileID, DataRoot: v.DataRoot, ManifestRoot: v.ManifestRoot,
		Tables: make([]canonicalTableManifest, len(v.Tables)),
	}
	for i, table := range v.Tables {
		out.Tables[i] = canonicalTableManifest{
			TableID: table.TableID, SchemaHash: table.SchemaHash, PartitionRoots: table.PartitionRoots,
			ActiveParts: make([]canonicalPartManifestEntry, len(table.ActiveParts)),
		}
		for j, part := range table.ActiveParts {
			out.Tables[i].ActiveParts[j] = canonicalPartManifestEntry{
				TableID: part.TableID, PartitionID: part.PartitionID, PartName: part.PartName,
				PartPhysHash: part.PartPhysHash, PartRowLtHash: part.PartRowLtHash, RowCount: part.RowCount,
				Bytes: part.Bytes,
			}
		}
	}
	return out
}

type canonicalSnapshotQueryReceipt struct {
	BlockSeq                  uint64                       `json:"block_seq"`
	StatementRoot             string                       `json:"statement_root"`
	InputRoot                 string                       `json:"input_root"`
	ReadSetRoot               string                       `json:"read_set_root"`
	ReadSnapshot              replay.SnapshotPin           `json:"read_snapshot"`
	SchemaSnapshotID          string                       `json:"schema_snapshot_id"`
	ExecutorProfileID         string                       `json:"executor_profile_id"`
	QueryProfileID            string                       `json:"query_profile_id"`
	ReservationID             string                       `json:"reservation_id"`
	FencingGeneration         uint64                       `json:"fencing_generation"`
	ExecutionOutcome          string                       `json:"execution_outcome"`
	AbortRecordRoot           string                       `json:"abort_record_root"`
	OutputRowCount            uint64                       `json:"output_row_count"`
	OutputRowsRoot            string                       `json:"output_rows_root"`
	SourceClaimRoot           string                       `json:"source_claim_root"`
	ComputedStateRoot         string                       `json:"computed_state_root"`
	MatchSourceRoot           bool                         `json:"match_source_root"`
	PartitionCommitmentsAfter []replay.PartitionCommitment `json:"partition_commitments_after"`
	AffectedParts             []canonicalPartManifestEntry `json:"affected_parts"`
	ReplayLogHash             string                       `json:"replay_log_hash"`
}

type canonicalSnapshotQueryAttestation struct {
	ReplicaID   string                        `json:"replica_id"`
	Receipt     canonicalSnapshotQueryReceipt `json:"receipt"`
	ReceiptHash string                        `json:"receipt_hash"`
	Signature   string                        `json:"signature"`
}

func canonicalPart(v replay.PartManifestEntry) canonicalPartManifestEntry {
	return canonicalPartManifestEntry{
		TableID: v.TableID, PartitionID: v.PartitionID, PartName: v.PartName,
		PartPhysHash: v.PartPhysHash, PartRowLtHash: v.PartRowLtHash, RowCount: v.RowCount,
		Bytes: v.Bytes,
	}
}

func canonicalAttestation(v replay.SnapshotQueryAttestation) canonicalSnapshotQueryAttestation {
	r := v.Receipt
	receipt := canonicalSnapshotQueryReceipt{
		BlockSeq: r.BlockSeq, StatementRoot: r.StatementRoot, InputRoot: r.InputRoot, ReadSetRoot: r.ReadSetRoot,
		ReadSnapshot: r.ReadSnapshot, SchemaSnapshotID: r.SchemaSnapshotID, ExecutorProfileID: r.ExecutorProfileID,
		QueryProfileID: r.QueryProfileID, ReservationID: r.ReservationID, FencingGeneration: r.FencingGeneration,
		ExecutionOutcome: r.ExecutionOutcome, AbortRecordRoot: r.AbortRecordRoot, OutputRowCount: r.OutputRowCount,
		OutputRowsRoot: r.OutputRowsRoot, SourceClaimRoot: r.SourceClaimRoot, ComputedStateRoot: r.ComputedStateRoot,
		MatchSourceRoot: r.MatchSourceRoot, PartitionCommitmentsAfter: r.PartitionCommitmentsAfter,
		AffectedParts: make([]canonicalPartManifestEntry, len(r.AffectedParts)), ReplayLogHash: r.ReplayLogHash,
	}
	for i, part := range r.AffectedParts {
		receipt.AffectedParts[i] = canonicalPart(part)
	}
	return canonicalSnapshotQueryAttestation{ReplicaID: v.ReplicaID, Receipt: receipt, ReceiptHash: v.ReceiptHash, Signature: v.Signature}
}

// MarshalJSON emits the action as an exact oneof object. This is deliberately
// separate from the protobuf mirror: protobuf needs all eleven Go fields while
// the command-root and administrator-JWS canonical form needs one member only.
func (v ArtifactDispositionActionV1) MarshalJSON() ([]byte, error) {
	if err := requireSingleDispositionAction(v); err != nil {
		return nil, err
	}
	switch {
	case v.BindPolicy != nil:
		return json.Marshal(struct {
			BindPolicy *ArtifactDispositionBindPolicyV1 `json:"bind_policy"`
		}{v.BindPolicy})
	case v.RegisterCandidate != nil:
		r := v.RegisterCandidate
		return json.Marshal(struct {
			RegisterCandidate struct {
				Pin                    replay.SnapshotPin            `json:"pin"`
				Manifest               canonicalSafeSnapshotManifest `json:"manifest"`
				PublisherID            string                        `json:"publisher_id"`
				RetentionPolicyID      string                        `json:"retention_policy_id"`
				PublicationReferenceID string                        `json:"publication_reference_id"`
				Origin                 ArtifactDispositionOriginV1   `json:"origin"`
			} `json:"register_candidate"`
		}{RegisterCandidate: struct {
			Pin                    replay.SnapshotPin            `json:"pin"`
			Manifest               canonicalSafeSnapshotManifest `json:"manifest"`
			PublisherID            string                        `json:"publisher_id"`
			RetentionPolicyID      string                        `json:"retention_policy_id"`
			PublicationReferenceID string                        `json:"publication_reference_id"`
			Origin                 ArtifactDispositionOriginV1   `json:"origin"`
		}{Pin: r.Pin, Manifest: canonicalManifest(r.Manifest), PublisherID: r.PublisherID, RetentionPolicyID: r.RetentionPolicyID, PublicationReferenceID: r.PublicationReferenceID, Origin: r.Origin}})
	case v.RecordReady != nil:
		return json.Marshal(struct {
			RecordReady *ArtifactDispositionRecordReadyV1 `json:"record_ready"`
		}{v.RecordReady})
	case v.PublishCandidate != nil:
		p := v.PublishCandidate
		return json.Marshal(struct {
			PublishCandidate struct {
				CandidateSeq       uint64                                    `json:"candidate_seq"`
				Manifest           canonicalSafeSnapshotManifest             `json:"manifest"`
				Transition         replay.ExecutorProfileTransition          `json:"transition"`
				TransitionReceipts []replay.ExecutorProfileTransitionReceipt `json:"transition_receipts"`
			} `json:"publish_candidate"`
		}{PublishCandidate: struct {
			CandidateSeq       uint64                                    `json:"candidate_seq"`
			Manifest           canonicalSafeSnapshotManifest             `json:"manifest"`
			Transition         replay.ExecutorProfileTransition          `json:"transition"`
			TransitionReceipts []replay.ExecutorProfileTransitionReceipt `json:"transition_receipts"`
		}{CandidateSeq: p.CandidateSeq, Manifest: canonicalManifest(p.Manifest), Transition: p.Transition, TransitionReceipts: p.TransitionReceipts}})
	case v.CancelCandidate != nil:
		return json.Marshal(struct {
			CancelCandidate *ArtifactDispositionCancelCandidateV1 `json:"cancel_candidate"`
		}{v.CancelCandidate})
	case v.BeginRetirement != nil:
		return json.Marshal(struct {
			BeginRetirement *ArtifactDispositionRetirementTargetV1 `json:"begin_retirement"`
		}{v.BeginRetirement})
	case v.FinishRetirement != nil:
		return json.Marshal(struct {
			FinishRetirement *ArtifactDispositionRetirementTargetV1 `json:"finish_retirement"`
		}{v.FinishRetirement})
	case v.AdmitUse != nil:
		return json.Marshal(struct {
			AdmitUse *ArtifactDispositionAdmitUseV1 `json:"admit_use"`
		}{v.AdmitUse})
	case v.CloseUse != nil:
		return json.Marshal(struct {
			CloseUse *ArtifactDispositionCloseUseV1 `json:"close_use"`
		}{v.CloseUse})
	case v.OpenChallenge != nil:
		o := v.OpenChallenge
		return json.Marshal(struct {
			OpenChallenge struct {
				Pin         replay.SnapshotPin                `json:"pin"`
				Origin      ArtifactDispositionOriginV1       `json:"origin"`
				Attestation canonicalSnapshotQueryAttestation `json:"attestation"`
				ReplayUse   ArtifactDispositionUseV1          `json:"replay_use"`
			} `json:"open_challenge"`
		}{OpenChallenge: struct {
			Pin         replay.SnapshotPin                `json:"pin"`
			Origin      ArtifactDispositionOriginV1       `json:"origin"`
			Attestation canonicalSnapshotQueryAttestation `json:"attestation"`
			ReplayUse   ArtifactDispositionUseV1          `json:"replay_use"`
		}{Pin: o.Pin, Origin: o.Origin, Attestation: canonicalAttestation(o.Attestation), ReplayUse: o.ReplayUse}})
	case v.GrantReservation != nil:
		return json.Marshal(struct {
			GrantReservation *ArtifactDispositionGrantReservationV1 `json:"grant_reservation"`
		}{v.GrantReservation})
	default:
		return json.Marshal(struct {
			ResolveObligation *ArtifactDispositionResolveObligationV1 `json:"resolve_obligation"`
		}{v.ResolveObligation})
	}
}

type ArtifactDispositionPolicyV1 struct {
	PolicyID               string   `json:"policy_id"`
	Kind                   string   `json:"kind"`
	AdministratorAddresses []string `json:"administrator_addresses"`
}

type ArtifactDispositionBindPolicyV1 struct {
	Policy ArtifactDispositionPolicyV1 `json:"policy"`
}

type ArtifactDispositionOriginV1 struct {
	Kind              string `json:"kind"`
	ParentSnapshotID  string `json:"parent_snapshot_id"`
	SafeBlockSeq      uint64 `json:"safe_block_seq"`
	ActivationID      string `json:"activation_id"`
	TransitionRoot    string `json:"transition_root"`
	ClientAccount     string `json:"client_account"`
	StatementID       string `json:"statement_id"`
	RequestID         string `json:"request_id"`
	ReservationID     string `json:"reservation_id"`
	FencingGeneration uint64 `json:"fencing_generation"`
	BlockSeq          uint64 `json:"block_seq"`
	StatementSeq      uint64 `json:"statement_seq"`
	StatementRoot     string `json:"statement_root"`
	InputRoot         string `json:"input_root"`
	UserJWSHash       string `json:"user_jws_hash"`
	ExecutionOutcome  string `json:"execution_outcome"`
	CandidateSeq      uint64 `json:"candidate_seq"`
}

type ArtifactDispositionUseV1 struct {
	ReferenceID               string                      `json:"reference_id"`
	Pin                       replay.SnapshotPin          `json:"pin"`
	PrincipalID               string                      `json:"principal_id"`
	Origin                    ArtifactDispositionOriginV1 `json:"origin"`
	ContinuationObligationSeq uint64                      `json:"continuation_obligation_seq"`
}

type ArtifactDispositionRegisterCandidateV1 struct {
	Pin                    replay.SnapshotPin          `json:"pin"`
	Manifest               replay.SafeSnapshotManifest `json:"manifest"`
	PublisherID            string                      `json:"publisher_id"`
	RetentionPolicyID      string                      `json:"retention_policy_id"`
	PublicationReferenceID string                      `json:"publication_reference_id"`
	Origin                 ArtifactDispositionOriginV1 `json:"origin"`
}

type ArtifactDispositionRecordReadyV1 struct {
	CandidateSeq uint64                                 `json:"candidate_seq"`
	Submission   replay.SnapshotArtifactReadySubmission `json:"submission"`
}

type ArtifactDispositionPublishCandidateV1 struct {
	CandidateSeq       uint64                                    `json:"candidate_seq"`
	Manifest           replay.SafeSnapshotManifest               `json:"manifest"`
	Transition         replay.ExecutorProfileTransition          `json:"transition"`
	TransitionReceipts []replay.ExecutorProfileTransitionReceipt `json:"transition_receipts"`
}

type ArtifactDispositionCancelCandidateV1 struct {
	CandidateSeq uint64 `json:"candidate_seq"`
	ReasonCode   string `json:"reason_code"`
}

type ArtifactDispositionRetirementTargetV1 struct {
	Pin               replay.SnapshotPin `json:"pin"`
	RetentionPolicyID string             `json:"retention_policy_id"`
}

type ArtifactDispositionAdmitUseV1 struct {
	Use ArtifactDispositionUseV1 `json:"use"`
}

type ArtifactDispositionCloseUseV1 struct {
	Use                      ArtifactDispositionUseV1 `json:"use"`
	ExpectedRegistryRevision uint64                   `json:"expected_registry_revision"`
}

type ArtifactDispositionOpenChallengeV1 struct {
	Pin         replay.SnapshotPin              `json:"pin"`
	Origin      ArtifactDispositionOriginV1     `json:"origin"`
	Attestation replay.SnapshotQueryAttestation `json:"attestation"`
	ReplayUse   ArtifactDispositionUseV1        `json:"replay_use"`
}

type ArtifactDispositionResolveObligationV1 struct {
	ObligationSeq uint64 `json:"obligation_seq"`
}

// ArtifactDispositionGrantReservationV1 is deliberately limited to the
// caller-owned, immutable query identity. Reservation IDs, fencing, pins,
// profiles, assignments, obligations, capacity and allocator/barrier
// decisions are server-owned transition results and have no wire fields.
type ArtifactDispositionGrantReservationV1 struct {
	ClientAccount        string `json:"client_account"`
	StatementID          string `json:"statement_id"`
	ControlBindingDigest string `json:"control_binding_digest"`
}

const artifactDispositionCommandDomain = "artifact-disposition-command-v1"

// ArtifactDispositionCommandRoot returns the public commitment for a
// disposition command. The enclosing AdministratorJWS and private Validation
// are intentionally absent from this type and can never affect its root.
func ArtifactDispositionCommandRoot(command ArtifactDispositionCommandV1) (string, error) {
	if err := requireSingleDispositionAction(command.Action); err != nil {
		return "", err
	}
	if err := rejectNilSlices(reflect.ValueOf(command)); err != nil {
		return "", fmt.Errorf("wire: artifact disposition canonical command: %w", err)
	}
	h, err := replay.CanonicalDigest(artifactDispositionCommandDomain, command)
	if err != nil {
		return "", fmt.Errorf("wire: hash artifact disposition command: %w", err)
	}
	return h, nil
}

// rejectNilSlices makes the command-root DTO unambiguous: all arrays have a
// concrete [] representation, never JSON null. It walks embedded replay
// records too, so a future action cannot silently reintroduce null arrays.
func rejectNilSlices(v reflect.Value) error {
	if !v.IsValid() {
		return nil
	}
	if v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return nil
		}
		return rejectNilSlices(v.Elem())
	}
	switch v.Kind() {
	case reflect.Slice:
		if v.IsNil() {
			return fmt.Errorf("array %s must be non-nil", v.Type())
		}
		for i := 0; i < v.Len(); i++ {
			if err := rejectNilSlices(v.Index(i)); err != nil {
				return err
			}
		}
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			if v.Type().Field(i).PkgPath != "" {
				continue
			}
			if err := rejectNilSlices(v.Field(i)); err != nil {
				return err
			}
		}
	}
	return nil
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
	case v.GrantReservation != nil:
		out.Action = &pb.ArtifactDispositionActionV1_GrantReservation{GrantReservation: &pb.ArtifactDispositionGrantReservationV1{
			ClientAccount: v.GrantReservation.ClientAccount, StatementId: v.GrantReservation.StatementID,
			ControlBindingDigest: v.GrantReservation.ControlBindingDigest}}
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
	case *pb.ArtifactDispositionActionV1_GrantReservation:
		return ArtifactDispositionActionV1{GrantReservation: &ArtifactDispositionGrantReservationV1{
			ClientAccount: a.GrantReservation.GetClientAccount(), StatementID: a.GrantReservation.GetStatementId(),
			ControlBindingDigest: a.GrantReservation.GetControlBindingDigest()}}
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
	if v.GrantReservation != nil {
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
