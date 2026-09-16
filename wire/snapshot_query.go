package wire

import (
	"bytes"
	"fmt"
	"github.com/housegate/housegate/pkg/replay"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// New snapshot-query arrays use [] in canonical JSON. Legacy mapSlice retains nil.
func snapshotMap[I, O any](in []I, f func(I) O) []O {
	out := make([]O, len(in))
	for i := range in {
		out[i] = f(in[i])
	}
	return out
}
func snapshotClone[T any](in []T) []T { return append([]T{}, in...) }
func snapshotOptional[I, O any](in *I, f func(I) *O) *O {
	if in == nil {
		return nil
	}
	return f(*in)
}
func snapshotFromOptional[I, O any](in *I, f func(*I) O) *O {
	if in == nil {
		return nil
	}
	out := f(in)
	return &out
}

// SnapshotPinToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotPinToPB(v replay.SnapshotPin) *pb.SnapshotPin {
	return &pb.SnapshotPin{
		NetworkId:        v.NetworkID,
		KeeperShardId:    v.KeeperShardID,
		SnapshotId:       v.SnapshotID,
		SafeBlockSeq:     v.SafeBlockSeq,
		ManifestRoot:     v.ManifestRoot,
		StateRoot:        v.StateRoot,
		SchemaSnapshotId: v.SchemaSnapshotID,
		SchemaRoot:       v.SchemaRoot,
	}
}

// SnapshotPinFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotPinFromPB(m *pb.SnapshotPin) replay.SnapshotPin {
	return replay.SnapshotPin{
		NetworkID:        m.GetNetworkId(),
		KeeperShardID:    m.GetKeeperShardId(),
		SnapshotID:       m.GetSnapshotId(),
		SafeBlockSeq:     m.GetSafeBlockSeq(),
		ManifestRoot:     m.GetManifestRoot(),
		StateRoot:        m.GetStateRoot(),
		SchemaSnapshotID: m.GetSchemaSnapshotId(),
		SchemaRoot:       m.GetSchemaRoot(),
	}
}

// SnapshotReadPartToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotReadPartToPB(v replay.SnapshotReadPart) *pb.SnapshotReadPart {
	return &pb.SnapshotReadPart{
		TableId:       v.TableID,
		PartitionId:   v.PartitionID,
		PartName:      v.PartName,
		PartPhysHash:  v.PartPhysHash,
		PartRowLthash: v.PartRowLtHash,
		RowCount:      v.RowCount,
		Bytes:         v.Bytes,
	}
}

// SnapshotReadPartFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotReadPartFromPB(m *pb.SnapshotReadPart) replay.SnapshotReadPart {
	return replay.SnapshotReadPart{
		TableID:       m.GetTableId(),
		PartitionID:   m.GetPartitionId(),
		PartName:      m.GetPartName(),
		PartPhysHash:  m.GetPartPhysHash(),
		PartRowLtHash: m.GetPartRowLthash(),
		RowCount:      m.GetRowCount(),
		Bytes:         m.GetBytes(),
	}
}

// SnapshotReadTableToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotReadTableToPB(v replay.SnapshotReadTable) *pb.SnapshotReadTable {
	return &pb.SnapshotReadTable{
		Database:       v.Database,
		Table:          v.Table,
		TableId:        v.TableID,
		SchemaHash:     v.SchemaHash,
		PartitionRoots: snapshotMap(v.PartitionRoots, partitionCommitmentToPB),
		ActiveParts:    snapshotMap(v.ActiveParts, SnapshotReadPartToPB),
	}
}

// SnapshotReadTableFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotReadTableFromPB(m *pb.SnapshotReadTable) replay.SnapshotReadTable {
	return replay.SnapshotReadTable{
		Database:       m.GetDatabase(),
		Table:          m.GetTable(),
		TableID:        m.GetTableId(),
		SchemaHash:     m.GetSchemaHash(),
		PartitionRoots: snapshotMap(m.GetPartitionRoots(), partitionCommitmentFromPB),
		ActiveParts:    snapshotMap(m.GetActiveParts(), SnapshotReadPartFromPB),
	}
}

// SnapshotReadSetToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotReadSetToPB(v replay.SnapshotReadSet) *pb.SnapshotReadSet {
	return &pb.SnapshotReadSet{
		ReadSnapshot: SnapshotPinToPB(v.ReadSnapshot),
		Tables:       snapshotMap(v.Tables, SnapshotReadTableToPB),
	}
}

// SnapshotReadSetFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotReadSetFromPB(m *pb.SnapshotReadSet) replay.SnapshotReadSet {
	return replay.SnapshotReadSet{
		ReadSnapshot: SnapshotPinFromPB(m.GetReadSnapshot()),
		Tables:       snapshotMap(m.GetTables(), SnapshotReadTableFromPB),
	}
}

// SnapshotQueryBindingToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryBindingToPB(v replay.SnapshotQueryBinding) *pb.SnapshotQueryBinding {
	return &pb.SnapshotQueryBinding{
		EnvelopeVersion:   v.EnvelopeVersion,
		InputKind:         v.InputKind,
		ClientAccount:     v.ClientAccount,
		StatementId:       v.StatementID,
		StatementKind:     v.StatementKind,
		NetworkId:         v.NetworkID,
		KeeperShardId:     v.KeeperShardID,
		SqlHash:           v.SQLHash,
		SettingsHash:      v.SettingsHash,
		TargetTableId:     v.TargetTableID,
		SchemaHash:        v.SchemaHash,
		RowIdProfileId:    v.RowIDProfileID,
		ClientRevision:    v.ClientRevision,
		ReadSnapshot:      SnapshotPinToPB(v.ReadSnapshot),
		ReadSetRoot:       v.ReadSetRoot,
		SchemaSnapshotId:  v.SchemaSnapshotID,
		SchemaRoot:        v.SchemaRoot,
		LogicalDatabase:   v.LogicalDatabase,
		QueryProfileId:    v.QueryProfileID,
		ExecutorProfileId: v.ExecutorProfileID,
		ReservationId:     v.ReservationID,
		FencingGeneration: v.FencingGeneration,
	}
}

// SnapshotQueryBindingFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryBindingFromPB(m *pb.SnapshotQueryBinding) replay.SnapshotQueryBinding {
	return replay.SnapshotQueryBinding{
		EnvelopeVersion:   m.GetEnvelopeVersion(),
		InputKind:         m.GetInputKind(),
		ClientAccount:     m.GetClientAccount(),
		StatementID:       m.GetStatementId(),
		StatementKind:     m.GetStatementKind(),
		NetworkID:         m.GetNetworkId(),
		KeeperShardID:     m.GetKeeperShardId(),
		SQLHash:           m.GetSqlHash(),
		SettingsHash:      m.GetSettingsHash(),
		TargetTableID:     m.GetTargetTableId(),
		SchemaHash:        m.GetSchemaHash(),
		RowIDProfileID:    m.GetRowIdProfileId(),
		ClientRevision:    m.GetClientRevision(),
		ReadSnapshot:      SnapshotPinFromPB(m.GetReadSnapshot()),
		ReadSetRoot:       m.GetReadSetRoot(),
		SchemaSnapshotID:  m.GetSchemaSnapshotId(),
		SchemaRoot:        m.GetSchemaRoot(),
		LogicalDatabase:   m.GetLogicalDatabase(),
		QueryProfileID:    m.GetQueryProfileId(),
		ExecutorProfileID: m.GetExecutorProfileId(),
		ReservationID:     m.GetReservationId(),
		FencingGeneration: m.GetFencingGeneration(),
	}
}

// SnapshotQueryInputToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryInputToPB(v replay.SnapshotQueryInput) *pb.SnapshotQueryInput {
	return &pb.SnapshotQueryInput{
		Binding: SnapshotQueryBindingToPB(v.Binding),
		Sql:     v.SQL,
		ReadSet: SnapshotReadSetToPB(v.ReadSet),
	}
}

// SnapshotQueryInputFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryInputFromPB(m *pb.SnapshotQueryInput) replay.SnapshotQueryInput {
	return replay.SnapshotQueryInput{
		Binding: SnapshotQueryBindingFromPB(m.GetBinding()),
		SQL:     m.GetSql(),
		ReadSet: SnapshotReadSetFromPB(m.GetReadSet()),
	}
}

// SnapshotQueryEnvelopeToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryEnvelopeToPB(v replay.SnapshotQueryEnvelope) *pb.SnapshotQueryEnvelope {
	return &pb.SnapshotQueryEnvelope{
		Input:     SnapshotQueryInputToPB(v.Input),
		InputRoot: v.InputRoot,
		UserJws:   v.UserJWS,
	}
}

// SnapshotQueryEnvelopeFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryEnvelopeFromPB(m *pb.SnapshotQueryEnvelope) replay.SnapshotQueryEnvelope {
	return replay.SnapshotQueryEnvelope{
		Input:     SnapshotQueryInputFromPB(m.GetInput()),
		InputRoot: m.GetInputRoot(),
		UserJWS:   m.GetUserJws(),
	}
}

// SnapshotQueryReservationToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryReservationToPB(v replay.SnapshotQueryReservation) *pb.SnapshotQueryReservation {
	return &pb.SnapshotQueryReservation{
		ReservationId:     v.ReservationID,
		FencingGeneration: v.FencingGeneration,
		ClientAccount:     v.ClientAccount,
		StatementId:       v.StatementID,
		ReadSnapshot:      SnapshotPinToPB(v.ReadSnapshot),
		ExecutorProfileId: v.ExecutorProfileID,
		QueryProfileId:    v.QueryProfileID,
		ActivationId:      v.ActivationID,
	}
}

// SnapshotQueryReservationFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryReservationFromPB(m *pb.SnapshotQueryReservation) replay.SnapshotQueryReservation {
	return replay.SnapshotQueryReservation{
		ReservationID:     m.GetReservationId(),
		FencingGeneration: m.GetFencingGeneration(),
		ClientAccount:     m.GetClientAccount(),
		StatementID:       m.GetStatementId(),
		ReadSnapshot:      SnapshotPinFromPB(m.GetReadSnapshot()),
		ExecutorProfileID: m.GetExecutorProfileId(),
		QueryProfileID:    m.GetQueryProfileId(),
		ActivationID:      m.GetActivationId(),
	}
}

// SnapshotQueryReservationStatusToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryReservationStatusToPB(v replay.SnapshotQueryReservationStatus) *pb.SnapshotQueryReservationStatus {
	return &pb.SnapshotQueryReservationStatus{
		Version:           v.Version,
		Found:             v.Found,
		State:             v.State,
		RequestId:         v.RequestID,
		ClientAccount:     v.ClientAccount,
		StatementId:       v.StatementID,
		FencingGeneration: v.FencingGeneration,
		Reservation:       snapshotOptional(v.Reservation, SnapshotQueryReservationToPB),
		BlockSeq:          v.BlockSeq,
		TerminalProof:     bytes.Clone(v.TerminalProof),
	}
}

// SnapshotQueryReservationStatusFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryReservationStatusFromPB(m *pb.SnapshotQueryReservationStatus) replay.SnapshotQueryReservationStatus {
	return replay.SnapshotQueryReservationStatus{
		Version:           m.GetVersion(),
		Found:             m.GetFound(),
		State:             m.GetState(),
		RequestID:         m.GetRequestId(),
		ClientAccount:     m.GetClientAccount(),
		StatementID:       m.GetStatementId(),
		FencingGeneration: m.GetFencingGeneration(),
		Reservation:       snapshotFromOptional(m.GetReservation(), SnapshotQueryReservationFromPB),
		BlockSeq:          m.GetBlockSeq(),
		TerminalProof:     bytes.Clone(m.GetTerminalProof()),
	}
}

// SnapshotQueryStatementToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryStatementToPB(v replay.SnapshotQueryStatement) *pb.SnapshotQueryStatement {
	return &pb.SnapshotQueryStatement{
		StatementSeq: v.StatementSeq,
		Envelope:     SnapshotQueryEnvelopeToPB(v.Envelope),
	}
}

// SnapshotQueryStatementFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryStatementFromPB(m *pb.SnapshotQueryStatement) replay.SnapshotQueryStatement {
	return replay.SnapshotQueryStatement{
		StatementSeq: m.GetStatementSeq(),
		Envelope:     SnapshotQueryEnvelopeFromPB(m.GetEnvelope()),
	}
}

// SnapshotQueryJobToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryJobToPB(v replay.SnapshotQueryJob) *pb.SnapshotQueryJob {
	return &pb.SnapshotQueryJob{
		BlockSeq:           v.BlockSeq,
		PrevSafeSnapshotId: v.PrevSafeSnapshotID,
		PrevStateRoot:      v.PrevStateRoot,
		SchemaSnapshotId:   v.SchemaSnapshotID,
		ExecutorProfileId:  v.ExecutorProfileID,
		QueryProfileId:     v.QueryProfileID,
		Reservation:        SnapshotQueryReservationToPB(v.Reservation),
		Statement:          SnapshotQueryStatementToPB(v.Statement),
		SourceClaimRoot:    v.SourceClaimRoot,
		SourceClaim:        snapshotOptional(v.SourceClaim, SnapshotQueryClaimToPB),
	}
}

// SnapshotQueryJobFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryJobFromPB(m *pb.SnapshotQueryJob) replay.SnapshotQueryJob {
	return replay.SnapshotQueryJob{
		BlockSeq:           m.GetBlockSeq(),
		PrevSafeSnapshotID: m.GetPrevSafeSnapshotId(),
		PrevStateRoot:      m.GetPrevStateRoot(),
		SchemaSnapshotID:   m.GetSchemaSnapshotId(),
		ExecutorProfileID:  m.GetExecutorProfileId(),
		QueryProfileID:     m.GetQueryProfileId(),
		Reservation:        SnapshotQueryReservationFromPB(m.GetReservation()),
		Statement:          SnapshotQueryStatementFromPB(m.GetStatement()),
		SourceClaimRoot:    m.GetSourceClaimRoot(),
		SourceClaim:        snapshotFromOptional(m.GetSourceClaim(), SnapshotQueryClaimFromPB),
	}
}

// SnapshotQueryEvidenceToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryEvidenceToPB(v replay.SnapshotQueryEvidence) *pb.SnapshotQueryEvidence {
	return &pb.SnapshotQueryEvidence{
		ExecutionOutcome: v.ExecutionOutcome,
		OutputRowCount:   v.OutputRowCount,
		OutputRowsRoot:   v.OutputRowsRoot,
	}
}

// SnapshotQueryEvidenceFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryEvidenceFromPB(m *pb.SnapshotQueryEvidence) replay.SnapshotQueryEvidence {
	return replay.SnapshotQueryEvidence{
		ExecutionOutcome: m.GetExecutionOutcome(),
		OutputRowCount:   m.GetOutputRowCount(),
		OutputRowsRoot:   m.GetOutputRowsRoot(),
	}
}

// SnapshotQueryReceiptToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryReceiptToPB(v replay.SnapshotQueryReceipt) *pb.SnapshotQueryReceipt {
	return &pb.SnapshotQueryReceipt{
		BlockSeq:                  v.BlockSeq,
		StatementRoot:             v.StatementRoot,
		InputRoot:                 v.InputRoot,
		ReadSetRoot:               v.ReadSetRoot,
		ReadSnapshot:              SnapshotPinToPB(v.ReadSnapshot),
		SchemaSnapshotId:          v.SchemaSnapshotID,
		ExecutorProfileId:         v.ExecutorProfileID,
		QueryProfileId:            v.QueryProfileID,
		ReservationId:             v.ReservationID,
		FencingGeneration:         v.FencingGeneration,
		ExecutionOutcome:          v.ExecutionOutcome,
		AbortRecordRoot:           v.AbortRecordRoot,
		OutputRowCount:            v.OutputRowCount,
		OutputRowsRoot:            v.OutputRowsRoot,
		SourceClaimRoot:           v.SourceClaimRoot,
		ComputedStateRoot:         v.ComputedStateRoot,
		MatchSourceRoot:           v.MatchSourceRoot,
		PartitionCommitmentsAfter: snapshotMap(v.PartitionCommitmentsAfter, partitionCommitmentToPB),
		AffectedParts:             snapshotMap(v.AffectedParts, partManifestEntryToPB),
		ReplayLogHash:             v.ReplayLogHash,
	}
}

// SnapshotQueryReceiptFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryReceiptFromPB(m *pb.SnapshotQueryReceipt) replay.SnapshotQueryReceipt {
	return replay.SnapshotQueryReceipt{
		BlockSeq:                  m.GetBlockSeq(),
		StatementRoot:             m.GetStatementRoot(),
		InputRoot:                 m.GetInputRoot(),
		ReadSetRoot:               m.GetReadSetRoot(),
		ReadSnapshot:              SnapshotPinFromPB(m.GetReadSnapshot()),
		SchemaSnapshotID:          m.GetSchemaSnapshotId(),
		ExecutorProfileID:         m.GetExecutorProfileId(),
		QueryProfileID:            m.GetQueryProfileId(),
		ReservationID:             m.GetReservationId(),
		FencingGeneration:         m.GetFencingGeneration(),
		ExecutionOutcome:          m.GetExecutionOutcome(),
		AbortRecordRoot:           m.GetAbortRecordRoot(),
		OutputRowCount:            m.GetOutputRowCount(),
		OutputRowsRoot:            m.GetOutputRowsRoot(),
		SourceClaimRoot:           m.GetSourceClaimRoot(),
		ComputedStateRoot:         m.GetComputedStateRoot(),
		MatchSourceRoot:           m.GetMatchSourceRoot(),
		PartitionCommitmentsAfter: snapshotMap(m.GetPartitionCommitmentsAfter(), partitionCommitmentFromPB),
		AffectedParts:             snapshotMap(m.GetAffectedParts(), partManifestEntryFromPB),
		ReplayLogHash:             m.GetReplayLogHash(),
	}
}

// SnapshotQueryAttestationToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryAttestationToPB(v replay.SnapshotQueryAttestation) *pb.SnapshotQueryAttestation {
	return &pb.SnapshotQueryAttestation{
		ReplicaId:   v.ReplicaID,
		Receipt:     SnapshotQueryReceiptToPB(v.Receipt),
		ReceiptHash: v.ReceiptHash,
		Signature:   v.Signature,
	}
}

// SnapshotQueryAttestationFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryAttestationFromPB(m *pb.SnapshotQueryAttestation) replay.SnapshotQueryAttestation {
	return replay.SnapshotQueryAttestation{
		ReplicaID:   m.GetReplicaId(),
		Receipt:     SnapshotQueryReceiptFromPB(m.GetReceipt()),
		ReceiptHash: m.GetReceiptHash(),
		Signature:   m.GetSignature(),
	}
}

// SnapshotQuerySubmitResultToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQuerySubmitResultToPB(v replay.SnapshotQuerySubmitResult) *pb.SnapshotQuerySubmitResult {
	return &pb.SnapshotQuerySubmitResult{
		AdmissionCode: v.AdmissionCode,
		Message:       v.Message,
		StatementSeq:  v.StatementSeq,
		BlockSeq:      v.BlockSeq,
		SourceNode:    v.SourceNode,
		InputRoot:     v.InputRoot,
		Reservation:   SnapshotQueryReservationToPB(v.Reservation),
	}
}

// SnapshotQuerySubmitResultFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQuerySubmitResultFromPB(m *pb.SnapshotQuerySubmitResult) replay.SnapshotQuerySubmitResult {
	return replay.SnapshotQuerySubmitResult{
		AdmissionCode: m.GetAdmissionCode(),
		Message:       m.GetMessage(),
		StatementSeq:  m.GetStatementSeq(),
		BlockSeq:      m.GetBlockSeq(),
		SourceNode:    m.GetSourceNode(),
		InputRoot:     m.GetInputRoot(),
		Reservation:   SnapshotQueryReservationFromPB(m.GetReservation()),
	}
}

// SnapshotQueryStatusToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryStatusToPB(v replay.SnapshotQueryStatus) *pb.SnapshotQueryStatus {
	return &pb.SnapshotQueryStatus{
		Version:          v.Version,
		Found:            v.Found,
		Accepted:         SnapshotQuerySubmitResultToPB(v.Accepted),
		Lifecycle:        v.Lifecycle,
		ExecutionOutcome: v.ExecutionOutcome,
		TerminalProof:    bytes.Clone(v.TerminalProof),
	}
}

// SnapshotQueryStatusFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryStatusFromPB(m *pb.SnapshotQueryStatus) replay.SnapshotQueryStatus {
	return replay.SnapshotQueryStatus{
		Version:          m.GetVersion(),
		Found:            m.GetFound(),
		Accepted:         SnapshotQuerySubmitResultFromPB(m.GetAccepted()),
		Lifecycle:        m.GetLifecycle(),
		ExecutionOutcome: m.GetExecutionOutcome(),
		TerminalProof:    bytes.Clone(m.GetTerminalProof()),
	}
}

// ActiveQueryPolicyToPB converts the complete new-lane record without hashing or authenticating it.
func ActiveQueryPolicyToPB(v replay.ActiveQueryPolicy) *pb.ActiveQueryPolicy {
	return &pb.ActiveQueryPolicy{
		ActivationId:       v.ActivationID,
		NetworkId:          v.NetworkID,
		KeeperShardId:      v.KeeperShardID,
		ActivationBlockSeq: v.ActivationBlockSeq,
		ExecutorProfileId:  v.ExecutorProfileID,
		QueryProfileId:     v.QueryProfileID,
		Enabled:            v.Enabled,
	}
}

// ActiveQueryPolicyFromPB converts the complete new-lane record without hashing or authenticating it.
func ActiveQueryPolicyFromPB(m *pb.ActiveQueryPolicy) replay.ActiveQueryPolicy {
	return replay.ActiveQueryPolicy{
		ActivationID:       m.GetActivationId(),
		NetworkID:          m.GetNetworkId(),
		KeeperShardID:      m.GetKeeperShardId(),
		ActivationBlockSeq: m.GetActivationBlockSeq(),
		ExecutorProfileID:  m.GetExecutorProfileId(),
		QueryProfileID:     m.GetQueryProfileId(),
		Enabled:            m.GetEnabled(),
	}
}

// ExecutorProfileTransitionToPB converts the complete new-lane record without hashing or authenticating it.
func ExecutorProfileTransitionToPB(v replay.ExecutorProfileTransition) *pb.ExecutorProfileTransition {
	return &pb.ExecutorProfileTransition{
		NetworkId:            v.NetworkID,
		KeeperShardId:        v.KeeperShardID,
		PrevSnapshotId:       v.PrevSnapshotID,
		PrevStateRoot:        v.PrevStateRoot,
		OldExecutorProfileId: v.OldExecutorProfileID,
		NewExecutorProfileId: v.NewExecutorProfileID,
		SchemaSnapshotId:     v.SchemaSnapshotID,
		SchemaRoot:           v.SchemaRoot,
		DataRoot:             v.DataRoot,
		NextSnapshotId:       v.NextSnapshotID,
		NextStateRoot:        v.NextStateRoot,
		NextManifestRoot:     v.NextManifestRoot,
		Activation:           ActiveQueryPolicyToPB(v.Activation),
	}
}

// ExecutorProfileTransitionFromPB converts the complete new-lane record without hashing or authenticating it.
func ExecutorProfileTransitionFromPB(m *pb.ExecutorProfileTransition) replay.ExecutorProfileTransition {
	return replay.ExecutorProfileTransition{
		NetworkID:            m.GetNetworkId(),
		KeeperShardID:        m.GetKeeperShardId(),
		PrevSnapshotID:       m.GetPrevSnapshotId(),
		PrevStateRoot:        m.GetPrevStateRoot(),
		OldExecutorProfileID: m.GetOldExecutorProfileId(),
		NewExecutorProfileID: m.GetNewExecutorProfileId(),
		SchemaSnapshotID:     m.GetSchemaSnapshotId(),
		SchemaRoot:           m.GetSchemaRoot(),
		DataRoot:             m.GetDataRoot(),
		NextSnapshotID:       m.GetNextSnapshotId(),
		NextStateRoot:        m.GetNextStateRoot(),
		NextManifestRoot:     m.GetNextManifestRoot(),
		Activation:           ActiveQueryPolicyFromPB(m.GetActivation()),
	}
}

// ExecutorProfileTransitionReceiptToPB converts the complete new-lane record without hashing or authenticating it.
func ExecutorProfileTransitionReceiptToPB(v replay.ExecutorProfileTransitionReceipt) *pb.ExecutorProfileTransitionReceipt {
	return &pb.ExecutorProfileTransitionReceipt{
		TransitionRoot: v.TransitionRoot,
		ReplicaId:      v.ReplicaID,
		Signature:      v.Signature,
	}
}

// ExecutorProfileTransitionReceiptFromPB converts the complete new-lane record without hashing or authenticating it.
func ExecutorProfileTransitionReceiptFromPB(m *pb.ExecutorProfileTransitionReceipt) replay.ExecutorProfileTransitionReceipt {
	return replay.ExecutorProfileTransitionReceipt{
		TransitionRoot: m.GetTransitionRoot(),
		ReplicaID:      m.GetReplicaId(),
		Signature:      m.GetSignature(),
	}
}

// SnapshotArtifactReadyToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotArtifactReadyToPB(v replay.SnapshotArtifactReady) *pb.SnapshotArtifactReady {
	return &pb.SnapshotArtifactReady{
		SnapshotId:        v.SnapshotID,
		ManifestRoot:      v.ManifestRoot,
		SchemaRoot:        v.SchemaRoot,
		ArtifactSetRoot:   v.ArtifactSetRoot,
		PublisherId:       v.PublisherID,
		RetentionPolicyId: v.RetentionPolicyID,
	}
}

// SnapshotArtifactReadyFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotArtifactReadyFromPB(m *pb.SnapshotArtifactReady) replay.SnapshotArtifactReady {
	return replay.SnapshotArtifactReady{
		SnapshotID:        m.GetSnapshotId(),
		ManifestRoot:      m.GetManifestRoot(),
		SchemaRoot:        m.GetSchemaRoot(),
		ArtifactSetRoot:   m.GetArtifactSetRoot(),
		PublisherID:       m.GetPublisherId(),
		RetentionPolicyID: m.GetRetentionPolicyId(),
	}
}

// SnapshotQueryAbortRecordToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryAbortRecordToPB(v replay.SnapshotQueryAbortRecord) *pb.SnapshotQueryAbortRecord {
	return &pb.SnapshotQueryAbortRecord{
		BlockSeq:                 v.BlockSeq,
		StatementId:              v.StatementID,
		InputRoot:                v.InputRoot,
		ReservationId:            v.ReservationID,
		FencingGeneration:        v.FencingGeneration,
		ReasonCode:               v.ReasonCode,
		CleanupAuthorizationRoot: v.CleanupAuthorizationRoot,
		PrevSnapshotId:           v.PrevSnapshotID,
		NextSnapshotId:           v.NextSnapshotID,
	}
}

// SnapshotQueryAbortRecordFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryAbortRecordFromPB(m *pb.SnapshotQueryAbortRecord) replay.SnapshotQueryAbortRecord {
	return replay.SnapshotQueryAbortRecord{
		BlockSeq:                 m.GetBlockSeq(),
		StatementID:              m.GetStatementId(),
		InputRoot:                m.GetInputRoot(),
		ReservationID:            m.GetReservationId(),
		FencingGeneration:        m.GetFencingGeneration(),
		ReasonCode:               m.GetReasonCode(),
		CleanupAuthorizationRoot: m.GetCleanupAuthorizationRoot(),
		PrevSnapshotID:           m.GetPrevSnapshotId(),
		NextSnapshotID:           m.GetNextSnapshotId(),
	}
}

// SnapshotQueryClaimToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryClaimToPB(v replay.SnapshotQueryClaim) *pb.SnapshotQueryClaim {
	return &pb.SnapshotQueryClaim{
		SourceNode:                v.SourceNode,
		StatementId:               v.StatementID,
		StatementSeq:              v.StatementSeq,
		BlockSeq:                  v.BlockSeq,
		InputRoot:                 v.InputRoot,
		ReservationId:             v.ReservationID,
		FencingGeneration:         v.FencingGeneration,
		ExecutionOutcome:          v.ExecutionOutcome,
		OutputRowCount:            v.OutputRowCount,
		OutputRowsRoot:            v.OutputRowsRoot,
		ComputedStateRoot:         v.ComputedStateRoot,
		PartitionDeltas:           snapshotMap(v.PartitionDeltas, partitionCommitmentToPB),
		PartitionCommitmentsAfter: snapshotMap(v.PartitionCommitmentsAfter, partitionCommitmentToPB),
		CandidateParts:            snapshotMap(v.CandidateParts, SnapshotReadPartToPB),
	}
}

// SnapshotQueryClaimFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryClaimFromPB(m *pb.SnapshotQueryClaim) replay.SnapshotQueryClaim {
	return replay.SnapshotQueryClaim{
		SourceNode:                m.GetSourceNode(),
		StatementID:               m.GetStatementId(),
		StatementSeq:              m.GetStatementSeq(),
		BlockSeq:                  m.GetBlockSeq(),
		InputRoot:                 m.GetInputRoot(),
		ReservationID:             m.GetReservationId(),
		FencingGeneration:         m.GetFencingGeneration(),
		ExecutionOutcome:          m.GetExecutionOutcome(),
		OutputRowCount:            m.GetOutputRowCount(),
		OutputRowsRoot:            m.GetOutputRowsRoot(),
		ComputedStateRoot:         m.GetComputedStateRoot(),
		PartitionDeltas:           snapshotMap(m.GetPartitionDeltas(), partitionCommitmentFromPB),
		PartitionCommitmentsAfter: snapshotMap(m.GetPartitionCommitmentsAfter(), partitionCommitmentFromPB),
		CandidateParts:            snapshotMap(m.GetCandidateParts(), SnapshotReadPartFromPB),
	}
}

// ProfileSettingToPB converts the complete new-lane record without hashing or authenticating it.
func ProfileSettingToPB(v replay.ProfileSetting) *pb.ProfileSetting {
	return &pb.ProfileSetting{
		Name:  v.Name,
		Value: v.Value,
	}
}

// ProfileSettingFromPB converts the complete new-lane record without hashing or authenticating it.
func ProfileSettingFromPB(m *pb.ProfileSetting) replay.ProfileSetting {
	return replay.ProfileSetting{
		Name:  m.GetName(),
		Value: m.GetValue(),
	}
}

// QueryLimitsToPB converts the complete new-lane record without hashing or authenticating it.
func QueryLimitsToPB(v replay.QueryLimits) *pb.QueryLimits {
	return &pb.QueryLimits{
		MaxSqlBytes:        v.MaxSQLBytes,
		MaxDescriptorBytes: v.MaxDescriptorBytes,
		MaxOutputRows:      v.MaxOutputRows,
		MaxOutputBytes:     v.MaxOutputBytes,
		MaxRestoreBytes:    v.MaxRestoreBytes,
		MaxSortMemoryBytes: v.MaxSortMemoryBytes,
		MaxSpillBytes:      v.MaxSpillBytes,
		MaxExecutionMs:     v.MaxExecutionMS,
	}
}

// QueryLimitsFromPB converts the complete new-lane record without hashing or authenticating it.
func QueryLimitsFromPB(m *pb.QueryLimits) replay.QueryLimits {
	return replay.QueryLimits{
		MaxSQLBytes:        m.GetMaxSqlBytes(),
		MaxDescriptorBytes: m.GetMaxDescriptorBytes(),
		MaxOutputRows:      m.GetMaxOutputRows(),
		MaxOutputBytes:     m.GetMaxOutputBytes(),
		MaxRestoreBytes:    m.GetMaxRestoreBytes(),
		MaxSortMemoryBytes: m.GetMaxSortMemoryBytes(),
		MaxSpillBytes:      m.GetMaxSpillBytes(),
		MaxExecutionMS:     m.GetMaxExecutionMs(),
	}
}

// QueryProfileRecordToPB converts the complete new-lane record without hashing or authenticating it.
func QueryProfileRecordToPB(v replay.QueryProfileRecord) *pb.QueryProfileRecord {
	return &pb.QueryProfileRecord{
		Version:                   v.Version,
		ClickhouseBuildDigest:     v.ClickHouseBuildDigest,
		Platform:                  v.Platform,
		NativeAnalyzerBuildDigest: v.NativeAnalyzerBuildDigest,
		GrpcAnalyzerBuildDigest:   v.GRPCAnalyzerBuildDigest,
		TzdataDigest:              v.TZDataDigest,
		Settings:                  snapshotMap(v.Settings, ProfileSettingToPB),
		ScalarOperators:           snapshotClone(v.ScalarOperators),
		ColumnProfileId:           v.ColumnProfileID,
		OutputOrderId:             v.OutputOrderID,
		Limits:                    QueryLimitsToPB(v.Limits),
	}
}

// QueryProfileRecordFromPB converts the complete new-lane record without hashing or authenticating it.
func QueryProfileRecordFromPB(m *pb.QueryProfileRecord) replay.QueryProfileRecord {
	return replay.QueryProfileRecord{
		Version:                   m.GetVersion(),
		ClickHouseBuildDigest:     m.GetClickhouseBuildDigest(),
		Platform:                  m.GetPlatform(),
		NativeAnalyzerBuildDigest: m.GetNativeAnalyzerBuildDigest(),
		GRPCAnalyzerBuildDigest:   m.GetGrpcAnalyzerBuildDigest(),
		TZDataDigest:              m.GetTzdataDigest(),
		Settings:                  snapshotMap(m.GetSettings(), ProfileSettingFromPB),
		ScalarOperators:           snapshotClone(m.GetScalarOperators()),
		ColumnProfileID:           m.GetColumnProfileId(),
		OutputOrderID:             m.GetOutputOrderId(),
		Limits:                    QueryLimitsFromPB(m.GetLimits()),
	}
}

// SnapshotArtifactEntryToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotArtifactEntryToPB(v replay.SnapshotArtifactEntry) *pb.SnapshotArtifactEntry {
	return &pb.SnapshotArtifactEntry{
		TableId:      v.TableID,
		PartitionId:  v.PartitionID,
		PartName:     v.PartName,
		PartPhysHash: v.PartPhysHash,
		ObjectDigest: v.ObjectDigest,
		Bytes:        v.Bytes,
	}
}

// SnapshotArtifactEntryFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotArtifactEntryFromPB(m *pb.SnapshotArtifactEntry) replay.SnapshotArtifactEntry {
	return replay.SnapshotArtifactEntry{
		TableID:      m.GetTableId(),
		PartitionID:  m.GetPartitionId(),
		PartName:     m.GetPartName(),
		PartPhysHash: m.GetPartPhysHash(),
		ObjectDigest: m.GetObjectDigest(),
		Bytes:        m.GetBytes(),
	}
}

// SnapshotArtifactSetToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotArtifactSetToPB(v replay.SnapshotArtifactSet) *pb.SnapshotArtifactSet {
	return &pb.SnapshotArtifactSet{
		Parts:                snapshotMap(v.Parts, SnapshotArtifactEntryToPB),
		SchemaArtifactDigest: v.SchemaArtifactDigest,
	}
}

// SnapshotArtifactSetFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotArtifactSetFromPB(m *pb.SnapshotArtifactSet) replay.SnapshotArtifactSet {
	return replay.SnapshotArtifactSet{
		Parts:                snapshotMap(m.GetParts(), SnapshotArtifactEntryFromPB),
		SchemaArtifactDigest: m.GetSchemaArtifactDigest(),
	}
}

// SnapshotArtifactReadySubmissionToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotArtifactReadySubmissionToPB(v replay.SnapshotArtifactReadySubmission) *pb.SnapshotArtifactReadySubmission {
	return &pb.SnapshotArtifactReadySubmission{
		Record:    SnapshotArtifactReadyToPB(v.Record),
		Signature: v.Signature,
	}
}

// SnapshotArtifactReadySubmissionFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotArtifactReadySubmissionFromPB(m *pb.SnapshotArtifactReadySubmission) replay.SnapshotArtifactReadySubmission {
	return replay.SnapshotArtifactReadySubmission{
		Record:    SnapshotArtifactReadyFromPB(m.GetRecord()),
		Signature: m.GetSignature(),
	}
}

// AcquireSnapshotQueryRequest is the new-lane transport record; authentication belongs to its runtime owner.
type AcquireSnapshotQueryRequest struct {
	NetworkID     string `json:"network_id"`
	KeeperShardID uint32 `json:"keeper_shard_id"`
	ClientAccount string `json:"client_account"`
	StatementID   string `json:"statement_id"`
	RequestID     string `json:"request_id"`
	ControlJWS    string `json:"control_jws"`
}

// AcquireSnapshotQueryRequestToPB converts the complete new-lane record without hashing or authenticating it.
func AcquireSnapshotQueryRequestToPB(v AcquireSnapshotQueryRequest) *pb.AcquireSnapshotQueryRequest {
	return &pb.AcquireSnapshotQueryRequest{
		NetworkId:     v.NetworkID,
		KeeperShardId: v.KeeperShardID,
		ClientAccount: v.ClientAccount,
		StatementId:   v.StatementID,
		RequestId:     v.RequestID,
		ControlJws:    v.ControlJWS,
	}
}

// AcquireSnapshotQueryRequestFromPB converts the complete new-lane record without hashing or authenticating it.
func AcquireSnapshotQueryRequestFromPB(m *pb.AcquireSnapshotQueryRequest) AcquireSnapshotQueryRequest {
	return AcquireSnapshotQueryRequest{
		NetworkID:     m.GetNetworkId(),
		KeeperShardID: m.GetKeeperShardId(),
		ClientAccount: m.GetClientAccount(),
		StatementID:   m.GetStatementId(),
		RequestID:     m.GetRequestId(),
		ControlJWS:    m.GetControlJws(),
	}
}

// GetSnapshotQueryReservationRequest is the new-lane transport record; authentication belongs to its runtime owner.
type GetSnapshotQueryReservationRequest struct {
	NetworkID         string `json:"network_id"`
	KeeperShardID     uint32 `json:"keeper_shard_id"`
	ClientAccount     string `json:"client_account"`
	StatementID       string `json:"statement_id"`
	RequestID         string `json:"request_id"`
	ControlJWS        string `json:"control_jws"`
	ReservationID     string `json:"reservation_id"`
	FencingGeneration uint64 `json:"fencing_generation"`
}

// GetSnapshotQueryReservationRequestToPB converts the complete new-lane record without hashing or authenticating it.
func GetSnapshotQueryReservationRequestToPB(v GetSnapshotQueryReservationRequest) *pb.GetSnapshotQueryReservationRequest {
	return &pb.GetSnapshotQueryReservationRequest{
		NetworkId:         v.NetworkID,
		KeeperShardId:     v.KeeperShardID,
		ClientAccount:     v.ClientAccount,
		StatementId:       v.StatementID,
		RequestId:         v.RequestID,
		ControlJws:        v.ControlJWS,
		ReservationId:     v.ReservationID,
		FencingGeneration: v.FencingGeneration,
	}
}

// GetSnapshotQueryReservationRequestFromPB converts the complete new-lane record without hashing or authenticating it.
func GetSnapshotQueryReservationRequestFromPB(m *pb.GetSnapshotQueryReservationRequest) GetSnapshotQueryReservationRequest {
	return GetSnapshotQueryReservationRequest{
		NetworkID:         m.GetNetworkId(),
		KeeperShardID:     m.GetKeeperShardId(),
		ClientAccount:     m.GetClientAccount(),
		StatementID:       m.GetStatementId(),
		RequestID:         m.GetRequestId(),
		ControlJWS:        m.GetControlJws(),
		ReservationID:     m.GetReservationId(),
		FencingGeneration: m.GetFencingGeneration(),
	}
}

// ReleaseSnapshotQueryRequest is the new-lane transport record; authentication belongs to its runtime owner.
type ReleaseSnapshotQueryRequest struct {
	NetworkID         string `json:"network_id"`
	KeeperShardID     uint32 `json:"keeper_shard_id"`
	ClientAccount     string `json:"client_account"`
	StatementID       string `json:"statement_id"`
	RequestID         string `json:"request_id"`
	ControlJWS        string `json:"control_jws"`
	ReservationID     string `json:"reservation_id"`
	FencingGeneration uint64 `json:"fencing_generation"`
}

// ReleaseSnapshotQueryRequestToPB converts the complete new-lane record without hashing or authenticating it.
func ReleaseSnapshotQueryRequestToPB(v ReleaseSnapshotQueryRequest) *pb.ReleaseSnapshotQueryRequest {
	return &pb.ReleaseSnapshotQueryRequest{
		NetworkId:         v.NetworkID,
		KeeperShardId:     v.KeeperShardID,
		ClientAccount:     v.ClientAccount,
		StatementId:       v.StatementID,
		RequestId:         v.RequestID,
		ControlJws:        v.ControlJWS,
		ReservationId:     v.ReservationID,
		FencingGeneration: v.FencingGeneration,
	}
}

// ReleaseSnapshotQueryRequestFromPB converts the complete new-lane record without hashing or authenticating it.
func ReleaseSnapshotQueryRequestFromPB(m *pb.ReleaseSnapshotQueryRequest) ReleaseSnapshotQueryRequest {
	return ReleaseSnapshotQueryRequest{
		NetworkID:         m.GetNetworkId(),
		KeeperShardID:     m.GetKeeperShardId(),
		ClientAccount:     m.GetClientAccount(),
		StatementID:       m.GetStatementId(),
		RequestID:         m.GetRequestId(),
		ControlJWS:        m.GetControlJws(),
		ReservationID:     m.GetReservationId(),
		FencingGeneration: m.GetFencingGeneration(),
	}
}

// GetSnapshotQueryStatusRequest is the new-lane transport record; authentication belongs to its runtime owner.
type GetSnapshotQueryStatusRequest struct {
	NetworkID           string `json:"network_id"`
	KeeperShardID       uint32 `json:"keeper_shard_id"`
	ClientAccount       string `json:"client_account"`
	StatementID         string `json:"statement_id"`
	ExpectedInputRoot   string `json:"expected_input_root"`
	ExpectedUserJWSHash string `json:"expected_user_jws_hash"`
}

// GetSnapshotQueryStatusRequestToPB converts the complete new-lane record without hashing or authenticating it.
func GetSnapshotQueryStatusRequestToPB(v GetSnapshotQueryStatusRequest) *pb.GetSnapshotQueryStatusRequest {
	return &pb.GetSnapshotQueryStatusRequest{
		NetworkId:           v.NetworkID,
		KeeperShardId:       v.KeeperShardID,
		ClientAccount:       v.ClientAccount,
		StatementId:         v.StatementID,
		ExpectedInputRoot:   v.ExpectedInputRoot,
		ExpectedUserJwsHash: v.ExpectedUserJWSHash,
	}
}

// GetSnapshotQueryStatusRequestFromPB converts the complete new-lane record without hashing or authenticating it.
func GetSnapshotQueryStatusRequestFromPB(m *pb.GetSnapshotQueryStatusRequest) GetSnapshotQueryStatusRequest {
	return GetSnapshotQueryStatusRequest{
		NetworkID:           m.GetNetworkId(),
		KeeperShardID:       m.GetKeeperShardId(),
		ClientAccount:       m.GetClientAccount(),
		StatementID:         m.GetStatementId(),
		ExpectedInputRoot:   m.GetExpectedInputRoot(),
		ExpectedUserJWSHash: m.GetExpectedUserJwsHash(),
	}
}

// SnapshotBarrier is the new-lane transport record; authentication belongs to its runtime owner.
type SnapshotBarrier struct {
	State         string                           `json:"state"`
	Generation    uint64                           `json:"generation"`
	RequestID     string                           `json:"request_id"`
	ClientAccount string                           `json:"client_account"`
	StatementID   string                           `json:"statement_id"`
	Reservation   *replay.SnapshotQueryReservation `json:"reservation"`
	BlockSeq      uint64                           `json:"block_seq"`
}

// SnapshotBarrierToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotBarrierToPB(v SnapshotBarrier) *pb.SnapshotBarrier {
	return &pb.SnapshotBarrier{
		State:         v.State,
		Generation:    v.Generation,
		RequestId:     v.RequestID,
		ClientAccount: v.ClientAccount,
		StatementId:   v.StatementID,
		Reservation:   snapshotOptional(v.Reservation, SnapshotQueryReservationToPB),
		BlockSeq:      v.BlockSeq,
	}
}

// SnapshotBarrierFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotBarrierFromPB(m *pb.SnapshotBarrier) SnapshotBarrier {
	return SnapshotBarrier{
		State:         m.GetState(),
		Generation:    m.GetGeneration(),
		RequestID:     m.GetRequestId(),
		ClientAccount: m.GetClientAccount(),
		StatementID:   m.GetStatementId(),
		Reservation:   snapshotFromOptional(m.GetReservation(), SnapshotQueryReservationFromPB),
		BlockSeq:      m.GetBlockSeq(),
	}
}

// GetPublishedSnapshotRequest is the new-lane transport record; authentication belongs to its runtime owner.
type GetPublishedSnapshotRequest struct {
	NetworkID     string `json:"network_id"`
	KeeperShardID uint32 `json:"keeper_shard_id"`
	SnapshotID    string `json:"snapshot_id"`
}

// GetPublishedSnapshotRequestToPB converts the complete new-lane record without hashing or authenticating it.
func GetPublishedSnapshotRequestToPB(v GetPublishedSnapshotRequest) *pb.GetPublishedSnapshotRequest {
	return &pb.GetPublishedSnapshotRequest{
		NetworkId:     v.NetworkID,
		KeeperShardId: v.KeeperShardID,
		SnapshotId:    v.SnapshotID,
	}
}

// GetPublishedSnapshotRequestFromPB converts the complete new-lane record without hashing or authenticating it.
func GetPublishedSnapshotRequestFromPB(m *pb.GetPublishedSnapshotRequest) GetPublishedSnapshotRequest {
	return GetPublishedSnapshotRequest{
		NetworkID:     m.GetNetworkId(),
		KeeperShardID: m.GetKeeperShardId(),
		SnapshotID:    m.GetSnapshotId(),
	}
}

// PublishedSnapshot is the new-lane transport record; authentication belongs to its runtime owner.
type PublishedSnapshot struct {
	Manifest      replay.SafeSnapshotManifest            `json:"manifest"`
	Activation    replay.ActiveQueryPolicy               `json:"activation"`
	ArtifactReady replay.SnapshotArtifactReadySubmission `json:"artifact_ready"`
	TerminalProof []byte                                 `json:"terminal_proof"`
}

// PublishedSnapshotToPB converts the complete new-lane record without hashing or authenticating it.
func PublishedSnapshotToPB(v PublishedSnapshot) *pb.PublishedSnapshot {
	return &pb.PublishedSnapshot{
		Manifest:      ManifestToPB(v.Manifest),
		Activation:    ActiveQueryPolicyToPB(v.Activation),
		ArtifactReady: SnapshotArtifactReadySubmissionToPB(v.ArtifactReady),
		TerminalProof: bytes.Clone(v.TerminalProof),
	}
}

// PublishedSnapshotFromPB converts the complete new-lane record without hashing or authenticating it.
func PublishedSnapshotFromPB(m *pb.PublishedSnapshot) PublishedSnapshot {
	return PublishedSnapshot{
		Manifest:      ManifestFromPB(m.GetManifest()),
		Activation:    ActiveQueryPolicyFromPB(m.GetActivation()),
		ArtifactReady: SnapshotArtifactReadySubmissionFromPB(m.GetArtifactReady()),
		TerminalProof: bytes.Clone(m.GetTerminalProof()),
	}
}

// GetQueryPolicyRequest is the new-lane transport record; authentication belongs to its runtime owner.
type GetQueryPolicyRequest struct {
	NetworkID     string `json:"network_id"`
	KeeperShardID uint32 `json:"keeper_shard_id"`
	ActivationID  string `json:"activation_id"`
	BlockSeq      uint64 `json:"block_seq"`
}

// GetQueryPolicyRequestToPB converts the complete new-lane record without hashing or authenticating it.
func GetQueryPolicyRequestToPB(v GetQueryPolicyRequest) *pb.GetQueryPolicyRequest {
	return &pb.GetQueryPolicyRequest{
		NetworkId:     v.NetworkID,
		KeeperShardId: v.KeeperShardID,
		ActivationId:  v.ActivationID,
		BlockSeq:      v.BlockSeq,
	}
}

// GetQueryPolicyRequestFromPB converts the complete new-lane record without hashing or authenticating it.
func GetQueryPolicyRequestFromPB(m *pb.GetQueryPolicyRequest) GetQueryPolicyRequest {
	return GetQueryPolicyRequest{
		NetworkID:     m.GetNetworkId(),
		KeeperShardID: m.GetKeeperShardId(),
		ActivationID:  m.GetActivationId(),
		BlockSeq:      m.GetBlockSeq(),
	}
}

// QueryPolicyStatus is the new-lane transport record; authentication belongs to its runtime owner.
type QueryPolicyStatus struct {
	Found         bool                     `json:"found"`
	Activation    replay.ActiveQueryPolicy `json:"activation"`
	TerminalProof []byte                   `json:"terminal_proof"`
}

// QueryPolicyStatusToPB converts the complete new-lane record without hashing or authenticating it.
func QueryPolicyStatusToPB(v QueryPolicyStatus) *pb.QueryPolicyStatus {
	return &pb.QueryPolicyStatus{
		Found:         v.Found,
		Activation:    ActiveQueryPolicyToPB(v.Activation),
		TerminalProof: bytes.Clone(v.TerminalProof),
	}
}

// QueryPolicyStatusFromPB converts the complete new-lane record without hashing or authenticating it.
func QueryPolicyStatusFromPB(m *pb.QueryPolicyStatus) QueryPolicyStatus {
	return QueryPolicyStatus{
		Found:         m.GetFound(),
		Activation:    ActiveQueryPolicyFromPB(m.GetActivation()),
		TerminalProof: bytes.Clone(m.GetTerminalProof()),
	}
}

// SnapshotQueryControlBinding is the new-lane transport record; authentication belongs to its runtime owner.
type SnapshotQueryControlBinding struct {
	Operation         string `json:"operation"`
	NetworkID         string `json:"network_id"`
	KeeperShardID     uint32 `json:"keeper_shard_id"`
	ClientAccount     string `json:"client_account"`
	StatementID       string `json:"statement_id"`
	RequestID         string `json:"request_id"`
	ReservationID     string `json:"reservation_id"`
	FencingGeneration uint64 `json:"fencing_generation"`
}

// SnapshotQueryControlBindingToPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryControlBindingToPB(v SnapshotQueryControlBinding) *pb.SnapshotQueryControlBinding {
	return &pb.SnapshotQueryControlBinding{
		Operation:         v.Operation,
		NetworkId:         v.NetworkID,
		KeeperShardId:     v.KeeperShardID,
		ClientAccount:     v.ClientAccount,
		StatementId:       v.StatementID,
		RequestId:         v.RequestID,
		ReservationId:     v.ReservationID,
		FencingGeneration: v.FencingGeneration,
	}
}

// SnapshotQueryControlBindingFromPB converts the complete new-lane record without hashing or authenticating it.
func SnapshotQueryControlBindingFromPB(m *pb.SnapshotQueryControlBinding) SnapshotQueryControlBinding {
	return SnapshotQueryControlBinding{
		Operation:         m.GetOperation(),
		NetworkID:         m.GetNetworkId(),
		KeeperShardID:     m.GetKeeperShardId(),
		ClientAccount:     m.GetClientAccount(),
		StatementID:       m.GetStatementId(),
		RequestID:         m.GetRequestId(),
		ReservationID:     m.GetReservationId(),
		FencingGeneration: m.GetFencingGeneration(),
	}
}

// BeginSnapshotQuery is the new-lane transport record; authentication belongs to its runtime owner.
type BeginSnapshotQuery struct {
	Request AcquireSnapshotQueryRequest `json:"request"`
}

// BeginSnapshotQueryToPB converts the complete new-lane record without hashing or authenticating it.
func BeginSnapshotQueryToPB(v BeginSnapshotQuery) *pb.BeginSnapshotQueryCmd {
	return &pb.BeginSnapshotQueryCmd{
		Request: AcquireSnapshotQueryRequestToPB(v.Request),
	}
}

// BeginSnapshotQueryFromPB converts the complete new-lane record without hashing or authenticating it.
func BeginSnapshotQueryFromPB(m *pb.BeginSnapshotQueryCmd) BeginSnapshotQuery {
	return BeginSnapshotQuery{
		Request: AcquireSnapshotQueryRequestFromPB(m.GetRequest()),
	}
}

// GrantSnapshotQuery is the new-lane transport record; authentication belongs to its runtime owner.
type GrantSnapshotQuery struct {
	RequestID   string                          `json:"request_id"`
	Reservation replay.SnapshotQueryReservation `json:"reservation"`
}

// GrantSnapshotQueryToPB converts the complete new-lane record without hashing or authenticating it.
func GrantSnapshotQueryToPB(v GrantSnapshotQuery) *pb.GrantSnapshotQueryCmd {
	return &pb.GrantSnapshotQueryCmd{
		RequestId:   v.RequestID,
		Reservation: SnapshotQueryReservationToPB(v.Reservation),
	}
}

// GrantSnapshotQueryFromPB converts the complete new-lane record without hashing or authenticating it.
func GrantSnapshotQueryFromPB(m *pb.GrantSnapshotQueryCmd) GrantSnapshotQuery {
	return GrantSnapshotQuery{
		RequestID:   m.GetRequestId(),
		Reservation: SnapshotQueryReservationFromPB(m.GetReservation()),
	}
}

// ReleaseSnapshotQuery is the new-lane transport record; authentication belongs to its runtime owner.
type ReleaseSnapshotQuery struct {
	Request ReleaseSnapshotQueryRequest `json:"request"`
}

// ReleaseSnapshotQueryToPB converts the complete new-lane record without hashing or authenticating it.
func ReleaseSnapshotQueryToPB(v ReleaseSnapshotQuery) *pb.ReleaseSnapshotQueryCmd {
	return &pb.ReleaseSnapshotQueryCmd{
		Request: ReleaseSnapshotQueryRequestToPB(v.Request),
	}
}

// ReleaseSnapshotQueryFromPB converts the complete new-lane record without hashing or authenticating it.
func ReleaseSnapshotQueryFromPB(m *pb.ReleaseSnapshotQueryCmd) ReleaseSnapshotQuery {
	return ReleaseSnapshotQuery{
		Request: ReleaseSnapshotQueryRequestFromPB(m.GetRequest()),
	}
}

// SubmitSnapshotQuery is the new-lane transport record; authentication belongs to its runtime owner.
type SubmitSnapshotQuery struct {
	Envelope           replay.SnapshotQueryEnvelope `json:"envelope"`
	NonMembershipProof []byte                       `json:"non_membership_proof"`
}

// SubmitSnapshotQueryToPB converts the complete new-lane record without hashing or authenticating it.
func SubmitSnapshotQueryToPB(v SubmitSnapshotQuery) *pb.SubmitSnapshotQueryCmd {
	return &pb.SubmitSnapshotQueryCmd{
		Envelope:           SnapshotQueryEnvelopeToPB(v.Envelope),
		NonMembershipProof: bytes.Clone(v.NonMembershipProof),
	}
}

// SubmitSnapshotQueryFromPB converts the complete new-lane record without hashing or authenticating it.
func SubmitSnapshotQueryFromPB(m *pb.SubmitSnapshotQueryCmd) SubmitSnapshotQuery {
	return SubmitSnapshotQuery{
		Envelope:           SnapshotQueryEnvelopeFromPB(m.GetEnvelope()),
		NonMembershipProof: bytes.Clone(m.GetNonMembershipProof()),
	}
}

// AbortSnapshotQuery is the new-lane transport record; authentication belongs to its runtime owner.
type AbortSnapshotQuery struct {
	Record       replay.SnapshotQueryAbortRecord `json:"record"`
	AuthorityJWS string                          `json:"authority_jws"`
}

// AbortSnapshotQueryToPB converts the complete new-lane record without hashing or authenticating it.
func AbortSnapshotQueryToPB(v AbortSnapshotQuery) *pb.AbortSnapshotQueryCmd {
	return &pb.AbortSnapshotQueryCmd{
		Record:       SnapshotQueryAbortRecordToPB(v.Record),
		AuthorityJws: v.AuthorityJWS,
	}
}

// AbortSnapshotQueryFromPB converts the complete new-lane record without hashing or authenticating it.
func AbortSnapshotQueryFromPB(m *pb.AbortSnapshotQueryCmd) AbortSnapshotQuery {
	return AbortSnapshotQuery{
		Record:       SnapshotQueryAbortRecordFromPB(m.GetRecord()),
		AuthorityJWS: m.GetAuthorityJws(),
	}
}

// ActivateQueryProfile is the new-lane transport record; authentication belongs to its runtime owner.
type ActivateQueryProfile struct {
	Activation   replay.ActiveQueryPolicy `json:"activation"`
	AuthorityJWS string                   `json:"authority_jws"`
}

// ActivateQueryProfileToPB converts the complete new-lane record without hashing or authenticating it.
func ActivateQueryProfileToPB(v ActivateQueryProfile) *pb.ActivateQueryProfileCmd {
	return &pb.ActivateQueryProfileCmd{
		Activation:   ActiveQueryPolicyToPB(v.Activation),
		AuthorityJws: v.AuthorityJWS,
	}
}

// ActivateQueryProfileFromPB converts the complete new-lane record without hashing or authenticating it.
func ActivateQueryProfileFromPB(m *pb.ActivateQueryProfileCmd) ActivateQueryProfile {
	return ActivateQueryProfile{
		Activation:   ActiveQueryPolicyFromPB(m.GetActivation()),
		AuthorityJWS: m.GetAuthorityJws(),
	}
}

// RecordSnapshotQueryClaim is the new-lane transport record; authentication belongs to its runtime owner.
type RecordSnapshotQueryClaim struct {
	Claim replay.SnapshotQueryClaim `json:"claim"`
}

// RecordSnapshotQueryClaimToPB converts the complete new-lane record without hashing or authenticating it.
func RecordSnapshotQueryClaimToPB(v RecordSnapshotQueryClaim) *pb.RecordSnapshotQueryClaimCmd {
	return &pb.RecordSnapshotQueryClaimCmd{
		Claim: SnapshotQueryClaimToPB(v.Claim),
	}
}

// RecordSnapshotQueryClaimFromPB converts the complete new-lane record without hashing or authenticating it.
func RecordSnapshotQueryClaimFromPB(m *pb.RecordSnapshotQueryClaimCmd) RecordSnapshotQueryClaim {
	return RecordSnapshotQueryClaim{
		Claim: SnapshotQueryClaimFromPB(m.GetClaim()),
	}
}

// RecordSnapshotQueryAttestation is the new-lane transport record; authentication belongs to its runtime owner.
type RecordSnapshotQueryAttestation struct {
	Attestation replay.SnapshotQueryAttestation `json:"attestation"`
}

// RecordSnapshotQueryAttestationToPB converts the complete new-lane record without hashing or authenticating it.
func RecordSnapshotQueryAttestationToPB(v RecordSnapshotQueryAttestation) *pb.RecordSnapshotQueryAttestationCmd {
	return &pb.RecordSnapshotQueryAttestationCmd{
		Attestation: SnapshotQueryAttestationToPB(v.Attestation),
	}
}

// RecordSnapshotQueryAttestationFromPB converts the complete new-lane record without hashing or authenticating it.
func RecordSnapshotQueryAttestationFromPB(m *pb.RecordSnapshotQueryAttestationCmd) RecordSnapshotQueryAttestation {
	return RecordSnapshotQueryAttestation{
		Attestation: SnapshotQueryAttestationFromPB(m.GetAttestation()),
	}
}

// PublishExecutorProfileTransition is the new-lane transport record; authentication belongs to its runtime owner.
type PublishExecutorProfileTransition struct {
	Transition   replay.ExecutorProfileTransition          `json:"transition"`
	Manifest     replay.SafeSnapshotManifest               `json:"manifest"`
	Receipts     []replay.ExecutorProfileTransitionReceipt `json:"receipts"`
	AuthorityJWS string                                    `json:"authority_jws"`
}

// PublishExecutorProfileTransitionToPB converts the complete new-lane record without hashing or authenticating it.
func PublishExecutorProfileTransitionToPB(v PublishExecutorProfileTransition) *pb.PublishExecutorProfileTransitionCmd {
	return &pb.PublishExecutorProfileTransitionCmd{
		Transition:   ExecutorProfileTransitionToPB(v.Transition),
		Manifest:     ManifestToPB(v.Manifest),
		Receipts:     snapshotMap(v.Receipts, ExecutorProfileTransitionReceiptToPB),
		AuthorityJws: v.AuthorityJWS,
	}
}

// PublishExecutorProfileTransitionFromPB converts the complete new-lane record without hashing or authenticating it.
func PublishExecutorProfileTransitionFromPB(m *pb.PublishExecutorProfileTransitionCmd) PublishExecutorProfileTransition {
	return PublishExecutorProfileTransition{
		Transition:   ExecutorProfileTransitionFromPB(m.GetTransition()),
		Manifest:     ManifestFromPB(m.GetManifest()),
		Receipts:     snapshotMap(m.GetReceipts(), ExecutorProfileTransitionReceiptFromPB),
		AuthorityJWS: m.GetAuthorityJws(),
	}
}

// RecordSnapshotArtifactReady is the new-lane transport record; authentication belongs to its runtime owner.
type RecordSnapshotArtifactReady struct {
	Submission replay.SnapshotArtifactReadySubmission `json:"submission"`
}

// RecordSnapshotArtifactReadyToPB converts the complete new-lane record without hashing or authenticating it.
func RecordSnapshotArtifactReadyToPB(v RecordSnapshotArtifactReady) *pb.RecordSnapshotArtifactReadyCmd {
	return &pb.RecordSnapshotArtifactReadyCmd{
		Submission: SnapshotArtifactReadySubmissionToPB(v.Submission),
	}
}

// RecordSnapshotArtifactReadyFromPB converts the complete new-lane record without hashing or authenticating it.
func RecordSnapshotArtifactReadyFromPB(m *pb.RecordSnapshotArtifactReadyCmd) RecordSnapshotArtifactReady {
	return RecordSnapshotArtifactReady{
		Submission: SnapshotArtifactReadySubmissionFromPB(m.GetSubmission()),
	}
}

// SnapshotQueryJobDispatch keeps the query lane separate from legacy replay jobs.
func SnapshotQueryJobDispatch(j replay.SnapshotQueryJob) *pb.VerifierDispatch {
	return &pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_SnapshotQueryJob{SnapshotQueryJob: SnapshotQueryJobToPB(j)}}
}

// validateCommandBytes checks presence before protobuf's last-one-wins decoding
// can erase mixed variants, duplicate singular fields, or unknown empty fields.
func validateCommandBytes(b []byte, desc protoreflect.MessageDescriptor, union bool) error {
	seen := map[protowire.Number]bool{}
	count := 0
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			return fmt.Errorf("wire: invalid field tag")
		}
		b = b[n:]
		fd := desc.Fields().ByNumber(protoreflect.FieldNumber(num))
		if fd == nil {
			return fmt.Errorf("wire: unknown %s field %d", desc.FullName(), num)
		}
		expected := protowire.VarintType
		switch fd.Kind() {
		case protoreflect.StringKind, protoreflect.BytesKind, protoreflect.MessageKind:
			expected = protowire.BytesType
		case protoreflect.Fixed32Kind, protoreflect.Sfixed32Kind, protoreflect.FloatKind:
			expected = protowire.Fixed32Type
		case protoreflect.Fixed64Kind, protoreflect.Sfixed64Kind, protoreflect.DoubleKind:
			expected = protowire.Fixed64Type
		}
		// Protobuf permits both packed and unpacked repeated scalar encodings.
		packed := fd.IsList() && expected != protowire.BytesType && typ == protowire.BytesType
		if typ != expected && !packed {
			return fmt.Errorf("wire: wrong wire type for %s field %d", desc.FullName(), num)
		}
		if seen[num] && !fd.IsList() {
			return fmt.Errorf("wire: duplicate field %d", num)
		}
		seen[num] = true
		count++
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			return fmt.Errorf("wire: invalid field %d", num)
		}
		if fd.Message() != nil {
			if typ != protowire.BytesType {
				return fmt.Errorf("wire: invalid message field %d", num)
			}
			nested, _ := protowire.ConsumeBytes(b)
			if err := validateCommandBytes(nested, fd.Message(), false); err != nil {
				return err
			}
		}
		b = b[n:]
	}
	if union && count != 1 {
		return fmt.Errorf("wire: exactly one command must be present, got %d", count)
	}
	return nil
}
