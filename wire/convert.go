package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// mapSlice converts a repeated field; empty input yields nil — THE frozen
// normalization point for the canonical Go form (see package doc).
func mapSlice[I, O any](in []I, f func(I) O) []O {
	if len(in) == 0 {
		return nil
	}
	out := make([]O, len(in))
	for i := range in {
		out[i] = f(in[i])
	}
	return out
}

func statementIDFromPB(m *pb.StatementID) arbiter.StatementID {
	return arbiter.StatementID{ClientAccount: m.GetClientAccount(), ClientSeq: m.GetClientSeq(), ClientNonce: m.GetClientNonce()}
}

func statementIDToPB(v arbiter.StatementID) *pb.StatementID {
	return &pb.StatementID{ClientAccount: v.ClientAccount, ClientSeq: v.ClientSeq, ClientNonce: v.ClientNonce}
}

func EnvelopeFromPB(m *pb.StatementEnvelopeV2) arbiter.StatementEnvelope {
	return arbiter.StatementEnvelope{
		StatementID:     statementIDFromPB(m.GetStatementId()),
		StatementKind:   arbiter.StatementKind(m.GetStatementKind()),
		SQL:             m.GetSql(),
		SQLHash:         m.GetSqlHash(),
		SettingsHash:    m.GetSettingsHash(),
		PayloadRef:      m.GetPayloadRef(),
		PayloadHash:     m.GetPayloadHash(),
		PayloadLength:   m.GetPayloadLength(),
		TargetTableID:   m.GetTargetTableId(),
		UserJWS:         m.GetUserJws(),
		EnvelopeVersion: m.GetEnvelopeVersion(),
		NetworkID:       m.GetNetworkId(),
		KeeperShardID:   m.GetKeeperShardId(),
		PayloadFormat:   m.GetPayloadFormat(),
		ClientRevision:  m.GetClientRevision(),
		SchemaHash:      m.GetSchemaHash(),
		RowIDProfileID:  m.GetRowIdProfileId(),
	}
}

func EnvelopeToPB(v arbiter.StatementEnvelope) *pb.StatementEnvelopeV2 {
	return &pb.StatementEnvelopeV2{
		StatementId:     statementIDToPB(v.StatementID),
		StatementKind:   pb.StatementKind(v.StatementKind),
		Sql:             v.SQL,
		SqlHash:         v.SQLHash,
		SettingsHash:    v.SettingsHash,
		PayloadRef:      v.PayloadRef,
		PayloadHash:     v.PayloadHash,
		PayloadLength:   v.PayloadLength,
		TargetTableId:   v.TargetTableID,
		UserJws:         v.UserJWS,
		EnvelopeVersion: v.EnvelopeVersion,
		NetworkId:       v.NetworkID,
		KeeperShardId:   v.KeeperShardID,
		PayloadFormat:   v.PayloadFormat,
		ClientRevision:  v.ClientRevision,
		SchemaHash:      v.SchemaHash,
		RowIdProfileId:  v.RowIDProfileID,
	}
}

func candidatePartFromPB(m *pb.CandidatePart) arbiter.CandidatePart {
	return arbiter.CandidatePart{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), PartName: m.GetPartName(),
		PartRowLtHash: m.GetPartRowLthash(), PartPhysHash: m.GetPartPhysHash(), RowCount: m.GetRowCount(), Bytes: m.GetBytes()}
}

func candidatePartToPB(v arbiter.CandidatePart) *pb.CandidatePart {
	return &pb.CandidatePart{TableId: v.TableID, PartitionId: v.PartitionID, PartName: v.PartName,
		PartRowLthash: v.PartRowLtHash, PartPhysHash: v.PartPhysHash, RowCount: v.RowCount, Bytes: v.Bytes}
}

func partitionSumFromPB(m *pb.PartitionLtHashSum) arbiter.PartitionLtHashSum {
	return arbiter.PartitionLtHashSum{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), NewPartsLtHashSum: m.GetNewPartsLthashSum()}
}

func partitionSumToPB(v arbiter.PartitionLtHashSum) *pb.PartitionLtHashSum {
	return &pb.PartitionLtHashSum{TableId: v.TableID, PartitionId: v.PartitionID, NewPartsLthashSum: v.NewPartsLtHashSum}
}

func RCFromPB(m *pb.RCRecord) arbiter.RCRecord {
	return arbiter.RCRecord{
		StatementID:          statementIDFromPB(m.GetStatementId()),
		SourceNode:           m.GetSourceNode(),
		CandidateParts:       mapSlice(m.GetCandidateParts(), candidatePartFromPB),
		SourceClaimRoot:      m.GetSourceClaimRoot(),
		PartitionNewPartSums: mapSlice(m.GetPartitionNewPartSums(), partitionSumFromPB),
	}
}

func RCToPB(v arbiter.RCRecord) *pb.RCRecord {
	return &pb.RCRecord{
		StatementId:          statementIDToPB(v.StatementID),
		SourceNode:           v.SourceNode,
		CandidateParts:       mapSlice(v.CandidateParts, candidatePartToPB),
		SourceClaimRoot:      v.SourceClaimRoot,
		PartitionNewPartSums: mapSlice(v.PartitionNewPartSums, partitionSumToPB),
	}
}

func partScanFromPB(m *pb.PartScan) arbiter.PartScan {
	return arbiter.PartScan{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(),
		ClaimedPartRowLtHash: m.GetClaimedPartRowLthash(), ScannedPartRowLtHash: m.GetScannedPartRowLthash(), LivePartName: m.GetLivePartName()}
}

func partScanToPB(v arbiter.PartScan) *pb.PartScan {
	return &pb.PartScan{TableId: v.TableID, PartitionId: v.PartitionID,
		ClaimedPartRowLthash: v.ClaimedPartRowLtHash, ScannedPartRowLthash: v.ScannedPartRowLtHash, LivePartName: v.LivePartName}
}

func ScanFromPB(m *pb.ByteSideScanMsg) arbiter.ByteSideScanMsg {
	return arbiter.ByteSideScanMsg{ReplicaID: m.GetReplicaId(), BlockSeq: m.GetBlockSeq(),
		Parts: mapSlice(m.GetParts(), partScanFromPB), ScanHash: m.GetScanHash(), Signature: m.GetSignature()}
}

func ScanToPB(v arbiter.ByteSideScanMsg) *pb.ByteSideScanMsg {
	return &pb.ByteSideScanMsg{ReplicaId: v.ReplicaID, BlockSeq: v.BlockSeq,
		Parts: mapSlice(v.Parts, partScanToPB), ScanHash: v.ScanHash, Signature: v.Signature}
}

func AnchorRefFromPB(m *pb.AnchorRef) arbiter.AnchorRef {
	return arbiter.AnchorRef{L3BlockHash: m.GetL3BlockHash(), StateRoot: m.GetStateRoot(),
		L2TxRef: m.GetL2TxRef(), L2BlockNumber: m.GetL2BlockNumber(), DARef: m.GetDaRef()}
}

func AnchorRefToPB(v arbiter.AnchorRef) *pb.AnchorRef {
	return &pb.AnchorRef{L3BlockHash: v.L3BlockHash, StateRoot: v.StateRoot,
		L2TxRef: v.L2TxRef, L2BlockNumber: v.L2BlockNumber, DaRef: v.DARef}
}

func RegistrationFromPB(m *pb.NodeRegistration) arbiter.NodeRegistration {
	return arbiter.NodeRegistration{NodeID: m.GetNodeId(),
		Roles:         mapSlice(m.GetRoles(), func(r pb.NodeRole) arbiter.NodeRole { return arbiter.NodeRole(r) }),
		Ed25519Pubkey: m.GetEd25519Pubkey(), DialAddr: m.GetDialAddr()}
}

func RegistrationToPB(v arbiter.NodeRegistration) *pb.NodeRegistration {
	return &pb.NodeRegistration{NodeId: v.NodeID,
		Roles:         mapSlice(v.Roles, func(r arbiter.NodeRole) pb.NodeRole { return pb.NodeRole(r) }),
		Ed25519Pubkey: v.Ed25519Pubkey, DialAddr: v.DialAddr}
}

func partRefFromPB(m *pb.PartRef) arbiter.PartRef {
	return arbiter.PartRef{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(),
		PartRowLtHash: m.GetPartRowLthash(), PartName: m.GetPartName()}
}

// PartRefsFromPB converts repeated PartRef wire messages to signing refs.
func PartRefsFromPB(ms []*pb.PartRef) []arbiter.PartRef {
	return mapSlice(ms, partRefFromPB)
}

func partRefToPB(v arbiter.PartRef) *pb.PartRef {
	return &pb.PartRef{TableId: v.TableID, PartitionId: v.PartitionID,
		PartRowLthash: v.PartRowLtHash, PartName: v.PartName}
}

// PromoteFromPB converts a promotion command from its wire form.
func PromoteFromPB(m *pb.PromoteSafePartition) arbiter.PromoteSafePartition {
	return arbiter.PromoteSafePartition{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), PromotionSeq: m.GetPromotionSeq(),
		BaseSafeSnapshotID: m.GetBaseSafeSnapshotId(), BasePartitionRoot: m.GetBasePartitionRoot(),
		CandidateParts: PartRefsFromPB(m.GetCandidateParts())}
}

func PromoteToPB(v arbiter.PromoteSafePartition) *pb.PromoteSafePartition {
	return &pb.PromoteSafePartition{TableId: v.TableID, PartitionId: v.PartitionID, PromotionSeq: v.PromotionSeq,
		BaseSafeSnapshotId: v.BaseSafeSnapshotID, BasePartitionRoot: v.BasePartitionRoot,
		CandidateParts: mapSlice(v.CandidateParts, partRefToPB)}
}

// CleanupFromPB converts an unsafe cleanup command from its wire form.
func CleanupFromPB(m *pb.UnsafeCleanup) arbiter.UnsafeCleanup {
	return arbiter.UnsafeCleanup{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), PromotionSeq: m.GetPromotionSeq(),
		Parts: PartRefsFromPB(m.GetParts())}
}

func CleanupToPB(v arbiter.UnsafeCleanup) *pb.UnsafeCleanup {
	return &pb.UnsafeCleanup{TableId: v.TableID, PartitionId: v.PartitionID, PromotionSeq: v.PromotionSeq,
		Parts: mapSlice(v.Parts, partRefToPB)}
}

func safePartMappingFromPB(m *pb.SafePartMapping) arbiter.SafePartMapping {
	return arbiter.SafePartMapping{PartRowLtHash: m.GetPartRowLthash(), SafePartName: m.GetSafePartName(), PartPhysHash: m.GetPartPhysHash()}
}

func safePartMappingToPB(v arbiter.SafePartMapping) *pb.SafePartMapping {
	return &pb.SafePartMapping{PartRowLthash: v.PartRowLtHash, SafePartName: v.SafePartName, PartPhysHash: v.PartPhysHash}
}

func PromotionAckFromPB(m *pb.PromotionAck) arbiter.PromotionAck {
	return arbiter.PromotionAck{NodeID: m.GetNodeId(), PromotionSeq: m.GetPromotionSeq(), TableID: m.GetTableId(), PartitionID: m.GetPartitionId(),
		PostPartitionCommitment: m.GetPostPartitionCommitment(), Parts: mapSlice(m.GetParts(), safePartMappingFromPB),
		Applied: m.GetApplied(), Detail: m.GetDetail()}
}

func PromotionAckToPB(v arbiter.PromotionAck) *pb.PromotionAck {
	return &pb.PromotionAck{NodeId: v.NodeID, PromotionSeq: v.PromotionSeq, TableId: v.TableID, PartitionId: v.PartitionID,
		PostPartitionCommitment: v.PostPartitionCommitment, Parts: mapSlice(v.Parts, safePartMappingToPB),
		Applied: v.Applied, Detail: v.Detail}
}

func CleanupAckFromPB(m *pb.CleanupAck) arbiter.CleanupAck {
	return arbiter.CleanupAck{NodeID: m.GetNodeId(), PromotionSeq: m.GetPromotionSeq(), TableID: m.GetTableId(), PartitionID: m.GetPartitionId()}
}

func CleanupAckToPB(v arbiter.CleanupAck) *pb.CleanupAck {
	return &pb.CleanupAck{NodeId: v.NodeID, PromotionSeq: v.PromotionSeq, TableId: v.TableID, PartitionId: v.PartitionID}
}

func partitionCommitmentFromPB(m *pb.PartitionCommitment) replay.PartitionCommitment {
	return replay.PartitionCommitment{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), Root: m.GetRoot()}
}

func partitionCommitmentToPB(v replay.PartitionCommitment) *pb.PartitionCommitment {
	return &pb.PartitionCommitment{TableId: v.TableID, PartitionId: v.PartitionID, Root: v.Root}
}

func partManifestEntryFromPB(m *pb.PartManifestEntry) replay.PartManifestEntry {
	return replay.PartManifestEntry{TableID: m.GetTableId(), PartitionID: m.GetPartitionId(), PartName: m.GetPartName(),
		PartPhysHash: m.GetPartPhysHash(), PartRowLtHash: m.GetPartRowLthash(), RowCount: m.GetRowCount(), Bytes: m.GetBytes(),
		StorageRefs: append([]string(nil), m.GetStorageRefs()...)}
}

func partManifestEntryToPB(v replay.PartManifestEntry) *pb.PartManifestEntry {
	return &pb.PartManifestEntry{TableId: v.TableID, PartitionId: v.PartitionID, PartName: v.PartName,
		PartPhysHash: v.PartPhysHash, PartRowLthash: v.PartRowLtHash, RowCount: v.RowCount, Bytes: v.Bytes,
		StorageRefs: v.StorageRefs}
}

func receiptFromPB(m *pb.ExecutionReceipt) replay.ExecutionReceipt {
	return replay.ExecutionReceipt{
		BlockSeq: m.GetBlockSeq(), PrevSafeSnapshotID: m.GetPrevSafeSnapshotId(), PrevStateRoot: m.GetPrevStateRoot(),
		SchemaSnapshotID: m.GetSchemaSnapshotId(), ExecutorProfileID: m.GetExecutorProfileId(),
		StatementRoot: m.GetStatementRoot(), PayloadRoot: m.GetPayloadRoot(), SourceClaimRoot: m.GetSourceClaimRoot(),
		ComputedStateRoot: m.GetComputedStateRoot(), MatchSourceRoot: m.GetMatchSourceRoot(),
		PartitionCommitmentsAfter: mapSlice(m.GetPartitionCommitmentsAfter(), partitionCommitmentFromPB),
		AffectedParts:             mapSlice(m.GetAffectedParts(), partManifestEntryFromPB),
		ReplayLogHash:             m.GetReplayLogHash(),
	}
}

func receiptToPB(v replay.ExecutionReceipt) *pb.ExecutionReceipt {
	return &pb.ExecutionReceipt{
		BlockSeq: v.BlockSeq, PrevSafeSnapshotId: v.PrevSafeSnapshotID, PrevStateRoot: v.PrevStateRoot,
		SchemaSnapshotId: v.SchemaSnapshotID, ExecutorProfileId: v.ExecutorProfileID,
		StatementRoot: v.StatementRoot, PayloadRoot: v.PayloadRoot, SourceClaimRoot: v.SourceClaimRoot,
		ComputedStateRoot: v.ComputedStateRoot, MatchSourceRoot: v.MatchSourceRoot,
		PartitionCommitmentsAfter: mapSlice(v.PartitionCommitmentsAfter, partitionCommitmentToPB),
		AffectedParts:             mapSlice(v.AffectedParts, partManifestEntryToPB),
		ReplayLogHash:             v.ReplayLogHash,
	}
}

func AttestationFromPB(m *pb.ReplayAttestation) replay.ReplayAttestation {
	return replay.ReplayAttestation{ReplicaID: m.GetReplicaId(), Receipt: receiptFromPB(m.GetReceipt()),
		ReceiptHash: m.GetReceiptHash(), Signature: m.GetSignature(), MatchSourceRoot: m.GetMatchSourceRoot()}
}

func AttestationToPB(v replay.ReplayAttestation) *pb.ReplayAttestation {
	return &pb.ReplayAttestation{ReplicaId: v.ReplicaID, Receipt: receiptToPB(v.Receipt),
		ReceiptHash: v.ReceiptHash, Signature: v.Signature, MatchSourceRoot: v.MatchSourceRoot}
}

func tableManifestFromPB(m *pb.TableManifest) replay.TableManifest {
	return replay.TableManifest{TableID: m.GetTableId(), SchemaHash: m.GetSchemaHash(),
		PartitionRoots: mapSlice(m.GetPartitionRoots(), partitionCommitmentFromPB),
		ActiveParts:    mapSlice(m.GetActiveParts(), partManifestEntryFromPB)}
}

func tableManifestToPB(v replay.TableManifest) *pb.TableManifest {
	return &pb.TableManifest{TableId: v.TableID, SchemaHash: v.SchemaHash,
		PartitionRoots: mapSlice(v.PartitionRoots, partitionCommitmentToPB),
		ActiveParts:    mapSlice(v.ActiveParts, partManifestEntryToPB)}
}

func ManifestFromPB(m *pb.SafeSnapshotManifest) replay.SafeSnapshotManifest {
	return replay.SafeSnapshotManifest{SnapshotID: m.GetSnapshotId(), ParentSnapshotID: m.GetParentSnapshotId(),
		SafeBlockSeq: m.GetSafeBlockSeq(), StateRoot: m.GetStateRoot(), SchemaSnapshotID: m.GetSchemaSnapshotId(),
		SchemaRoot: m.GetSchemaRoot(), ExecutorProfileID: m.GetExecutorProfileId(), DataRoot: m.GetDataRoot(),
		ManifestRoot: m.GetManifestRoot(), Tables: mapSlice(m.GetTables(), tableManifestFromPB)}
}

func ManifestToPB(v replay.SafeSnapshotManifest) *pb.SafeSnapshotManifest {
	return &pb.SafeSnapshotManifest{SnapshotId: v.SnapshotID, ParentSnapshotId: v.ParentSnapshotID,
		SafeBlockSeq: v.SafeBlockSeq, StateRoot: v.StateRoot, SchemaSnapshotId: v.SchemaSnapshotID,
		SchemaRoot: v.SchemaRoot, ExecutorProfileId: v.ExecutorProfileID, DataRoot: v.DataRoot,
		ManifestRoot: v.ManifestRoot, Tables: mapSlice(v.Tables, tableManifestToPB)}
}
