package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// Dispatch builders: leader->data-plane stream payloads (design §6). The
// replay.Statement wire form was field-name-frozen against pkg/replay in
// P0; these builders are the send-side complement of the P1a converters.

func statementToPB(s replay.Statement) *pb.Statement {
	return &pb.Statement{
		StatementId:    s.StatementID,
		StatementSeq:   s.StatementSeq,
		Sql:            s.SQL,
		SqlHash:        s.SQLHash,
		SettingsHash:   s.SettingsHash,
		PayloadRef:     s.PayloadRef,
		PayloadHash:    s.PayloadHash,
		PayloadLength:  s.PayloadLength,
		TargetTableId:  s.TargetTableID,
		UserJws:        s.UserJWS,
		PayloadFormat:  s.PayloadFormat,
		ClientRevision: s.ClientRevision,
		SchemaHash:     s.SchemaHash,
	}
}

// StatementFromPB converts a replay statement from its wire form.
func StatementFromPB(m *pb.Statement) replay.Statement {
	return replay.Statement{
		StatementID:    m.GetStatementId(),
		StatementSeq:   m.GetStatementSeq(),
		SQL:            m.GetSql(),
		SQLHash:        m.GetSqlHash(),
		SettingsHash:   m.GetSettingsHash(),
		PayloadRef:     m.GetPayloadRef(),
		PayloadHash:    m.GetPayloadHash(),
		PayloadLength:  m.GetPayloadLength(),
		TargetTableID:  m.GetTargetTableId(),
		UserJWS:        m.GetUserJws(),
		PayloadFormat:  m.GetPayloadFormat(),
		ClientRevision: m.GetClientRevision(),
		SchemaHash:     m.GetSchemaHash(),
	}
}

// ReplayJobToPB converts the §5.6 job to its wire form.
func ReplayJobToPB(j replay.ReplayJob) *pb.ReplayJob {
	return &pb.ReplayJob{
		BlockSeq:           j.BlockSeq,
		PrevSafeSnapshotId: j.PrevSafeSnapshotID,
		PrevStateRoot:      j.PrevStateRoot,
		SchemaSnapshotId:   j.SchemaSnapshotID,
		ExecutorProfileId:  j.ExecutorProfileID,
		SourceClaimRoot:    j.SourceClaimRoot,
		Statements:         mapSlice(j.Statements, statementToPB),
	}
}

// ReplayJobFromPB converts a verifier replay job from its wire form.
func ReplayJobFromPB(m *pb.ReplayJob) replay.ReplayJob {
	return replay.ReplayJob{
		BlockSeq:           m.GetBlockSeq(),
		PrevSafeSnapshotID: m.GetPrevSafeSnapshotId(),
		PrevStateRoot:      m.GetPrevStateRoot(),
		SchemaSnapshotID:   m.GetSchemaSnapshotId(),
		ExecutorProfileID:  m.GetExecutorProfileId(),
		SourceClaimRoot:    m.GetSourceClaimRoot(),
		Statements:         mapSlice(m.GetStatements(), StatementFromPB),
	}
}

// ReplayJobDispatch wraps a job in the VerifierDispatch oneof.
func ReplayJobDispatch(j replay.ReplayJob) *pb.VerifierDispatch {
	return &pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: ReplayJobToPB(j)}}
}

// ByteSideScanDispatch wraps the check-3 scan request (§7.1 round two).
func ByteSideScanDispatch(blockSeq uint64, parts []arbiter.PartRef) *pb.VerifierDispatch {
	return &pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ByteSideScan{ByteSideScan: &pb.ByteSideScanRequest{
		BlockSeq: blockSeq,
		Parts:    mapSlice(parts, partRefToPB),
	}}}
}

// PromotionCommandPB wraps a signed promotion for the SNode stream.
func PromotionCommandPB(p arbiter.PromoteSafePartition, jws string) *pb.PromotionCommand {
	return &pb.PromotionCommand{Cmd: &pb.PromotionCommand_Promote{Promote: PromoteToPB(p)}, AuthorityJws: jws}
}

// CleanupCommandPB wraps a signed cleanup for the SNode stream.
func CleanupCommandPB(c arbiter.UnsafeCleanup, jws string) *pb.PromotionCommand {
	return &pb.PromotionCommand{Cmd: &pb.PromotionCommand_Cleanup{Cleanup: CleanupToPB(c)}, AuthorityJws: jws}
}
