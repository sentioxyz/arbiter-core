package wire

import (
	"slices"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// Request converters (housegate spec 2026-10-10 §6.5). A signed RPC keeps its
// request message: the signature rides in a request-only field beside a body
// that excludes it, and the Raft command carries it in a field of its own.
// Servers decode a request with *FromRequest and propose the result (through
// StripSignedClaims before the signed-claims activation); data-plane clients
// build requests with *ToRequest.

// RegisterRCFromRequest decodes a SourceClaims.RegisterResultClaim request.
func RegisterRCFromRequest(m *pb.RCRecord) RegisterRC {
	return RegisterRC{RC: RCFromPB(m), SourceJWS: m.GetSourceJws()}
}

// RegisterRCToRequest builds a RegisterResultClaim request.
func RegisterRCToRequest(c RegisterRC) *pb.RCRecord {
	m := RCToPB(c.RC)
	m.SourceJws = c.SourceJWS
	return m
}

// RecordPromotionAckFromRequest decodes a PromotionGateway.AckPromotion request.
func RecordPromotionAckFromRequest(m *pb.PromotionAck) RecordPromotionAck {
	return RecordPromotionAck{Ack: PromotionAckFromPB(m), SourceJWS: m.GetSourceJws()}
}

// RecordPromotionAckToRequest builds an AckPromotion request.
func RecordPromotionAckToRequest(c RecordPromotionAck) *pb.PromotionAck {
	m := PromotionAckToPB(c.Ack)
	m.SourceJws = c.SourceJWS
	return m
}

// RecordCleanupAckFromRequest decodes a PromotionGateway.AckCleanup request.
func RecordCleanupAckFromRequest(m *pb.CleanupAck) RecordCleanupAck {
	return RecordCleanupAck{Ack: CleanupAckFromPB(m), SourceJWS: m.GetSourceJws()}
}

// RecordCleanupAckToRequest builds an AckCleanup request.
func RecordCleanupAckToRequest(c RecordCleanupAck) *pb.CleanupAck {
	m := CleanupAckToPB(c.Ack)
	m.SourceJws = c.SourceJWS
	return m
}

// RegisterNodeFromRequest decodes a Membership.RegisterNode request. The
// request-only features stay out; the leader keeps them in its feature book.
func RegisterNodeFromRequest(m *pb.NodeRegistration) RegisterNode {
	return RegisterNode{Registration: RegistrationFromPB(m), SignerJWS: m.GetSignerJws(), Ed25519Signature: m.GetEd25519Signature()}
}

// RegisterNodeToRequest builds a RegisterNode request advertising features.
func RegisterNodeToRequest(c RegisterNode, features []string) *pb.NodeRegistration {
	m := RegistrationToPB(c.Registration)
	m.SignerJws, m.Ed25519Signature = c.SignerJWS, c.Ed25519Signature
	m.Features = slices.Clone(features)
	return m
}

// MarkActiveFromRequest decodes a Membership.MarkActive request.
func MarkActiveFromRequest(m *pb.NodeRef) MarkActive {
	return MarkActive{NodeID: m.GetNodeId(), RegistrationSeq: m.GetRegistrationSeq(),
		SignerJWS: m.GetSignerJws(), Ed25519Signature: m.GetEd25519Signature()}
}

// MarkActiveToRequest builds a MarkActive request.
func MarkActiveToRequest(c MarkActive) *pb.NodeRef {
	return &pb.NodeRef{NodeId: c.NodeID, RegistrationSeq: c.RegistrationSeq, SignerJws: c.SignerJWS, Ed25519Signature: c.Ed25519Signature}
}

// RecordTablePurgedFromRequest decodes a SubmitTablePurged request, which is
// the RecordTablePurgedCmd itself.
func RecordTablePurgedFromRequest(m *pb.RecordTablePurgedCmd) RecordTablePurged {
	return RecordTablePurged{NodeID: m.GetNodeId(), IncarnationSeq: m.GetIncarnationSeq(),
		SignerJWS: m.GetSignerJws(), Ed25519Signature: m.GetEd25519Signature()}
}

// RecordTablePurgedToRequest builds the SubmitTablePurged request (and the
// Raft command body).
func RecordTablePurgedToRequest(c RecordTablePurged) *pb.RecordTablePurgedCmd {
	return &pb.RecordTablePurgedCmd{NodeId: c.NodeID, IncarnationSeq: c.IncarnationSeq,
		SignerJws: c.SignerJWS, Ed25519Signature: c.Ed25519Signature}
}

// EvictNodeFromRequest decodes a ConsensusAdmin.EvictNode request.
func EvictNodeFromRequest(m *pb.EvictNodeRequest) EvictNode {
	return EvictNode{NodeID: m.GetNodeId(), Reason: m.GetReason(),
		ExpectedRegistrationSeq: m.GetExpectedRegistrationSeq(), AuthorityJWS: m.GetAuthorityJws()}
}

// EvictNodeToRequest builds an EvictNode request (arbiter-admin).
func EvictNodeToRequest(c EvictNode) *pb.EvictNodeRequest {
	return &pb.EvictNodeRequest{NodeId: c.NodeID, ExpectedRegistrationSeq: c.ExpectedRegistrationSeq,
		Reason: c.Reason, AuthorityJws: c.AuthorityJWS}
}
