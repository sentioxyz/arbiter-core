package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestSignedClaimsFieldNumbers pins, on the side that encodes and strictly
// decodes them, every arbiter-proto field the signed-claims stage adds
// (housegate spec 2026-10-10 §6; plan S1-A CONTRACT §1, §3a). wire.Decode
// refuses unknown fields, so a voter built against other numbers would refuse
// or misread the first command that carries one of them.
func TestSignedClaimsFieldNumbers(t *testing.T) {
	const (
		kStr = protoreflect.StringKind
		kU64 = protoreflect.Uint64Kind
		kMsg = protoreflect.MessageKind
	)
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		optional bool
		msgType  protoreflect.FullName
	}{
		{&pb.SIIndexerEntry{}, "indexer_id", 1, kU64, false, false, ""},
		{&pb.SIIndexerEntry{}, "activation_block", 2, kU64, false, false, ""},
		{&pb.SIIndexerEntry{}, "signer", 3, kStr, false, false, ""},
		{&pb.SIIndexerEntry{}, "snode_node_id", 4, kStr, false, false, ""},
		{&pb.SIIndexerEntry{}, "enrollment_jws", 5, kStr, false, false, ""},
		{&pb.VerifierEntry{}, "node_id", 1, kStr, false, false, ""},
		{&pb.VerifierEntry{}, "ed25519_pubkey", 2, protoreflect.BytesKind, false, false, ""},
		{&pb.ConsensusParamsUpdate{}, "si_indexers", 11, kMsg, true, false, "arbiter.SIIndexerEntry"},
		{&pb.ConsensusParamsUpdate{}, "verifiers", 12, kMsg, true, false, "arbiter.VerifierEntry"},
		{&pb.ConsensusMutableParams{}, "si_indexers", 6, kMsg, true, false, "arbiter.SIIndexerEntry"},
		{&pb.ConsensusMutableParams{}, "verifiers", 7, kMsg, true, false, "arbiter.VerifierEntry"},
		{&pb.EvictNodeRequest{}, "node_id", 1, kStr, false, false, ""},
		{&pb.EvictNodeRequest{}, "expected_registration_seq", 2, kU64, false, false, ""},
		{&pb.EvictNodeRequest{}, "reason", 3, kStr, false, false, ""},
		{&pb.EvictNodeRequest{}, "authority_jws", 4, kStr, false, false, ""},
		{&pb.TableRegistrySnapshot{}, "si_indexers", 7, kMsg, true, false, "arbiter.SIIndexerEntry"},
		{&pb.TableRegistrySnapshot{}, "seeded_indexers", 8, kU64, true, false, ""},
		{&pb.TableIncarnation{}, "owner_indexer_id", 18, kU64, false, true, ""},
		{&pb.AddTableCmd{}, "owner_indexer_id", 8, kU64, false, true, ""},
		{&pb.SeedLegacyTablesCmd{}, "indexer_id", 3, kU64, false, true, ""},
		{&pb.RecordTablePurgedCmd{}, "signer_jws", 3, kStr, false, false, ""},
		{&pb.RecordTablePurgedCmd{}, "ed25519_signature", 4, kStr, false, false, ""},
		{&pb.NodeRegistration{}, "registration_seq", 6, kU64, false, false, ""},
		{&pb.NodeRegistration{}, "signer_jws", 7, kStr, false, false, ""},
		{&pb.NodeRegistration{}, "ed25519_signature", 8, kStr, false, false, ""},
		{&pb.NodeRef{}, "registration_seq", 2, kU64, false, false, ""},
		{&pb.NodeRef{}, "signer_jws", 3, kStr, false, false, ""},
		{&pb.NodeRef{}, "ed25519_signature", 4, kStr, false, false, ""},
		{&pb.RCRecord{}, "source_jws", 6, kStr, false, false, ""},
		{&pb.PromotionAck{}, "source_jws", 10, kStr, false, false, ""},
		{&pb.CleanupAck{}, "source_jws", 5, kStr, false, false, ""},
		{&pb.RegisterRCCmd{}, "source_jws", 2, kStr, false, false, ""},
		{&pb.RecordPromotionAckCmd{}, "source_jws", 2, kStr, false, false, ""},
		{&pb.RecordCleanupAckCmd{}, "source_jws", 2, kStr, false, false, ""},
		{&pb.RegisterNodeCmd{}, "signer_jws", 2, kStr, false, false, ""},
		{&pb.RegisterNodeCmd{}, "ed25519_signature", 3, kStr, false, false, ""},
		{&pb.MarkActiveCmd{}, "registration_seq", 2, kU64, false, false, ""},
		{&pb.MarkActiveCmd{}, "signer_jws", 3, kStr, false, false, ""},
		{&pb.MarkActiveCmd{}, "ed25519_signature", 4, kStr, false, false, ""},
		{&pb.EvictNodeCmd{}, "expected_registration_seq", 3, kU64, false, false, ""},
		{&pb.EvictNodeCmd{}, "authority_jws", 4, kStr, false, false, ""},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated || f.HasOptionalKeyword() != tc.optional {
				t.Fatalf("field = %v, want number %d kind %s repeated %v optional %v", f, tc.number, tc.kind, tc.repeated, tc.optional)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_SOURCE_UNAVAILABLE); got != 10 {
		t.Fatalf("ADMISSION_CODE_SOURCE_UNAVAILABLE = %d, want 10", got)
	}
	m := pb.File_consensus_proto.Services().ByName("ConsensusAdmin").Methods().ByName("EvictNode")
	if m == nil || m.Input().FullName() != "arbiter.EvictNodeRequest" || m.Output().FullName() != "arbiter.Ack" || m.IsStreamingClient() || m.IsStreamingServer() {
		t.Fatalf("ConsensusAdmin.EvictNode = %v, want unary arbiter.EvictNodeRequest -> arbiter.Ack", m)
	}
}
