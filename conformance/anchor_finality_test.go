package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestAnchorFinalityFieldNumbers pins, on the side that encodes and strictly
// decodes them, every arbiter-proto field the anchor finality quorum and the
// part-claim uniqueness gate add. wire.Decode refuses unknown fields, so a
// voter built against other numbers would refuse or misread the first update
// or attestation that carries one of them.
func TestAnchorFinalityFieldNumbers(t *testing.T) {
	const (
		kStr   = protoreflect.StringKind
		kU32   = protoreflect.Uint32Kind
		kU64   = protoreflect.Uint64Kind
		kBool  = protoreflect.BoolKind
		kBytes = protoreflect.BytesKind
		kMsg   = protoreflect.MessageKind
	)
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		msgType  protoreflect.FullName
	}{
		{&pb.ConsensusParamsUpdate{}, "part_claim_uniqueness", 18, kBool, false, ""},
		{&pb.ConsensusParamsUpdate{}, "voters", 19, kMsg, true, "arbiter.VoterEntry"},
		{&pb.ConsensusParamsUpdate{}, "anchor_finality", 20, kMsg, false, "arbiter.AnchorFinalityParams"},
		{&pb.ConsensusMutableParams{}, "part_claim_uniqueness", 13, kBool, false, ""},
		{&pb.ConsensusMutableParams{}, "voters", 14, kMsg, true, "arbiter.VoterEntry"},
		{&pb.ConsensusMutableParams{}, "anchor_finality", 15, kMsg, false, "arbiter.AnchorFinalityParams"},
		{&pb.VoterEntry{}, "voter_id", 1, kStr, false, ""},
		{&pb.VoterEntry{}, "ed25519_pubkey", 2, kBytes, false, ""},
		{&pb.VoterEntry{}, "consent_sig", 3, kStr, false, ""},
		{&pb.AnchorFinalityParams{}, "quorum", 1, kU32, false, ""},
		{&pb.AnchorFinalityParams{}, "finality_mode", 2, kStr, false, ""},
		{&pb.AnchorFinalityParams{}, "finality_confirmations", 3, kU64, false, ""},
		{&pb.AnchorFinalityParams{}, "last_mergeable_confirmations", 4, kU64, false, ""},
		{&pb.AnchorFinalityParams{}, "l2_chain_id", 5, kU64, false, ""},
		{&pb.AnchorFinalityParams{}, "registry_contract", 6, kStr, false, ""},
		{&pb.AnchorFinalityAttestation{}, "voter_id", 1, kStr, false, ""},
		{&pb.AnchorFinalityAttestation{}, "l3_block_seq", 2, kU64, false, ""},
		{&pb.AnchorFinalityAttestation{}, "l3_block_hash", 3, kStr, false, ""},
		{&pb.AnchorFinalityAttestation{}, "state_root", 4, kStr, false, ""},
		{&pb.AnchorFinalityAttestation{}, "l2_block_number", 5, kU64, false, ""},
		{&pb.AnchorFinalityAttestation{}, "final", 6, kBool, false, ""},
		{&pb.AnchorFinalityAttestation{}, "last_mergeable", 7, kBool, false, ""},
		{&pb.AnchorFinalityAttestation{}, "signature", 8, kStr, false, ""},
		{&pb.RecordAnchorFinalityAttestationCmd{}, "attestation", 1, kMsg, false, "arbiter.AnchorFinalityAttestation"},
		{&pb.RaftCommand{}, "record_anchor_finality_attestation", 43, kMsg, false, "arbiter.RecordAnchorFinalityAttestationCmd"},
		{&pb.BlockVerification{}, "finality_attestations", 23, kMsg, true, "arbiter.AnchorFinalityAttestation"},
		{&pb.ProtocolInfo{}, "voter_ed25519_pubkey", 5, kBytes, false, ""},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() != tc.repeated || f.HasOptionalKeyword() {
				t.Fatalf("field = %v, want number %d kind %s repeated %v", f, tc.number, tc.kind, tc.repeated)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
}
