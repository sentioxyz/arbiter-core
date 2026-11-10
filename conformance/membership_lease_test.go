package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/sentioxyz/arbiter-core/authority"
)

// TestMembershipLeaseFieldNumbers pins, on the side that encodes and strictly
// decodes them, every arbiter-proto field the membership lease adds
// (arbiter-core#66). wire.Decode refuses unknown fields, so a voter built
// against other numbers would refuse or misread the first update or renewal
// that carries one of them.
func TestMembershipLeaseFieldNumbers(t *testing.T) {
	const (
		kStr = protoreflect.StringKind
		kU64 = protoreflect.Uint64Kind
		kMsg = protoreflect.MessageKind
	)
	for _, tc := range []struct {
		msg     proto.Message
		name    protoreflect.Name
		number  protoreflect.FieldNumber
		kind    protoreflect.Kind
		msgType protoreflect.FullName
	}{
		{&pb.ConsensusParamsUpdate{}, "membership_lease_l2_blocks", 22, kU64, ""},
		{&pb.ConsensusMutableParams{}, "membership_lease_l2_blocks", 17, kU64, ""},
		{&pb.NodeLeaseRenewal{}, "node_id", 1, kStr, ""},
		{&pb.NodeLeaseRenewal{}, "registration_seq", 2, kU64, ""},
		{&pb.NodeLeaseRenewal{}, "lease_seq", 3, kU64, ""},
		{&pb.NodeLeaseRenewal{}, "signer_jws", 4, kStr, ""},
		{&pb.NodeLeaseRenewal{}, "ed25519_signature", 5, kStr, ""},
		{&pb.RenewNodeLeaseCmd{}, "renewal", 1, kMsg, "arbiter.NodeLeaseRenewal"},
		{&pb.RaftCommand{}, "renew_node_lease", 44, kMsg, "arbiter.RenewNodeLeaseCmd"},
	} {
		d := tc.msg.ProtoReflect().Descriptor()
		t.Run(string(d.Name())+"."+string(tc.name), func(t *testing.T) {
			f := d.Fields().ByName(tc.name)
			if f == nil || f.Number() != tc.number || f.Kind() != tc.kind || f.IsList() || f.HasOptionalKeyword() {
				t.Fatalf("field = %v, want number %d kind %s", f, tc.number, tc.kind)
			}
			if tc.msgType != "" && f.Message().FullName() != tc.msgType {
				t.Fatalf("message type = %s, want %s", f.Message().FullName(), tc.msgType)
			}
		})
	}
	if n := (&pb.NodeLeaseRenewal{}).ProtoReflect().Descriptor().Fields().Len(); n != 5 {
		t.Fatalf("NodeLeaseRenewal has %d fields, want 5", n)
	}
	// The signed body is the renewal minus its two signatures.
	assertMirror(t, authority.LeaseRenewalBody{}, &pb.NodeLeaseRenewal{}, "signer_jws", "ed25519_signature")
}
