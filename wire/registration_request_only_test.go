package wire

import (
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// TestRegistrationFeaturesNeverEnterARaftCommand pins housegate spec
// 2026-10-09 §5.6: NodeRegistration.features is request-only. A leader that
// encoded it into a RegisterNode command would make every voter that does not
// know field 5 reject the entry while upgraded voters apply it, forking
// membership.
func TestRegistrationFeaturesNeverEnterARaftCommand(t *testing.T) {
	req := &pb.NodeRegistration{NodeId: "v1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER},
		Ed25519Pubkey: make([]byte, 32), Features: []string{arbiter.ClientLanesFeature}}
	reg := RegistrationFromPB(req)
	if out := RegistrationToPB(reg); len(out.GetFeatures()) != 0 {
		t.Fatalf("RegistrationToPB set features %v", out.GetFeatures())
	}
	b, err := Encode(Command{RegisterNode: &RegisterNode{Registration: reg}})
	if err != nil {
		t.Fatal(err)
	}
	var cmd pb.RaftCommand
	if err := proto.Unmarshal(b, &cmd); err != nil {
		t.Fatal(err)
	}
	if got := cmd.GetRegisterNode().GetRegistration(); len(got.GetFeatures()) != 0 || len(got.ProtoReflect().GetUnknown()) != 0 {
		t.Fatalf("encoded registration carries features or unknown fields: %v", got)
	}
}

// TestDecodeRefusesARegistrationCommandCarryingFeatures makes a buggy encoder
// fail identically on every voter: an old voter refuses field 5 as unknown, a
// lane-aware voter refuses it here, so no voter applies it.
func TestDecodeRefusesARegistrationCommandCarryingFeatures(t *testing.T) {
	b, err := proto.Marshal(&pb.RaftCommand{Cmd: &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{
		Registration: &pb.NodeRegistration{NodeId: "s1", Roles: []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}, Features: []string{"client_lanes_v1"}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(b); err == nil || !strings.Contains(err.Error(), "request-only") {
		t.Fatalf("Decode = %v, want a request-only refusal", err)
	}
}
