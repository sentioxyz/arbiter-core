package conformance

import (
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// assertMirror pins a canonical Go mirror struct to its pb message: the set
// of json tags must equal the set of proto field names. This is the same
// freeze discipline as replay_wire_test.go, mechanized.
func assertMirror(t *testing.T, goValue any, msg proto.Message) {
	t.Helper()
	goTags := map[string]bool{}
	rt := reflect.TypeOf(goValue)
	for i := 0; i < rt.NumField(); i++ {
		tag := strings.Split(rt.Field(i).Tag.Get("json"), ",")[0]
		if tag == "" || tag == "-" {
			t.Fatalf("%s.%s: every mirror field needs a json tag", rt.Name(), rt.Field(i).Name)
		}
		goTags[tag] = true
	}
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		name := string(fields.Get(i).Name())
		if !goTags[name] {
			t.Errorf("%s: proto field %q has no Go mirror json tag", rt.Name(), name)
		}
		delete(goTags, name)
	}
	for tag := range goTags {
		t.Errorf("%s: Go json tag %q has no proto field", rt.Name(), tag)
	}
}

func TestArbiterMirrorsMatchProto(t *testing.T) {
	assertMirror(t, arbiter.StatementID{}, &pb.StatementID{})
	assertMirror(t, arbiter.StatementEnvelope{}, &pb.StatementEnvelopeV2{})
	assertMirror(t, arbiter.CandidatePart{}, &pb.CandidatePart{})
	assertMirror(t, arbiter.PartitionLtHashSum{}, &pb.PartitionLtHashSum{})
	assertMirror(t, arbiter.RCRecord{}, &pb.RCRecord{})
	assertMirror(t, arbiter.PartScan{}, &pb.PartScan{})
	assertMirror(t, arbiter.ByteSideScanMsg{}, &pb.ByteSideScanMsg{})
	assertMirror(t, arbiter.AnchorRef{}, &pb.AnchorRef{})
	assertMirror(t, arbiter.NodeRegistration{}, &pb.NodeRegistration{})
	assertMirror(t, arbiter.SafePartMapping{}, &pb.SafePartMapping{})
	assertMirror(t, arbiter.PromotionAck{}, &pb.PromotionAck{})
	assertMirror(t, arbiter.CleanupAck{}, &pb.CleanupAck{})
}

func TestEnumNumbersMatchProto(t *testing.T) {
	if int32(arbiter.AdmissionCodeUnspecified) != int32(pb.AdmissionCode_ADMISSION_CODE_UNSPECIFIED) ||
		int32(arbiter.AdmissionCodeAccepted) != int32(pb.AdmissionCode_ADMISSION_CODE_ACCEPTED) ||
		int32(arbiter.AdmissionCodeDuplicateClientSeq) != int32(pb.AdmissionCode_ADMISSION_CODE_DUPLICATE_CLIENT_SEQ) ||
		int32(arbiter.AdmissionCodeSchemaNotAllowed) != int32(pb.AdmissionCode_ADMISSION_CODE_SCHEMA_NOT_ALLOWED) ||
		int32(arbiter.AdmissionCodeKindNotAdmitted) != int32(pb.AdmissionCode_ADMISSION_CODE_KIND_NOT_ADMITTED) ||
		int32(arbiter.AdmissionCodeInvalidSignature) != int32(pb.AdmissionCode_ADMISSION_CODE_INVALID_SIGNATURE) ||
		int32(arbiter.AdmissionCodeInvalidProof) != int32(pb.AdmissionCode_ADMISSION_CODE_INVALID_PROOF) ||
		int32(arbiter.AdmissionCodeMalformed) != int32(pb.AdmissionCode_ADMISSION_CODE_MALFORMED) ||
		int32(arbiter.AdmissionCodeGapBudgetExceeded) != int32(pb.AdmissionCode_ADMISSION_CODE_GAP_BUDGET_EXCEEDED) {
		t.Fatal("AdmissionCode Go constants drifted from pb enum numbers")
	}
	if int32(arbiter.NodeRoleUnspecified) != int32(pb.NodeRole_NODE_ROLE_UNSPECIFIED) ||
		int32(arbiter.NodeRoleVerifier) != int32(pb.NodeRole_NODE_ROLE_VERIFIER) ||
		int32(arbiter.NodeRoleSNode) != int32(pb.NodeRole_NODE_ROLE_SNODE) {
		t.Fatal("NodeRole Go constants drifted from pb enum numbers")
	}
	if int32(arbiter.StatementKindUnspecified) != int32(pb.StatementKind_STATEMENT_KIND_UNSPECIFIED) ||
		int32(arbiter.StatementKindInsert) != int32(pb.StatementKind_STATEMENT_KIND_INSERT) {
		t.Fatal("StatementKind Go constants drifted from pb enum numbers")
	}
}
