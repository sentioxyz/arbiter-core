package conformance

import (
	"reflect"
	"slices"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/sentioxyz/arbiter-core"
)

// assertMirrorOrder pins that a canonical stage-3 mirror lists its fields in
// proto field-number order (request-only fields skipped), so its canonical
// JSON, and every digest over it, follows the wire order (plan S3-E1 contract
// §41). assertMirror pins the names; this pins the order.
func assertMirrorOrder(t *testing.T, goValue any, msg proto.Message, requestOnly ...string) {
	t.Helper()
	rt := reflect.TypeOf(goValue)
	var goNames []string
	for i := 0; i < rt.NumField(); i++ {
		goNames = append(goNames, strings.Split(rt.Field(i).Tag.Get("json"), ",")[0])
	}
	var protoNames []string
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		if name := string(fields.Get(i).Name()); !slices.Contains(requestOnly, name) {
			protoNames = append(protoNames, name)
		}
	}
	if !slices.Equal(goNames, protoNames) {
		t.Errorf("%s fields %v are not in the proto order %v", rt.Name(), goNames, protoNames)
	}
}

// TestStage3CanonicalMirrorsMatchProto pins every stage-3 canonical type of
// contract §41 to its pb message by name and order. The signatures beside
// the signed bodies (authority_jws, ed25519_signature) are request-only. The
// closed exclusion-reason set is the pb enum without UNSPECIFIED, and the
// switch names are the last seven pb.Stage3Params fields in order.
func TestStage3CanonicalMirrorsMatchProto(t *testing.T) {
	for _, tc := range []struct {
		goValue     any
		msg         proto.Message
		requestOnly []string
	}{
		{arbiter.Stage3Params{}, &pb.Stage3Params{}, nil},
		{arbiter.IndexerFailureDomain{}, &pb.IndexerFailureDomain{}, nil},
		{arbiter.VerifierEntry{}, &pb.VerifierEntry{}, nil},
		{arbiter.RebalanceGroupCommand{}, &pb.RebalanceGroupCmd{}, []string{"authority_jws"}},
		{arbiter.ReselectVerifiersCommand{}, &pb.ReselectVerifiersCmd{}, []string{"authority_jws"}},
		{arbiter.MarkMemberLaggingCommand{}, &pb.MarkMemberLaggingCmd{}, []string{"authority_jws"}},
		{arbiter.MemberReadyBody{}, &pb.MemberReadyCmd{}, []string{"ed25519_signature"}},
		{arbiter.MemberCheckpoint{}, &pb.MemberCheckpoint{}, nil},
		{arbiter.CheckpointGroup{}, &pb.CheckpointGroup{}, nil},
		{arbiter.ExcludedTable{}, &pb.ExcludedTable{}, nil},
	} {
		assertMirror(t, tc.goValue, tc.msg, tc.requestOnly...)
		assertMirrorOrder(t, tc.goValue, tc.msg, tc.requestOnly...)
	}
	values := pb.CheckpointExclusionReason(0).Descriptor().Values()
	var reasons []arbiter.CheckpointExclusionReason
	for i := 0; i < values.Len(); i++ {
		if n := values.Get(i).Number(); n != 0 {
			reasons = append(reasons, arbiter.CheckpointExclusionReason(strings.ToLower(strings.TrimPrefix(string(values.Get(i).Name()), "CHECKPOINT_EXCLUSION_REASON_"))))
		}
	}
	if want := []arbiter.CheckpointExclusionReason{arbiter.ExclusionNotInstalled, arbiter.ExclusionDiverged, arbiter.ExclusionForeign,
		arbiter.ExclusionDiskThreshold, arbiter.ExclusionAuditPending, arbiter.ExclusionRetiring}; !slices.Equal(reasons, want) {
		t.Fatalf("exclusion reasons %v, want the pb enum's %v", want, reasons)
	}
	fields := (&pb.Stage3Params{}).ProtoReflect().Descriptor().Fields()
	for i, name := range arbiter.Stage3Switches {
		f := fields.ByNumber(protoreflect.FieldNumber(12 + i))
		if f == nil || string(f.Name()) != name || f.Kind() != protoreflect.BoolKind {
			t.Fatalf("switch %s is not pb.Stage3Params field %d: %v", name, 12+i, f)
		}
	}
	if fields.Len() != 11+len(arbiter.Stage3Switches) {
		t.Fatalf("pb.Stage3Params has %d fields, want %d", fields.Len(), 11+len(arbiter.Stage3Switches))
	}
}

// TestStage3AdmissionCodesMatchProto pins the two stage-3 admission codes
// (housegate spec 2026-10-14 §7.3, §9.3; plan S3-E1 K59).
func TestStage3AdmissionCodesMatchProto(t *testing.T) {
	if int32(arbiter.AdmissionCodeVerifiersUnavailable) != int32(pb.AdmissionCode_ADMISSION_CODE_VERIFIERS_UNAVAILABLE) || arbiter.AdmissionCodeVerifiersUnavailable != 12 ||
		int32(arbiter.AdmissionCodeCopiesBackpressure) != int32(pb.AdmissionCode_ADMISSION_CODE_COPIES_BACKPRESSURE) || arbiter.AdmissionCodeCopiesBackpressure != 13 {
		t.Fatal("the stage-3 AdmissionCode constants drifted from the pb enum numbers 12 and 13")
	}
}
