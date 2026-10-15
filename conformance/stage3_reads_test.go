package conformance

import (
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/sentioxyz/arbiter-core/wire"
)

// TestStage3ReadFieldNumbers pins, on the side that converts them, every field
// of the stage-3 reads (housegate spec 2026-10-14 §15; plan S3-A contract §1).
// The wire converters name each field; a renumbered or retyped field would
// pass the Go build and silently read another value.
func TestStage3ReadFieldNumbers(t *testing.T) {
	const (
		kStr  = protoreflect.StringKind
		kU64  = protoreflect.Uint64Kind
		kU32  = protoreflect.Uint32Kind
		kBool = protoreflect.BoolKind
		kMsg  = protoreflect.MessageKind
	)
	for _, tc := range []struct {
		msg      proto.Message
		name     protoreflect.Name
		number   protoreflect.FieldNumber
		kind     protoreflect.Kind
		repeated bool
		msgType  protoreflect.FullName
	}{
		{&pb.GetPartitionStatesRequest{}, "table_id", 1, kStr, false, ""},
		{&pb.GetPartitionStatesRequest{}, "partition_id", 2, kStr, false, ""},
		{&pb.PartitionStateEntry{}, "table_id", 1, kStr, false, ""},
		{&pb.PartitionStateEntry{}, "partition_id", 2, kStr, false, ""},
		{&pb.PartitionStateEntry{}, "base_safe_snapshot_id", 3, kStr, false, ""},
		{&pb.PartitionStateEntry{}, "base_partition_root", 4, kStr, false, ""},
		{&pb.PartitionStateEntry{}, "publish_seq", 5, kU64, false, ""},
		{&pb.PartitionStateEntry{}, "safe_parts", 6, kMsg, true, "arbiter.PartManifestEntry"},
		{&pb.PartitionStateEntry{}, "promoted_unsafe_part_row_lthashes", 7, kStr, true, ""},
		{&pb.PartitionStateEntry{}, "unacked_promotion_seq", 8, kU64, false, ""},
		{&pb.PartitionStateEntry{}, "last_acked_promotion_seq", 9, kU64, false, ""},
		{&pb.PartitionStateEntry{}, "inventory_unrefreshed", 10, kBool, false, ""},
		{&pb.PartitionStates{}, "promotion_seq", 1, kU64, false, ""},
		{&pb.PartitionStates{}, "safe_watermark", 2, kMsg, false, "arbiter.SafeWatermark"},
		{&pb.PartitionStates{}, "partitions", 3, kMsg, true, "arbiter.PartitionStateEntry"},
		{&pb.GetPromotionOutcomesRequest{}, "table_id", 1, kStr, false, ""},
		{&pb.GetPromotionOutcomesRequest{}, "partition_id", 2, kStr, false, ""},
		{&pb.GetPromotionOutcomesRequest{}, "after_promotion_seq", 3, kU64, false, ""},
		{&pb.GetPromotionOutcomesRequest{}, "limit", 4, kU32, false, ""},
		{&pb.PromotionOutcome{}, "promotion_seq", 1, kU64, false, ""},
		{&pb.PromotionOutcome{}, "promote", 2, kMsg, false, "arbiter.PromoteSafePartition"},
		{&pb.PromotionOutcome{}, "statement_seqs", 3, kU64, true, ""},
		{&pb.PromotionOutcome{}, "block_seq", 4, kU64, false, ""},
		{&pb.PromotionOutcome{}, "track", 5, kMsg, false, "arbiter.L3BlockTrack"},
		{&pb.PromotionOutcome{}, "source_node", 6, kStr, false, ""},
		{&pb.PromotionOutcome{}, "authority_jws", 7, kStr, false, ""},
		{&pb.PromotionOutcome{}, "authority_epoch", 8, kU64, false, ""},
		{&pb.PromotionOutcome{}, "acked", 9, kBool, false, ""},
		{&pb.PromotionOutcome{}, "ack", 10, kMsg, false, "arbiter.PromotionAck"},
		{&pb.PromotionOutcome{}, "ack_jws", 11, kStr, false, ""},
		{&pb.PromotionOutcome{}, "ack_epoch", 12, kU64, false, ""},
		{&pb.PromotionOutcome{}, "cleanup_pending", 13, kBool, false, ""},
		{&pb.PromotionOutcomes{}, "promotion_seq", 1, kU64, false, ""},
		{&pb.PromotionOutcomes{}, "outcomes", 2, kMsg, true, "arbiter.PromotionOutcome"},
		{&pb.PromotionOutcomes{}, "next_after_promotion_seq", 3, kU64, false, ""},
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

// TestStage3ReadTypesCarryEveryProtoField: each wire read type has exactly as
// many fields as its message, so a field a later arbiter-proto adds fails here
// until its converter copies it (the converters drop nothing silently).
func TestStage3ReadTypesCarryEveryProtoField(t *testing.T) {
	for _, tc := range []struct {
		goValue any
		msg     proto.Message
	}{
		{wire.PartitionStatesQuery{}, &pb.GetPartitionStatesRequest{}},
		{wire.PartitionState{}, &pb.PartitionStateEntry{}},
		{wire.PartitionStates{}, &pb.PartitionStates{}},
		{wire.PromotionOutcomesQuery{}, &pb.GetPromotionOutcomesRequest{}},
		{wire.PromotionOutcome{}, &pb.PromotionOutcome{}},
		{wire.PromotionOutcomes{}, &pb.PromotionOutcomes{}},
	} {
		goType, d := reflect.TypeOf(tc.goValue), tc.msg.ProtoReflect().Descriptor()
		if got, want := goType.NumField(), d.Fields().Len(); got != want {
			t.Errorf("wire.%s has %d fields, %s has %d", goType.Name(), got, d.FullName(), want)
		}
	}
}

// TestStage3ReadMethodsAreUnarySafeStateRPCs pins the three stage-3 reads on
// the SafeState service (contract §1). GetBlockVerification has no canonical
// Go type in S3-A; its presence is pinned so the tools that read it raw can
// rely on it.
func TestStage3ReadMethodsAreUnarySafeStateRPCs(t *testing.T) {
	safeState := pb.File_arbiter_proto.Services().ByName("SafeState")
	if safeState == nil {
		t.Fatal("arbiter.proto has no SafeState service")
	}
	for method, types := range map[protoreflect.Name][2]protoreflect.FullName{
		"GetPartitionStates":   {"arbiter.GetPartitionStatesRequest", "arbiter.PartitionStates"},
		"GetPromotionOutcomes": {"arbiter.GetPromotionOutcomesRequest", "arbiter.PromotionOutcomes"},
		"GetBlockVerification": {"arbiter.L3BlockRef", "arbiter.BlockVerification"},
	} {
		m := safeState.Methods().ByName(method)
		if m == nil || m.Input().FullName() != types[0] || m.Output().FullName() != types[1] || m.IsStreamingClient() || m.IsStreamingServer() {
			t.Fatalf("SafeState.%s = %v, want unary %s -> %s", method, m, types[0], types[1])
		}
	}
}
