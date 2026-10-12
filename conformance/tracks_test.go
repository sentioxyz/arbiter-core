package conformance

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// TestTracksFieldNumbers pins, on the side that encodes and strictly decodes
// them, every arbiter-proto field the tracks stage adds that arbiter-core
// carries (housegate spec 2026-10-10 §7; plan S2-A contract §1). wire.Decode
// refuses unknown fields, so a voter built against other numbers would refuse
// or misread the first command that carries one of them.
func TestTracksFieldNumbers(t *testing.T) {
	const (
		kStr  = protoreflect.StringKind
		kU64  = protoreflect.Uint64Kind
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
		{&pb.TrackFrontier{}, "indexer_id", 1, kU64, false, ""},
		{&pb.TrackFrontier{}, "safe_block_seq", 2, kU64, false, ""},
		{&pb.TrackFrontier{}, "applied_promotion_seq", 3, kU64, false, ""},
		{&pb.SafeSnapshotManifest{}, "track_frontiers", 11, kMsg, true, "arbiter.TrackFrontier"},
		{&pb.UnsafeCleanup{}, "rejected_block_seq", 5, kU64, false, ""},
		{&pb.CleanupAck{}, "rejected_block_seq", 6, kU64, false, ""},
		{&pb.ConsensusParamsUpdate{}, "tracks_enabled", 13, kBool, false, ""},
		{&pb.ConsensusMutableParams{}, "tracks_enabled", 8, kBool, false, ""},
		{&pb.QuarantineIndexerCmd{}, "indexer_id", 1, kU64, false, ""},
		{&pb.QuarantineIndexerCmd{}, "expected_generation", 2, kU64, false, ""},
		{&pb.QuarantineIndexerCmd{}, "reason", 3, kStr, false, ""},
		{&pb.QuarantineIndexerCmd{}, "authority_jws", 4, kStr, false, ""},
		{&pb.ReleaseIndexerCmd{}, "indexer_id", 1, kU64, false, ""},
		{&pb.ReleaseIndexerCmd{}, "expected_generation", 2, kU64, false, ""},
		{&pb.ReleaseIndexerCmd{}, "acknowledged_rejected_blocks", 3, kU64, true, ""},
		{&pb.ReleaseIndexerCmd{}, "authority_jws", 4, kStr, false, ""},
		{&pb.RaftCommand{}, "quarantine_indexer", 36, kMsg, false, "arbiter.QuarantineIndexerCmd"},
		{&pb.RaftCommand{}, "release_indexer", 37, kMsg, false, "arbiter.ReleaseIndexerCmd"},
		{&pb.TableRegistrySnapshot{}, "quarantined_indexers", 9, kU64, true, ""},
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
	raft := (&pb.RaftCommand{}).ProtoReflect().Descriptor()
	for _, name := range []protoreflect.Name{"quarantine_indexer", "release_indexer"} {
		if f := raft.Fields().ByName(name); f == nil || f.ContainingOneof() == nil || f.ContainingOneof().Name() != "cmd" {
			t.Fatalf("RaftCommand.%s is not a member of oneof cmd: %v", name, f)
		}
	}
	if f := raft.Fields().ByNumber(29); f != nil {
		t.Fatalf("RaftCommand tag 29 must stay unused, found %s", f.Name())
	}
	if got := int32(pb.AdmissionCode_ADMISSION_CODE_INDEXER_QUARANTINED); got != 11 {
		t.Fatalf("ADMISSION_CODE_INDEXER_QUARANTINED = %d, want 11", got)
	}
	admin := pb.File_consensus_proto.Services().ByName("ConsensusAdmin")
	for method, input := range map[protoreflect.Name]protoreflect.FullName{
		"QuarantineIndexer": "arbiter.QuarantineIndexerCmd",
		"ReleaseIndexer":    "arbiter.ReleaseIndexerCmd",
	} {
		m := admin.Methods().ByName(method)
		if m == nil || m.Input().FullName() != input || m.Output().FullName() != "arbiter.Ack" || m.IsStreamingClient() || m.IsStreamingServer() {
			t.Fatalf("ConsensusAdmin.%s = %v, want unary %s -> arbiter.Ack", method, m, input)
		}
	}
}
