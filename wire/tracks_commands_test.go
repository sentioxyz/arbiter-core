package wire

import (
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// TestTrackCommandsRoundTrip: QuarantineIndexer and ReleaseIndexer encode at
// Raft tags 36 and 37 and decode to the identical command, indexer 0 and
// generation 0 included (plan S2-A contract §1-§2). An arbiter-core built
// before this change refuses both tags ("wire: unknown arbiter.RaftCommand
// field 36"), which is why arbiter proposes neither before the tracks
// activation.
func TestTrackCommandsRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		tag protowire.Number
		cmd Command
	}{
		{36, Command{QuarantineIndexer: &QuarantineIndexer{IndexerID: 0, ExpectedGeneration: 0, Reason: "snode-1 stalled", AuthorityJWS: "h.p.s"}}},
		{36, Command{QuarantineIndexer: &QuarantineIndexer{IndexerID: 1, ExpectedGeneration: 4, Reason: "block 7 rejected", AuthorityJWS: "h.p.s"}}},
		{37, Command{ReleaseIndexer: &ReleaseIndexer{IndexerID: 1, ExpectedGeneration: 2, AcknowledgedRejectedBlocks: []uint64{7, 9}, AuthorityJWS: "h.p.s"}}},
		{37, Command{ReleaseIndexer: &ReleaseIndexer{IndexerID: 0, ExpectedGeneration: 1, AuthorityJWS: "h.p.s"}}},
	} {
		b, err := Encode(tc.cmd)
		if err != nil {
			t.Fatal(err)
		}
		if num, typ, n := protowire.ConsumeTag(b); n < 0 || num != tc.tag || typ != protowire.BytesType {
			t.Fatalf("outer tag = (%d, %d), want (%d, bytes)", num, typ, tc.tag)
		}
		mustRoundTrip(t, tc.cmd)
	}
	b, err := Encode(Command{ReleaseIndexer: &ReleaseIndexer{IndexerID: 1, ExpectedGeneration: 1, AcknowledgedRejectedBlocks: []uint64{}, AuthorityJWS: "h.p.s"}})
	if err != nil {
		t.Fatal(err)
	}
	got, err := Decode(b)
	if err != nil || got.ReleaseIndexer == nil || got.ReleaseIndexer.AcknowledgedRejectedBlocks != nil {
		t.Fatalf("an empty acknowledged list must decode to nil: %+v, %v", got.ReleaseIndexer, err)
	}
	if _, err := Encode(Command{QuarantineIndexer: &QuarantineIndexer{}, ReleaseIndexer: &ReleaseIndexer{}}); err == nil {
		t.Fatal("two commands encoded together")
	}
}

// TestTrackCommandsCanonicalForms: Canonical drops the token and, for a
// release, sorts and dedupes the acknowledged blocks into a fresh slice, nil
// when empty, so a signer and the FSM hash one form (authority.ReleaseIndexerHash
// refuses anything else).
func TestTrackCommandsCanonicalForms(t *testing.T) {
	q := QuarantineIndexer{IndexerID: 1, ExpectedGeneration: 3, Reason: "stalled", AuthorityJWS: "h.p.s"}
	if q.Canonical() != (arbiter.QuarantineIndexerCommand{IndexerID: 1, ExpectedGeneration: 3, Reason: "stalled"}) {
		t.Fatalf("quarantine Canonical = %+v", q.Canonical())
	}
	blocks := []uint64{9, 7, 9}
	r := ReleaseIndexer{IndexerID: 1, ExpectedGeneration: 2, AcknowledgedRejectedBlocks: blocks, AuthorityJWS: "h.p.s"}
	want := arbiter.ReleaseIndexerCommand{IndexerID: 1, ExpectedGeneration: 2, AcknowledgedRejectedBlocks: []uint64{7, 9}}
	if got := r.Canonical(); !reflect.DeepEqual(got, want) {
		t.Fatalf("release Canonical = %+v, want %+v", got, want)
	}
	if !reflect.DeepEqual(blocks, []uint64{9, 7, 9}) {
		t.Fatal("Canonical modified the command's list")
	}
	if err := r.Canonical().Validate(); err != nil {
		t.Fatal(err)
	}
	for _, empty := range [][]uint64{nil, {}} {
		if got := (ReleaseIndexer{IndexerID: 1, AcknowledgedRejectedBlocks: empty}).Canonical(); got.AcknowledgedRejectedBlocks != nil {
			t.Fatalf("%#v: Canonical list = %#v, want nil", empty, got.AcknowledgedRejectedBlocks)
		}
	}
}

// TestTrackCommandRequestConverters: the ConsensusAdmin request is the Raft
// command message itself, so FromRequest / ToRequest are exact inverses that
// carry the token; an empty list decodes to nil, and FromRequest keeps an
// unsorted list as sent (the server refuses it, contract §3.8).
func TestTrackCommandRequestConverters(t *testing.T) {
	q := QuarantineIndexer{IndexerID: 0, ExpectedGeneration: 0, Reason: "snode-1 stalled", AuthorityJWS: "h.p.s"}
	req := QuarantineIndexerToRequest(q)
	if req.GetIndexerId() != 0 || req.GetExpectedGeneration() != 0 || req.GetReason() != "snode-1 stalled" || req.GetAuthorityJws() != "h.p.s" {
		t.Fatalf("quarantine request = %v", req)
	}
	if got := QuarantineIndexerFromRequest(req); got != q {
		t.Fatalf("quarantine request round trip = %+v", got)
	}
	r := ReleaseIndexer{IndexerID: 1, ExpectedGeneration: 2, AcknowledgedRejectedBlocks: []uint64{7, 9}, AuthorityJWS: "h.p.s"}
	rreq := ReleaseIndexerToRequest(r)
	if !reflect.DeepEqual(rreq.GetAcknowledgedRejectedBlocks(), []uint64{7, 9}) || rreq.GetAuthorityJws() != "h.p.s" {
		t.Fatalf("release request = %v", rreq)
	}
	if got := ReleaseIndexerFromRequest(rreq); !reflect.DeepEqual(got, r) {
		t.Fatalf("release request round trip = %+v", got)
	}
	rreq.AcknowledgedRejectedBlocks[0] = 8
	if r.AcknowledgedRejectedBlocks[0] != 7 {
		t.Fatal("ReleaseIndexerToRequest shares the command's list")
	}
	if got := ReleaseIndexerFromRequest(&pb.ReleaseIndexerCmd{IndexerId: 1, AcknowledgedRejectedBlocks: []uint64{}}); got.AcknowledgedRejectedBlocks != nil {
		t.Fatal("an empty acknowledged list must decode to nil")
	}
	if got := ReleaseIndexerFromRequest(&pb.ReleaseIndexerCmd{AcknowledgedRejectedBlocks: []uint64{9, 7}}); !reflect.DeepEqual(got.AcknowledgedRejectedBlocks, []uint64{9, 7}) {
		t.Fatalf("FromRequest must keep the list as sent, got %v", got.AcknowledgedRejectedBlocks)
	}
}

// TestDecodeRefusesUnknownFieldsInsideTrackMessages: the strict decoder covers
// the new messages at every depth, like every other command.
func TestDecodeRefusesUnknownFieldsInsideTrackMessages(t *testing.T) {
	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 9, protowire.VarintType), 1)
	quarantine, err := proto.Marshal(&pb.QuarantineIndexerCmd{IndexerId: 1, Reason: "r", AuthorityJws: "h.p.s"})
	if err != nil {
		t.Fatal(err)
	}
	release, err := proto.Marshal(&pb.ReleaseIndexerCmd{IndexerId: 1, AcknowledgedRejectedBlocks: []uint64{7}, AuthorityJws: "h.p.s"})
	if err != nil {
		t.Fatal(err)
	}
	frontier, err := proto.Marshal(&pb.TrackFrontier{IndexerId: 1, SafeBlockSeq: 2})
	if err != nil {
		t.Fatal(err)
	}
	manifest := unionMessage(11, append(frontier, unknown...))
	for name, tc := range map[string]struct {
		raw  []byte
		want string
	}{
		"QuarantineIndexerCmd": {unionMessage(36, append(quarantine, unknown...)), "wire: unknown arbiter.QuarantineIndexerCmd field 9"},
		"ReleaseIndexerCmd":    {unionMessage(37, append(release, unknown...)), "wire: unknown arbiter.ReleaseIndexerCmd field 9"},
		"TrackFrontier":        {unionMessage(10, unionMessage(1, manifest)), "wire: unknown arbiter.TrackFrontier field 9"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(tc.raw); err == nil || err.Error() != tc.want {
				t.Fatalf("Decode = %v, want %q", err, tc.want)
			}
		})
	}
}
