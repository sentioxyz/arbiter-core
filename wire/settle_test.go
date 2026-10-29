package wire

import (
	"reflect"
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// TestSettleTrackRequestRoundTrip: the SettleTrack request converters copy
// every field both ways, keep the list as sent (Canonical never sorts: the
// leader refuses a list that is not strictly ascending), turn an empty list
// into nil, and Canonical is the signing form without the token and timeout
// (plan S3-D contract §34).
func TestSettleTrackRequestRoundTrip(t *testing.T) {
	want := SettleTrack{IndexerID: 2, ExpectedGeneration: 3, PromotionSeqs: []uint64{41, 42}, AuthorityJWS: "h.p.s", TimeoutMS: 600_000}
	m := SettleTrackToRequest(want)
	if !proto.Equal(m, &pb.SettleTrackRequest{IndexerId: 2, ExpectedGeneration: 3, PromotionSeqs: []uint64{41, 42}, AuthorityJws: "h.p.s", TimeoutMs: 600_000}) {
		t.Fatalf("SettleTrackToRequest = %v", m)
	}
	got := SettleTrackFromRequest(m)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip = %+v, want %+v", got, want)
	}
	m.PromotionSeqs[0] = 99
	if got.PromotionSeqs[0] != 41 || want.PromotionSeqs[0] != 41 {
		t.Fatal("the converters must copy the promotion list")
	}
	if canonical := want.Canonical(); !reflect.DeepEqual(canonical, arbiter.SettleTrackCommand{IndexerID: 2, ExpectedGeneration: 3, PromotionSeqs: []uint64{41, 42}}) {
		t.Fatalf("Canonical = %+v", canonical)
	}
	unsorted := SettleTrackFromRequest(&pb.SettleTrackRequest{IndexerId: 0, PromotionSeqs: []uint64{42, 41}})
	if !slices.Equal(unsorted.Canonical().PromotionSeqs, []uint64{42, 41}) || unsorted.Canonical().Validate() == nil {
		t.Fatalf("Canonical changed an unsorted list: %+v", unsorted.Canonical())
	}
	if empty := SettleTrackFromRequest(&pb.SettleTrackRequest{IndexerId: 1, PromotionSeqs: []uint64{}}); empty.PromotionSeqs != nil {
		t.Fatalf("an empty list must decode as nil, got %#v", empty.PromotionSeqs)
	}
	if zero := SettleTrackFromRequest(nil); !reflect.DeepEqual(zero, SettleTrack{}) {
		t.Fatalf("a nil request = %+v", zero)
	}
}

// TestSettlePromotionCommandPBSetsSettle: a settle-streamed promotion is the
// ordinary promotion command with settle set and nothing else changed, so an
// SNode that ignores the field would see exactly PromotionCommandPB's message.
func TestSettlePromotionCommandPBSetsSettle(t *testing.T) {
	p := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p_p0", PromotionSeq: 41, BaseSafeSnapshotID: "0xsnap",
		BasePartitionRoot: "0xroot", CandidateParts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p_p0", PartRowLtHash: "0xaa", PartName: "p0_1_1_0"}}}
	settle := SettlePromotionCommandPB(p, "h.p.s")
	if !settle.GetSettle() {
		t.Fatal("settle must be set")
	}
	plain := PromotionCommandPB(p, "h.p.s")
	if plain.GetSettle() {
		t.Fatal("PromotionCommandPB must leave settle unset")
	}
	settle.Settle = false
	if !proto.Equal(settle, plain) {
		t.Fatalf("SettlePromotionCommandPB differs from PromotionCommandPB beyond settle:\n%v\n%v", settle, plain)
	}
}
