package wire

import (
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// TestPruneHistoryCommand (arbiter#149): tag 46 round-trips through the
// strict codec with every field.
func TestPruneHistoryCommand(t *testing.T) {
	c := PruneHistory{ThroughBlockSeq: 128, ArchiveRef: "da://ref", ArchiveSHA256: "0x" + "ab", RecordSetDigest: "0x" + "cd"}
	mustRoundTrip(t, Command{PruneHistory: &c})
	b, err := Encode(Command{PruneHistory: &c})
	if err != nil {
		t.Fatal(err)
	}
	var raw pb.RaftCommand
	if err := proto.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	p := raw.GetPruneHistory()
	if p.GetThroughBlockSeq() != 128 || p.GetArchiveRef() != "da://ref" || p.GetArchiveSha256() != "0xab" || p.GetRecordSetDigest() != "0xcd" {
		t.Fatalf("encoded command = %v", &raw)
	}
	got, err := Decode(b)
	if err != nil || !reflect.DeepEqual(got.PruneHistory, &c) {
		t.Fatalf("decoded = %+v, %v", got.PruneHistory, err)
	}
	if got := PruneHistoryFromPB(nil); got != (PruneHistory{}) {
		t.Fatalf("a missing command decoded to %+v", got)
	}
}

// TestConsensusParamsUpdateCarriesHistoryRetainBlocks: the converters copy
// the window both ways; 0 stays off the wire.
func TestConsensusParamsUpdateCarriesHistoryRetainBlocks(t *testing.T) {
	u := legacyGoldenUpdate()
	if ConsensusParamsUpdateToPB(u).GetHistoryRetainBlocks() != 0 {
		t.Fatal("an update without the parameter set it")
	}
	u.HistoryRetainBlocks = 64
	m := ConsensusParamsUpdateToPB(u)
	if m.GetHistoryRetainBlocks() != 64 || ConsensusParamsUpdateFromPB(m).HistoryRetainBlocks != 64 {
		t.Fatalf("parameter not carried: %v", m)
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
}

// TestPromotionOutcomeCarriesPruned: a compacted outcome's flag survives the
// read converters; false stays off the wire.
func TestPromotionOutcomeCarriesPruned(t *testing.T) {
	v := PromotionOutcome{PromotionSeq: 7, BlockSeq: 3, Acked: true, Pruned: true}
	m := promotionOutcomeToPB(v)
	if !m.GetPruned() || !promotionOutcomeFromPB(m).Pruned {
		t.Fatalf("pruned not carried: %v", m)
	}
	v.Pruned = false
	a, _ := proto.Marshal(promotionOutcomeToPB(v))
	b, _ := proto.Marshal(&pb.PromotionOutcome{PromotionSeq: 7, Promote: PromoteToPB(v.Promote), BlockSeq: 3, Acked: true})
	if string(a) != string(b) {
		t.Fatal("an unpruned outcome must encode as before")
	}
}

// TestConsensusParamsUpdateCarriesTheAuthorityChangeDelay (arbiter#173
// Slice 3): the converters copy the delay and the pending change both ways;
// unset stays off the wire.
func TestConsensusParamsUpdateCarriesTheAuthorityChangeDelay(t *testing.T) {
	u := legacyGoldenUpdate()
	if m := ConsensusParamsUpdateToPB(u); m.GetAuthorityChangeDelayL2Blocks() != 0 || m.GetPendingAuthorityChange() != nil {
		t.Fatal("an update without the delay carried it")
	}
	u.AuthorityChangeDelayL2Blocks = 43200
	u.PendingAuthorityChange = &arbiter.PendingAuthorityChange{AuthorityAddresses: []string{"0x0a", "0x0b"}, AuthorityThreshold: 2, AuthorityChangeDelayL2Blocks: 3600, NotBeforeL2Block: 77}
	m := ConsensusParamsUpdateToPB(u)
	back := ConsensusParamsUpdateFromPB(m)
	if back.AuthorityChangeDelayL2Blocks != 43200 || !reflect.DeepEqual(back.PendingAuthorityChange, u.PendingAuthorityChange) {
		t.Fatalf("round trip = %+v", back)
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
}
