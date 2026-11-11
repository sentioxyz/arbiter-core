package wire

import (
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
)

// TestRecordCustodyProgressCommand (arbiter#200): tag 45 round-trips through
// the strict codec with every field, and the stages keep their numbers.
func TestRecordCustodyProgressCommand(t *testing.T) {
	if CustodyStageReplayPinned != CustodyStage(pb.CustodyStage_CUSTODY_STAGE_REPLAY_PINNED) ||
		CustodyStageAuditDone != CustodyStage(pb.CustodyStage_CUSTODY_STAGE_AUDIT_DONE) ||
		CustodyStageUnspecified != CustodyStage(pb.CustodyStage_CUSTODY_STAGE_UNSPECIFIED) {
		t.Fatal("custody stage numbers drifted from arbiter-proto")
	}
	c := RecordCustodyProgress{Marks: []CustodyMark{
		{BlockSeq: 3, Stage: CustodyStageReplayPinned},
		{BlockSeq: 9, Stage: CustodyStageAuditDone},
	}}
	mustRoundTrip(t, Command{RecordCustodyProgress: &c})
	b, err := Encode(Command{RecordCustodyProgress: &c})
	if err != nil {
		t.Fatal(err)
	}
	var raw pb.RaftCommand
	if err := proto.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	marks := raw.GetRecordCustodyProgress().GetMarks()
	if len(marks) != 2 || marks[1].GetBlockSeq() != 9 || marks[1].GetStage() != pb.CustodyStage_CUSTODY_STAGE_AUDIT_DONE {
		t.Fatalf("encoded command = %v", &raw)
	}
	got, err := Decode(b)
	if err != nil || !reflect.DeepEqual(got.RecordCustodyProgress, &c) {
		t.Fatalf("decoded = %+v, %v", got.RecordCustodyProgress, err)
	}
	if got := RecordCustodyProgressFromPB(nil); got.Marks != nil {
		t.Fatalf("a missing command decoded to %+v", got)
	}
	empty, err := Encode(Command{RecordCustodyProgress: &RecordCustodyProgress{}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := Decode(empty); err != nil || got.RecordCustodyProgress == nil || got.RecordCustodyProgress.Marks != nil {
		t.Fatalf("empty command decoded to %+v, %v (marks must stay nil)", got.RecordCustodyProgress, err)
	}
}

// TestConsensusParamsUpdateCarriesCustodyFrontier: the converters copy the
// switch both ways; false stays off the wire.
func TestConsensusParamsUpdateCarriesCustodyFrontier(t *testing.T) {
	u := legacyGoldenUpdate()
	if ConsensusParamsUpdateToPB(u).GetCustodyFrontier() {
		t.Fatal("an update without the switch set it")
	}
	u.CustodyFrontier = true
	m := ConsensusParamsUpdateToPB(u)
	if !m.GetCustodyFrontier() || !ConsensusParamsUpdateFromPB(m).CustodyFrontier {
		t.Fatalf("switch not carried: %v", m)
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
}
