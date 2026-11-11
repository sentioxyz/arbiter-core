package wire

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func TestRaftMembershipChangeRoundTrip(t *testing.T) {
	for _, c := range []arbiter.RaftMembershipChange{
		{Op: arbiter.RaftMembershipAddVoter, ServerID: "arbiter-3", RaftAddress: "arbiter-3:7090"},
		{Op: arbiter.RaftMembershipRemoveServer, ServerID: "arbiter-1"},
	} {
		if got := RaftMembershipChangeFromPB(RaftMembershipChangeToPB(c)); got != c {
			t.Fatalf("round trip %+v = %+v", c, got)
		}
	}
	if got := RaftMembershipChangeFromPB(&pb.RaftMembershipChange{Op: 99, ServerId: "a"}); got.Op != "" {
		t.Fatalf("unknown op decoded as %q", got.Op)
	}
	if got := RaftMembershipChangeToPB(arbiter.RaftMembershipChange{Op: "demote"}); got.GetOp() != pb.RaftMembershipOp_RAFT_MEMBERSHIP_OP_UNSPECIFIED {
		t.Fatalf("unknown op encoded as %v", got.GetOp())
	}
	if got := RaftMembershipChangeFromPB(nil); got != (arbiter.RaftMembershipChange{}) {
		t.Fatalf("nil decoded as %+v", got)
	}
}
