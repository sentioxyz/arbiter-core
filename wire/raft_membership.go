package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

// RaftMembershipChangeFromPB decodes the signed body of a
// ConsensusAdmin.ChangeRaftMembership request (arbiter#172). An unspecified
// or unknown op decodes to an empty op, which authority.RaftMembershipHash
// refuses.
func RaftMembershipChangeFromPB(m *pb.RaftMembershipChange) arbiter.RaftMembershipChange {
	var op arbiter.RaftMembershipOp
	switch m.GetOp() {
	case pb.RaftMembershipOp_RAFT_MEMBERSHIP_OP_ADD_VOTER:
		op = arbiter.RaftMembershipAddVoter
	case pb.RaftMembershipOp_RAFT_MEMBERSHIP_OP_REMOVE_SERVER:
		op = arbiter.RaftMembershipRemoveServer
	}
	return arbiter.RaftMembershipChange{Op: op, ServerID: m.GetServerId(), RaftAddress: m.GetRaftAddress()}
}

// RaftMembershipChangeToPB encodes a membership change's signed body
// (arbiter-admin). An unknown op encodes as unspecified.
func RaftMembershipChangeToPB(c arbiter.RaftMembershipChange) *pb.RaftMembershipChange {
	op := pb.RaftMembershipOp_RAFT_MEMBERSHIP_OP_UNSPECIFIED
	switch c.Op {
	case arbiter.RaftMembershipAddVoter:
		op = pb.RaftMembershipOp_RAFT_MEMBERSHIP_OP_ADD_VOTER
	case arbiter.RaftMembershipRemoveServer:
		op = pb.RaftMembershipOp_RAFT_MEMBERSHIP_OP_REMOVE_SERVER
	}
	return &pb.RaftMembershipChange{Op: op, ServerId: c.ServerID, RaftAddress: c.RaftAddress}
}
