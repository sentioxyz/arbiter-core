package arbiter

// RaftMembershipOp names the Raft configuration change of an authority-signed
// membership change (arbiter#172). The values are the signed form: they are
// hashed into the token's command digest, so they never change.
type RaftMembershipOp string

const (
	// RaftMembershipAddVoter adds the server as a voter at its Raft address.
	RaftMembershipAddVoter RaftMembershipOp = "add_voter"
	// RaftMembershipRemoveServer removes the server from the configuration.
	RaftMembershipRemoveServer RaftMembershipOp = "remove_server"
)

// RaftMembershipChange is the canonical signing form of an authority-signed
// Raft membership change; its wire form is pb.RaftMembershipChange.
// RaftAddress is set for an addition only (no omitempty: an empty address is
// signed as such). The token binds the network, genesis and authority epoch,
// so any consensus update voids it.
type RaftMembershipChange struct {
	Op          RaftMembershipOp `json:"op"`
	ServerID    string           `json:"server_id"`
	RaftAddress string           `json:"raft_address"`
}
