package authority

import (
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// RaftMembershipPurpose is the token family of an authority-signed Raft
// membership change (arbiter#172). No other token authorizes one, and a
// membership token authorizes nothing else.
const RaftMembershipPurpose = "arbiter-raft-membership-v1"

const raftMembershipCommandDomain = "arbiter-raft-membership-command-v1"

// RaftMembershipHash is the membership change's command hash. It refuses a
// change that names no known op, an empty or padded server id, an addition
// without a Raft address, and a removal with one.
func RaftMembershipHash(cmd arbiter.RaftMembershipChange) (string, error) {
	if err := validateRaftMembershipChange(cmd); err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(raftMembershipCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash raft membership change: %w", err)
	}
	return h, nil
}

func validateRaftMembershipChange(cmd arbiter.RaftMembershipChange) error {
	if cmd.ServerID == "" || strings.TrimSpace(cmd.ServerID) != cmd.ServerID {
		return fmt.Errorf("raft membership change: server ID must be non-empty and carry no surrounding whitespace")
	}
	switch cmd.Op {
	case arbiter.RaftMembershipAddVoter:
		if cmd.RaftAddress == "" || strings.TrimSpace(cmd.RaftAddress) != cmd.RaftAddress {
			return fmt.Errorf("raft membership change: adding a voter needs its Raft address")
		}
	case arbiter.RaftMembershipRemoveServer:
		if cmd.RaftAddress != "" {
			return fmt.Errorf("raft membership change: a removal carries no Raft address")
		}
	default:
		return fmt.Errorf("raft membership change: unknown op %q", cmd.Op)
	}
	return nil
}

// SignRaftMembershipWithContext signs a membership change bound to one
// network incarnation and authority epoch.
func (s *Signer) SignRaftMembershipWithContext(cmd arbiter.RaftMembershipChange, ctx ConsensusContext) (string, error) {
	return s.SignRaftMembershipWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignRaftMembershipWithContextAt signs with an explicit issue time.
func (s *Signer) SignRaftMembershipWithContextAt(cmd arbiter.RaftMembershipChange, ctx ConsensusContext, iat int64) (string, error) {
	h, err := RaftMembershipHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, RaftMembershipPurpose, ctx, iat)
}

// VerifyRaftMembership checks signature, purpose, command hash, the allowlist
// and its threshold, and that every component carries exactly ctx. It reads
// no clock.
func (v *Validator) VerifyRaftMembership(cmd arbiter.RaftMembershipChange, token string, ctx ConsensusContext) (string, error) {
	return v.verifyRaftMembership(cmd, token, ctx, false)
}

// AuthorizeRaftMembership additionally enforces token age, for the leader's
// ChangeRaftMembership RPC.
func (v *Validator) AuthorizeRaftMembership(cmd arbiter.RaftMembershipChange, token string, ctx ConsensusContext) (string, error) {
	return v.verifyRaftMembership(cmd, token, ctx, true)
}

func (v *Validator) verifyRaftMembership(cmd arbiter.RaftMembershipChange, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	h, err := RaftMembershipHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("raft membership", h, RaftMembershipPurpose, token, ctx, enforceAge)
}
