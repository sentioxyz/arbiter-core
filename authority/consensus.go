package authority

import (
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// ConsensusParamsUpdatePurpose is a separate, versioned token family. A
// promotion, cleanup, query or peer-relay token cannot authorize an update.
const ConsensusParamsUpdatePurpose = "arbiter-consensus-params-update-v1"

const consensusParamsUpdateDomain = "arbiter-consensus-params-update-command-v1"

// NormalizeConsensusParamsUpdate validates structural constraints and returns
// a copy with a lowercase, sorted, duplicate-free authority set. It never
// mutates the caller's slice. State-dependent validation belongs to the FSM.
func NormalizeConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate) (arbiter.ConsensusParamsUpdate, error) {
	if strings.TrimSpace(cmd.NetworkID) == "" || strings.TrimSpace(cmd.GenesisSnapshotID) == "" || strings.TrimSpace(cmd.PreviousParamsDigest) == "" {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: network ID, genesis snapshot ID and previous params digest must be non-empty")
	}
	if cmd.MaxWriters == 0 {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: max writers must be positive")
	}
	if cmd.ArtifactDispositionCapability > 1 {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: artifact disposition capability must be 0 or 1")
	}
	if len(cmd.AuthorityAddresses) == 0 {
		return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: authority addresses must be non-empty")
	}
	addresses := make([]string, len(cmd.AuthorityAddresses))
	for i, address := range cmd.AuthorityAddresses {
		if len(address) != 42 || !strings.EqualFold(address[:2], "0x") {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: authority address %q must be a 0x-prefixed 20-byte hex address", address)
		}
		decoded, err := hex.DecodeString(address[2:])
		if err != nil {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: invalid authority address %q: %w", address, err)
		}
		if !slices.ContainsFunc(decoded, func(b byte) bool { return b != 0 }) {
			return arbiter.ConsensusParamsUpdate{}, fmt.Errorf("consensus params update: zero authority address is not allowed")
		}
		addresses[i] = strings.ToLower(address)
	}
	slices.Sort(addresses)
	cmd.AuthorityAddresses = slices.Compact(addresses)
	return cmd, nil
}

// ConsensusParamsUpdateHash binds every transition field after address-set
// normalization. Both live authorization and deterministic replay use it.
func ConsensusParamsUpdateHash(cmd arbiter.ConsensusParamsUpdate) (string, error) {
	normalized, err := NormalizeConsensusParamsUpdate(cmd)
	if err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(consensusParamsUpdateDomain, normalized)
	if err != nil {
		return "", fmt.Errorf("hash consensus params update: %w", err)
	}
	return h, nil
}

// SignConsensusParamsUpdate signs one complete transition at the current time.
func (s *Signer) SignConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate) (string, error) {
	return s.SignConsensusParamsUpdateAt(cmd, time.Now().Unix())
}

// SignConsensusParamsUpdateAt signs with an explicit Unix issue time. It is
// useful for reproducible audit fixtures; issue time is not a replay guard.
func (s *Signer) SignConsensusParamsUpdateAt(cmd arbiter.ConsensusParamsUpdate, iat int64) (string, error) {
	h, err := ConsensusParamsUpdateHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: ConsensusParamsUpdatePurpose, CmdHash: h})
}

// VerifyConsensusParamsUpdate checks the signature, purpose, normalized command
// hash and current authority allowlist without reading the clock. Raft Apply
// and snapshot validation must use this deterministic path and enforce the
// signed epoch/digest/promotion-sequence preconditions themselves. MaxTokenAge
// is deliberately unused; an empty allowlist still fails closed.
func (v *Validator) VerifyConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate, token string) (string, error) {
	h, err := ConsensusParamsUpdateHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verify(h, ConsensusParamsUpdatePurpose, token, false)
}

// AuthorizeConsensusParamsUpdate additionally enforces token age for an API
// boundary. It must not be used inside replicated Apply or snapshot replay.
func (v *Validator) AuthorizeConsensusParamsUpdate(cmd arbiter.ConsensusParamsUpdate, token string) (string, error) {
	h, err := ConsensusParamsUpdateHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verify(h, ConsensusParamsUpdatePurpose, token, true)
}
