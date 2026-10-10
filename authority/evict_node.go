package authority

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// EvictNodePurpose is the token family of an authority-signed node eviction
// (housegate spec 2026-10-10 §6.5). A promotion, cleanup or consensus-update
// token cannot evict a node, and an eviction token authorizes nothing else.
const EvictNodePurpose = "arbiter-evict-node-v1"

const evictNodeCommandDomain = "arbiter-evict-node-command-v1"

// EvictNodeHash is the eviction's command hash.
func EvictNodeHash(cmd arbiter.EvictNodeCommand) (string, error) {
	if strings.TrimSpace(cmd.NodeID) == "" || strings.TrimSpace(cmd.Reason) == "" {
		return "", fmt.Errorf("evict node: node ID and reason must be non-empty")
	}
	h, err := replay.CanonicalDigest(evictNodeCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash evict node command: %w", err)
	}
	return h, nil
}

// SignEvictNodeWithContext signs an eviction bound to one network incarnation
// and authority epoch, like SignPromotionWithContext.
func (s *Signer) SignEvictNodeWithContext(cmd arbiter.EvictNodeCommand, ctx ConsensusContext) (string, error) {
	return s.SignEvictNodeWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignEvictNodeWithContextAt signs with an explicit issue time.
func (s *Signer) SignEvictNodeWithContextAt(cmd arbiter.EvictNodeCommand, ctx ConsensusContext, iat int64) (string, error) {
	h, err := EvictNodeHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, EvictNodePurpose, ctx, iat)
}

// VerifyEvictNode checks signature, purpose, command hash, the allowlist and
// that the token carries exactly ctx (all three context fields; unlike legacy
// promotion tokens there is no context-less form). It reads no clock: Raft
// Apply uses it with the current authority set and context.
func (v *Validator) VerifyEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyEvictNode(cmd, token, ctx, false)
}

// AuthorizeEvictNode additionally enforces token age for the EvictNode RPC.
// It must not be used inside replicated Apply or snapshot replay.
func (v *Validator) AuthorizeEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyEvictNode(cmd, token, ctx, true)
}

func (v *Validator) verifyEvictNode(cmd arbiter.EvictNodeCommand, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	h, err := EvictNodeHash(cmd)
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(ctx.NetworkID) == "" || strings.TrimSpace(ctx.GenesisSnapshotID) == "" {
		return "", fmt.Errorf("evict node: network ID and genesis snapshot ID must be non-empty")
	}
	addr, err := v.verify(h, EvictNodePurpose, token, enforceAge)
	if err != nil {
		return "", err
	}
	// verify authenticated these payload bytes; read the context they carry.
	data, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	if err != nil {
		return "", fmt.Errorf("evict node token payload: %w", err)
	}
	var payload JWSCommandPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("evict node token payload: %w", err)
	}
	if payload.NetworkID != ctx.NetworkID || payload.GenesisSnapshotID != ctx.GenesisSnapshotID ||
		payload.AuthorityEpoch == nil || *payload.AuthorityEpoch != ctx.AuthorityEpoch {
		return "", fmt.Errorf("evict node token: consensus context mismatch")
	}
	return addr, nil
}
