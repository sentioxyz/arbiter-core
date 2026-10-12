package authority

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/sentioxyz/arbiter-core"
)

// ConsensusContext binds promotion and cleanup audit tokens to one network
// incarnation and authority epoch. The command's existing purpose and hash
// remain unchanged so existing SNode validators can authenticate the token.
// The Arbiter FSM additionally validates this context during Apply and restore.
type ConsensusContext struct {
	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
	AuthorityEpoch    uint64 `json:"authority_epoch"`
}

// SignPromotionWithContext signs a promotion bound to its authority epoch.
func (s *Signer) SignPromotionWithContext(cmd arbiter.PromoteSafePartition, context ConsensusContext) (string, error) {
	return s.SignPromotionWithContextAt(cmd, context, time.Now().Unix())
}

// SignPromotionWithContextAt accepts an explicit Unix issue time for audit
// fixtures. Context validation is independent of wall time.
func (s *Signer) SignPromotionWithContextAt(cmd arbiter.PromoteSafePartition, context ConsensusContext, iat int64) (string, error) {
	hash, err := PromoteCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContext(hash, context, iat)
}

// SignCleanupWithContext signs a cleanup under the current authority epoch.
// A cleanup may reference a promotion issued in an earlier epoch, so the
// authorization epoch cannot be inferred from its promotion sequence alone.
func (s *Signer) SignCleanupWithContext(cmd arbiter.UnsafeCleanup, context ConsensusContext) (string, error) {
	return s.SignCleanupWithContextAt(cmd, context, time.Now().Unix())
}

// SignCleanupWithContextAt is SignCleanupWithContext with an explicit issue time.
func (s *Signer) SignCleanupWithContextAt(cmd arbiter.UnsafeCleanup, context ConsensusContext, iat int64) (string, error) {
	hash, err := CleanupCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContext(hash, context, iat)
}

func (s *Signer) signWithContext(hash string, context ConsensusContext, iat int64) (string, error) {
	return s.signWithContextPurpose(hash, PromotionPurpose, context, iat)
}

// signWithContextPurpose signs hash under purpose with the three consensus
// context fields every context-bound authority token carries.
func (s *Signer) signWithContextPurpose(hash, purpose string, context ConsensusContext, iat int64) (string, error) {
	if strings.TrimSpace(context.NetworkID) == "" || strings.TrimSpace(context.GenesisSnapshotID) == "" {
		return "", fmt.Errorf("authority context: network ID and genesis snapshot ID must be non-empty")
	}
	return s.signPayload(JWSCommandPayload{
		Iat: iat, Purpose: purpose, CmdHash: hash,
		NetworkID: context.NetworkID, GenesisSnapshotID: context.GenesisSnapshotID, AuthorityEpoch: &context.AuthorityEpoch,
	})
}

// verifyExactContext is the deterministic core every authority command bound
// to one consensus context shares (node eviction, indexer quarantine and
// release, housegate spec 2026-10-10 §6.5, §7.4): verify checks hash,
// purpose, signature, the allowlist and, only when enforceAge, token age; the
// token must then carry exactly ctx, all three fields, with no context-less
// form. label names the command in every refusal; "evict node" keeps the
// eviction's wording of arbiter-core v0.13.0.
func (v *Validator) verifyExactContext(label, hash, purpose, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	if strings.TrimSpace(ctx.NetworkID) == "" || strings.TrimSpace(ctx.GenesisSnapshotID) == "" {
		return "", fmt.Errorf("%s: network ID and genesis snapshot ID must be non-empty", label)
	}
	addr, err := v.verify(hash, purpose, token, enforceAge)
	if err != nil {
		return "", err
	}
	// verify authenticated these payload bytes; read the context they carry.
	data, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
	if err != nil {
		return "", fmt.Errorf("%s token payload: %w", label, err)
	}
	var payload JWSCommandPayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return "", fmt.Errorf("%s token payload: %w", label, err)
	}
	if payload.NetworkID != ctx.NetworkID || payload.GenesisSnapshotID != ctx.GenesisSnapshotID ||
		payload.AuthorityEpoch == nil || *payload.AuthorityEpoch != ctx.AuthorityEpoch {
		return "", fmt.Errorf("%s token: consensus context mismatch", label)
	}
	return addr, nil
}
