package authority

import (
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
	if strings.TrimSpace(context.NetworkID) == "" || strings.TrimSpace(context.GenesisSnapshotID) == "" {
		return "", fmt.Errorf("authority context: network ID and genesis snapshot ID must be non-empty")
	}
	return s.signPayload(JWSCommandPayload{
		Iat: iat, Purpose: PromotionPurpose, CmdHash: hash,
		NetworkID: context.NetworkID, GenesisSnapshotID: context.GenesisSnapshotID, AuthorityEpoch: &context.AuthorityEpoch,
	})
}
