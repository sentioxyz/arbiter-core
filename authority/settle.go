package authority

import (
	"fmt"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// SettleTrackPurpose is the token family of an authority-signed settle of a
// quarantined track (housegate spec 2026-10-14 §14 "Exits from the release
// refusals"; plan S3-D clarification K53): the leader re-streams the listed,
// still unacknowledged promotions with PromotionCommand.settle set, and the
// SNode answers each from its journal. A settle token authorizes nothing else,
// and no other token authorizes a settle.
const SettleTrackPurpose = "arbiter-settle-track-v1"

const settleTrackCommandDomain = "arbiter-settle-track-command-v1"

// SettleTrackHash is the settle's command hash. The promotion list must be
// non-empty and strictly ascending (arbiter.SettleTrackCommand.Validate; it
// never sorts); indexer 0 and generation 0 are signed like any other value.
func SettleTrackHash(cmd arbiter.SettleTrackCommand) (string, error) {
	if err := cmd.Validate(); err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(settleTrackCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash settle track command: %w", err)
	}
	return h, nil
}

// SignSettleTrackWithContext signs a settle bound to one network incarnation
// and authority epoch, like SignQuarantineIndexerWithContext.
func (s *Signer) SignSettleTrackWithContext(cmd arbiter.SettleTrackCommand, ctx ConsensusContext) (string, error) {
	return s.SignSettleTrackWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignSettleTrackWithContextAt signs with an explicit issue time.
func (s *Signer) SignSettleTrackWithContextAt(cmd arbiter.SettleTrackCommand, ctx ConsensusContext, iat int64) (string, error) {
	h, err := SettleTrackHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, SettleTrackPurpose, ctx, iat)
}

// AuthorizeSettleTrack checks signature, purpose, command hash, the
// allowlist, token age and that the token carries exactly ctx, for the
// leader's SettleTrack RPC. There is no Verify variant: a settle is never a
// Raft command, so no Apply or replay reads it.
func (v *Validator) AuthorizeSettleTrack(cmd arbiter.SettleTrackCommand, token string, ctx ConsensusContext) (string, error) {
	h, err := SettleTrackHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("settle track", h, SettleTrackPurpose, token, ctx, true)
}
