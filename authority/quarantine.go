package authority

import (
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// QuarantineIndexerPurpose is the token family of an authority-signed
// quarantine of one SI indexer's track (housegate spec 2026-10-10 §7.4). A
// promotion, cleanup, consensus-update, eviction or release token cannot
// quarantine an indexer, and a quarantine token authorizes nothing else.
const QuarantineIndexerPurpose = "arbiter-quarantine-indexer-v1"

// ReleaseIndexerPurpose is the token family of an authority-signed release of
// a quarantined indexer (spec §7.4, §7.5).
const ReleaseIndexerPurpose = "arbiter-release-indexer-v1"

const (
	quarantineIndexerCommandDomain = "arbiter-quarantine-indexer-command-v1"
	releaseIndexerCommandDomain    = "arbiter-release-indexer-command-v1"
)

// QuarantineIndexerHash is the quarantine's command hash. Indexer 0 and
// generation 0 are signed like any other value; the reason is required.
func QuarantineIndexerHash(cmd arbiter.QuarantineIndexerCommand) (string, error) {
	if strings.TrimSpace(cmd.Reason) == "" {
		return "", fmt.Errorf("quarantine indexer: reason must be non-empty")
	}
	h, err := replay.CanonicalDigest(quarantineIndexerCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash quarantine indexer command: %w", err)
	}
	return h, nil
}

// ReleaseIndexerHash is the release's command hash over its normalized form:
// the acknowledged rejected blocks must be sorted ascending without
// duplicates (arbiter.ReleaseIndexerCommand.Validate; it never sorts), and an
// empty list hashes exactly like an absent one.
func ReleaseIndexerHash(cmd arbiter.ReleaseIndexerCommand) (string, error) {
	if err := cmd.Validate(); err != nil {
		return "", err
	}
	cmd.AcknowledgedRejectedBlocks = nilIfEmpty(cmd.AcknowledgedRejectedBlocks)
	h, err := replay.CanonicalDigest(releaseIndexerCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash release indexer command: %w", err)
	}
	return h, nil
}

// SignQuarantineIndexerWithContext signs a quarantine bound to one network
// incarnation and authority epoch, like SignEvictNodeWithContext.
func (s *Signer) SignQuarantineIndexerWithContext(cmd arbiter.QuarantineIndexerCommand, ctx ConsensusContext) (string, error) {
	return s.SignQuarantineIndexerWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignQuarantineIndexerWithContextAt signs with an explicit issue time.
func (s *Signer) SignQuarantineIndexerWithContextAt(cmd arbiter.QuarantineIndexerCommand, ctx ConsensusContext, iat int64) (string, error) {
	h, err := QuarantineIndexerHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, QuarantineIndexerPurpose, ctx, iat)
}

// SignReleaseIndexerWithContext signs a release bound to one network
// incarnation and authority epoch.
func (s *Signer) SignReleaseIndexerWithContext(cmd arbiter.ReleaseIndexerCommand, ctx ConsensusContext) (string, error) {
	return s.SignReleaseIndexerWithContextAt(cmd, ctx, time.Now().Unix())
}

// SignReleaseIndexerWithContextAt signs with an explicit issue time.
func (s *Signer) SignReleaseIndexerWithContextAt(cmd arbiter.ReleaseIndexerCommand, ctx ConsensusContext, iat int64) (string, error) {
	h, err := ReleaseIndexerHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signWithContextPurpose(h, ReleaseIndexerPurpose, ctx, iat)
}

// VerifyQuarantineIndexer checks signature, purpose, command hash, the
// allowlist and that the token carries exactly ctx. It reads no clock: Raft
// Apply uses it with the current authority set and context.
func (v *Validator) VerifyQuarantineIndexer(cmd arbiter.QuarantineIndexerCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyQuarantineIndexer(cmd, token, ctx, false)
}

// AuthorizeQuarantineIndexer additionally enforces token age for the
// QuarantineIndexer RPC. It must not be used inside replicated Apply or
// snapshot replay.
func (v *Validator) AuthorizeQuarantineIndexer(cmd arbiter.QuarantineIndexerCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyQuarantineIndexer(cmd, token, ctx, true)
}

// VerifyReleaseIndexer is VerifyQuarantineIndexer for a release.
func (v *Validator) VerifyReleaseIndexer(cmd arbiter.ReleaseIndexerCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyReleaseIndexer(cmd, token, ctx, false)
}

// AuthorizeReleaseIndexer is AuthorizeQuarantineIndexer for a release.
func (v *Validator) AuthorizeReleaseIndexer(cmd arbiter.ReleaseIndexerCommand, token string, ctx ConsensusContext) (string, error) {
	return v.verifyReleaseIndexer(cmd, token, ctx, true)
}

func (v *Validator) verifyQuarantineIndexer(cmd arbiter.QuarantineIndexerCommand, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	h, err := QuarantineIndexerHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("quarantine indexer", h, QuarantineIndexerPurpose, token, ctx, enforceAge)
}

func (v *Validator) verifyReleaseIndexer(cmd arbiter.ReleaseIndexerCommand, token string, ctx ConsensusContext, enforceAge bool) (string, error) {
	h, err := ReleaseIndexerHash(cmd)
	if err != nil {
		return "", err
	}
	return v.verifyExactContext("release indexer", h, ReleaseIndexerPurpose, token, ctx, enforceAge)
}
