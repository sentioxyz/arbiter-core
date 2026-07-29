// Package authority implements the Arbiter's secp256k1 command-signing
// scheme (design §8.1): the single authority key (shared across Raft nodes,
// leader-only use) signs PromoteSafePartition / UnsafeCleanup as a JWS whose
// payload is purpose-claim domain-separated from housegate's query and
// peer-login JWS families; SNode authorizes by address recovery against an
// allowlist (the pkg/auth EthValidator pattern).
//
// Seam-shape note: design §3.4 sketches PromotionSigner/PromotionValidator
// with raw signature bytes ("Sign(cmd) (sig []byte, err)"). This package
// deliberately realizes the seam as JWS compact tokens instead — the
// purpose claim and iat that P0 freezes need a claims payload, which raw
// signature bytes cannot carry. The trust chain is unchanged: secp256k1
// recovery against the authority allowlist.
package authority

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// PromotionPurpose is the JWSCommandPayload.Purpose value for
// Arbiter-authority command tokens. Domain-separated from housegate's query
// JWS (no purpose claim) and peer-relay JWS (purpose "peer-relay") so no
// token family can be replayed as another.
const PromotionPurpose = "arbiter-promotion"

// CanonicalDigest domains — one per command kind, so a cleanup signature can
// never authorize a promotion over field-identical content.
const (
	promoteCommandDomain = "arbiter-promote-command-v1"
	cleanupCommandDomain = "arbiter-cleanup-command-v1"
)

// JWSCommandPayload is the signed payload. CmdHash binds the token to one
// exact command via replay.CanonicalDigest over the canonical Go struct
// (never re-encoded proto bytes, design §4.3).
type JWSCommandPayload struct {
	Iat     int64  `json:"iat"`
	Purpose string `json:"purpose"`
	CmdHash string `json:"cmd_hash"`
}

// PromoteCommandHash is the canonical cmd-hash entry point for a
// PromoteSafePartition command: the exact digest a JWSCommandPayload.CmdHash
// must bind to. Consumed both here (Signer/Validator) and by the FSM's
// audit verification (fsm.applyRecordPromotionIssued) so both sides hash
// the identical canonical form via replay.CanonicalDigest.
func PromoteCommandHash(cmd arbiter.PromoteSafePartition) (string, error) {
	h, err := replay.CanonicalDigest(promoteCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash promote command: %w", err)
	}
	return h, nil
}

// CleanupCommandHash is the canonical cmd-hash entry point for an
// UnsafeCleanup command: the exact digest a JWSCommandPayload.CmdHash must
// bind to. Consumed both here (Signer/Validator) and by the FSM's audit
// verification, mirroring PromoteCommandHash.
func CleanupCommandHash(cmd arbiter.UnsafeCleanup) (string, error) {
	h, err := replay.CanonicalDigest(cleanupCommandDomain, cmd)
	if err != nil {
		return "", fmt.Errorf("hash cleanup command: %w", err)
	}
	return h, nil
}
