package authority

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter-core"
)

// clockSkewToleranceSeconds mirrors housegate pkg/auth.
const clockSkewToleranceSeconds = 5

// Validator authorizes authority command tokens by secp256k1 address
// recovery against an allowlist (design §8.1). SNode must never act on a
// command that fails Authorize* (§13).
type Validator struct {
	// AllowedAddresses is the lowercase, 0x-prefixed authority allowlist.
	// Empty allowlist fails closed — every command is rejected.
	AllowedAddresses map[string]bool
	// MaxTokenAge caps iat age in Authorize methods and MUST be positive:
	// a non-positive value fails closed, the same shape as the empty
	// allowlist — a zero value must never silently mean "no expiry".
	// Promotion re-sends after failover re-sign, so short ages are safe
	// (§10.2). Deterministic Verify methods deliberately ignore this value.
	MaxTokenAge time.Duration
	// Threshold is the number of distinct allowlisted signers a token must
	// carry (k of k-of-n). 0 and 1 both mean one signer, and then only a
	// single compact JWS is accepted, exactly as before thresholds existed.
	// Above 1 the token is the multi-signature form (multisig.go). Callers
	// pass the committed authority_threshold for governance commands and 1
	// for operational ones (promotion, cleanup, member lagging, verifier
	// reselection), which the leader signs with one online key.
	Threshold uint32
}

// AuthorizePromotion checks token against the received command and returns
// the recovered authority address.
func (v *Validator) AuthorizePromotion(cmd arbiter.PromoteSafePartition, token string) (string, error) {
	h, err := PromoteCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return v.authorize(h, token)
}

// AuthorizeCleanup is AuthorizePromotion for cleanup commands.
func (v *Validator) AuthorizeCleanup(cmd arbiter.UnsafeCleanup, token string) (string, error) {
	h, err := CleanupCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return v.authorize(h, token)
}

func (v *Validator) authorize(wantCmdHash, token string) (string, error) {
	return v.verify(wantCmdHash, PromotionPurpose, token, true)
}

// verify applies the validator's threshold to verifyOne and returns the first
// (lowest) signer.
func (v *Validator) verify(wantCmdHash, wantPurpose, token string, enforceAge bool) (string, error) {
	signers, err := v.verifyThreshold(token, func(component string) (string, error) {
		return v.verifyOne(wantCmdHash, wantPurpose, component, enforceAge)
	})
	if err != nil {
		return "", err
	}
	return signers[0], nil
}

// verifyOne checks one compact JWS: alg, purpose, command hash, token age
// when enforceAge, and that the recovered signer is allowlisted.
func (v *Validator) verifyOne(wantCmdHash, wantPurpose, token string, enforceAge bool) (string, error) {
	if len(v.AllowedAddresses) == 0 {
		return "", tokenErrorf(ErrSignerNotAllowed, "authority allowlist is empty: refusing to authorize any command")
	}
	if enforceAge && v.MaxTokenAge <= 0 {
		return "", fmt.Errorf("authority validator: MaxTokenAge must be positive (fail-closed; a zero value would mean never-expiring tokens)")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", tokenErrorf(ErrMalformedToken, "authority token: want JWS compact form with 3 parts, got %d", len(parts))
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", tokenErrorf(ErrMalformedToken, "authority token header: %w", err)
	}
	var header jwsHeader
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return "", tokenErrorf(ErrMalformedToken, "authority token header: %w", err)
	}
	// "secp256k1" is accepted for parity with housegate pkg/auth's
	// historical dual-alg tolerance; this package's own Signer only ever
	// emits "ES256K".
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return "", tokenErrorf(ErrMalformedToken, "authority token: unexpected alg %q", header.Alg)
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", tokenErrorf(ErrMalformedToken, "authority token payload: %w", err)
	}
	var payload JWSCommandPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return "", tokenErrorf(ErrMalformedToken, "authority token payload: %w", err)
	}
	if payload.Purpose != wantPurpose {
		return "", tokenErrorf(ErrTokenCommandMismatch, "authority token: unexpected purpose %q (want %q)", payload.Purpose, wantPurpose)
	}
	if !strings.EqualFold(payload.CmdHash, wantCmdHash) {
		return "", tokenErrorf(ErrTokenCommandMismatch, "authority token: command hash mismatch")
	}
	if enforceAge {
		now := time.Now().Unix()
		if payload.Iat-now > clockSkewToleranceSeconds {
			return "", tokenErrorf(ErrTokenAge, "authority token issued in the future")
		}
		if now-payload.Iat > int64(v.MaxTokenAge.Seconds())+clockSkewToleranceSeconds {
			return "", tokenErrorf(ErrTokenAge, "authority token expired")
		}
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", tokenErrorf(ErrMalformedToken, "authority token signature: %w", err)
	}
	if len(sig) != 65 {
		return "", tokenErrorf(ErrMalformedToken, "authority token signature: want 65 bytes, got %d", len(sig))
	}
	if enforceAge {
		// Admission only (see CheckTokenEncoding): a Verify method must keep
		// accepting a committed token whose signature segment the decoder
		// read past a stray CR or LF.
		if err := checkCompactEncoding(token); err != nil {
			return "", err
		}
	}
	recSig := make([]byte, 65)
	copy(recSig, sig)
	if recSig[64] >= 27 {
		recSig[64] -= 27
	}
	signingInput := parts[0] + "." + parts[1]
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(signingInput)), recSig)
	if err != nil {
		return "", tokenErrorf(ErrSignerNotAllowed, "recover authority address: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex())
	if !v.AllowedAddresses[addr] {
		return "", tokenErrorf(ErrSignerNotAllowed, "authority address %s not in allowlist", addr)
	}
	return addr, nil
}
