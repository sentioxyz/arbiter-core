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
	// MaxTokenAge caps iat age and MUST be positive: a non-positive value
	// fails closed (every token rejected), the same shape as the empty
	// allowlist — a zero value must never silently mean "no expiry".
	// Promotion re-sends after failover re-sign, so short ages are safe
	// (§10.2).
	MaxTokenAge time.Duration
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
	if len(v.AllowedAddresses) == 0 {
		return "", fmt.Errorf("authority allowlist is empty: refusing to authorize any command")
	}
	if v.MaxTokenAge <= 0 {
		return "", fmt.Errorf("authority validator: MaxTokenAge must be positive (fail-closed; a zero value would mean never-expiring tokens)")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("authority token: want JWS compact form with 3 parts, got %d", len(parts))
	}
	headerJSON, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("authority token header: %w", err)
	}
	var header jwsHeader
	if err := json.Unmarshal(headerJSON, &header); err != nil {
		return "", fmt.Errorf("authority token header: %w", err)
	}
	// "secp256k1" is accepted for parity with housegate pkg/auth's
	// historical dual-alg tolerance; this package's own Signer only ever
	// emits "ES256K".
	if header.Alg != "ES256K" && header.Alg != "secp256k1" {
		return "", fmt.Errorf("authority token: unexpected alg %q", header.Alg)
	}
	payloadJSON, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("authority token payload: %w", err)
	}
	var payload JWSCommandPayload
	if err := json.Unmarshal(payloadJSON, &payload); err != nil {
		return "", fmt.Errorf("authority token payload: %w", err)
	}
	if payload.Purpose != PromotionPurpose {
		return "", fmt.Errorf("authority token: unexpected purpose %q (want %q)", payload.Purpose, PromotionPurpose)
	}
	if !strings.EqualFold(payload.CmdHash, wantCmdHash) {
		return "", fmt.Errorf("authority token: command hash mismatch")
	}
	now := time.Now().Unix()
	if payload.Iat-now > clockSkewToleranceSeconds {
		return "", fmt.Errorf("authority token issued in the future")
	}
	if now-payload.Iat > int64(v.MaxTokenAge.Seconds())+clockSkewToleranceSeconds {
		return "", fmt.Errorf("authority token expired")
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("authority token signature: %w", err)
	}
	if len(sig) != 65 {
		return "", fmt.Errorf("authority token signature: want 65 bytes, got %d", len(sig))
	}
	recSig := make([]byte, 65)
	copy(recSig, sig)
	if recSig[64] >= 27 {
		recSig[64] -= 27
	}
	signingInput := parts[0] + "." + parts[1]
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(signingInput)), recSig)
	if err != nil {
		return "", fmt.Errorf("recover authority address: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex())
	if !v.AllowedAddresses[addr] {
		return "", fmt.Errorf("authority address %s not in allowlist", addr)
	}
	return addr, nil
}
