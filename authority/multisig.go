package authority

import (
	"encoding/base64"
	"fmt"
	"slices"
	"strings"

	"github.com/ethereum/go-ethereum/crypto"
)

// TokenSetSeparator joins the components of a multi-signature authority
// token. It is outside the base64url alphabet and is not ".", so it never
// occurs inside a compact JWS and the split is unambiguous.
const TokenSetSeparator = "~"

// A multi-signature authority token (arbiter#173, design 2026-11-11 §3) is two
// or more complete compact JWS joined by TokenSetSeparator. Each component is
// the token one key would produce alone for the same command (same purpose,
// command hash and consensus context; iat may differ) and is verified by the
// kind's single-token rules. Components are ordered by strictly ascending
// recovered signer address (lowercase 0x hex, byte-wise), which also rules
// out a repeated signer, so a given list of component JWS has exactly one
// encoding. With a threshold of 0 or 1 only the single compact JWS form is
// accepted, so a validator that never sees a threshold accepts exactly the
// tokens it accepted before multi-signature tokens existed.

// verifyThreshold splits token by the validator's threshold and returns the
// signers one recovered from each component, in token order (ascending).
// Every component must pass one; any failure refuses the whole token.
func (v *Validator) verifyThreshold(token string, one func(component string) (string, error)) ([]string, error) {
	if v.Threshold <= 1 {
		if strings.Contains(token, TokenSetSeparator) {
			return nil, tokenErrorf(ErrMalformedToken, "authority token: a multi-signature token is not accepted at authority threshold 1")
		}
		signer, err := one(token)
		if err != nil {
			return nil, err
		}
		return []string{signer}, nil
	}
	if len(v.AllowedAddresses) == 0 {
		return nil, tokenErrorf(ErrSignerNotAllowed, "authority allowlist is empty: refusing to authorize any command")
	}
	if int(v.Threshold) > len(v.AllowedAddresses) {
		return nil, fmt.Errorf("authority threshold %d exceeds the %d allowlisted addresses", v.Threshold, len(v.AllowedAddresses))
	}
	components := strings.Split(token, TokenSetSeparator)
	// Bound the work before recovering anything: distinct allowlisted signers
	// can never outnumber the allowlist.
	if len(components) > len(v.AllowedAddresses) {
		return nil, tokenErrorf(ErrMalformedToken, "authority token: %d signatures exceed the %d allowlisted addresses", len(components), len(v.AllowedAddresses))
	}
	if len(components) < int(v.Threshold) {
		return nil, tokenErrorf(ErrThresholdNotMet, "authority token: %d signatures, authority threshold requires %d", len(components), v.Threshold)
	}
	signers := make([]string, 0, len(components))
	for i, component := range components {
		if component == "" {
			return nil, tokenErrorf(ErrMalformedToken, "authority token: signature %d is empty", i)
		}
		signer, err := one(component)
		if err != nil {
			return nil, fmt.Errorf("authority token signature %d: %w", i, err)
		}
		if i > 0 && signer <= signers[i-1] {
			return nil, tokenErrorf(ErrMalformedToken, "authority token: signers must be distinct and in strictly ascending order (%s after %s)", signer, signers[i-1])
		}
		signers = append(signers, signer)
	}
	return signers, nil
}

// TokenSigners returns the signer recovered from each component of an
// authority token, single or multi-signature, in token order. It checks only
// the JWS shape and signature recovery: call it on a token a Verify or
// Authorize method already accepted, to record its full signer list.
func TokenSigners(token string) ([]string, error) {
	components := strings.Split(token, TokenSetSeparator)
	signers := make([]string, len(components))
	for i, component := range components {
		signer, err := recoverCompactSigner(component)
		if err != nil {
			return nil, fmt.Errorf("authority token signature %d: %w", i, err)
		}
		signers[i] = signer
	}
	return signers, nil
}

// CombineTokens joins single-signature tokens over the same command into the
// canonical multi-signature form: components ordered by ascending signer, a
// repeated signer refused. It does not check that the components sign the same
// command; the verifier does. One token is returned unchanged.
func CombineTokens(tokens ...string) (string, error) {
	if len(tokens) == 0 {
		return "", fmt.Errorf("combine authority tokens: no tokens")
	}
	type signed struct{ signer, token string }
	parts := make([]signed, len(tokens))
	for i, token := range tokens {
		if token == "" || strings.Contains(token, TokenSetSeparator) {
			return "", fmt.Errorf("combine authority tokens: token %d must be one compact JWS", i)
		}
		signer, err := recoverCompactSigner(token)
		if err != nil {
			return "", fmt.Errorf("combine authority tokens: token %d: %w", i, err)
		}
		parts[i] = signed{signer: signer, token: token}
	}
	slices.SortFunc(parts, func(a, b signed) int { return strings.Compare(a.signer, b.signer) })
	out := make([]string, len(parts))
	for i, p := range parts {
		if i > 0 && p.signer == parts[i-1].signer {
			return "", fmt.Errorf("combine authority tokens: signer %s appears twice", p.signer)
		}
		out[i] = p.token
	}
	return strings.Join(out, TokenSetSeparator), nil
}

// recoverCompactSigner recovers the lowercase signer address of one ES256K
// compact JWS without checking its header or payload.
func recoverCompactSigner(token string) (string, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return "", fmt.Errorf("want JWS compact form with 3 parts, got %d", len(parts))
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return "", fmt.Errorf("signature: %w", err)
	}
	if len(sig) != 65 {
		return "", fmt.Errorf("signature: want 65 bytes, got %d", len(sig))
	}
	rec := slices.Clone(sig)
	if rec[64] >= 27 {
		rec[64] -= 27
	}
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(parts[0]+"."+parts[1])), rec)
	if err != nil {
		return "", fmt.Errorf("recover signer: %w", err)
	}
	return strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()), nil
}
