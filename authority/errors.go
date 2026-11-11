package authority

import (
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
)

// Sentinel errors classify why the Validator refused an authority token, so a
// caller can tell an uninterpretable request from a refused one with
// errors.Is instead of matching message text. Every refusal keeps the exact
// Error() text it had before these sentinels existed: a deterministic caller
// may have committed or compared that text, and a sentinel only adds an
// Unwrap target.
var (
	// ErrMalformedToken: the token is not a well-formed ES256K compact JWS
	// (part count, segment encoding, header or payload JSON, signature length,
	// algorithm, or a multi-signature form the threshold does not accept).
	// It is decided without a key: a well-formed signature that fails to
	// verify is ErrSignerNotAllowed, not this.
	ErrMalformedToken = errors.New("malformed authority token")
	// ErrTokenCommandMismatch: the token is well formed but signs another
	// command (purpose or command hash).
	ErrTokenCommandMismatch = errors.New("authority token signs another command")
	// ErrTokenAge: the token's iat is outside the admission window. Only the
	// Authorize methods check age; Verify methods never return it.
	ErrTokenAge = errors.New("authority token age outside the admission window")
	// ErrSignerNotAllowed: no allowlisted key signed the token. The signature
	// recovers no key, the recovered signer is not in the allowlist, or the
	// allowlist is empty.
	ErrSignerNotAllowed = errors.New("authority token signer is not allowlisted")
	// ErrThresholdNotMet: a multi-signature token carries fewer signatures
	// than the authority threshold requires.
	ErrThresholdNotMet = errors.New("authority token does not meet the authority threshold")
)

// tokenError carries a refusal's unchanged message, its classifying sentinel
// and, when the refusal wraps a decoding error, that error too.
type tokenError struct {
	kind  error
	msg   string
	cause error
}

func (e *tokenError) Error() string { return e.msg }

func (e *tokenError) Unwrap() []error {
	if e.cause == nil {
		return []error{e.kind}
	}
	return []error{e.kind, e.cause}
}

// tokenErrorf formats exactly as fmt.Errorf would, so Error() is byte for byte
// the message it replaces, and keeps a %w operand reachable.
func tokenErrorf(kind error, format string, args ...any) error {
	formatted := fmt.Errorf(format, args...)
	return &tokenError{kind: kind, msg: formatted.Error(), cause: errors.Unwrap(formatted)}
}

// CheckTokenEncoding reports whether every component of token (a single
// compact JWS, or several joined by TokenSetSeparator) has three non-empty
// segments in the unpadded base64url alphabet and a 65-byte signature.
//
// encoding/base64 silently skips CR and LF, so a successful decode does not
// prove the alphabet. The header and payload are hashed as sent, so a stray
// byte there already breaks the signature, but the signature segment is only
// decoded, so a newline appended to a genuine token would still verify. The
// Authorize methods run this check, and an admission path can run it on its
// own before spending any work. The Verify methods deliberately do not: they
// judge tokens already committed to replicated history, and must keep
// accepting exactly what they accepted before this check existed.
//
// Every failure wraps ErrMalformedToken and never quotes the token.
func CheckTokenEncoding(token string) error {
	for _, component := range strings.Split(token, TokenSetSeparator) {
		if err := checkCompactEncoding(component); err != nil {
			return err
		}
	}
	return nil
}

var compactSegmentNames = [3]string{"header", "payload", "signature"}

func checkCompactEncoding(token string) error {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return tokenErrorf(ErrMalformedToken, "authority token: want JWS compact form with 3 parts, got %d", len(parts))
	}
	for i, segment := range parts {
		name := compactSegmentNames[i]
		if segment == "" {
			return tokenErrorf(ErrMalformedToken, "authority token %s is empty", name)
		}
		for j := 0; j < len(segment); j++ {
			switch c := segment[j]; {
			case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9', c == '-', c == '_':
			default:
				return tokenErrorf(ErrMalformedToken, "authority token %s is not unpadded base64url", name)
			}
		}
	}
	// The alphabet is checked, so this decode cannot skip a byte; only a
	// length that is not a whole base64 group can still fail it.
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return tokenErrorf(ErrMalformedToken, "authority token signature: %w", err)
	}
	if len(sig) != 65 {
		return tokenErrorf(ErrMalformedToken, "authority token signature: want 65 bytes, got %d", len(sig))
	}
	return nil
}
