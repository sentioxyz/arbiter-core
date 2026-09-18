package authority

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter-core/wire"
)

// ArtifactDispositionAdminPurpose is deliberately distinct from every legacy
// authority token family. It authorizes one public command, never validation.
const ArtifactDispositionAdminPurpose = "housegate-artifact-disposition-command-v1"

// ArtifactDispositionAdminVersion is the only accepted administrator-payload
// version for the C1 command family.
const ArtifactDispositionAdminVersion uint32 = 1

// ArtifactDispositionAdminPayloadV1 is the exact signed JSON payload. Its
// declaration order is protocol order and all fields are required.
type ArtifactDispositionAdminPayloadV1 struct {
	Purpose string                            `json:"purpose"`
	Version uint32                            `json:"version"`
	Iat     int64                             `json:"iat"`
	Command wire.ArtifactDispositionCommandV1 `json:"command"`
}

// SignArtifactDispositionCommand signs a complete public command at the
// current time. The signing key must be the command's canonical actor.
func (s *Signer) SignArtifactDispositionCommand(command wire.ArtifactDispositionCommandV1) (string, error) {
	return s.SignArtifactDispositionCommandAt(command, time.Now().Unix())
}

// SignArtifactDispositionCommandAt is the reproducible-fixture form.
func (s *Signer) SignArtifactDispositionCommandAt(command wire.ArtifactDispositionCommandV1, iat int64) (string, error) {
	if _, err := wire.ArtifactDispositionCommandRoot(command); err != nil {
		return "", err
	}
	if command.ActorID != s.address {
		return "", fmt.Errorf("artifact disposition command actor %q does not match signing authority %s", command.ActorID, s.address)
	}
	if iat <= 0 {
		return "", fmt.Errorf("artifact disposition administrator token iat must be positive")
	}
	payload := ArtifactDispositionAdminPayloadV1{
		Purpose: ArtifactDispositionAdminPurpose,
		Version: ArtifactDispositionAdminVersion,
		Iat:     iat,
		Command: command,
	}
	return s.signArtifactDispositionPayload(payload)
}

func (s *Signer) signArtifactDispositionPayload(payload ArtifactDispositionAdminPayloadV1) (string, error) {
	headerJSON := []byte(`{"alg":"ES256K","typ":"JWT"}`)
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal artifact disposition administrator payload: %w", err)
	}
	input := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	sig, err := crypto.Sign(crypto.Keccak256([]byte(input)), s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign artifact disposition administrator token: %w", err)
	}
	sig[64] += 27
	return input + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// VerifyArtifactDispositionCommand is a timeless, deterministic verifier for
// replicated apply and historical proof paths. Freshness belongs to a live
// admission boundary, so MaxTokenAge is intentionally ignored here.
func (v *Validator) VerifyArtifactDispositionCommand(command wire.ArtifactDispositionCommandV1, token string) (string, error) {
	if len(v.AllowedAddresses) == 0 {
		return "", fmt.Errorf("authority allowlist is empty: refusing to authorize any command")
	}
	if _, err := wire.ArtifactDispositionCommandRoot(command); err != nil {
		return "", err
	}
	if command.ActorID != strings.ToLower(command.ActorID) {
		return "", fmt.Errorf("artifact disposition command actor must be lowercase")
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", fmt.Errorf("artifact disposition token: want non-empty JWS compact form with 3 parts")
	}
	header, err := strictRawBase64URL(parts[0])
	if err != nil {
		return "", fmt.Errorf("artifact disposition token header: %w", err)
	}
	if !bytes.Equal(header, []byte(`{"alg":"ES256K","typ":"JWT"}`)) {
		return "", fmt.Errorf("artifact disposition token: non-canonical header")
	}
	payloadBytes, err := strictRawBase64URL(parts[1])
	if err != nil {
		return "", fmt.Errorf("artifact disposition token payload: %w", err)
	}
	payload, err := parseArtifactDispositionPayload(payloadBytes, command)
	if err != nil {
		return "", err
	}
	canonicalPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal canonical artifact disposition payload: %w", err)
	}
	if !bytes.Equal(payloadBytes, canonicalPayload) {
		return "", fmt.Errorf("artifact disposition token: non-canonical payload")
	}
	sig, err := strictRawBase64URL(parts[2])
	if err != nil {
		return "", fmt.Errorf("artifact disposition token signature: %w", err)
	}
	if err := validateArtifactDispositionSignature(sig); err != nil {
		return "", err
	}
	recovery := append([]byte(nil), sig...)
	recovery[64] -= 27
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(parts[0]+"."+parts[1])), recovery)
	if err != nil {
		return "", fmt.Errorf("recover artifact disposition authority address: %w", err)
	}
	actor := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex())
	if actor != command.ActorID {
		return "", fmt.Errorf("artifact disposition token actor %s does not bind command actor %s", actor, command.ActorID)
	}
	if !v.AllowedAddresses[actor] {
		return "", fmt.Errorf("authority address %s not in allowlist", actor)
	}
	return actor, nil
}

func strictRawBase64URL(s string) ([]byte, error) {
	b, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	if base64.RawURLEncoding.EncodeToString(b) != s {
		return nil, fmt.Errorf("must use canonical raw base64url")
	}
	return b, nil
}

func parseArtifactDispositionPayload(b []byte, command wire.ArtifactDispositionCommandV1) (ArtifactDispositionAdminPayloadV1, error) {
	fields, err := strictJSONObject(b, "purpose", "version", "iat", "command")
	if err != nil {
		return ArtifactDispositionAdminPayloadV1{}, fmt.Errorf("artifact disposition token payload: %w", err)
	}
	var purpose string
	var version uint32
	var iat int64
	if err := json.Unmarshal(fields["purpose"], &purpose); err != nil {
		return ArtifactDispositionAdminPayloadV1{}, fmt.Errorf("artifact disposition token purpose: %w", err)
	}
	if err := json.Unmarshal(fields["version"], &version); err != nil {
		return ArtifactDispositionAdminPayloadV1{}, fmt.Errorf("artifact disposition token version: %w", err)
	}
	if err := json.Unmarshal(fields["iat"], &iat); err != nil {
		return ArtifactDispositionAdminPayloadV1{}, fmt.Errorf("artifact disposition token iat: %w", err)
	}
	if purpose != ArtifactDispositionAdminPurpose || version != ArtifactDispositionAdminVersion || iat <= 0 {
		return ArtifactDispositionAdminPayloadV1{}, fmt.Errorf("artifact disposition token: unexpected purpose, version, or non-positive iat")
	}
	commandJSON, err := json.Marshal(command)
	if err != nil {
		return ArtifactDispositionAdminPayloadV1{}, fmt.Errorf("marshal canonical command: %w", err)
	}
	if !bytes.Equal(fields["command"], commandJSON) {
		return ArtifactDispositionAdminPayloadV1{}, fmt.Errorf("artifact disposition token: command mismatch or non-canonical command")
	}
	return ArtifactDispositionAdminPayloadV1{Purpose: purpose, Version: version, Iat: iat, Command: command}, nil
}

func strictJSONObject(b []byte, allowed ...string) (map[string]json.RawMessage, error) {
	allowedSet := make(map[string]bool, len(allowed))
	for _, key := range allowed {
		allowedSet[key] = true
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return nil, fmt.Errorf("must be an object")
	}
	fields := make(map[string]json.RawMessage, len(allowed))
	for dec.More() {
		token, err := dec.Token()
		if err != nil {
			return nil, err
		}
		key, ok := token.(string)
		if !ok || !allowedSet[key] {
			return nil, fmt.Errorf("unknown field %q", key)
		}
		if _, exists := fields[key]; exists {
			return nil, fmt.Errorf("duplicate field %q", key)
		}
		var value json.RawMessage
		if err := dec.Decode(&value); err != nil {
			return nil, err
		}
		if bytes.Equal(value, []byte("null")) {
			return nil, fmt.Errorf("null field %q", key)
		}
		fields[key] = value
	}
	if token, err = dec.Token(); err != nil || token != json.Delim('}') {
		return nil, fmt.Errorf("unterminated object")
	}
	if dec.More() {
		return nil, fmt.Errorf("trailing JSON")
	}
	for _, key := range allowed {
		if _, ok := fields[key]; !ok {
			return nil, fmt.Errorf("missing field %q", key)
		}
	}
	return fields, nil
}

func validateArtifactDispositionSignature(sig []byte) error {
	if len(sig) != 65 {
		return fmt.Errorf("artifact disposition token signature: want 65 bytes, got %d", len(sig))
	}
	if sig[64] != 27 && sig[64] != 28 {
		return fmt.Errorf("artifact disposition token signature: recovery V must be 27 or 28")
	}
	n := crypto.S256().Params().N
	r := new(big.Int).SetBytes(sig[:32])
	s := new(big.Int).SetBytes(sig[32:64])
	if r.Sign() <= 0 || r.Cmp(n) >= 0 || s.Sign() <= 0 || s.Cmp(n) >= 0 || s.Cmp(new(big.Int).Rsh(new(big.Int).Set(n), 1)) > 0 {
		return fmt.Errorf("artifact disposition token signature: require canonical low-S R||S")
	}
	return nil
}
