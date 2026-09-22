package authority

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/housegate/housegate/pkg/replay"
)

// QueryProfileActivationPurpose is the token family for Raft tag 23's
// (ActivateQueryProfile) proposer authorization. A promotion, cleanup,
// consensus-update, disposition, or abort token cannot authorize an
// activation, and an activation token authorizes nothing else. It is never
// inferred from a candidate publication; that path installs its own
// "publication" activation kind independently of this authority purpose.
const QueryProfileActivationPurpose = "housegate-query-profile-activation-v1"

// QueryProfileActivationVersion is the only accepted activation-payload version.
const QueryProfileActivationVersion uint32 = 1

// queryProfileActivationCommandDomain keeps the signed command digest apart
// from any record-level digest a future caller may define for
// replay.ActiveQueryPolicy: one domain per command kind, as payload.go
// explains.
const queryProfileActivationCommandDomain = "arbiter-query-profile-activation-command-v1"

// QueryProfileActivationPayloadV1 is the exact signed JSON payload. Its
// declaration order is protocol order and all fields are required.
type QueryProfileActivationPayloadV1 struct {
	Purpose    string                   `json:"purpose"`
	Version    uint32                   `json:"version"`
	Iat        int64                    `json:"iat"`
	Activation replay.ActiveQueryPolicy `json:"activation"`
}

// ValidateQueryProfileActivation enforces the constraints every activation
// record must satisfy regardless of committed state. State-dependent
// validation (block sequencing, executor/query profile existence, prior
// activation lineage) belongs to the Arbiter FSM.
func ValidateQueryProfileActivation(p replay.ActiveQueryPolicy) error {
	checkField := func(name, value string) error {
		if strings.TrimSpace(value) == "" || strings.ContainsRune(value, 0) {
			return fmt.Errorf("query profile activation: %s must be non-empty and NUL-free", name)
		}
		return nil
	}
	if err := checkField("activation_id", p.ActivationID); err != nil {
		return err
	}
	if err := checkField("network_id", p.NetworkID); err != nil {
		return err
	}
	if p.KeeperShardID != 0 {
		return fmt.Errorf("query profile activation: keeper_shard_id must be zero")
	}
	if err := checkField("executor_profile_id", p.ExecutorProfileID); err != nil {
		return err
	}
	if err := checkField("query_profile_id", p.QueryProfileID); err != nil {
		return err
	}
	if !p.Enabled {
		return fmt.Errorf("query profile activation: enabled must be true")
	}
	return nil
}

// QueryProfileActivationHash binds every activation field under the command
// domain. Both live authorization and deterministic replay use it.
func QueryProfileActivationHash(p replay.ActiveQueryPolicy) (string, error) {
	if err := ValidateQueryProfileActivation(p); err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(queryProfileActivationCommandDomain, p)
	if err != nil {
		return "", fmt.Errorf("hash query profile activation: %w", err)
	}
	return h, nil
}

// SignQueryProfileActivation signs one complete activation at the current time.
func (s *Signer) SignQueryProfileActivation(p replay.ActiveQueryPolicy) (string, error) {
	return s.SignQueryProfileActivationAt(p, time.Now().Unix())
}

// SignQueryProfileActivationAt signs with an explicit Unix issue time; issue
// time is an API-boundary freshness guard, never a replay guard.
func (s *Signer) SignQueryProfileActivationAt(p replay.ActiveQueryPolicy, iat int64) (string, error) {
	if _, err := QueryProfileActivationHash(p); err != nil {
		return "", err
	}
	if iat <= 0 {
		return "", fmt.Errorf("query profile activation token iat must be positive")
	}
	payload := QueryProfileActivationPayloadV1{
		Purpose:    QueryProfileActivationPurpose,
		Version:    QueryProfileActivationVersion,
		Iat:        iat,
		Activation: p,
	}
	return s.signQueryProfileActivationPayload(payload)
}

func (s *Signer) signQueryProfileActivationPayload(payload QueryProfileActivationPayloadV1) (string, error) {
	headerJSON := []byte(`{"alg":"ES256K","typ":"JWT"}`)
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal query profile activation payload: %w", err)
	}
	input := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	sig, err := crypto.Sign(crypto.Keccak256([]byte(input)), s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign query profile activation token: %w", err)
	}
	sig[64] += 27
	return input + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}

// VerifyQueryProfileActivation is a timeless, deterministic verifier for
// replicated apply and historical proof paths. Freshness belongs to a live
// admission boundary, so MaxTokenAge is intentionally ignored here.
func (v *Validator) VerifyQueryProfileActivation(p replay.ActiveQueryPolicy, token string) (string, error) {
	return v.verifyQueryProfileActivation(p, token, false)
}

// AuthorizeQueryProfileActivation additionally enforces token age for an API
// boundary. It must not be used inside replicated Apply or snapshot replay.
func (v *Validator) AuthorizeQueryProfileActivation(p replay.ActiveQueryPolicy, token string) (string, error) {
	return v.verifyQueryProfileActivation(p, token, true)
}

func (v *Validator) verifyQueryProfileActivation(p replay.ActiveQueryPolicy, token string, enforceAge bool) (string, error) {
	if len(v.AllowedAddresses) == 0 {
		return "", fmt.Errorf("authority allowlist is empty: refusing to authorize any command")
	}
	if enforceAge && v.MaxTokenAge <= 0 {
		return "", fmt.Errorf("authority validator: MaxTokenAge must be positive (fail-closed; a zero value would mean never-expiring tokens)")
	}
	if _, err := QueryProfileActivationHash(p); err != nil {
		return "", err
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return "", fmt.Errorf("query profile activation token: want non-empty JWS compact form with 3 parts")
	}
	header, err := strictRawBase64URL(parts[0])
	if err != nil {
		return "", fmt.Errorf("query profile activation token header: %w", err)
	}
	if !bytes.Equal(header, []byte(`{"alg":"ES256K","typ":"JWT"}`)) {
		return "", fmt.Errorf("query profile activation token: non-canonical header")
	}
	payloadBytes, err := strictRawBase64URL(parts[1])
	if err != nil {
		return "", fmt.Errorf("query profile activation token payload: %w", err)
	}
	payload, err := parseQueryProfileActivationPayload(payloadBytes, p)
	if err != nil {
		return "", err
	}
	canonicalPayload, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal canonical query profile activation payload: %w", err)
	}
	if !bytes.Equal(payloadBytes, canonicalPayload) {
		return "", fmt.Errorf("query profile activation token: non-canonical payload")
	}
	sig, err := strictRawBase64URL(parts[2])
	if err != nil {
		return "", fmt.Errorf("query profile activation token signature: %w", err)
	}
	if len(sig) != 65 {
		return "", fmt.Errorf("query profile activation token signature: want 65 bytes, got %d", len(sig))
	}
	if enforceAge {
		now := time.Now().Unix()
		if payload.Iat-now > clockSkewToleranceSeconds {
			return "", fmt.Errorf("query profile activation token issued in the future")
		}
		if now-payload.Iat > int64(v.MaxTokenAge.Seconds())+clockSkewToleranceSeconds {
			return "", fmt.Errorf("query profile activation token expired")
		}
	}
	recovery := append([]byte(nil), sig...)
	if recovery[64] >= 27 {
		recovery[64] -= 27
	}
	pub, err := crypto.SigToPub(crypto.Keccak256([]byte(parts[0]+"."+parts[1])), recovery)
	if err != nil {
		return "", fmt.Errorf("recover query profile activation authority address: %w", err)
	}
	actor := strings.ToLower(crypto.PubkeyToAddress(*pub).Hex())
	if !v.AllowedAddresses[actor] {
		return "", fmt.Errorf("authority address %s not in allowlist", actor)
	}
	return actor, nil
}

func parseQueryProfileActivationPayload(b []byte, activation replay.ActiveQueryPolicy) (QueryProfileActivationPayloadV1, error) {
	fields, err := strictJSONObject(b, "purpose", "version", "iat", "activation")
	if err != nil {
		return QueryProfileActivationPayloadV1{}, fmt.Errorf("query profile activation token payload: %w", err)
	}
	var purpose string
	var version uint32
	var iat int64
	if err := json.Unmarshal(fields["purpose"], &purpose); err != nil {
		return QueryProfileActivationPayloadV1{}, fmt.Errorf("query profile activation token purpose: %w", err)
	}
	if err := json.Unmarshal(fields["version"], &version); err != nil {
		return QueryProfileActivationPayloadV1{}, fmt.Errorf("query profile activation token version: %w", err)
	}
	if err := json.Unmarshal(fields["iat"], &iat); err != nil {
		return QueryProfileActivationPayloadV1{}, fmt.Errorf("query profile activation token iat: %w", err)
	}
	if purpose != QueryProfileActivationPurpose || version != QueryProfileActivationVersion || iat <= 0 {
		return QueryProfileActivationPayloadV1{}, fmt.Errorf("query profile activation token: unexpected purpose, version, or non-positive iat")
	}
	activationJSON, err := json.Marshal(activation)
	if err != nil {
		return QueryProfileActivationPayloadV1{}, fmt.Errorf("marshal canonical activation: %w", err)
	}
	if !bytes.Equal(fields["activation"], activationJSON) {
		return QueryProfileActivationPayloadV1{}, fmt.Errorf("query profile activation token: activation mismatch or non-canonical activation")
	}
	return QueryProfileActivationPayloadV1{Purpose: purpose, Version: version, Iat: iat, Activation: activation}, nil
}
