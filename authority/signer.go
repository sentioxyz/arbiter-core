package authority

import (
	"crypto/ecdsa"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter-core"
)

type jwsHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
}

// Signer holds the Arbiter authority secp256k1 key. The key is provisioned
// to every Raft node but USED only by the verified leader (design §8.1,
// §10.2) — that discipline lives in the orchestrator, not here.
type Signer struct {
	privateKey *ecdsa.PrivateKey
	address    string
}

// NewSignerFromHex loads a 32-byte secp256k1 private key from hex (with or
// without 0x prefix).
func NewSignerFromHex(privateKeyHex string) (*Signer, error) {
	key, err := crypto.HexToECDSA(strings.TrimPrefix(privateKeyHex, "0x"))
	if err != nil {
		return nil, fmt.Errorf("parse authority key: %w", err)
	}
	addr := strings.ToLower(crypto.PubkeyToAddress(key.PublicKey).Hex())
	return &Signer{privateKey: key, address: addr}, nil
}

// Address returns the lowercase 0x-prefixed authority address.
func (s *Signer) Address() string { return s.address }

// SignPromotion produces the JWS compact token for one promotion command.
func (s *Signer) SignPromotion(cmd arbiter.PromoteSafePartition) (string, error) {
	return s.signPromotionAt(cmd, time.Now().Unix())
}

// SignCleanup produces the JWS compact token for one cleanup command.
func (s *Signer) SignCleanup(cmd arbiter.UnsafeCleanup) (string, error) {
	h, err := CleanupCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: time.Now().Unix(), Purpose: PromotionPurpose, CmdHash: h})
}

func (s *Signer) signPromotionAt(cmd arbiter.PromoteSafePartition, iat int64) (string, error) {
	h, err := PromoteCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: PromotionPurpose, CmdHash: h})
}

// signWithPurpose exists for negative tests (wrong-purpose tokens).
func (s *Signer) signWithPurpose(cmd arbiter.PromoteSafePartition, purpose string) (string, error) {
	h, err := PromoteCommandHash(cmd)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: time.Now().Unix(), Purpose: purpose, CmdHash: h})
}

// signPayload builds the ES256K JWS compact serialization: keccak256 over
// base64url(header) + "." + base64url(payload), signed with the Ethereum
// V+27 recovery convention — byte-compatible with housegate pkg/auth.
func (s *Signer) signPayload(payload JWSCommandPayload) (string, error) {
	headerJSON, err := json.Marshal(jwsHeader{Alg: "ES256K", Typ: "JWT"})
	if err != nil {
		return "", fmt.Errorf("marshal header: %w", err)
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("marshal payload: %w", err)
	}
	signingInput := base64.RawURLEncoding.EncodeToString(headerJSON) + "." + base64.RawURLEncoding.EncodeToString(payloadJSON)
	sig, err := crypto.Sign(crypto.Keccak256([]byte(signingInput)), s.privateKey)
	if err != nil {
		return "", fmt.Errorf("sign command token: %w", err)
	}
	sig[64] += 27
	return signingInput + "." + base64.RawURLEncoding.EncodeToString(sig), nil
}
