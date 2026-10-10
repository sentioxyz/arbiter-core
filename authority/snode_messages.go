package authority

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// SNodeEnrollmentPurpose is the JWSCommandPayload.Purpose of an SNode
// enrolment statement (housegate spec 2026-10-10 §6.2): the indexer's
// on-chain signer consents to, and controls, one SNode identity of one network.
const SNodeEnrollmentPurpose = "arbiter-snode-enrollment-v1"

// SNodeMessagePurpose is the purpose of every message an SNode signs with its
// indexer's key (spec D6, §6.5); the hash binds the message kind.
const SNodeMessagePurpose = "arbiter-snode-message-v1"

const (
	snodeEnrollmentDomain = "arbiter-snode-enrollment-statement-v1"
	snodeMessageDomain    = "arbiter-snode-message-body-v1"
	verifierMessageDomain = "arbiter-verifier-message-body-v1"
)

// SNodeEnrollmentStatement is what an enrolment JWS signs.
type SNodeEnrollmentStatement struct {
	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
	IndexerID         uint64 `json:"indexer_id"`
	SNodeNodeID       string `json:"snode_node_id"`
}

// SNodeEnrollmentHash is the statement's command hash.
func SNodeEnrollmentHash(stmt SNodeEnrollmentStatement) (string, error) {
	if err := (MessageContext{NetworkID: stmt.NetworkID, GenesisSnapshotID: stmt.GenesisSnapshotID}).validate(); err != nil {
		return "", fmt.Errorf("snode enrollment: %w", err)
	}
	if strings.TrimSpace(stmt.SNodeNodeID) == "" {
		return "", fmt.Errorf("snode enrollment: snode node ID must be non-empty")
	}
	h, err := replay.CanonicalDigest(snodeEnrollmentDomain, stmt)
	if err != nil {
		return "", fmt.Errorf("hash snode enrollment: %w", err)
	}
	return h, nil
}

// SignSNodeEnrollment signs stmt at the current time.
func (s *Signer) SignSNodeEnrollment(stmt SNodeEnrollmentStatement) (string, error) {
	return s.SignSNodeEnrollmentAt(stmt, time.Now().Unix())
}

// SignSNodeEnrollmentAt signs stmt with an explicit issue time (fixtures and
// CLIs that want reproducible output); iat is not a replay guard.
func (s *Signer) SignSNodeEnrollmentAt(stmt SNodeEnrollmentStatement, iat int64) (string, error) {
	h, err := SNodeEnrollmentHash(stmt)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: SNodeEnrollmentPurpose, CmdHash: h})
}

// VerifySNodeEnrollment checks that jws is signer's enrolment of stmt. It is
// deterministic (no clock) and therefore usable in Raft Apply.
func VerifySNodeEnrollment(stmt SNodeEnrollmentStatement, jws, signer string) error {
	h, err := SNodeEnrollmentHash(stmt)
	if err != nil {
		return err
	}
	return verifySignedBy(h, SNodeEnrollmentPurpose, jws, signer)
}

// SNodeMessageKind names one signed SNode message.
type SNodeMessageKind string

const (
	SNodeMessageRegistration SNodeMessageKind = "registration"
	SNodeMessageMarkActive   SNodeMessageKind = "mark_active"
	SNodeMessageResultClaim  SNodeMessageKind = "result_claim"
	SNodeMessagePromotionAck SNodeMessageKind = "promotion_ack"
	SNodeMessageCleanupAck   SNodeMessageKind = "cleanup_ack"
	SNodeMessageTablePurged  SNodeMessageKind = "table_purged"
)

// VerifierMessageKind names one signed verifier message.
type VerifierMessageKind string

const (
	VerifierMessageRegistration VerifierMessageKind = "registration"
	VerifierMessageMarkActive   VerifierMessageKind = "mark_active"
	VerifierMessageTablePurged  VerifierMessageKind = "table_purged"
)

// MessageContext binds a signed data-plane message to one network
// incarnation (spec §6.5).
type MessageContext struct {
	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
}

func (c MessageContext) validate() error {
	if strings.TrimSpace(c.NetworkID) == "" || strings.TrimSpace(c.GenesisSnapshotID) == "" {
		return fmt.Errorf("message context: network ID and genesis snapshot ID must be non-empty")
	}
	return nil
}

// MarkActiveBody is the signed body of a MarkActive request.
type MarkActiveBody struct {
	NodeID          string `json:"node_id"`
	RegistrationSeq uint64 `json:"registration_seq"`
}

// TablePurgedBody is the signed body of a SubmitTablePurged report.
type TablePurgedBody struct {
	NodeID         string `json:"node_id"`
	IncarnationSeq uint64 `json:"incarnation_seq"`
}

type messagePreimage struct {
	Kind    string         `json:"kind"`
	Context MessageContext `json:"context"`
	Body    any            `json:"body"`
}

// SNodeMessageHash is the command hash of one SNode message.
func SNodeMessageHash(kind SNodeMessageKind, ctx MessageContext, body any) (string, error) {
	canonical, err := canonicalSNodeBody(kind, body)
	if err != nil {
		return "", err
	}
	return messageHash(snodeMessageDomain, string(kind), ctx, canonical)
}

// SignSNodeMessage signs one SNode message at the current time.
func (s *Signer) SignSNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any) (string, error) {
	return s.SignSNodeMessageAt(kind, ctx, body, time.Now().Unix())
}

// SignSNodeMessageAt signs with an explicit issue time; iat is informational.
func (s *Signer) SignSNodeMessageAt(kind SNodeMessageKind, ctx MessageContext, body any, iat int64) (string, error) {
	h, err := SNodeMessageHash(kind, ctx, body)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: SNodeMessagePurpose, CmdHash: h})
}

// VerifySNodeMessage checks that jws is signer's signature of the message.
// Deterministic: no clock, usable in Raft Apply and snapshot restore.
func VerifySNodeMessage(kind SNodeMessageKind, ctx MessageContext, body any, jws, signer string) error {
	h, err := SNodeMessageHash(kind, ctx, body)
	if err != nil {
		return err
	}
	return verifySignedBy(h, SNodeMessagePurpose, jws, signer)
}

// VerifierMessageHash is the hash a verifier message's ed25519 signature
// covers; its digest domain is separate from the SNode messages'.
func VerifierMessageHash(kind VerifierMessageKind, ctx MessageContext, body any) (string, error) {
	canonical, err := canonicalVerifierBody(kind, body)
	if err != nil {
		return "", err
	}
	return messageHash(verifierMessageDomain, string(kind), ctx, canonical)
}

// SignVerifierMessage signs the message hash's string bytes with priv and
// returns lowercase hex: the convention of ByteSideScanMsg and attestations.
func SignVerifierMessage(priv ed25519.PrivateKey, kind VerifierMessageKind, ctx MessageContext, body any) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("verifier message: ed25519 private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	h, err := VerifierMessageHash(kind, ctx, body)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(ed25519.Sign(priv, []byte(h))), nil
}

// VerifyVerifierMessage checks a verifier message signature. Deterministic.
func VerifyVerifierMessage(pub ed25519.PublicKey, kind VerifierMessageKind, ctx MessageContext, body any, sigHex string) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("verifier message: ed25519 public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	h, err := VerifierMessageHash(kind, ctx, body)
	if err != nil {
		return err
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("verifier message: signature must be %d bytes of hex", ed25519.SignatureSize)
	}
	if !ed25519.Verify(pub, []byte(h), sig) {
		return fmt.Errorf("verifier message: %s signature does not verify", kind)
	}
	return nil
}

func messageHash(domain, kind string, ctx MessageContext, body any) (string, error) {
	if err := ctx.validate(); err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(domain, messagePreimage{Kind: kind, Context: ctx, Body: body})
	if err != nil {
		return "", fmt.Errorf("hash %s message: %w", kind, err)
	}
	return h, nil
}

// canonicalSNodeBody pairs each kind with its one body type (a value, never a
// pointer) and returns the body as the FSM decodes it: wire converters turn
// every empty repeated field into nil, so [] hashes as null here too.
func canonicalSNodeBody(kind SNodeMessageKind, body any) (any, error) {
	switch kind {
	case SNodeMessageRegistration:
		if v, ok := body.(arbiter.NodeRegistration); ok {
			return canonicalRegistration(v), nil
		}
	case SNodeMessageMarkActive:
		if v, ok := body.(MarkActiveBody); ok {
			return v, nil
		}
	case SNodeMessageResultClaim:
		if v, ok := body.(arbiter.RCRecord); ok {
			v.CandidateParts = nilIfEmpty(v.CandidateParts)
			v.PartitionNewPartSums = nilIfEmpty(v.PartitionNewPartSums)
			return v, nil
		}
	case SNodeMessagePromotionAck:
		if v, ok := body.(arbiter.PromotionAck); ok {
			v.Parts = nilIfEmpty(v.Parts)
			v.SafePartitionParts = nilIfEmpty(v.SafePartitionParts)
			return v, nil
		}
	case SNodeMessageCleanupAck:
		if v, ok := body.(arbiter.CleanupAck); ok {
			return v, nil
		}
	case SNodeMessageTablePurged:
		if v, ok := body.(TablePurgedBody); ok {
			return v, nil
		}
	default:
		return nil, fmt.Errorf("snode message: unknown kind %q", kind)
	}
	return nil, fmt.Errorf("snode message: kind %q does not take a %T body", kind, body)
}

func canonicalVerifierBody(kind VerifierMessageKind, body any) (any, error) {
	switch kind {
	case VerifierMessageRegistration:
		if v, ok := body.(arbiter.NodeRegistration); ok {
			return canonicalRegistration(v), nil
		}
	case VerifierMessageMarkActive:
		if v, ok := body.(MarkActiveBody); ok {
			return v, nil
		}
	case VerifierMessageTablePurged:
		if v, ok := body.(TablePurgedBody); ok {
			return v, nil
		}
	default:
		return nil, fmt.Errorf("verifier message: unknown kind %q", kind)
	}
	return nil, fmt.Errorf("verifier message: kind %q does not take a %T body", kind, body)
}

func canonicalRegistration(r arbiter.NodeRegistration) arbiter.NodeRegistration {
	r.Roles = nilIfEmpty(r.Roles)
	r.Ed25519Pubkey = nilIfEmpty(r.Ed25519Pubkey)
	return r
}

func nilIfEmpty[T any](s []T) []T {
	if len(s) == 0 {
		return nil
	}
	return s
}

// verifySignedBy is the deterministic single-signer form of Validator.verify:
// the same ES256K compact JWS, purpose and command-hash checks, no token-age
// check, and the recovered address must be signer (compared lowercase).
func verifySignedBy(wantHash, purpose, token, signer string) error {
	want, err := normalizeSignerAddress(signer)
	if err != nil {
		return err
	}
	v := Validator{AllowedAddresses: map[string]bool{want: true}}
	if _, err := v.verify(wantHash, purpose, token, false); err != nil {
		return fmt.Errorf("%s: %w", purpose, err)
	}
	return nil
}

func normalizeSignerAddress(signer string) (string, error) {
	s := strings.ToLower(signer)
	if len(s) != 42 || !strings.HasPrefix(s, "0x") {
		return "", fmt.Errorf("signer %q must be a 0x-prefixed 20-byte address", signer)
	}
	b, err := hex.DecodeString(s[2:])
	if err != nil || !slices.ContainsFunc(b, func(x byte) bool { return x != 0 }) {
		return "", fmt.Errorf("signer %q must be a non-zero hex address", signer)
	}
	return s, nil
}
