package authority

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	"github.com/sentioxyz/arbiter-core"
)

// voterMessageDomain is the CanonicalDigest domain of every message a Raft
// voter signs with its enrolled ed25519 key (consensus parameter voters); it
// is separate from the SNode and verifier message domains.
const voterMessageDomain = "arbiter-voter-message-body-v1"

// VoterMessageKind names one signed voter message.
type VoterMessageKind string

const (
	// VoterMessageConsent is the enrolment consent of a VoterEntry.
	VoterMessageConsent VoterMessageKind = "consent"
	// VoterMessageAnchorFinality is an anchor finality attestation.
	VoterMessageAnchorFinality VoterMessageKind = "anchor_finality"
)

// VoterConsentBody is the signed body of a VoterEntry's consent_sig.
type VoterConsentBody struct {
	VoterID       string `json:"voter_id"`
	Ed25519Pubkey []byte `json:"ed25519_pubkey"`
}

// AnchorFinalityBody is the signed body of an anchor finality attestation:
// the attestation minus its signature, plus the committed policy, which is
// not on the wire.
type AnchorFinalityBody struct {
	VoterID       string                       `json:"voter_id"`
	L3BlockSeq    uint64                       `json:"l3_block_seq"`
	L3BlockHash   string                       `json:"l3_block_hash"`
	StateRoot     string                       `json:"state_root"`
	L2BlockNumber uint64                       `json:"l2_block_number"`
	Final         bool                         `json:"final"`
	LastMergeable bool                         `json:"last_mergeable"`
	Policy        arbiter.AnchorFinalityPolicy `json:"policy"`
}

// NewAnchorFinalityBody is the body att signs under policy.
func NewAnchorFinalityBody(att arbiter.AnchorFinalityAttestation, policy arbiter.AnchorFinalityPolicy) AnchorFinalityBody {
	return AnchorFinalityBody{VoterID: att.VoterID, L3BlockSeq: att.L3BlockSeq, L3BlockHash: att.L3BlockHash,
		StateRoot: att.StateRoot, L2BlockNumber: att.L2BlockNumber, Final: att.Final, LastMergeable: att.LastMergeable, Policy: policy}
}

// VoterMessageHash is the hash a voter message's ed25519 signature covers.
func VoterMessageHash(kind VoterMessageKind, ctx MessageContext, body any) (string, error) {
	canonical, err := canonicalVoterBody(kind, body)
	if err != nil {
		return "", err
	}
	return messageHash(voterMessageDomain, string(kind), ctx, canonical)
}

// SignVoterMessage signs the message hash's string bytes with priv and
// returns lowercase hex, the verifier message convention.
func SignVoterMessage(priv ed25519.PrivateKey, kind VoterMessageKind, ctx MessageContext, body any) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("voter message: ed25519 private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	h, err := VoterMessageHash(kind, ctx, body)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(ed25519.Sign(priv, []byte(h))), nil
}

// VerifyVoterMessage checks a voter message signature. Deterministic.
func VerifyVoterMessage(pub ed25519.PublicKey, kind VoterMessageKind, ctx MessageContext, body any, sigHex string) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("voter message: ed25519 public key must be %d bytes, got %d", ed25519.PublicKeySize, len(pub))
	}
	h, err := VoterMessageHash(kind, ctx, body)
	if err != nil {
		return err
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil || len(sig) != ed25519.SignatureSize {
		return fmt.Errorf("voter message: signature must be %d bytes of hex", ed25519.SignatureSize)
	}
	if !ed25519.Verify(pub, []byte(h), sig) {
		return fmt.Errorf("voter message: %s signature does not verify", kind)
	}
	return nil
}

// SignVoterConsent returns the consent_sig of the VoterEntry that enrols
// priv's public key as voterID.
func SignVoterConsent(priv ed25519.PrivateKey, ctx MessageContext, voterID string) (string, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return "", fmt.Errorf("voter message: ed25519 private key must be %d bytes, got %d", ed25519.PrivateKeySize, len(priv))
	}
	pub := priv.Public().(ed25519.PublicKey)
	return SignVoterMessage(priv, VoterMessageConsent, ctx, VoterConsentBody{VoterID: voterID, Ed25519Pubkey: pub})
}

// VerifyVoterConsent checks e's consent_sig.
func VerifyVoterConsent(e arbiter.VoterEntry, ctx MessageContext) error {
	return VerifyVoterMessage(e.Ed25519Pubkey, VoterMessageConsent, ctx,
		VoterConsentBody{VoterID: e.VoterID, Ed25519Pubkey: e.Ed25519Pubkey}, e.ConsentSig)
}

// SignAnchorFinality returns att with Signature set by priv under policy.
func SignAnchorFinality(priv ed25519.PrivateKey, ctx MessageContext, policy arbiter.AnchorFinalityPolicy, att arbiter.AnchorFinalityAttestation) (arbiter.AnchorFinalityAttestation, error) {
	sig, err := SignVoterMessage(priv, VoterMessageAnchorFinality, ctx, NewAnchorFinalityBody(att, policy))
	if err != nil {
		return arbiter.AnchorFinalityAttestation{}, err
	}
	att.Signature = sig
	return att, nil
}

// VerifyAnchorFinality checks att's signature by pub under policy.
// Deterministic; every rule over committed state is the FSM's.
func VerifyAnchorFinality(pub ed25519.PublicKey, ctx MessageContext, policy arbiter.AnchorFinalityPolicy, att arbiter.AnchorFinalityAttestation) error {
	return VerifyVoterMessage(pub, VoterMessageAnchorFinality, ctx, NewAnchorFinalityBody(att, policy), att.Signature)
}

func canonicalVoterBody(kind VoterMessageKind, body any) (any, error) {
	switch kind {
	case VoterMessageConsent:
		if v, ok := body.(VoterConsentBody); ok {
			v.Ed25519Pubkey = nilIfEmpty(v.Ed25519Pubkey)
			return v, nil
		}
	case VoterMessageAnchorFinality:
		if v, ok := body.(AnchorFinalityBody); ok {
			return v, nil
		}
	default:
		return nil, fmt.Errorf("voter message: unknown kind %q", kind)
	}
	return nil, fmt.Errorf("voter message: kind %q does not take a %T body", kind, body)
}

// normalizeVoters returns a validated copy of the voters set sorted by voter
// id (nil when empty), each key detached from the caller's, ids and keys
// unique and every consent signature verifying in ctx.
func normalizeVoters(in []arbiter.VoterEntry, ctx MessageContext) ([]arbiter.VoterEntry, error) {
	if len(in) == 0 {
		return nil, nil
	}
	out := make([]arbiter.VoterEntry, len(in))
	for i, v := range in {
		if err := v.Validate(); err != nil {
			return nil, err
		}
		v.Ed25519Pubkey = slices.Clone(v.Ed25519Pubkey)
		if err := VerifyVoterConsent(v, ctx); err != nil {
			return nil, fmt.Errorf("voters: %s: consent_sig: %w", v.VoterID, err)
		}
		out[i] = v
	}
	slices.SortFunc(out, func(a, b arbiter.VoterEntry) int { return strings.Compare(a.VoterID, b.VoterID) })
	keys := make(map[string]bool, len(out))
	for i, v := range out {
		if i > 0 && out[i-1].VoterID == v.VoterID {
			return nil, fmt.Errorf("voters: duplicate voter_id %q", v.VoterID)
		}
		if keys[string(v.Ed25519Pubkey)] {
			return nil, fmt.Errorf("voters: %s reuses another voter's ed25519_pubkey", v.VoterID)
		}
		keys[string(v.Ed25519Pubkey)] = true
	}
	return out, nil
}

// normalizeAnchorFinality returns a validated copy with a lowercase registry
// contract; absent stays nil. It needs the voters set and the promotion
// finality gate in the same update.
func normalizeAnchorFinality(in *arbiter.AnchorFinalityParams, voters int, promotionFinalityGate bool) (*arbiter.AnchorFinalityParams, error) {
	if in == nil {
		return nil, nil
	}
	p := *in
	p.RegistryContract = strings.ToLower(p.RegistryContract)
	if !promotionFinalityGate {
		return nil, fmt.Errorf("anchor_finality requires promotion_finality_gate in the same update")
	}
	if err := p.Validate(voters); err != nil {
		return nil, err
	}
	return &p, nil
}
