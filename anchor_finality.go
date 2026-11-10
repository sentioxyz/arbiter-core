package arbiter

import (
	"crypto/ed25519"
	"fmt"
	"regexp"
)

// The anchor finality modes of AnchorFinalityParams.FinalityMode, with the
// semantics of the arbiter's anchor/evm JudgeFinality: in the tag modes the
// L2's "finalized" or "safe" block must have reached the anchor's entry block;
// in "confirmations" mode the entry block needs FinalityConfirmations
// confirmations.
const (
	AnchorFinalityModeFinalized     = "finalized"
	AnchorFinalityModeSafe          = "safe"
	AnchorFinalityModeConfirmations = "confirmations"
)

// registryContractPattern is a normalized (lowercase) 20-byte address.
var registryContractPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

// VoterEntry is one enrolled Raft voter (consensus parameter voters). VoterID
// is the voter's Raft server id; ConsentSig is lowercase hex of the ed25519
// signature, by Ed25519Pubkey, over the voter message of kind "consent"
// (authority.VoterConsentBody), proof that the key's holder agreed to this
// enrolment. authority.NormalizeConsensusParamsUpdate verifies it.
type VoterEntry struct {
	VoterID       string `json:"voter_id"`
	Ed25519Pubkey []byte `json:"ed25519_pubkey"`
	ConsentSig    string `json:"consent_sig"`
}

// Validate checks one entry's static form; the consent signature is
// verified by authority.NormalizeConsensusParamsUpdate.
func (e VoterEntry) Validate() error {
	switch {
	case !validNodeID(e.VoterID):
		return fmt.Errorf("voters: voter_id %q must be non-empty and free of whitespace and control characters", e.VoterID)
	case len(e.Ed25519Pubkey) != ed25519.PublicKeySize:
		return fmt.Errorf("voters: %s: ed25519_pubkey must be %d bytes, got %d", e.VoterID, ed25519.PublicKeySize, len(e.Ed25519Pubkey))
	case e.ConsentSig == "":
		return fmt.Errorf("voters: %s: consent_sig is required", e.VoterID)
	}
	return nil
}

// AnchorFinalityPolicy is the part of AnchorFinalityParams every voter
// attestation signs: how an anchor entry becomes final and last-mergeable,
// and on which chain and registry contract. It is immutable once committed.
type AnchorFinalityPolicy struct {
	FinalityMode               string `json:"finality_mode"`
	FinalityConfirmations      uint64 `json:"finality_confirmations"`
	LastMergeableConfirmations uint64 `json:"last_mergeable_confirmations"`
	L2ChainID                  uint64 `json:"l2_chain_id"`
	RegistryContract           string `json:"registry_contract"`
}

// AnchorFinalityParams is the anchor finality quorum (consensus parameter
// anchor_finality): once committed, a block's anchor finality and
// last-mergeable flags latch only with signed attestations from at least
// Quorum distinct enrolled voters, judged under the policy. Quorum is a
// majority of the enrolled voters; every other field is immutable once
// committed (the FSM's rule).
type AnchorFinalityParams struct {
	Quorum                     uint32 `json:"quorum"`
	FinalityMode               string `json:"finality_mode"`
	FinalityConfirmations      uint64 `json:"finality_confirmations,omitempty"`
	LastMergeableConfirmations uint64 `json:"last_mergeable_confirmations,omitempty"`
	L2ChainID                  uint64 `json:"l2_chain_id"`
	RegistryContract           string `json:"registry_contract"`
}

// Policy is the signed policy part of the parameter.
func (p AnchorFinalityParams) Policy() AnchorFinalityPolicy {
	return AnchorFinalityPolicy{FinalityMode: p.FinalityMode, FinalityConfirmations: p.FinalityConfirmations,
		LastMergeableConfirmations: p.LastMergeableConfirmations, L2ChainID: p.L2ChainID, RegistryContract: p.RegistryContract}
}

// Validate checks a normalized (lowercase contract) parameter against the
// number of enrolled voters in the same update.
func (p AnchorFinalityParams) Validate(voters int) error {
	switch p.FinalityMode {
	case AnchorFinalityModeFinalized, AnchorFinalityModeSafe:
	case AnchorFinalityModeConfirmations:
		if p.FinalityConfirmations == 0 {
			return fmt.Errorf("anchor_finality: finality_mode %q needs finality_confirmations above 0", p.FinalityMode)
		}
	default:
		return fmt.Errorf("anchor_finality: finality_mode %q is not %q, %q or %q", p.FinalityMode,
			AnchorFinalityModeFinalized, AnchorFinalityModeSafe, AnchorFinalityModeConfirmations)
	}
	switch {
	case p.L2ChainID == 0:
		return fmt.Errorf("anchor_finality: l2_chain_id must be above 0")
	case !registryContractPattern.MatchString(p.RegistryContract) || p.RegistryContract == zeroAddress:
		return fmt.Errorf("anchor_finality: registry_contract must be a non-zero 0x-prefixed 20-byte address")
	case voters == 0:
		return fmt.Errorf("anchor_finality: needs a non-empty voters set in the same update")
	case int(p.Quorum) <= voters/2 || int(p.Quorum) > voters:
		return fmt.Errorf("anchor_finality: quorum %d is not a majority of the %d enrolled voters (%d to %d)", p.Quorum, voters, voters/2+1, voters)
	}
	return nil
}

// AnchorFinalityAttestation is one enrolled voter's signed judgment that a
// block's anchor registry entry, read with the voter's own L2 RPC, holds the
// block's StateRoot at L2BlockNumber and is Final (and possibly
// LastMergeable) under the committed policy. Signature is lowercase hex of
// the voter key's ed25519 signature over authority.AnchorFinalityHash. It
// mirrors pb.AnchorFinalityAttestation.
type AnchorFinalityAttestation struct {
	VoterID       string `json:"voter_id"`
	L3BlockSeq    uint64 `json:"l3_block_seq"`
	L3BlockHash   string `json:"l3_block_hash"`
	StateRoot     string `json:"state_root"`
	L2BlockNumber uint64 `json:"l2_block_number"`
	Final         bool   `json:"final"`
	LastMergeable bool   `json:"last_mergeable"`
	Signature     string `json:"signature"`
}
