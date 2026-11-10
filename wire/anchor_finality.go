package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

// VoterEntriesFromPB copies a repeated VoterEntry; validation belongs to
// authority normalisation. An empty list and an empty key decode to nil.
func VoterEntriesFromPB(ms []*pb.VoterEntry) []arbiter.VoterEntry {
	return mapSlice(ms, func(m *pb.VoterEntry) arbiter.VoterEntry {
		return arbiter.VoterEntry{VoterID: m.GetVoterId(), Ed25519Pubkey: cloneBytes(m.GetEd25519Pubkey()), ConsentSig: m.GetConsentSig()}
	})
}

// VoterEntriesToPB returns independent transport messages (nil for an empty list).
func VoterEntriesToPB(v []arbiter.VoterEntry) []*pb.VoterEntry {
	return mapSlice(v, func(e arbiter.VoterEntry) *pb.VoterEntry {
		return &pb.VoterEntry{VoterId: e.VoterID, Ed25519Pubkey: cloneBytes(e.Ed25519Pubkey), ConsentSig: e.ConsentSig}
	})
}

// AnchorFinalityParamsFromPB copies the transport fields; validation belongs
// to authority normalisation. A missing message decodes to nil.
func AnchorFinalityParamsFromPB(m *pb.AnchorFinalityParams) *arbiter.AnchorFinalityParams {
	if m == nil {
		return nil
	}
	return &arbiter.AnchorFinalityParams{Quorum: m.GetQuorum(), FinalityMode: m.GetFinalityMode(),
		FinalityConfirmations: m.GetFinalityConfirmations(), LastMergeableConfirmations: m.GetLastMergeableConfirmations(),
		L2ChainID: m.GetL2ChainId(), RegistryContract: m.GetRegistryContract()}
}

// AnchorFinalityParamsToPB returns an independent transport message.
func AnchorFinalityParamsToPB(v *arbiter.AnchorFinalityParams) *pb.AnchorFinalityParams {
	if v == nil {
		return nil
	}
	return &pb.AnchorFinalityParams{Quorum: v.Quorum, FinalityMode: v.FinalityMode,
		FinalityConfirmations: v.FinalityConfirmations, LastMergeableConfirmations: v.LastMergeableConfirmations,
		L2ChainId: v.L2ChainID, RegistryContract: v.RegistryContract}
}

// AnchorFinalityAttestationFromPB copies the transport fields; a missing
// message decodes to the zero attestation, which the FSM refuses.
func AnchorFinalityAttestationFromPB(m *pb.AnchorFinalityAttestation) arbiter.AnchorFinalityAttestation {
	return arbiter.AnchorFinalityAttestation{VoterID: m.GetVoterId(), L3BlockSeq: m.GetL3BlockSeq(),
		L3BlockHash: m.GetL3BlockHash(), StateRoot: m.GetStateRoot(), L2BlockNumber: m.GetL2BlockNumber(),
		Final: m.GetFinal(), LastMergeable: m.GetLastMergeable(), Signature: m.GetSignature()}
}

// AnchorFinalityAttestationToPB returns an independent transport message.
func AnchorFinalityAttestationToPB(v arbiter.AnchorFinalityAttestation) *pb.AnchorFinalityAttestation {
	return &pb.AnchorFinalityAttestation{VoterId: v.VoterID, L3BlockSeq: v.L3BlockSeq,
		L3BlockHash: v.L3BlockHash, StateRoot: v.StateRoot, L2BlockNumber: v.L2BlockNumber,
		Final: v.Final, LastMergeable: v.LastMergeable, Signature: v.Signature}
}
