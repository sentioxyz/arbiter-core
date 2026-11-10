package wire

import (
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// TestConsensusParamsUpdateCarriesAnchorFinality: the converters copy the
// part-claim uniqueness gate, the voters and the anchor finality quorum both
// ways; an update without them is the earlier message, whose bytes the legacy
// golden tests keep pinned.
func TestConsensusParamsUpdateCarriesAnchorFinality(t *testing.T) {
	u := legacyGoldenUpdate()
	m := ConsensusParamsUpdateToPB(u)
	if m.GetPartClaimUniqueness() || m.GetVoters() != nil || m.GetAnchorFinality() != nil {
		t.Fatal("an update without the new fields set one of them")
	}
	u.TracksEnabled, u.PromotionFinalityGate, u.PartClaimUniqueness = true, true, true
	u.Voters = []arbiter.VoterEntry{{VoterID: "arbiter-0", Ed25519Pubkey: []byte{1, 2, 3}, ConsentSig: "aa"}, {VoterID: "arbiter-1", Ed25519Pubkey: []byte{4}, ConsentSig: "bb"}}
	u.AnchorFinality = &arbiter.AnchorFinalityParams{Quorum: 2, FinalityMode: "confirmations", FinalityConfirmations: 3,
		LastMergeableConfirmations: 4, L2ChainID: 8453, RegistryContract: "0xabc"}
	m = ConsensusParamsUpdateToPB(u)
	if !m.GetPartClaimUniqueness() || len(m.GetVoters()) != 2 || m.GetAnchorFinality().GetL2ChainId() != 8453 {
		t.Fatalf("ConsensusParamsUpdateToPB dropped a field: %v", m)
	}
	if got := ConsensusParamsUpdateFromPB(m); !reflect.DeepEqual(got, u) {
		t.Fatalf("update round trip\n got %+v\nwant %+v", got, u)
	}
	// The key bytes are detached from the message's.
	m.GetVoters()[0].Ed25519Pubkey[0] = 9
	if u.Voters[0].Ed25519Pubkey[0] != 1 {
		t.Fatal("ConsensusParamsUpdateToPB shares key bytes")
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
}

// TestRecordAnchorFinalityAttestationCommand: tag 43 round-trips through the
// strict codec with every field.
func TestRecordAnchorFinalityAttestationCommand(t *testing.T) {
	att := arbiter.AnchorFinalityAttestation{VoterID: "arbiter-0", L3BlockSeq: 7, L3BlockHash: "0xh", StateRoot: "0xr",
		L2BlockNumber: 100, Final: true, LastMergeable: true, Signature: "ab"}
	if got := AnchorFinalityAttestationFromPB(AnchorFinalityAttestationToPB(att)); got != att {
		t.Fatalf("attestation round trip = %+v", got)
	}
	mustRoundTrip(t, Command{RecordAnchorFinalityAttestation: &RecordAnchorFinalityAttestation{Attestation: att}})
	b, err := Encode(Command{RecordAnchorFinalityAttestation: &RecordAnchorFinalityAttestation{Attestation: att}})
	if err != nil {
		t.Fatal(err)
	}
	var raw pb.RaftCommand
	if err := proto.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.GetRecordAnchorFinalityAttestation().GetAttestation().GetVoterId() != "arbiter-0" {
		t.Fatalf("encoded command = %v", &raw)
	}
	if got := AnchorFinalityAttestationFromPB(nil); got != (arbiter.AnchorFinalityAttestation{}) {
		t.Fatalf("a missing attestation decoded to %+v", got)
	}
}

// TestBlockVerificationCarriesFinalityAttestations: the read converter keeps
// the attestations in order both ways; none decodes to nil.
func TestBlockVerificationCarriesFinalityAttestations(t *testing.T) {
	v := BlockVerification{Header: BlockHeaderView{L3BlockSeq: 3}, ChainHash: "h",
		FinalityAttestations: []arbiter.AnchorFinalityAttestation{{VoterID: "a", L3BlockSeq: 3, Final: true}, {VoterID: "b", L3BlockSeq: 3, LastMergeable: true}}}
	got, err := BlockVerificationFromPB(BlockVerificationToPB(v))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.FinalityAttestations, v.FinalityAttestations) {
		t.Fatalf("attestations = %+v", got.FinalityAttestations)
	}
	v.FinalityAttestations = nil
	if got, err := BlockVerificationFromPB(BlockVerificationToPB(v)); err != nil || got.FinalityAttestations != nil {
		t.Fatalf("no attestations decoded to %+v, %v", got.FinalityAttestations, err)
	}
}
