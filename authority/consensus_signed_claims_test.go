package authority

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func signedClaimsTestUpdate() arbiter.ConsensusParamsUpdate {
	u := testConsensusUpdate()
	u.SIIndexers = []arbiter.SIIndexerEntry{
		{IndexerID: 1, ActivationBlock: 200, Signer: "0x89FD7C610AC4AA2E17C3B15A2C386A4B215F96D9", SNodeNodeID: "snode-2", EnrollmentJWS: "h.p.s"},
		{IndexerID: 0, ActivationBlock: 100, Signer: "0x563Bd9e11d18b6eA60c2f159F8D3062d30E8039e", SNodeNodeID: "snode-1", EnrollmentJWS: "h.p.s"},
	}
	u.Verifiers = []arbiter.VerifierEntry{
		{NodeID: "verifier-3", Ed25519Pubkey: bytes.Repeat([]byte{3}, 32)},
		{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{1}, 32)},
		{NodeID: "verifier-2", Ed25519Pubkey: bytes.Repeat([]byte{2}, 32)},
	}
	return u
}

func TestNormalizeConsensusParamsUpdateSignedClaims(t *testing.T) {
	in := signedClaimsTestUpdate()
	callerEntries := append([]arbiter.SIIndexerEntry(nil), in.SIIndexers...)
	got, err := NormalizeConsensusParamsUpdate(in)
	if err != nil {
		t.Fatal(err)
	}
	if got.SIIndexers[0].IndexerID != 0 || got.SIIndexers[1].IndexerID != 1 ||
		got.SIIndexers[0].Signer != "0x563bd9e11d18b6ea60c2f159f8d3062d30e8039e" || got.SIIndexers[1].Signer != "0x89fd7c610ac4aa2e17c3b15a2c386a4b215f96d9" {
		t.Fatalf("si_indexers = %+v: want sorted by indexer_id with lowercase signers", got.SIIndexers)
	}
	if !reflect.DeepEqual(in.SIIndexers, callerEntries) {
		t.Fatal("normalization mutated the caller's si_indexers")
	}
	if got.Verifiers[0].NodeID != "verifier-1" || got.Verifiers[1].NodeID != "verifier-2" || got.Verifiers[2].NodeID != "verifier-3" {
		t.Fatalf("verifiers = %+v: want sorted by node_id", got.Verifiers)
	}
	got.Verifiers[0].Ed25519Pubkey[0] = 0xff
	if in.Verifiers[1].Ed25519Pubkey[0] != 1 {
		t.Fatal("normalized verifier keys alias the caller's bytes")
	}
	for name, mutate := range map[string]func(*arbiter.ConsensusParamsUpdate){
		"zero activation block": func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].ActivationBlock = 0 },
		"short signer":          func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].Signer = "0x89fd" },
		"zero signer": func(u *arbiter.ConsensusParamsUpdate) {
			u.SIIndexers[0].Signer = "0x0000000000000000000000000000000000000000"
		},
		"empty snode node id":         func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].SNodeNodeID = "" },
		"snode node id with a space":  func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].SNodeNodeID = "snode 2" },
		"missing enrolment":           func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].EnrollmentJWS = "" },
		"duplicate indexer":           func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].IndexerID = 0 },
		"duplicate snode node id":     func(u *arbiter.ConsensusParamsUpdate) { u.SIIndexers[0].SNodeNodeID = "snode-1" },
		"fewer writers than indexers": func(u *arbiter.ConsensusParamsUpdate) { u.MaxWriters = 1 },
		"empty verifier node id":      func(u *arbiter.ConsensusParamsUpdate) { u.Verifiers[0].NodeID = "" },
		"short verifier key":          func(u *arbiter.ConsensusParamsUpdate) { u.Verifiers[0].Ed25519Pubkey = make([]byte, 31) },
		"duplicate verifier":          func(u *arbiter.ConsensusParamsUpdate) { u.Verifiers[0].NodeID = "verifier-1" },
		"shared verifier key":         func(u *arbiter.ConsensusParamsUpdate) { u.Verifiers[0].Ed25519Pubkey = bytes.Repeat([]byte{1}, 32) },
	} {
		t.Run(name, func(t *testing.T) {
			u := signedClaimsTestUpdate()
			mutate(&u)
			_, err := NormalizeConsensusParamsUpdate(u)
			if err == nil || (!strings.Contains(err.Error(), "si_indexers") && !strings.Contains(err.Error(), "verifiers")) {
				t.Fatalf("err = %v, want a refusal naming the list", err)
			}
		})
	}
}

func TestNormalizeConsensusParamsUpdateKeepsAbsentListsNil(t *testing.T) {
	u := testConsensusUpdate()
	u.SIIndexers, u.Verifiers = []arbiter.SIIndexerEntry{}, []arbiter.VerifierEntry{}
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || got.SIIndexers != nil || got.Verifiers != nil {
		t.Fatalf("empty lists normalised to %#v / %#v (%v): want nil, never []", got.SIIndexers, got.Verifiers, err)
	}
	a, errA := ConsensusParamsUpdateHash(testConsensusUpdate())
	b, errB := ConsensusParamsUpdateHash(u)
	if errA != nil || errB != nil || a != b {
		t.Fatalf("an empty list changed the digest: %s vs %s (%v, %v)", a, b, errA, errB)
	}
}

func TestConsensusParamsUpdateHashBindsSignedClaims(t *testing.T) {
	hash := func(u arbiter.ConsensusParamsUpdate) string {
		t.Helper()
		h, err := ConsensusParamsUpdateHash(u)
		if err != nil {
			t.Fatal(err)
		}
		return h
	}
	full := hash(signedClaimsTestUpdate())
	rotated := signedClaimsTestUpdate()
	rotated.SIIndexers[0].EnrollmentJWS = "other.p.s"
	fewer := signedClaimsTestUpdate()
	fewer.Verifiers = fewer.Verifiers[:2]
	if hash(testConsensusUpdate()) == full || hash(rotated) == full || hash(fewer) == full {
		t.Fatal("si_indexers and verifiers must be bound by the update hash")
	}
	shuffled := signedClaimsTestUpdate()
	shuffled.SIIndexers[0], shuffled.SIIndexers[1] = shuffled.SIIndexers[1], shuffled.SIIndexers[0]
	if hash(shuffled) != full {
		t.Fatal("entry order must not change the digest: normalisation sorts")
	}
}
