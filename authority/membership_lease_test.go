package authority

import (
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestNormalizeConsensusParamsUpdateMembershipLease (arbiter-core#66): the
// lease needs table_registry in the same update and at least
// arbiter.MinMembershipLeaseL2Blocks, survives normalization, is a signed
// field, and leaves the canonical form (and so every earlier hash) unchanged
// while zero.
func TestNormalizeConsensusParamsUpdateMembershipLease(t *testing.T) {
	u := testConsensusUpdate()
	u.TableRegistry = validRegistryParams()
	u.MembershipLeaseL2Blocks = arbiter.MinMembershipLeaseL2Blocks
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || got.MembershipLeaseL2Blocks != arbiter.MinMembershipLeaseL2Blocks {
		t.Fatalf("lease at the floor: %v, lease %d", err, got.MembershipLeaseL2Blocks)
	}
	for _, short := range []uint64{1, arbiter.MinMembershipLeaseL2Blocks - 1} {
		u.MembershipLeaseL2Blocks = short
		if _, err := NormalizeConsensusParamsUpdate(u); err == nil || !strings.Contains(err.Error(), "below the floor") {
			t.Fatalf("lease %d: err = %v", short, err)
		}
	}
	u = testConsensusUpdate()
	u.MembershipLeaseL2Blocks = 600
	const want = "consensus params update: membership_lease_l2_blocks requires table_registry in the same update"
	if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != want {
		t.Fatalf("lease without the registry: err = %v, want %q", err, want)
	}
	u.MembershipLeaseL2Blocks = 0
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.MembershipLeaseL2Blocks != 0 {
		t.Fatalf("unset: %v, lease %d", err, got.MembershipLeaseL2Blocks)
	}

	s, v := newTestPair(t)
	base := testConsensusUpdate()
	base.TableRegistry = validRegistryParams()
	token, err := s.SignConsensusParamsUpdateAt(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	leased := base
	leased.MembershipLeaseL2Blocks = 600
	if _, err := v.VerifyConsensusParamsUpdate(leased, token); err == nil {
		t.Fatal("the lease is not covered by the authority signature")
	}
	h0, err := ConsensusParamsUpdateHash(base)
	if err != nil {
		t.Fatal(err)
	}
	h1, err := ConsensusParamsUpdateHash(leased)
	if err != nil || h0 == h1 {
		t.Fatalf("leased update hash = %s (%v), unset %s", h1, err, h0)
	}
}

// TestLeaseRenewalMessages: both roles sign the lease_renewal kind over
// {node_id, registration_seq, lease_seq}; every field is bound, and the body
// is not interchangeable with another kind's.
func TestLeaseRenewalMessages(t *testing.T) {
	ctx := MessageContext{NetworkID: "net", GenesisSnapshotID: "0xgenesis"}
	body := LeaseRenewalBody{NodeID: "n", RegistrationSeq: 7, LeaseSeq: 3}
	priv := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	sig, err := SignVerifierMessage(priv, VerifierMessageLeaseRenewal, ctx, body)
	if err != nil {
		t.Fatal(err)
	}
	signer, _ := newTestPair(t)
	jws, err := signer.SignSNodeMessageAt(SNodeMessageLeaseRenewal, ctx, body, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySNodeMessage(SNodeMessageLeaseRenewal, ctx, body, jws, signer.Address()); err != nil {
		t.Fatalf("snode renewal: %v", err)
	}
	for name, mutated := range map[string]LeaseRenewalBody{
		"node":         {NodeID: "m", RegistrationSeq: 7, LeaseSeq: 3},
		"registration": {NodeID: "n", RegistrationSeq: 8, LeaseSeq: 3},
		"lease seq":    {NodeID: "n", RegistrationSeq: 7, LeaseSeq: 4},
	} {
		if err := VerifySNodeMessage(SNodeMessageLeaseRenewal, ctx, mutated, jws, signer.Address()); err == nil {
			t.Fatalf("snode renewal accepted a changed %s", name)
		}
		if err := VerifyVerifierMessage(priv.Public().(ed25519.PublicKey), VerifierMessageLeaseRenewal, ctx, mutated, sig); err == nil {
			t.Fatalf("verifier renewal accepted a changed %s", name)
		}
	}
	if _, err := SNodeMessageHash(SNodeMessageLeaseRenewal, ctx, MarkActiveBody{NodeID: "n", RegistrationSeq: 7}); err == nil {
		t.Fatal("lease_renewal took a MarkActive body")
	}
	h1, _ := SNodeMessageHash(SNodeMessageLeaseRenewal, ctx, body)
	h2, _ := SNodeMessageHash(SNodeMessageMarkActive, ctx, MarkActiveBody{NodeID: "n", RegistrationSeq: 7})
	if h1 == h2 {
		t.Fatal("lease_renewal and mark_active hash alike")
	}
}
