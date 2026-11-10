package verifier

import (
	"context"
	"crypto/ed25519"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
)

// TestLease_VerifierRenewsWhileActive (arbiter-core#66): the verifier
// advertises membership_lease_v1 and, once active, renews at its MarkActive's
// registration_seq with a rising lease_seq signed by its ed25519 key; it
// stops at Deactivate.
func TestLease_VerifierRenewsWhileActive(t *testing.T) {
	fake := &membershipFakeV{}
	now := fixtureMillisV
	role := newSigningVerifier(t, signingConfigV(t), startMembershipFakeV(t, fake), &now)
	ctx := context.Background()
	if got := role.lease.RenewOnce(ctx); got != dataplane.LeaseIdle {
		t.Fatalf("renewal before Register = %v, want idle", got)
	}
	if err := role.Register(ctx); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if got := role.lease.RenewOnce(ctx); got != dataplane.LeaseRenewed {
			t.Fatalf("renewal = %v, want renewed", got)
		}
	}
	regs, marks := fake.snapshot()
	if len(regs) != 1 || !slices.Contains(regs[0].GetFeatures(), arbiter.MembershipLeaseFeature) {
		t.Fatalf("registration %v must advertise %s", regs, arbiter.MembershipLeaseFeature)
	}
	pub := authoritytest.VerifierKey(1).Public().(ed25519.PublicKey)
	renewals := fake.leaseRenewals()
	if len(renewals) != 2 {
		t.Fatalf("renewals = %v", renewals)
	}
	for i, m := range renewals {
		body := authority.LeaseRenewalBody{NodeID: m.GetNodeId(), RegistrationSeq: m.GetRegistrationSeq(), LeaseSeq: m.GetLeaseSeq()}
		if body.RegistrationSeq != marks[0].GetRegistrationSeq() || body.LeaseSeq != uint64(i+1) || m.GetSignerJws() != "" {
			t.Fatalf("renewal %d = %v", i, m)
		}
		if err := authority.VerifyVerifierMessage(pub, authority.VerifierMessageLeaseRenewal, authoritytest.Context(), body,
			m.GetEd25519Signature()); err != nil {
			t.Fatalf("renewal %d: %v", i, err)
		}
	}
	if err := role.Deactivate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := role.lease.RenewOnce(ctx); got != dataplane.LeaseIdle {
		t.Fatalf("renewal after Deactivate = %v, want idle", got)
	}
}
