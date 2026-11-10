package snode

import (
	"context"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
)

// TestLease_SigningSNodeRenewsWhileActive (arbiter-core#66): a signing SNode
// advertises membership_lease_v1, renews nothing before Register, renews at
// its MarkActive's registration_seq with a rising lease_seq signed by the
// indexer key once active, and stops at Deactivate.
func TestLease_SigningSNodeRenewsWhileActive(t *testing.T) {
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
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
	regs, marks := fake.registrations()
	if len(regs) != 1 || !slices.Contains(regs[0].GetFeatures(), arbiter.MembershipLeaseFeature) {
		t.Fatalf("registration %v must advertise %s", regs, arbiter.MembershipLeaseFeature)
	}
	renewals := fake.leaseRenewals()
	if len(renewals) != 2 {
		t.Fatalf("renewals = %v", renewals)
	}
	for i, m := range renewals {
		body := authority.LeaseRenewalBody{NodeID: m.GetNodeId(), RegistrationSeq: m.GetRegistrationSeq(), LeaseSeq: m.GetLeaseSeq()}
		if body.RegistrationSeq != marks[0].GetRegistrationSeq() || body.LeaseSeq != uint64(i+1) || m.GetEd25519Signature() != "" {
			t.Fatalf("renewal %d = %v, want registration_seq %d lease_seq %d", i, m, marks[0].GetRegistrationSeq(), i+1)
		}
		if err := authority.VerifySNodeMessage(authority.SNodeMessageLeaseRenewal, authoritytest.Context(), body,
			m.GetSignerJws(), authoritytest.IndexerAddr1); err != nil {
			t.Fatalf("renewal %d is not signed with the indexer key: %v", i, err)
		}
	}
	if err := role.Deactivate(ctx); err != nil {
		t.Fatal(err)
	}
	if got := role.lease.RenewOnce(ctx); got != dataplane.LeaseIdle {
		t.Fatalf("renewal after Deactivate = %v, want idle", got)
	}
}

// TestLease_LegacySNodeNeitherAdvertisesNorRenews: without a claim signer the
// SNode cannot sign renewals, so it neither advertises the feature (the lease
// activation gate must not count it) nor runs a renewer.
func TestLease_LegacySNodeNeitherAdvertisesNorRenews(t *testing.T) {
	r := &Role{}
	if slices.Contains(r.nodeFeatures(), arbiter.MembershipLeaseFeature) {
		t.Fatalf("legacy SNode features %v advertise %s", r.nodeFeatures(), arbiter.MembershipLeaseFeature)
	}
	if r.lease != nil {
		t.Fatal("legacy SNode has a lease renewer")
	}
	stop := r.startLeaseRenewer(context.Background())
	stop()
}
