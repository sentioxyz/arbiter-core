package snode

import (
	"context"
	"testing"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// TestDeactivate_SendsAFreshRegistrationAndNoActivation is the graceful
// withdrawal (arbiter-core#15): a signed RegisterNode above the active
// registration_seq and no MarkActive, which the FSM applies as Active →
// Syncing. A second call moves the registration forward again.
func TestDeactivate_SendsAFreshRegistrationAndNoActivation(t *testing.T) {
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := role.Deactivate(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	regs, marks := fake.registrations()
	if len(regs) != 3 || len(marks) != 1 {
		t.Fatalf("%d registrations, %d activations: Deactivate sends a registration and no activation", len(regs), len(marks))
	}
	for i := 1; i < len(regs); i++ {
		if regs[i].GetRegistrationSeq() <= regs[i-1].GetRegistrationSeq() {
			t.Fatalf("registration %d seq %d does not exceed %d", i, regs[i].GetRegistrationSeq(), regs[i-1].GetRegistrationSeq())
		}
		if err := authority.VerifySNodeMessage(authority.SNodeMessageRegistration, authoritytest.Context(), wire.RegistrationFromPB(regs[i]),
			regs[i].GetSignerJws(), authoritytest.IndexerAddr1); err != nil {
			t.Fatalf("deactivation %d is not signed with the indexer key: %v", i, err)
		}
	}
}

func TestDeactivate_WithoutARegistrationSendsNothing(t *testing.T) {
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if regs, marks := fake.registrations(); len(regs) != 0 || len(marks) != 0 {
		t.Fatalf("a role that never registered sent %d registrations, %d activations", len(regs), len(marks))
	}
}

// TestDeactivate_ThenRegisterReturnsThroughSyncing: after a withdrawal the
// next Register sends both requests at a seq above the withdrawal's.
func TestDeactivate_ThenRegisterReturnsThroughSyncing(t *testing.T) {
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	for _, step := range []func(context.Context) error{role.Register, role.Deactivate, role.Register} {
		if err := step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	regs, marks := fake.registrations()
	if len(regs) != 3 || len(marks) != 2 || marks[1].GetRegistrationSeq() != regs[2].GetRegistrationSeq() ||
		regs[2].GetRegistrationSeq() <= regs[1].GetRegistrationSeq() {
		t.Fatalf("registrations %v, activations %v", regs, marks)
	}
}

func TestDeactivate_WithoutAClaimSignerSendsTheLegacyRegistration(t *testing.T) {
	fake := &claimsFakeS{}
	role, err := New(testConfigS(t), Deps{Client: startClaimsFakeS(t, fake)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := role.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if len(regs) != 2 || len(marks) != 1 || regs[1].GetRegistrationSeq() != 0 || regs[1].GetSignerJws() != "" {
		t.Fatalf("legacy deactivation sent %v / %v", regs, marks)
	}
}
