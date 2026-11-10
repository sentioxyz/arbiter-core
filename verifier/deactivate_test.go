package verifier

import (
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/wire"
)

// TestDeactivate_VerifierSendsAFreshRegistrationAndNoActivation: the
// verifier withdraws like the SNode (arbiter-core#15), signed with its
// evidence key; repeated calls stay safe and a later Register re-activates.
func TestDeactivate_VerifierSendsAFreshRegistrationAndNoActivation(t *testing.T) {
	fake := &membershipFakeV{}
	now := fixtureMillisV
	role := newSigningVerifier(t, signingConfigV(t), startMembershipFakeV(t, fake), &now)
	if err := role.Deactivate(context.Background()); err != nil {
		t.Fatal(err)
	}
	if regs, marks := fake.snapshot(); len(regs) != 0 || len(marks) != 0 {
		t.Fatalf("a verifier that never registered sent %d registrations, %d activations", len(regs), len(marks))
	}
	for _, step := range []func(context.Context) error{role.Register, role.Deactivate, role.Deactivate, role.Register} {
		if err := step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	regs, marks := fake.snapshot()
	if len(regs) != 4 || len(marks) != 2 || marks[1].GetRegistrationSeq() != regs[3].GetRegistrationSeq() {
		t.Fatalf("registrations %v, activations %v", regs, marks)
	}
	pub := authoritytest.VerifierKey(1).Public().(ed25519.PublicKey)
	for i := 1; i < len(regs); i++ {
		if regs[i].GetRegistrationSeq() <= regs[i-1].GetRegistrationSeq() {
			t.Fatalf("registration %d seq does not move forward: %v", i, regs)
		}
		if err := authority.VerifyVerifierMessage(pub, authority.VerifierMessageRegistration, authoritytest.Context(),
			wire.RegistrationFromPB(regs[i]), regs[i].GetEd25519Signature()); err != nil {
			t.Fatalf("registration %d: %v", i, err)
		}
	}
}
