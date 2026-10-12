package verifier

import (
	"context"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestVerifierRegistrationAdvertisesTrackManifests: a verifier always sends
// every capability of its build, track_manifests_v1 included (plan S2-A
// contract §2). The tracks gate may count it because its manifest store
// validates track-aware base manifests
// (dataplane TestManifestStore_AcceptsATrackAwareManifest).
func TestVerifierRegistrationAdvertisesTrackManifests(t *testing.T) {
	fake := &membershipFakeV{}
	now := fixtureMillisV
	role := newSigningVerifier(t, signingConfigV(t), startMembershipFakeV(t, fake), &now)
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, _ := fake.snapshot()
	if len(regs) != 1 || !slices.Equal(regs[0].GetFeatures(), arbiter.LocalNodeFeatures()) ||
		!slices.Contains(regs[0].GetFeatures(), arbiter.TrackManifestsFeature) {
		t.Fatalf("verifier registrations = %v, want features %v", regs, arbiter.LocalNodeFeatures())
	}
}
