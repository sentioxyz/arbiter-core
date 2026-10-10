package verifier

import (
	"crypto/ed25519"
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func TestVerifierRegistrationAdvertisesSignedClaims(t *testing.T) {
	role, _ := newRoleHarnessV(t, &fakeReplayCore{}, &fakeScanner{})
	pub := ed25519.NewKeyFromSeed(testSeedV()).Public().(ed25519.PublicKey)
	req, ref, err := role.registrationRequests()
	if err != nil {
		t.Fatal(err)
	}
	if req.GetNodeId() != "v1" || !slices.Equal(req.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}) ||
		!slices.Equal(req.GetEd25519Pubkey(), []byte(pub)) || ref.GetNodeId() != "v1" {
		t.Fatalf("registration = %v, activation = %v", req, ref)
	}
	if !slices.Contains(req.GetFeatures(), arbiter.ClientLanesFeature) || !slices.Contains(req.GetFeatures(), arbiter.SignedClaimsFeature) {
		t.Fatalf("features = %v, want %s and %s", req.GetFeatures(), arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature)
	}
}
