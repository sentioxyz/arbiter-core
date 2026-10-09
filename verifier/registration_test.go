package verifier

import (
	"crypto/ed25519"
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func TestVerifierRegistrationAdvertisesClientLanes(t *testing.T) {
	pub := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	req := registrationRequest("v1", pub)
	if req.GetNodeId() != "v1" || !slices.Equal(req.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}) || !slices.Equal(req.GetEd25519Pubkey(), []byte(pub)) {
		t.Fatalf("registration = %v", req)
	}
	if !slices.Contains(req.GetFeatures(), arbiter.ClientLanesFeature) {
		t.Fatalf("features = %v, want %s", req.GetFeatures(), arbiter.ClientLanesFeature)
	}
}
