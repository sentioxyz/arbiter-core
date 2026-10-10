package snode

import (
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

// An SNode without a claim signer sends the legacy requests; it advertises
// client_lanes_v1 but never signed_claims_v1, so the activation gate (spec
// §6.7) cannot count an SNode that cannot sign.
func TestSNodeRegistrationWithoutAClaimSigner(t *testing.T) {
	req, ref, err := (&Role{cfg: Config{NodeID: "s1"}}).registrationRequests()
	if err != nil {
		t.Fatal(err)
	}
	if req.GetNodeId() != "s1" || !slices.Equal(req.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}) || ref.GetNodeId() != "s1" {
		t.Fatalf("registration = %v, activation = %v", req, ref)
	}
	if !slices.Contains(req.GetFeatures(), arbiter.ClientLanesFeature) || slices.Contains(req.GetFeatures(), arbiter.SignedClaimsFeature) {
		t.Fatalf("features = %v", req.GetFeatures())
	}
	if req.GetRegistrationSeq() != 0 || req.GetSignerJws() != "" || ref.GetRegistrationSeq() != 0 || ref.GetSignerJws() != "" {
		t.Fatalf("legacy requests carry signed-claims fields: %v / %v", req, ref)
	}
}
