package snode

import (
	"slices"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

func TestSNodeRegistrationAdvertisesClientLanes(t *testing.T) {
	req := registrationRequest("s1")
	if req.GetNodeId() != "s1" || !slices.Equal(req.GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}) {
		t.Fatalf("registration = %v", req)
	}
	if !slices.Contains(req.GetFeatures(), arbiter.ClientLanesFeature) {
		t.Fatalf("features = %v, want %s", req.GetFeatures(), arbiter.ClientLanesFeature)
	}
}
