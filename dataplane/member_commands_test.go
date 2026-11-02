package dataplane

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// memberGateway serves the two member RPCs of VerifierGateway and records
// every request. A peer with hint answers NotLeader naming it; one with err
// refuses with it; otherwise it acknowledges.
type memberGateway struct {
	pb.UnimplementedVerifierGatewayServer

	mu          sync.Mutex
	hint        string
	err         error
	ready       []*pb.MemberReadyCmd
	checkpoints []*pb.MemberCheckpointCmd
}

func (g *memberGateway) refusal() error {
	if g.hint != "" {
		st, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&pb.NotLeader{LeaderAddr: g.hint})
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		return st.Err()
	}
	return g.err
}

func (g *memberGateway) SubmitMemberReady(_ context.Context, cmd *pb.MemberReadyCmd) (*pb.Ack, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.ready = append(g.ready, cmd)
	if err := g.refusal(); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}

func (g *memberGateway) SubmitMemberCheckpoint(_ context.Context, cmd *pb.MemberCheckpointCmd) (*pb.Ack, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.checkpoints = append(g.checkpoints, cmd)
	if err := g.refusal(); err != nil {
		return nil, err
	}
	return &pb.Ack{}, nil
}

func (g *memberGateway) setErr(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.err = err
}

func (g *memberGateway) requests() ([]*pb.MemberReadyCmd, []*pb.MemberCheckpointCmd) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]*pb.MemberReadyCmd(nil), g.ready...), append([]*pb.MemberCheckpointCmd(nil), g.checkpoints...)
}

func startMemberGateway(t *testing.T, g *memberGateway) string {
	t.Helper()
	srv := grpc.NewServer()
	pb.RegisterVerifierGatewayServer(srv, g)
	return serveGRPCForDataplaneTest(t, srv)
}

func memberReadyFixture() wire.MemberReady {
	return wire.MemberReady{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex", Generation: 3, Ed25519Signature: "ab12"}
}

func memberCheckpointFixture() wire.MemberCheckpoint {
	return wire.MemberCheckpoint{
		Checkpoint: arbiter.MemberCheckpoint{
			NodeID: "member-b", OwnerIndexerID: 0, CheckpointSeq: 7, SnapshotID: "0xsnap",
			Groups: []arbiter.CheckpointGroup{{DatabaseID: "dex", GroupGeneration: 3,
				Excluded: []arbiter.ExcludedTable{{TableID: "dex.pools", Reason: arbiter.ExclusionNotInstalled}}}},
			Entry: replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 90, AppliedPromotionSeq: 40},
		},
		ReplicaJWS: "h.p.s",
	}
}

// switchOffErr is the leader's answer before a stage-3 switch (plan S3-E1
// K67): FAILED_PRECONDITION without a NotLeader detail.
func switchOffErr(text string) error { return status.Error(codes.FailedPrecondition, text) }

// TestSubmitMemberReadyAcksAndMapsTheSwitchRefusal: a follower's NotLeader is
// followed and the leader receives the request as given; an Ack is nil; the
// leader's refusal before the placement switch wraps ErrStage3SwitchOff with
// its text and is not retried; any other refusal comes back unchanged.
func TestSubmitMemberReadyAcksAndMapsTheSwitchRefusal(t *testing.T) {
	follower := &memberGateway{hint: "n2"}
	leader := &memberGateway{}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberGateway(t, follower)}, Peer{ID: "n2", GRPCAddr: startMemberGateway(t, leader)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := memberReadyFixture()

	if err := c.SubmitMemberReady(ctx, cmd); err != nil {
		t.Fatalf("SubmitMemberReady: %v", err)
	}
	followerReqs, _ := follower.requests()
	leaderReqs, _ := leader.requests()
	if len(followerReqs) != 1 || len(leaderReqs) != 1 || !proto.Equal(leaderReqs[0], wire.MemberReadyToRequest(cmd)) {
		t.Fatalf("requests: follower %v, leader %v; want one each, the leader's equal to %v", followerReqs, leaderReqs, wire.MemberReadyToRequest(cmd))
	}

	const text = "member readiness is refused until the placement switch is on"
	leader.setErr(switchOffErr(text))
	err := c.SubmitMemberReady(ctx, cmd)
	if !errors.Is(err, ErrStage3SwitchOff) || !strings.Contains(err.Error(), text) {
		t.Fatalf("SubmitMemberReady before the placement switch = %v, want ErrStage3SwitchOff carrying %q", err, text)
	}
	if leaderReqs, _ = leader.requests(); len(leaderReqs) != 2 {
		t.Fatalf("the switch refusal was asked %d times in all, want 2 (one per call, never retried)", len(leaderReqs))
	}

	leader.setErr(status.Error(codes.InvalidArgument, "member ready: signature does not verify"))
	err = c.SubmitMemberReady(ctx, cmd)
	if errors.Is(err, ErrStage3SwitchOff) || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SubmitMemberReady refused with InvalidArgument = %v, want the gRPC error unchanged", err)
	}
}

// TestSubmitMemberCheckpointAcksAndMapsTheSwitchRefusal: the same for the
// checkpoint, with every field of the signed checkpoint on the wire.
func TestSubmitMemberCheckpointAcksAndMapsTheSwitchRefusal(t *testing.T) {
	follower := &memberGateway{hint: "n2"}
	leader := &memberGateway{}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberGateway(t, follower)}, Peer{ID: "n2", GRPCAddr: startMemberGateway(t, leader)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := memberCheckpointFixture()

	if err := c.SubmitMemberCheckpoint(ctx, cmd); err != nil {
		t.Fatalf("SubmitMemberCheckpoint: %v", err)
	}
	_, followerReqs := follower.requests()
	_, leaderReqs := leader.requests()
	if len(followerReqs) != 1 || len(leaderReqs) != 1 || !proto.Equal(leaderReqs[0], wire.MemberCheckpointToRequest(cmd)) {
		t.Fatalf("requests: follower %v, leader %v; want one each, the leader's equal to %v", followerReqs, leaderReqs, wire.MemberCheckpointToRequest(cmd))
	}
	if got := leaderReqs[0].GetCheckpoint(); got.GetCheckpointSeq() != 7 || got.GetEntry().GetSafeBlockSeq() != 90 ||
		got.GetGroups()[0].GetExcluded()[0].GetReason() != pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_NOT_INSTALLED || leaderReqs[0].GetReplicaJws() != "h.p.s" {
		t.Fatalf("checkpoint on the wire = %v", leaderReqs[0])
	}

	const text = "member checkpoints are refused until the checkpoints switch is on"
	leader.setErr(switchOffErr(text))
	err := c.SubmitMemberCheckpoint(ctx, cmd)
	if !errors.Is(err, ErrStage3SwitchOff) || !strings.Contains(err.Error(), text) {
		t.Fatalf("SubmitMemberCheckpoint before the checkpoints switch = %v, want ErrStage3SwitchOff carrying %q", err, text)
	}
	if _, leaderReqs = leader.requests(); len(leaderReqs) != 2 {
		t.Fatalf("the switch refusal was asked %d times in all, want 2 (one per call, never retried)", len(leaderReqs))
	}

	leader.setErr(status.Error(codes.InvalidArgument, "member checkpoint: checkpoint_seq 7 is not above the stored 9"))
	err = c.SubmitMemberCheckpoint(ctx, cmd)
	if errors.Is(err, ErrStage3SwitchOff) || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SubmitMemberCheckpoint refused with InvalidArgument = %v, want the gRPC error unchanged", err)
	}
}
