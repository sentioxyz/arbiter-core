package dataplane

import (
	"context"
	"errors"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeGateway struct {
	pb.UnimplementedVerifierGatewayServer
	pb.UnimplementedPromotionGatewayServer

	mu                 sync.Mutex
	verifierCalls      int
	promotionCalls     int
	verifierDispatches []*pb.VerifierDispatch
	promotions         []*pb.PromotionCommand
	err                error
	block              bool
}

func (g *fakeGateway) SubscribeVerifierDispatch(_ *pb.VerifierHello, stream grpc.ServerStreamingServer[pb.VerifierDispatch]) error {
	g.mu.Lock()
	g.verifierCalls++
	msgs := append([]*pb.VerifierDispatch(nil), g.verifierDispatches...)
	err := g.err
	block := g.block
	g.mu.Unlock()

	for _, msg := range msgs {
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	if block {
		<-stream.Context().Done()
		return stream.Context().Err()
	}
	return err
}

func (g *fakeGateway) SubscribePromotions(_ *pb.SNodeHello, stream grpc.ServerStreamingServer[pb.PromotionCommand]) error {
	g.mu.Lock()
	g.promotionCalls++
	msgs := append([]*pb.PromotionCommand(nil), g.promotions...)
	err := g.err
	g.mu.Unlock()

	for _, msg := range msgs {
		if err := stream.Send(msg); err != nil {
			return err
		}
	}
	return err
}

func (g *fakeGateway) verifierCallCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.verifierCalls
}

func startGatewayPeer(t *testing.T, fake *fakeGateway) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterVerifierGatewayServer(srv, fake)
	pb.RegisterPromotionGatewayServer(srv, fake)
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ln)
		close(done)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return ln.Addr().String()
}

func streamNotLeaderErr(t *testing.T, hint string) error {
	t.Helper()

	st := status.New(codes.FailedPrecondition, "not leader")
	withDetails, err := st.WithDetails(&pb.NotLeader{LeaderAddr: hint})
	if err != nil {
		t.Fatalf("not leader detail: %v", err)
	}
	return withDetails.Err()
}

func verifierDispatch(blockSeq uint64) *pb.VerifierDispatch {
	return &pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: &pb.ReplayJob{BlockSeq: blockSeq}}}
}

func promotionCommand(seq uint64) *pb.PromotionCommand {
	return &pb.PromotionCommand{Cmd: &pb.PromotionCommand_Promote{Promote: &pb.PromoteSafePartition{PromotionSeq: seq}}}
}

func TestRunVerifierSubscription_RehomesAndKeepsDelivering(t *testing.T) {
	// Given
	n1 := &fakeGateway{
		verifierDispatches: []*pb.VerifierDispatch{verifierDispatch(1), verifierDispatch(2)},
		err:                streamNotLeaderErr(t, "n2"),
	}
	n2 := &fakeGateway{
		verifierDispatches: []*pb.VerifierDispatch{verifierDispatch(3), verifierDispatch(4)},
		block:              true,
	}
	cl, err := New(Config{
		Peers: []Peer{
			{ID: "n1", GRPCAddr: startGatewayPeer(t, n1)},
			{ID: "n2", GRPCAddr: startGatewayPeer(t, n2)},
		},
		RetryBackoffMin: time.Millisecond,
		RetryBackoffMax: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var received []uint64

	// When
	err = cl.RunVerifierSubscription(ctx, "replica-1", func(msg *pb.VerifierDispatch) error {
		received = append(received, msg.GetReplayJob().GetBlockSeq())
		if len(received) == 1 {
			return errors.New("callback failure must not stop the loop")
		}
		if len(received) == 4 {
			cancel()
		}
		return nil
	})

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("subscription must end with context cancellation, got %v", err)
	}
	if want := []uint64{1, 2, 3, 4}; !reflect.DeepEqual(received, want) {
		t.Fatalf("received dispatches: got %v want %v", received, want)
	}
	if got := n1.verifierCallCount(); got != 1 {
		t.Fatalf("n1 subscribe calls: got %d want 1", got)
	}
	if got := n2.verifierCallCount(); got != 1 {
		t.Fatalf("n2 subscribe calls: got %d want 1", got)
	}
}

func TestRunVerifierSubscription_ContextCancelStopsIdleLoop(t *testing.T) {
	// Given
	gateway := &fakeGateway{block: true}
	cl, err := New(Config{Peers: []Peer{{ID: "n1", GRPCAddr: startGatewayPeer(t, gateway)}}})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()

	// When
	err = cl.RunVerifierSubscription(ctx, "replica-1", func(*pb.VerifierDispatch) error {
		t.Fatal("no dispatch should arrive")
		return nil
	})

	// Then
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("subscription must stop on context deadline, got %v", err)
	}
}

func TestRunPromotionSubscription_DeliversCommands(t *testing.T) {
	// Given
	gateway := &fakeGateway{promotions: []*pb.PromotionCommand{promotionCommand(7)}}
	cl, err := New(Config{Peers: []Peer{{ID: "n1", GRPCAddr: startGatewayPeer(t, gateway)}}})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var got uint64

	// When
	err = cl.RunPromotionSubscription(ctx, "snode-1", func(cmd *pb.PromotionCommand) error {
		got = cmd.GetPromote().GetPromotionSeq()
		cancel()
		return nil
	})

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("subscription must end with context cancellation, got %v", err)
	}
	if got != 7 {
		t.Fatalf("promotion seq: got %d want 7", got)
	}
}
