package dataplane

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeSafeState struct {
	pb.UnimplementedSafeStateServer

	mu       sync.Mutex
	code     codes.Code
	hint     string
	calls    int
	success  int
	snapshot string
}

func (s *fakeSafeState) GetSafeWatermark(context.Context, *pb.GetSafeWatermarkRequest) (*pb.SafeWatermark, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	switch s.code {
	case codes.OK:
		s.success++
		return &pb.SafeWatermark{SnapshotId: s.snapshot, SafeBlockSeq: uint64(s.success), ManifestRoot: "0xmanifest"}, nil
	case codes.FailedPrecondition:
		st := status.New(codes.FailedPrecondition, "not leader")
		withDetails, err := st.WithDetails(&pb.NotLeader{LeaderAddr: s.hint})
		if err != nil {
			return nil, st.Err()
		}
		return nil, withDetails.Err()
	default:
		return nil, status.Error(s.code, s.code.String())
	}
}

func (s *fakeSafeState) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *fakeSafeState) successCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.success
}

func startSafeStatePeer(t *testing.T, fake *fakeSafeState) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterSafeStateServer(srv, fake)
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

func deadAddr(t *testing.T) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen dead addr: %v", err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatalf("close dead addr: %v", err)
	}
	return addr
}

func callSafeWatermark(ctx context.Context, conn *grpc.ClientConn) error {
	_, err := pb.NewSafeStateClient(conn).GetSafeWatermark(ctx, &pb.GetSafeWatermarkRequest{})
	return err
}

func TestWithLeaderRetry_FollowsNotLeaderHint(t *testing.T) {
	// Given
	n1 := &fakeSafeState{code: codes.FailedPrecondition, hint: "n3"}
	n2 := &fakeSafeState{code: codes.FailedPrecondition, hint: "n3"}
	n3 := &fakeSafeState{code: codes.OK, snapshot: "safe"}
	cl, err := New(Config{Peers: []Peer{
		{ID: "n1", GRPCAddr: startSafeStatePeer(t, n1)},
		{ID: "n2", GRPCAddr: startSafeStatePeer(t, n2)},
		{ID: "n3", GRPCAddr: startSafeStatePeer(t, n3)},
	}})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// When
	err = cl.WithLeaderRetry(ctx, callSafeWatermark)

	// Then
	if err != nil {
		t.Fatalf("WithLeaderRetry: %v", err)
	}
	if got := n3.successCount(); got != 1 {
		t.Fatalf("leader successes: got %d want 1", got)
	}
	if got := n1.callCount() + n2.callCount(); got > 1 {
		t.Fatalf("hint must short-circuit non-leader rotation, non-leader calls=%d", got)
	}
}

func TestWithLeaderRetry_RotatesOnUnavailableAndStopsOnCtx(t *testing.T) {
	// Given
	n2 := &fakeSafeState{code: codes.FailedPrecondition}
	n3 := &fakeSafeState{code: codes.OK, snapshot: "safe"}
	cl, err := New(Config{
		Peers: []Peer{
			{ID: "n1", GRPCAddr: deadAddr(t)},
			{ID: "n2", GRPCAddr: startSafeStatePeer(t, n2)},
			{ID: "n3", GRPCAddr: startSafeStatePeer(t, n3)},
		},
		RetryBackoffMin: time.Millisecond,
		RetryBackoffMax: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// When
	err = cl.WithLeaderRetry(ctx, callSafeWatermark)

	// Then
	if err != nil {
		t.Fatalf("WithLeaderRetry should rotate to leader: %v", err)
	}
	if got := n3.successCount(); got != 1 {
		t.Fatalf("leader successes: got %d want 1", got)
	}

	// Given
	dead, err := New(Config{
		Peers: []Peer{
			{ID: "d1", GRPCAddr: deadAddr(t)},
			{ID: "d2", GRPCAddr: deadAddr(t)},
			{ID: "d3", GRPCAddr: deadAddr(t)},
		},
		RetryBackoffMin: time.Millisecond,
		RetryBackoffMax: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new dead client: %v", err)
	}
	t.Cleanup(dead.Close)
	deadCtx, deadCancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer deadCancel()

	// When
	err = dead.WithLeaderRetry(deadCtx, callSafeWatermark)

	// Then
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("all-dead peers must stop on context deadline, got %v", err)
	}
}

func TestWithLeaderRetry_NonRetryableStatusPropagates(t *testing.T) {
	// Given
	bad := &fakeSafeState{code: codes.InvalidArgument}
	leader := &fakeSafeState{code: codes.OK, snapshot: "safe"}
	cl, err := New(Config{Peers: []Peer{
		{ID: "bad", GRPCAddr: startSafeStatePeer(t, bad)},
		{ID: "leader", GRPCAddr: startSafeStatePeer(t, leader)},
	}})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// When
	err = cl.WithLeaderRetry(ctx, callSafeWatermark)

	// Then
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("expected InvalidArgument, got %v", err)
	}
	if got := leader.callCount(); got != 0 {
		t.Fatalf("non-retryable error must not rotate to next peer, leader calls=%d", got)
	}
}

func TestNew_ValidatesPeers(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("empty peers must fail")
	}
	if _, err := New(Config{Peers: []Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}, {ID: "n1", GRPCAddr: "127.0.0.1:2"}}}); err == nil {
		t.Fatal("duplicate peer id must fail")
	}
	if _, err := New(Config{Peers: []Peer{{ID: "", GRPCAddr: "127.0.0.1:1"}}}); err == nil {
		t.Fatal("missing peer id must fail")
	}
	if _, err := New(Config{Peers: []Peer{{ID: "n1"}}}); err == nil {
		t.Fatal("missing peer address must fail")
	}
}
