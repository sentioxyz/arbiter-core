package dataplane

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// perPeerLimitedErr is what the arbiter's per-peer limits answer in enforce
// (arbiter transportsec.Limits.refuse).
func perPeerLimitedErr(class string) error {
	return status.Errorf(codes.ResourceExhausted, "per-peer %s limit reached; retry later", class)
}

// During an election two followers can each name the other as the leader.
// Following a hint must not bypass the backoff, or the client opens streams
// in a tight loop (devnet2-si-v3, 2026-11-26: ~8k opens/s per method).
func TestRunVerifierSubscription_HintPingPongBacksOff(t *testing.T) {
	// Given
	n1 := &fakeGateway{err: streamNotLeaderErr(t, "n2")}
	n2 := &fakeGateway{err: streamNotLeaderErr(t, "n1")}
	cl, err := New(Config{
		Peers: []Peer{
			{ID: "n1", GRPCAddr: startGatewayPeer(t, n1)},
			{ID: "n2", GRPCAddr: startGatewayPeer(t, n2)},
		},
		RetryBackoffMin: 20 * time.Millisecond,
		RetryBackoffMax: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()

	// When
	err = cl.RunVerifierSubscription(ctx, "replica-1", func(*pb.VerifierDispatch) error { return nil })

	// Then
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("subscription must stop on context deadline, got %v", err)
	}
	// At most two opens per backoff round (the open plus one followed hint),
	// with every round waiting at least RetryBackoffMin/2.
	calls := n1.verifierCallCount() + n2.verifierCallCount()
	if calls > 2*(400/10)+2 {
		t.Fatalf("subscribe opens during a hint ping-pong: got %d, want them paced by the backoff", calls)
	}
	if calls < 2 {
		t.Fatalf("subscribe opens: got %d, want the loop to keep retrying", calls)
	}
}

// A per-peer limit refusal is retried after a backoff, not returned: under
// enforce, returning it would stop the verifier's or the SNode's
// subscription for good.
func TestRunVerifierSubscription_RetriesPerPeerLimit(t *testing.T) {
	// Given
	n1 := &fakeGateway{
		verifierDispatches: []*pb.VerifierDispatch{verifierDispatch(7)},
		errFirst:           2,
		firstErr:           perPeerLimitedErr("stream"),
		block:              true,
	}
	cl, err := New(Config{
		Peers:           []Peer{{ID: "n1", GRPCAddr: startGatewayPeer(t, n1)}},
		RetryBackoffMin: time.Millisecond,
		RetryBackoffMax: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var got uint64

	// When
	err = cl.RunVerifierSubscription(ctx, "replica-1", func(msg *pb.VerifierDispatch) error {
		got = msg.GetReplayJob().GetBlockSeq()
		cancel()
		return nil
	})

	// Then
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("subscription must survive the per-peer limit and end on cancel, got %v", err)
	}
	if got != 7 {
		t.Fatalf("dispatch after the limit: got %d want 7", got)
	}
	if calls := n1.verifierCallCount(); calls != 3 {
		t.Fatalf("subscribe opens: got %d want 3 (two refused, one served)", calls)
	}
}

// The registry's own oversize ResourceExhausted (a client receive limit) is
// not a per-peer limit.
func TestPerPeerLimitedOnlyMatchesTheArbiterRefusal(t *testing.T) {
	if !perPeerLimited(perPeerLimitedErr("statement")) {
		t.Fatal("per-peer limit refusal not recognised")
	}
	for _, err := range []error{
		status.Error(codes.ResourceExhausted, "grpc: received message larger than max (5000000 vs. 4194304)"),
		status.Error(codes.ResourceExhausted, "track settle attempt identity exhausted"),
		status.Error(codes.Unavailable, "per-peer stream limit reached; retry later"),
		errors.New("per-peer stream limit reached; retry later"),
		nil,
	} {
		if perPeerLimited(err) {
			t.Fatalf("%v must not count as a per-peer limit", err)
		}
	}
}

// WithLeaderRetry paces a hint ping-pong in the same way.
func TestWithLeaderRetry_HintPingPongBacksOff(t *testing.T) {
	// Given
	cl, err := New(Config{
		Peers: []Peer{
			{ID: "n1", GRPCAddr: "127.0.0.1:1"},
			{ID: "n2", GRPCAddr: "127.0.0.1:2"},
		},
		RetryBackoffMin: 20 * time.Millisecond,
		RetryBackoffMax: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer cancel()
	var calls atomic.Int64

	// When
	err = cl.WithLeaderRetry(ctx, func(_ context.Context, conn *grpc.ClientConn) error {
		calls.Add(1)
		other := "n1"
		if conn.Target() == "127.0.0.1:1" {
			other = "n2"
		}
		st, _ := status.New(codes.FailedPrecondition, "not leader").WithDetails(&pb.NotLeader{LeaderAddr: other})
		return st.Err()
	})

	// Then
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("WithLeaderRetry must stop on context deadline, got %v", err)
	}
	if got := calls.Load(); got > 2*(400/10)+2 {
		t.Fatalf("calls during a hint ping-pong: got %d, want them paced by the backoff", got)
	}
}

// WithLeaderRetry retries a per-peer limit refusal after a backoff.
func TestWithLeaderRetry_RetriesPerPeerLimit(t *testing.T) {
	cl, err := New(Config{
		Peers:           []Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}},
		RetryBackoffMin: time.Millisecond,
		RetryBackoffMax: 2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	var calls int
	err = cl.WithLeaderRetry(context.Background(), func(context.Context, *grpc.ClientConn) error {
		calls++
		if calls < 3 {
			return perPeerLimitedErr("read")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("WithLeaderRetry: err %v after %d calls, want success on the third", err, calls)
	}
}

func TestJitteredBackoffStaysInHalfToFull(t *testing.T) {
	for i := 0; i < 1000; i++ {
		if d := jittered(100 * time.Millisecond); d < 50*time.Millisecond || d > 100*time.Millisecond {
			t.Fatalf("jittered(100ms) = %v, want [50ms, 100ms]", d)
		}
	}
	if d := jittered(0); d != 0 {
		t.Fatalf("jittered(0) = %v", d)
	}
}
