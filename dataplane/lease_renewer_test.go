package dataplane

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// fakeMembership answers RenewNodeLease with renewErr (nil = Ack) and records
// every renewal and MarkActive it receives.
type fakeMembership struct {
	pb.UnimplementedMembershipServer
	mu       sync.Mutex
	renewErr error
	renewals []*pb.NodeLeaseRenewal
	marks    []*pb.NodeRef
}

func (f *fakeMembership) RenewNodeLease(_ context.Context, req *pb.NodeLeaseRenewal) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewals = append(f.renewals, req)
	if f.renewErr != nil {
		return nil, f.renewErr
	}
	return &pb.Ack{}, nil
}

func (f *fakeMembership) MarkActive(_ context.Context, req *pb.NodeRef) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, req)
	return &pb.Ack{}, nil
}

func (f *fakeMembership) setRenewErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.renewErr = err
}

func (f *fakeMembership) snapshot() ([]*pb.NodeLeaseRenewal, []*pb.NodeRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]*pb.NodeLeaseRenewal(nil), f.renewals...), append([]*pb.NodeRef(nil), f.marks...)
}

func startMembershipPeer(t *testing.T, f *fakeMembership, register func(*grpc.Server)) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	register(srv)
	done := make(chan struct{})
	go func() { _ = srv.Serve(ln); close(done) }()
	t.Cleanup(func() { srv.Stop(); <-done })
	return ln.Addr().String()
}

func fakeSign(body authority.LeaseRenewalBody) (wire.RenewNodeLease, error) {
	return wire.RenewNodeLease{NodeID: body.NodeID, RegistrationSeq: body.RegistrationSeq, LeaseSeq: body.LeaseSeq, SignerJWS: "jws"}, nil
}

func newLeaseFixture(t *testing.T) (*fakeMembership, *LeaseRenewer) {
	t.Helper()
	f := &fakeMembership{}
	addr := startMembershipPeer(t, f, func(s *grpc.Server) { pb.RegisterMembershipServer(s, f) })
	return f, NewLeaseRenewer(newTestClient(t, Peer{ID: "n1", GRPCAddr: addr}), "snode-1", time.Hour, fakeSign, nil)
}

// TestLeaseRenewerSendsNothingUntilActivated: a role that never registered
// here, or deactivated, renews nothing.
func TestLeaseRenewerSendsNothingUntilActivated(t *testing.T) {
	f, l := newLeaseFixture(t)
	if got := l.RenewOnce(context.Background()); got != LeaseIdle {
		t.Fatalf("outcome = %v, want idle", got)
	}
	l.Activate(7, &pb.NodeRef{NodeId: "snode-1", RegistrationSeq: 7})
	l.Deactivate()
	if got := l.RenewOnce(context.Background()); got != LeaseIdle {
		t.Fatalf("outcome after Deactivate = %v, want idle", got)
	}
	if renewals, _ := f.snapshot(); len(renewals) != 0 {
		t.Fatalf("renewals = %v, want none", renewals)
	}
}

// TestLeaseRenewerRaisesLeaseSeqPerRegistration: every renewal carries the
// registration's seq and a lease_seq that strictly increases within it and
// restarts with the next registration.
func TestLeaseRenewerRaisesLeaseSeqPerRegistration(t *testing.T) {
	f, l := newLeaseFixture(t)
	ctx := context.Background()
	l.Activate(7, &pb.NodeRef{NodeId: "snode-1", RegistrationSeq: 7})
	for range 3 {
		if got := l.RenewOnce(ctx); got != LeaseRenewed {
			t.Fatalf("outcome = %v, want renewed", got)
		}
	}
	l.Activate(9, &pb.NodeRef{NodeId: "snode-1", RegistrationSeq: 9})
	if got := l.RenewOnce(ctx); got != LeaseRenewed {
		t.Fatalf("outcome = %v, want renewed", got)
	}
	renewals, _ := f.snapshot()
	want := []*pb.NodeLeaseRenewal{
		{NodeId: "snode-1", RegistrationSeq: 7, LeaseSeq: 1, SignerJws: "jws"},
		{NodeId: "snode-1", RegistrationSeq: 7, LeaseSeq: 2, SignerJws: "jws"},
		{NodeId: "snode-1", RegistrationSeq: 7, LeaseSeq: 3, SignerJws: "jws"},
		{NodeId: "snode-1", RegistrationSeq: 9, LeaseSeq: 1, SignerJws: "jws"},
	}
	if len(renewals) != len(want) {
		t.Fatalf("renewals = %v, want %v", renewals, want)
	}
	for i := range want {
		if !proto.Equal(renewals[i], want[i]) {
			t.Fatalf("renewal %d = %v, want %v", i, renewals[i], want[i])
		}
	}
}

// TestLeaseRenewerReactivatesAnExpiredNode: the arbiter's "not active"
// refusal makes a running, still-registered role resend its MarkActive of the
// same registration; nothing is resent after Deactivate.
func TestLeaseRenewerReactivatesAnExpiredNode(t *testing.T) {
	f, l := newLeaseFixture(t)
	ctx := context.Background()
	mark := &pb.NodeRef{NodeId: "snode-1", RegistrationSeq: 7, SignerJws: "mark"}
	l.Activate(7, mark)
	f.setRenewErr(status.Error(codes.InvalidArgument, arbiter.MembershipLeaseNotActiveReason))
	if got := l.RenewOnce(ctx); got != LeaseReactivated {
		t.Fatalf("outcome = %v, want reactivated", got)
	}
	_, marks := f.snapshot()
	if len(marks) != 1 || !proto.Equal(marks[0], mark) {
		t.Fatalf("MarkActive requests = %v, want exactly %v", marks, mark)
	}
	l.Deactivate()
	if got := l.RenewOnce(ctx); got != LeaseIdle {
		t.Fatalf("outcome after Deactivate = %v, want idle", got)
	}
	if _, marks := f.snapshot(); len(marks) != 1 {
		t.Fatalf("MarkActive after Deactivate: %v", marks)
	}
}

// TestLeaseRenewerToleratesALeaseThatIsOff: before the lease is committed,
// and against an arbiter that predates the RPC, a renewal is a quiet no-op.
func TestLeaseRenewerToleratesALeaseThatIsOff(t *testing.T) {
	ctx := context.Background()
	f, l := newLeaseFixture(t)
	l.Activate(7, &pb.NodeRef{NodeId: "snode-1", RegistrationSeq: 7})
	f.setRenewErr(status.Error(codes.InvalidArgument, arbiter.MembershipLeaseOffReason))
	if got := l.RenewOnce(ctx); got != LeaseOff {
		t.Fatalf("outcome = %v, want off", got)
	}

	old := &fakeMembership{}
	addr := startMembershipPeer(t, old, func(s *grpc.Server) { pb.RegisterMembershipServer(s, pb.UnimplementedMembershipServer{}) })
	l2 := NewLeaseRenewer(newTestClient(t, Peer{ID: "n1", GRPCAddr: addr}), "snode-1", time.Hour, fakeSign, nil)
	l2.Activate(7, &pb.NodeRef{NodeId: "snode-1", RegistrationSeq: 7})
	if got := l2.RenewOnce(ctx); got != LeaseOff {
		t.Fatalf("outcome against an old arbiter = %v, want off", got)
	}
	if _, marks := f.snapshot(); len(marks) != 0 {
		t.Fatalf("an off lease must not re-activate: %v", marks)
	}
}

// TestLeaseRenewerRunTicks: Run renews every interval until ctx ends.
func TestLeaseRenewerRunTicks(t *testing.T) {
	f := &fakeMembership{}
	addr := startMembershipPeer(t, f, func(s *grpc.Server) { pb.RegisterMembershipServer(s, f) })
	l := NewLeaseRenewer(newTestClient(t, Peer{ID: "n1", GRPCAddr: addr}), "snode-1", 10*time.Millisecond, fakeSign, nil)
	l.Activate(7, &pb.NodeRef{NodeId: "snode-1", RegistrationSeq: 7})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { l.Run(ctx); close(done) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if renewals, _ := f.snapshot(); len(renewals) >= 2 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Run did not renew twice")
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done
}

func TestNewLeaseRenewerDefaultInterval(t *testing.T) {
	if got := NewLeaseRenewer(nil, "n", 0, fakeSign, nil).Interval(); got != DefaultLeaseRenewInterval {
		t.Fatalf("interval = %v, want %v", got, DefaultLeaseRenewInterval)
	}
}
