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
	"google.golang.org/protobuf/types/known/emptypb"
)

// fakeRegistry serves TableRegistry and the purge RPC of PromotionGateway.
type fakeRegistry struct {
	pb.UnimplementedTableRegistryServer
	pb.UnimplementedPromotionGatewayServer

	mu        sync.Mutex
	getSnap   *pb.TableRegistrySnapshot
	getErr    error
	watchErr  error
	watchReqs []uint64
	sends     chan *pb.TableRegistrySnapshot
	end       chan error
	purgeErr  error
	purges    []*pb.RecordTablePurgedCmd
	nodeSet   []string
}

func newFakeRegistry() *fakeRegistry {
	return &fakeRegistry{sends: make(chan *pb.TableRegistrySnapshot, 16), end: make(chan error, 1)}
}

func (r *fakeRegistry) GetTableRegistry(context.Context, *emptypb.Empty) (*pb.TableRegistrySnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.getSnap, r.getErr
}

func (r *fakeRegistry) WatchTableRegistry(req *pb.WatchTableRegistryRequest, stream grpc.ServerStreamingServer[pb.TableRegistrySnapshot]) error {
	r.mu.Lock()
	r.watchReqs = append(r.watchReqs, req.GetSinceVersion())
	err := r.watchErr
	r.mu.Unlock()
	if err != nil {
		return err
	}
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case err := <-r.end:
			return err
		case m := <-r.sends:
			if err := stream.Send(m); err != nil {
				return err
			}
		}
	}
}

func (r *fakeRegistry) SubmitTablePurged(_ context.Context, cmd *pb.RecordTablePurgedCmd) (*pb.Ack, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.purges = append(r.purges, cmd)
	if r.purgeErr != nil {
		return nil, r.purgeErr
	}
	return &pb.Ack{}, nil
}

func (r *fakeRegistry) GetPurgeNodeSet(context.Context, *emptypb.Empty) (*pb.PurgeNodeSet, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return &pb.PurgeNodeSet{NodeIds: r.nodeSet}, nil
}

func (r *fakeRegistry) watchRequests() []uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint64(nil), r.watchReqs...)
}

func startRegistryPeer(t *testing.T, r *fakeRegistry) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterTableRegistryServer(srv, r)
	pb.RegisterPromotionGatewayServer(srv, r)
	done := make(chan struct{})
	go func() { _ = srv.Serve(ln); close(done) }()
	t.Cleanup(func() { srv.Stop(); <-done })
	return ln.Addr().String()
}

func registrySnapshotPB(version uint64) *pb.TableRegistrySnapshot {
	return &pb.TableRegistrySnapshot{Version: version, Seeded: true, Incarnations: []*pb.TableIncarnation{{
		Seq: 1, DatabaseId: "db", TableId: "g", SchemaHash: "0xg",
		Origin: pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_GENESIS,
		Status: pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE,
	}}}
}

func disabledErr() error {
	return status.Error(codes.FailedPrecondition, TableRegistryDisabledMessage)
}

func newTestClient(t *testing.T, peers ...Peer) *Client {
	t.Helper()
	c, err := New(Config{Peers: peers, RetryBackoffMin: 5 * time.Millisecond, RetryBackoffMax: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func runFollower(t *testing.T, f *RegistryFollower) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- f.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run returned %v, want context.Canceled", err)
		}
	})
}

func waitChanged(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func TestRegistryFollower_DisabledThenFirstSnapshot(t *testing.T) {
	r := newFakeRegistry()
	r.getErr = disabledErr()
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), nil)
	runFollower(t, f)

	waitChanged(t, f.Ready(), "ready")
	if _, enabled := f.View(); enabled {
		t.Fatal("a disabled registry must report enabled=false")
	}
	changed := f.Changed()
	r.sends <- registrySnapshotPB(1)
	waitChanged(t, changed, "first snapshot")
	snap, enabled := f.View()
	if !enabled || snap.Version != 1 || len(snap.Incarnations) != 1 {
		t.Fatalf("view = %+v enabled=%v", snap, enabled)
	}
	if got := r.watchRequests(); len(got) != 1 || got[0] != 0 {
		t.Fatalf("watch since_version = %v, want [0]", got)
	}
}

func TestRegistryFollower_ResumesFromLastVersionAfterDisconnect(t *testing.T) {
	r := newFakeRegistry()
	r.getSnap = registrySnapshotPB(3)
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), nil)
	runFollower(t, f)
	waitChanged(t, f.Ready(), "ready")
	if snap, enabled := f.View(); !enabled || snap.Version != 3 {
		t.Fatalf("view after Get = %+v enabled=%v", snap, enabled)
	}
	changed := f.Changed()
	r.sends <- registrySnapshotPB(4)
	waitChanged(t, changed, "version 4")
	r.end <- status.Error(codes.Unavailable, "stream reset")
	changed = f.Changed()
	r.sends <- registrySnapshotPB(6)
	waitChanged(t, changed, "version 6 after reconnect")
	if got := r.watchRequests(); len(got) != 2 || got[0] != 3 || got[1] != 4 {
		t.Fatalf("watch since_version = %v, want [3 4]", got)
	}
	if !f.Connected() {
		t.Fatal("an open stream must report connected")
	}
}

func TestRegistryFollower_FollowsNotLeader(t *testing.T) {
	follower := newFakeRegistry()
	follower.getErr = streamNotLeaderErr(t, "n2")
	follower.watchErr = streamNotLeaderErr(t, "n2")
	leader := newFakeRegistry()
	leader.getSnap = registrySnapshotPB(2)
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, follower)}, Peer{ID: "n2", GRPCAddr: startRegistryPeer(t, leader)})
	f := NewRegistryFollower(c, nil)
	runFollower(t, f)
	waitChanged(t, f.Ready(), "ready")
	if snap, enabled := f.View(); !enabled || snap.Version != 2 {
		t.Fatalf("view = %+v enabled=%v", snap, enabled)
	}
	changed := f.Changed()
	leader.sends <- registrySnapshotPB(3)
	waitChanged(t, changed, "version 3 from the leader")
	if got := leader.watchRequests(); len(got) == 0 || got[0] != 2 {
		t.Fatalf("leader watch since_version = %v, want [2 ...]", got)
	}
}

func TestRegistryFollower_IgnoresStaleVersions(t *testing.T) {
	r := newFakeRegistry()
	r.getSnap = registrySnapshotPB(5)
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), nil)
	runFollower(t, f)
	waitChanged(t, f.Ready(), "ready")
	changed := f.Changed()
	r.sends <- registrySnapshotPB(4)
	r.sends <- registrySnapshotPB(5)
	bad := registrySnapshotPB(6)
	bad.Incarnations[0].Status = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_UNSPECIFIED
	r.sends <- bad
	r.sends <- registrySnapshotPB(7)
	waitChanged(t, changed, "version 7")
	if snap, _ := f.View(); snap.Version != 7 {
		t.Fatalf("version = %d, want 7 (4 and 5 are stale, 6 does not decode)", snap.Version)
	}
}

func TestSubmitTablePurged_PreconditionIsNotSuccessAndNotALeaderMiss(t *testing.T) {
	r := newFakeRegistry()
	r.purgeErr = status.Error(codes.FailedPrecondition, "record table purged: incarnation 4 is not purging")
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := c.SubmitTablePurged(ctx, "s1", 4)
	if !errors.Is(err, ErrTableNotPurging) {
		t.Fatalf("err = %v, want ErrTableNotPurging", err)
	}
	r.mu.Lock()
	calls := len(r.purges)
	r.mu.Unlock()
	if calls != 1 {
		t.Fatalf("calls = %d, want exactly 1 (no leader-miss retry loop)", calls)
	}
	r.mu.Lock()
	r.purgeErr = nil
	r.nodeSet = []string{"s1", "v1"}
	r.mu.Unlock()
	if err := c.SubmitTablePurged(ctx, "s1", 4); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if got, err := c.PurgeNodeSet(ctx); err != nil || len(got) != 2 || got[1] != "v1" {
		t.Fatalf("PurgeNodeSet = %v, %v", got, err)
	}
}

func TestSubmitTablePurged_FollowsNotLeader(t *testing.T) {
	follower := newFakeRegistry()
	follower.purgeErr = streamNotLeaderErr(t, "n2")
	leader := newFakeRegistry()
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, follower)}, Peer{ID: "n2", GRPCAddr: startRegistryPeer(t, leader)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.SubmitTablePurged(ctx, "v1", 9); err != nil {
		t.Fatal(err)
	}
	leader.mu.Lock()
	defer leader.mu.Unlock()
	if len(leader.purges) != 1 || leader.purges[0].GetNodeId() != "v1" || leader.purges[0].GetIncarnationSeq() != 9 {
		t.Fatalf("leader purges = %v", leader.purges)
	}
}

func TestSubmitTablePurged_UnimplementedIsReturned(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterPromotionGatewayServer(srv, &pb.UnimplementedPromotionGatewayServer{})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: ln.Addr().String()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.SubmitTablePurged(ctx, "s1", 1); status.Code(err) != codes.Unimplemented {
		t.Fatalf("err = %v, want Unimplemented", err)
	}
}

func TestWaitReady_TimesOutWithoutAnArbiter(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close() // nothing listens: every Get fails with Unavailable
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: addr}), nil)
	runFollower(t, f)
	err = WaitReady(context.Background(), f, 200*time.Millisecond)
	if !errors.Is(err, ErrRegistryNotReady) {
		t.Fatalf("err = %v, want ErrRegistryNotReady", err)
	}
}

func TestRegistryFollower_ArbiterWithoutTheServiceCountsAsDisabled(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterPromotionGatewayServer(srv, &pb.UnimplementedPromotionGatewayServer{})
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: ln.Addr().String()}), nil)
	runFollower(t, f)
	if err := WaitReady(context.Background(), f, 5*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, enabled := f.View(); enabled {
		t.Fatal("an arbiter without TableRegistry must read as a disabled registry")
	}
}
