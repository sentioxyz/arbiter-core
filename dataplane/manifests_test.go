package dataplane

import (
	"context"
	"net"
	"reflect"
	"sync"
	"testing"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type manifestSafeState struct {
	pb.UnimplementedSafeStateServer

	mu        sync.Mutex
	manifests map[string]replay.SafeSnapshotManifest
	calls     int
}

func (s *manifestSafeState) GetManifest(_ context.Context, ref *pb.SnapshotRef) (*pb.SafeSnapshotManifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	manifest, ok := s.manifests[ref.GetSnapshotId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "manifest not found")
	}
	return wire.ManifestToPB(manifest), nil
}

func (s *manifestSafeState) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func sealedDataplaneManifest(t *testing.T) replay.SafeSnapshotManifest {
	t.Helper()

	manifest, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq:      7,
		SchemaSnapshotID:  "schema-genesis",
		SchemaRoot:        "0xschema-root",
		ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []replay.TableManifest{{
			TableID:    "db.t",
			SchemaHash: "0xtable-schema",
			PartitionRoots: []replay.PartitionCommitment{{
				TableID:     "db.t",
				PartitionID: "202607",
				Root:        "0xpartition-root",
			}},
		}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal manifest: %v", err)
	}
	return manifest
}

func serveGRPCForDataplaneTest(t *testing.T, srv *grpc.Server) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
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

func TestManifestStore_FetchesValidatesAndCaches(t *testing.T) {
	// Given
	manifest := sealedDataplaneManifest(t)
	state := &manifestSafeState{manifests: map[string]replay.SafeSnapshotManifest{manifest.SnapshotID: manifest}}
	srv := grpc.NewServer()
	pb.RegisterSafeStateServer(srv, state)
	addr := serveGRPCForDataplaneTest(t, srv)
	cl, err := New(Config{Peers: []Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	store := NewManifestStore(cl)

	// When
	got, err := store.GetSafeSnapshot(context.Background(), manifest.SnapshotID)

	// Then
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("manifest must validate: %v", err)
	}
	if !reflect.DeepEqual(got, manifest) {
		t.Fatalf("manifest mismatch:\n got=%+v\nwant=%+v", got, manifest)
	}
	if calls := state.callCount(); calls != 1 {
		t.Fatalf("server calls after first fetch: got %d want 1", calls)
	}

	// When
	got, err = store.GetSafeSnapshot(context.Background(), manifest.SnapshotID)

	// Then
	if err != nil || !reflect.DeepEqual(got, manifest) {
		t.Fatalf("cached fetch: got=%+v err=%v", got, err)
	}
	if calls := state.callCount(); calls != 1 {
		t.Fatalf("cached fetch must not hit server again, calls=%d", calls)
	}
}

func TestManifestStore_RejectsEmptyAndPropagatesUnknown(t *testing.T) {
	// Given
	state := &manifestSafeState{manifests: map[string]replay.SafeSnapshotManifest{}}
	srv := grpc.NewServer()
	pb.RegisterSafeStateServer(srv, state)
	addr := serveGRPCForDataplaneTest(t, srv)
	cl, err := New(Config{Peers: []Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	store := NewManifestStore(cl)

	// When
	_, err = store.GetSafeSnapshot(context.Background(), "")

	// Then
	if err == nil {
		t.Fatal("empty snapshot id must fail")
	}
	if calls := state.callCount(); calls != 0 {
		t.Fatalf("empty snapshot id must not call server, calls=%d", calls)
	}

	// When
	_, err = store.GetSafeSnapshot(context.Background(), "missing")

	// Then
	if status.Code(err) != codes.NotFound {
		t.Fatalf("unknown manifest must propagate NotFound, got %v", err)
	}
}
