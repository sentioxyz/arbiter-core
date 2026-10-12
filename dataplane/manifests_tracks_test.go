package dataplane

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// pbManifestSafeState serves fixed transport manifests, so a test decides the
// exact message a verifier's manifest store receives.
type pbManifestSafeState struct {
	pb.UnimplementedSafeStateServer
	manifests map[string]*pb.SafeSnapshotManifest
}

func (s *pbManifestSafeState) GetManifest(_ context.Context, ref *pb.SnapshotRef) (*pb.SafeSnapshotManifest, error) {
	m, ok := s.manifests[ref.GetSnapshotId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "manifest not found")
	}
	return proto.Clone(m).(*pb.SafeSnapshotManifest), nil
}

// trackAwareDataplaneManifest is sealedDataplaneManifest's content published
// after the tracks activation: one track_frontiers entry per enrolled
// indexer, indexer 0 included (housegate spec 2026-10-10 §7.3).
func trackAwareDataplaneManifest(t *testing.T) replay.SafeSnapshotManifest {
	t.Helper()
	m, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq:      7,
		SchemaSnapshotID:  "schema-genesis",
		SchemaRoot:        "0xschema-root",
		ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []replay.TableManifest{{
			TableID:        "db.t",
			SchemaHash:     "0xtable-schema",
			PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "202607", Root: "0xpartition-root"}},
		}},
		TrackFrontiers: []replay.TrackFrontier{
			{IndexerID: 0, SafeBlockSeq: 7, AppliedPromotionSeq: 3},
			{IndexerID: 1, SafeBlockSeq: 9, AppliedPromotionSeq: 5},
		},
	}).Seal()
	if err != nil {
		t.Fatalf("seal track-aware manifest: %v", err)
	}
	return m
}

func newManifestStoreServing(t *testing.T, manifests map[string]*pb.SafeSnapshotManifest) *ManifestStore {
	t.Helper()
	srv := grpc.NewServer()
	pb.RegisterSafeStateServer(srv, &pbManifestSafeState{manifests: manifests})
	addr := serveGRPCForDataplaneTest(t, srv)
	cl, err := New(Config{Peers: []Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(cl.Close)
	return NewManifestStore(cl)
}

// TestManifestStore_AcceptsATrackAwareManifest: a verifier resolves a base
// manifest published after the tracks activation; the frontiers survive the
// transport, and the manifest validates because its manifest_root covers
// them (it differs from the same content without frontiers).
func TestManifestStore_AcceptsATrackAwareManifest(t *testing.T) {
	manifest := trackAwareDataplaneManifest(t)
	if manifest.ManifestRoot == sealedDataplaneManifest(t).ManifestRoot {
		t.Fatal("track_frontiers must enter the manifest root of a track-aware manifest")
	}
	store := newManifestStoreServing(t, map[string]*pb.SafeSnapshotManifest{manifest.SnapshotID: wire.ManifestToPB(manifest)})
	got, err := store.GetSafeSnapshot(context.Background(), manifest.SnapshotID)
	if err != nil {
		t.Fatalf("get track-aware snapshot: %v", err)
	}
	if !reflect.DeepEqual(got, manifest) || len(got.TrackFrontiers) != 2 {
		t.Fatalf("manifest mismatch:\n got=%+v\nwant=%+v", got, manifest)
	}
	if err := got.Validate(); err != nil {
		t.Fatalf("track-aware manifest must validate: %v", err)
	}
}

// TestManifestStore_RefusesATrackAwareManifestStrippedOfItsFrontiers: what a
// verifier without track support sees. The same manifest with track_frontiers
// dropped, in transit or by a converter, fails validation because its
// manifest_root covers them; this is why every data-plane node must run this
// release before the tracks activation.
func TestManifestStore_RefusesATrackAwareManifestStrippedOfItsFrontiers(t *testing.T) {
	manifest := trackAwareDataplaneManifest(t)
	stripped := wire.ManifestToPB(manifest)
	stripped.TrackFrontiers = nil
	store := newManifestStoreServing(t, map[string]*pb.SafeSnapshotManifest{manifest.SnapshotID: stripped})
	_, err := store.GetSafeSnapshot(context.Background(), manifest.SnapshotID)
	if err == nil || !strings.Contains(err.Error(), "failed validation") || !strings.Contains(err.Error(), "manifest_root mismatch") {
		t.Fatalf("stripped manifest: err = %v, want a manifest_root validation failure", err)
	}
}
