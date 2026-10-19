package objstore

import (
	"context"
	"encoding/hex"
	"net"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// elementHex is the LtHash accumulator of one element, as manifests carry it.
func elementHex(element string) string {
	return "0x" + hex.EncodeToString(lthash.ElementHash([]byte(element)).Bytes())
}

// sumHex is the LtHash sum of accumulators.
func sumHex(t *testing.T, hashes ...string) string {
	t.Helper()
	acc := lthash.New()
	for _, h := range hashes {
		raw, err := hex.DecodeString(strings.TrimPrefix(h, "0x"))
		if err != nil {
			t.Fatal(err)
		}
		v, err := lthash.FromBytes(raw)
		if err != nil {
			t.Fatal(err)
		}
		acc.AddHash(v)
	}
	return "0x" + hex.EncodeToString(acc.Bytes())
}

// testManifest is a sealed manifest of table db.t with parts a and b in
// partition p_x, and track frontiers for indexers 0 and 1.
func testManifest(t *testing.T, root string) replay.SafeSnapshotManifest {
	t.Helper()
	a, b := elementHex("a"), elementHex("b")
	if root == "" {
		root = sumHex(t, a, b)
	}
	m, err := (replay.SafeSnapshotManifest{
		SafeBlockSeq: 9, SchemaSnapshotID: "schema", SchemaRoot: "0xschema-root", ExecutorProfileID: "profile",
		Tables: []replay.TableManifest{{
			TableID: "db.t", SchemaHash: "0xtable-schema",
			PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p_x", Root: root}},
			ActiveParts: []replay.PartManifestEntry{
				{TableID: "db.t", PartitionID: "p_x", PartName: "x_1_1_0", PartPhysHash: "pa", PartRowLtHash: a, RowCount: 1},
				{TableID: "db.t", PartitionID: "p_x", PartName: "x_2_2_0", PartPhysHash: "pb", PartRowLtHash: b, RowCount: 1},
			},
		}},
		TrackFrontiers: []replay.TrackFrontier{{IndexerID: 0, SafeBlockSeq: 9, AppliedPromotionSeq: 3}, {IndexerID: 1, SafeBlockSeq: 8, AppliedPromotionSeq: 4}},
	}).Seal()
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	return m
}

func TestCheckManifestRefusesAWrongIDRootOrLedger(t *testing.T) {
	// Given
	good := testManifest(t, "")
	foreign := good
	foreign.SnapshotID = "0xsomething-else"
	tampered := good
	tampered.Tables = append([]replay.TableManifest(nil), good.Tables...)
	tampered.Tables[0].SchemaHash = "0xother"
	badLedger := testManifest(t, elementHex("not the sum"))

	// When, Then
	if err := CheckManifest(good.SnapshotID, good); err != nil {
		t.Fatalf("a good manifest: %v", err)
	}
	for _, c := range []struct {
		name      string
		requested string
		m         replay.SafeSnapshotManifest
		want      string
	}{
		{"answered for another id", "0xrequested", good, "objstore: manifest " + good.SnapshotID + " answered for 0xrequested"},
		{"not its own root", foreign.SnapshotID, foreign, "objstore: manifest 0xsomething-else is not its own root " + good.ManifestRoot},
		{"fails validation", tampered.SnapshotID, tampered, "objstore: manifest " + tampered.SnapshotID + ": manifest_root mismatch"},
		{"fails the ledger", badLedger.SnapshotID, badLedger, "objstore: manifest " + badLedger.SnapshotID + ": table db.t partition p_x: parts sum != root"},
	} {
		err := CheckManifest(c.requested, c.m)
		if err == nil || !strings.HasPrefix(err.Error(), c.want) {
			t.Errorf("%s: got %v, want %s", c.name, err, c.want)
		}
	}
	if _, err := ownerEntry(good, 2); err == nil || err.Error() != "objstore: manifest "+good.SnapshotID+" has no track frontier for indexer 2" {
		t.Errorf("an absent owner: %v", err)
	}
	if e, err := ownerEntry(good, 0); err != nil || e != (replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 9, AppliedPromotionSeq: 3}) {
		t.Errorf("indexer 0: %+v, %v", e, err)
	}
}

// fakeSafeState answers one watermark and a set of manifests by id; answer
// overrides what GetManifest returns for an id.
type fakeSafeState struct {
	pb.UnimplementedSafeStateServer

	mu        sync.Mutex
	watermark string
	manifests map[string]replay.SafeSnapshotManifest
	answer    map[string]replay.SafeSnapshotManifest
}

func (s *fakeSafeState) GetSafeWatermark(context.Context, *pb.GetSafeWatermarkRequest) (*pb.SafeWatermark, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return &pb.SafeWatermark{SnapshotId: s.watermark}, nil
}

func (s *fakeSafeState) GetManifest(_ context.Context, ref *pb.SnapshotRef) (*pb.SafeSnapshotManifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if m, ok := s.answer[ref.GetSnapshotId()]; ok {
		return wire.ManifestToPB(m), nil
	}
	m, ok := s.manifests[ref.GetSnapshotId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "manifest not found")
	}
	return wire.ManifestToPB(m), nil
}

func startFakeArbiter(t *testing.T, state *fakeSafeState) *dataplane.Client {
	t.Helper()
	srv := grpc.NewServer()
	pb.RegisterSafeStateServer(srv, state)
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
	c, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "voter-0", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestArbiterManifestsChecksEveryAnswer(t *testing.T) {
	// Given
	good := testManifest(t, "")
	other := testManifest(t, elementHex("x"))
	state := &fakeSafeState{manifests: map[string]replay.SafeSnapshotManifest{good.SnapshotID: good}}
	src := NewArbiterManifests(startFakeArbiter(t, state))
	ctx := context.Background()

	// When, Then: no publication yet.
	if _, err := src.Latest(ctx); err == nil || err.Error() != "objstore: the arbiter has published no manifest yet" {
		t.Fatalf("no watermark: %v", err)
	}
	state.mu.Lock()
	state.watermark = good.SnapshotID
	state.mu.Unlock()
	latest, err := src.Latest(ctx)
	if err != nil || latest.SnapshotID != good.SnapshotID {
		t.Fatalf("latest: %s, %v", latest.SnapshotID, err)
	}
	byID, err := src.Manifest(ctx, good.SnapshotID)
	if err != nil || byID.ManifestRoot != good.ManifestRoot {
		t.Fatalf("by id: %v", err)
	}
	state.mu.Lock()
	state.answer = map[string]replay.SafeSnapshotManifest{good.SnapshotID: other}
	state.mu.Unlock()
	if _, err := src.Manifest(ctx, good.SnapshotID); err == nil || !strings.Contains(err.Error(), "answered for "+good.SnapshotID) {
		t.Fatalf("a wrong answer must be refused: %v", err)
	}
	if _, err := src.Manifest(ctx, "0xmissing"); err == nil || status.Code(errorsCause(err)) != codes.NotFound {
		t.Fatalf("a missing manifest: %v", err)
	}
	if _, err := src.Manifest(ctx, ""); err == nil {
		t.Fatal("an empty id must be refused")
	}
}

// errorsCause unwraps to the gRPC status error.
func errorsCause(err error) error {
	for {
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return err
		}
		next := u.Unwrap()
		if next == nil {
			return err
		}
		err = next
	}
}
