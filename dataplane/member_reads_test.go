package dataplane

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// memberReadsSafeState serves the three reads a P2 member makes and records
// every method it is asked. A peer with hint answers NotLeader naming it, one
// with err refuses with it, and otherwise it answers from its maps with the
// arbiter's NotFound texts (server/safestate.go, plan S3-A contract §4).
type memberReadsSafeState struct {
	pb.UnimplementedSafeStateServer

	mu        sync.Mutex
	hint      string
	err       error
	watermark *pb.SafeWatermark
	manifests map[string]*pb.SafeSnapshotManifest
	blocks    map[uint64]*pb.BlockVerification
	calls     []string
}

func (s *memberReadsSafeState) refusal(method string) error {
	s.calls = append(s.calls, method)
	if s.hint != "" {
		st, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&pb.NotLeader{LeaderAddr: s.hint})
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		return st.Err()
	}
	return s.err
}

func (s *memberReadsSafeState) GetSafeWatermark(context.Context, *pb.GetSafeWatermarkRequest) (*pb.SafeWatermark, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refusal("GetSafeWatermark"); err != nil {
		return nil, err
	}
	return s.watermark, nil
}

func (s *memberReadsSafeState) GetManifest(_ context.Context, ref *pb.SnapshotRef) (*pb.SafeSnapshotManifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refusal("GetManifest"); err != nil {
		return nil, err
	}
	m, ok := s.manifests[ref.GetSnapshotId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown snapshot id")
	}
	return m, nil
}

func (s *memberReadsSafeState) GetBlockVerification(_ context.Context, ref *pb.L3BlockRef) (*pb.BlockVerification, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refusal("GetBlockVerification"); err != nil {
		return nil, err
	}
	b, ok := s.blocks[ref.GetL3BlockSeq()]
	if !ok {
		return nil, status.Error(codes.NotFound, "no sealed block at that seq")
	}
	return b, nil
}

func (s *memberReadsSafeState) methods() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.calls...)
}

func startMemberReadsPeer(t *testing.T, s *memberReadsSafeState) string {
	t.Helper()
	srv := grpc.NewServer()
	pb.RegisterSafeStateServer(srv, s)
	return serveGRPCForDataplaneTest(t, srv)
}

func memberBlockFixture(seq uint64) wire.BlockVerification {
	owner := uint64(0)
	return wire.BlockVerification{
		Header:      wire.BlockHeaderView{L3BlockSeq: seq, StatementSeqStart: 10, StatementCount: 1, PrevSafeSnapshotID: "0xprev", PrevStateRoot: "0xstate", TrackIndexerID: &owner},
		ChainHash:   "0xchain",
		VerifierSet: []string{"verifier-1", "verifier-2", "verifier-3"},
		Evidence:    []wire.VerifierEvidence{{ReplicaID: "verifier-1", Evaluated: true, Check1RootMatch: true, Pass: true}},
		Quorum:      true, VerdictEvaluated: true,
	}
}

// TestGetSafeWatermarkFollowsTheLeader: the read is leader-barriered, so a
// follower answers NotLeader and the client asks the leader it names, and the
// leader first the next time (plan S3-C2 contract §21.1).
func TestGetSafeWatermarkFollowsTheLeader(t *testing.T) {
	follower := &memberReadsSafeState{hint: "n2"}
	leader := &memberReadsSafeState{watermark: &pb.SafeWatermark{SnapshotId: "0xsnap", SafeBlockSeq: 7, ManifestRoot: "0xsnap"}}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberReadsPeer(t, follower)}, Peer{ID: "n2", GRPCAddr: startMemberReadsPeer(t, leader)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.GetSafeWatermark(ctx)

	if err != nil {
		t.Fatalf("GetSafeWatermark: %v", err)
	}
	if want := (arbiter.SafeWatermark{SnapshotID: "0xsnap", SafeBlockSeq: 7, ManifestRoot: "0xsnap"}); got != want {
		t.Fatalf("watermark = %+v, want %+v", got, want)
	}
	if _, err := c.GetSafeWatermark(ctx); err != nil {
		t.Fatalf("second read: %v", err)
	}
	if f, l := follower.methods(), leader.methods(); len(f) != 1 || len(l) != 2 {
		t.Fatalf("calls: follower %v, leader %v; want the follower once and the leader twice", f, l)
	}
}

// TestGetManifestUnknownSnapshotIsErrManifestNotFound: a known id is
// answered, and the leader's NotFound is ErrManifestNotFound naming the id,
// returned at once (NotFound is not retried).
func TestGetManifestUnknownSnapshotIsErrManifestNotFound(t *testing.T) {
	manifest := sealedDataplaneManifest(t)
	peer := &memberReadsSafeState{manifests: map[string]*pb.SafeSnapshotManifest{manifest.SnapshotID: wire.ManifestToPB(manifest)}}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberReadsPeer(t, peer)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.GetManifest(ctx, manifest.SnapshotID)
	if err != nil || !reflect.DeepEqual(got, manifest) {
		t.Fatalf("GetManifest(known) = %+v, %v; want the manifest", got, err)
	}
	began := time.Now()
	_, err = c.GetManifest(ctx, "0xmissing")

	if !errors.Is(err, ErrManifestNotFound) || err.Error() != "dataplane: the arbiter has no manifest with that snapshot id: 0xmissing" {
		t.Fatalf("GetManifest(unknown) = %v, want ErrManifestNotFound naming 0xmissing", err)
	}
	if elapsed, calls := time.Since(began), len(peer.methods()); elapsed > 2*time.Second || calls != 2 {
		t.Fatalf("NotFound must not be retried: %d calls in %v", calls, elapsed)
	}
}

// TestGetManifestReturnsTheAnswerUnvalidated: the client converts and returns
// what the arbiter sent; accepting it is the member feed's job.
func TestGetManifestReturnsTheAnswerUnvalidated(t *testing.T) {
	tampered := sealedDataplaneManifest(t)
	tampered.ManifestRoot = "0xnot-the-root"
	if tampered.Validate() == nil {
		t.Fatal("the fixture must fail Validate")
	}
	peer := &memberReadsSafeState{manifests: map[string]*pb.SafeSnapshotManifest{tampered.SnapshotID: wire.ManifestToPB(tampered)}}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberReadsPeer(t, peer)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.GetManifest(ctx, tampered.SnapshotID)

	if err != nil || !reflect.DeepEqual(got, tampered) {
		t.Fatalf("GetManifest = %+v, %v; want the tampered manifest as sent", got, err)
	}
}

// TestGetManifestRefusesAnEmptyID: an empty id is refused before any call.
func TestGetManifestRefusesAnEmptyID(t *testing.T) {
	peer := &memberReadsSafeState{}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberReadsPeer(t, peer)})

	_, err := c.GetManifest(context.Background(), "")

	if err == nil || err.Error() != "dataplane: GetManifest needs a snapshot id" {
		t.Fatalf("GetManifest(\"\") = %v, want the empty-id refusal", err)
	}
	if calls := peer.methods(); len(calls) != 0 {
		t.Fatalf("the refusal must come before any call: %v", calls)
	}
}

// TestGetBlockVerificationUnsealedIsErrBlockNotSealed: the leader's NotFound
// is ErrBlockNotSealed naming the seq, returned at once.
func TestGetBlockVerificationUnsealedIsErrBlockNotSealed(t *testing.T) {
	peer := &memberReadsSafeState{}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberReadsPeer(t, peer)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	began := time.Now()

	_, err := c.GetBlockVerification(ctx, 42)

	if !errors.Is(err, ErrBlockNotSealed) || err.Error() != "dataplane: no sealed block at that seq: 42" {
		t.Fatalf("GetBlockVerification(42) = %v, want ErrBlockNotSealed naming 42", err)
	}
	if elapsed, calls := time.Since(began), len(peer.methods()); elapsed > 2*time.Second || calls != 1 {
		t.Fatalf("NotFound must not be retried: %d calls in %v", calls, elapsed)
	}
}

// TestGetBlockVerificationRefusesSeqZero: block seqs start at 1, so 0 is
// refused before any call.
func TestGetBlockVerificationRefusesSeqZero(t *testing.T) {
	peer := &memberReadsSafeState{}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberReadsPeer(t, peer)})

	_, err := c.GetBlockVerification(context.Background(), 0)

	if err == nil || err.Error() != "dataplane: GetBlockVerification needs a block seq" || len(peer.methods()) != 0 {
		t.Fatalf("GetBlockVerification(0) = %v after %v, want the refusal before any call", err, peer.methods())
	}
}

// TestGetBlockVerificationRefusesAnAnswerForAnotherBlock: the requested block
// is answered as the canonical type; an answer whose header names another
// block, or no block, is refused.
func TestGetBlockVerificationRefusesAnAnswerForAnotherBlock(t *testing.T) {
	want := memberBlockFixture(42)
	peer := &memberReadsSafeState{blocks: map[uint64]*pb.BlockVerification{
		42: wire.BlockVerificationToPB(want),
		43: wire.BlockVerificationToPB(memberBlockFixture(44)),
		45: {ChainHash: "0xchain"},
	}}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startMemberReadsPeer(t, peer)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.GetBlockVerification(ctx, 42)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("GetBlockVerification(42) = %+v, %v; want %+v", got, err, want)
	}
	for seq, refusal := range map[uint64]string{
		43: "dataplane: GetBlockVerification(43) answered block 44",
		45: "dataplane: GetBlockVerification(45): wire: block verification has no header",
	} {
		if _, err := c.GetBlockVerification(ctx, seq); err == nil || err.Error() != refusal {
			t.Fatalf("GetBlockVerification(%d) = %v, want %q", seq, err, refusal)
		}
	}
}

// TestGetBlockVerificationUnimplementedIsErrStage3ReadsUnavailable: an
// arbiter v0.12.0 serves SafeState without the read; Unimplemented is
// ErrStage3ReadsUnavailable at once, never a retry.
func TestGetBlockVerificationUnimplementedIsErrStage3ReadsUnavailable(t *testing.T) {
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startSafeStatePeer(t, &fakeSafeState{code: codes.OK, snapshot: "safe"})})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	began := time.Now()

	_, err := c.GetBlockVerification(ctx, 42)

	if !errors.Is(err, ErrStage3ReadsUnavailable) || errors.Is(err, ErrBlockNotSealed) {
		t.Fatalf("GetBlockVerification = %v, want ErrStage3ReadsUnavailable", err)
	}
	if elapsed := time.Since(began); elapsed > 2*time.Second {
		t.Fatalf("Unimplemented must not be retried: %v", elapsed)
	}
}
