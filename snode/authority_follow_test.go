package snode

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// historyServer is an arbiter that serves only GetConsensusHistory.
type historyServer struct {
	pb.UnimplementedConsensusHistoryServiceServer
	mu        sync.Mutex
	network   string
	genesis   string
	updates   []*pb.UpdateConsensusParamsCmd
	requested []uint64
}

func (s *historyServer) GetConsensusHistory(_ context.Context, req *pb.GetConsensusHistoryRequest) (*pb.ConsensusHistory, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.requested = append(s.requested, req.GetFromEpoch())
	out := &pb.ConsensusHistory{NetworkId: s.network, GenesisSnapshotId: s.genesis, CurrentEpoch: uint64(len(s.updates))}
	if from := req.GetFromEpoch(); from < uint64(len(s.updates)) {
		out.Updates = s.updates[from:]
	}
	return out, nil
}

func (s *historyServer) commit(u *pb.UpdateConsensusParamsCmd) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.updates = append(s.updates, u)
}

func startGRPC(t *testing.T, register func(*grpc.Server)) *dataplane.Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: lis.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

func followConfig(t *testing.T, anchor *authority.Signer) (Config, string) {
	t.Helper()
	cfg := testConfigS(t)
	cfg.AuthorityAddresses = []string{anchor.Address()}
	cfg.FollowConsensusAuthority = true
	genesis, err := dataplane.GenesisSnapshotID(cfg.NetworkID, cfg.SchemaSnapshotID, cfg.ExecutorProfileID, cfg.Tables)
	if err != nil {
		t.Fatal(err)
	}
	return cfg, genesis
}

func rotateTo(t *testing.T, cfg Config, genesis string, by *authority.Signer, epoch uint64, to *authority.Signer) *pb.UpdateConsensusParamsCmd {
	t.Helper()
	u := arbiter.ConsensusParamsUpdate{
		NetworkID: cfg.NetworkID, GenesisSnapshotID: genesis, ExpectedEpoch: epoch,
		PreviousParamsDigest: "0xprev", MaxWriters: 1, AuthorityAddresses: []string{to.Address()},
	}
	jws, err := by.SignConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.UpdateConsensusParamsCmd{Update: wire.ConsensusParamsUpdateToPB(u), AuthorityJws: jws}
}

func TestSNodeFollowsAuthorityRotationWithoutRestart(t *testing.T) {
	ctx := context.Background()
	a, err := authority.NewSignerFromHex(promoteAuthorityKeyHex)
	if err != nil {
		t.Fatal(err)
	}
	b := mustTestSigner(t, "b")
	cfg, genesis := followConfig(t, a)
	hist := &historyServer{network: cfg.NetworkID, genesis: genesis}
	client := startGRPC(t, func(s *grpc.Server) { pb.RegisterConsensusHistoryServiceServer(s, hist) })
	role, err := New(cfg, Deps{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	stop, err := role.startAuthorityFollower(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()

	cmd := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p", PromotionSeq: 4}
	sign := func(s *authority.Signer, epoch uint64) string {
		tok, err := s.SignPromotionWithContext(cmd, authority.ConsensusContext{NetworkID: cfg.NetworkID, GenesisSnapshotID: genesis, AuthorityEpoch: epoch})
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	if err := role.authorizePromotion(ctx, cmd, sign(a, 0)); err != nil {
		t.Fatalf("anchor key: %v", err)
	}
	legacy, err := a.SignPromotion(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.authorizePromotion(ctx, cmd, legacy); err == nil {
		t.Fatal("a following SNode accepted a token without consensus context")
	}

	hist.commit(rotateTo(t, cfg, genesis, a, 0, b))
	if err := role.authorizePromotion(ctx, cmd, sign(b, 1)); err != nil {
		t.Fatalf("new key after rotation: %v", err)
	}
	if err := role.authorizePromotion(ctx, cmd, sign(a, 1)); err == nil {
		t.Fatal("rotated-out key accepted")
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p", PromotionSeq: 4}
	ctok, err := b.SignCleanupWithContext(cleanup, authority.ConsensusContext{NetworkID: cfg.NetworkID, GenesisSnapshotID: genesis, AuthorityEpoch: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.authorizeCleanup(ctx, cleanup, ctok); err != nil {
		t.Fatalf("cleanup with new key: %v", err)
	}
	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), sign(a, 1), false); err == nil || !strings.Contains(err.Error(), "promote authority") {
		t.Fatalf("handlePromote with rotated-out key = %v", err)
	}
}

func TestSNodeFollowRequiresHistoryRPC(t *testing.T) {
	a := mustTestSigner(t, "a")
	cfg, _ := followConfig(t, a)
	client := startGRPC(t, func(*grpc.Server) {})
	role, err := New(cfg, Deps{Client: client})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := role.startAuthorityFollower(context.Background()); !errors.Is(err, dataplane.ErrAuthorityHistoryUnimplemented) {
		t.Fatalf("err = %v, want unimplemented", err)
	}
}

func TestSNodeStaticAllowlistUnchangedWithoutFollow(t *testing.T) {
	a := mustTestSigner(t, "a")
	cfg := testConfigS(t)
	cfg.AuthorityAddresses = []string{a.Address()}
	role, err := New(cfg, Deps{Client: startGRPC(t, func(*grpc.Server) {})})
	if err != nil {
		t.Fatal(err)
	}
	if role.follower != nil {
		t.Fatal("follower built without follow_consensus_authority")
	}
	stop, err := role.startAuthorityFollower(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	stop()
	cmd := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p", PromotionSeq: 1}
	legacy, err := a.SignPromotion(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.authorizePromotion(context.Background(), cmd, legacy); err != nil {
		t.Fatalf("static allowlist: %v", err)
	}
}

func TestConfigAuthorityTrustEpochRequiresFollow(t *testing.T) {
	cfg := testConfigS(t)
	cfg.AuthorityTrustEpoch = 3
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "authority trust epoch") {
		t.Fatalf("err = %v", err)
	}
	cfg.FollowConsensusAuthority = true
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConfigAuthorityTrustThreshold(t *testing.T) {
	cfg := testConfigS(t)
	cfg.AuthorityTrustThreshold = 1
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "authority trust threshold requires") {
		t.Fatalf("err = %v", err)
	}
	cfg.FollowConsensusAuthority = true
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	cfg.AuthorityTrustThreshold = uint32(len(cfg.AuthorityAddresses)) + 1
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v", err)
	}
}

func mustTestSigner(t *testing.T, seed string) *authority.Signer {
	t.Helper()
	hexKey := strings.Repeat("0", 62) + map[string]string{"a": "a1", "b": "b2"}[seed]
	s, err := authority.NewSignerFromHex(hexKey)
	if err != nil {
		t.Fatal(err)
	}
	return s
}
