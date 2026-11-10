package dataplane

import (
	"context"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

const (
	followNetwork = "devnet-follow"
	followGenesis = "0xfollowgenesis"
)

func followSigner(t *testing.T) *authority.Signer {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	s, err := authority.NewSignerFromHex(hex.EncodeToString(crypto.FromECDSA(key)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func followRotation(t *testing.T, by *authority.Signer, epoch uint64, to *authority.Signer) *pb.UpdateConsensusParamsCmd {
	t.Helper()
	u := arbiter.ConsensusParamsUpdate{
		NetworkID: followNetwork, GenesisSnapshotID: followGenesis, ExpectedEpoch: epoch,
		PreviousParamsDigest: "0xprev", MaxWriters: 1, AuthorityAddresses: []string{to.Address()},
	}
	jws, err := by.SignConsensusParamsUpdateAt(u, 1)
	if err != nil {
		t.Fatal(err)
	}
	return &pb.UpdateConsensusParamsCmd{Update: wire.ConsensusParamsUpdateToPB(u), AuthorityJws: jws}
}

// fakeHistory serves a mutable committed history.
type fakeHistory struct {
	updates []*pb.UpdateConsensusParamsCmd
	current *uint64 // overrides current_epoch when set
	err     error
	calls   int
}

func (h *fakeHistory) fetch(_ context.Context, from uint64) (*pb.ConsensusHistory, error) {
	h.calls++
	if h.err != nil {
		return nil, h.err
	}
	current := uint64(len(h.updates))
	if h.current != nil {
		current = *h.current
	}
	out := &pb.ConsensusHistory{NetworkId: followNetwork, GenesisSnapshotId: followGenesis, CurrentEpoch: current}
	if from < current && int(current) <= len(h.updates) {
		out.Updates = h.updates[from:current]
	}
	return out, nil
}

func newFollower(t *testing.T, dir string, anchorEpoch uint64, anchor *authority.Signer, h *fakeHistory) *AuthorityFollower {
	t.Helper()
	f, err := NewAuthorityFollower(nil, AuthorityFollowerConfig{
		Anchor:   authority.AuthorityAnchor{NetworkID: followNetwork, GenesisSnapshotID: followGenesis, Epoch: anchorEpoch, Addresses: []string{anchor.Address()}},
		StateDir: dir, MaxTokenAge: time.Minute,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.fetch = h.fetch
	return f
}

func promotion() arbiter.PromoteSafePartition {
	return arbiter.PromoteSafePartition{PromotionSeq: 9, TableID: "t", PartitionID: "p", BasePartitionRoot: "0xbase"}
}

func signedPromotion(t *testing.T, s *authority.Signer, epoch uint64) string {
	t.Helper()
	tok, err := s.SignPromotionWithContext(promotion(), authority.ConsensusContext{NetworkID: followNetwork, GenesisSnapshotID: followGenesis, AuthorityEpoch: epoch})
	if err != nil {
		t.Fatal(err)
	}
	return tok
}

func TestAuthorityFollowerAdoptsRotationOnDemand(t *testing.T) {
	ctx := context.Background()
	a, b := followSigner(t), followSigner(t)
	h := &fakeHistory{}
	f := newFollower(t, t.TempDir(), 0, a, h)

	if _, err := f.AuthorizePromotion(ctx, promotion(), signedPromotion(t, a, 0)); err != nil {
		t.Fatalf("anchor set: %v", err)
	}
	if h.calls != 0 {
		t.Fatalf("a current-epoch token fetched history %d times", h.calls)
	}
	// Rotation a -> b commits; the first token of epoch 1 refreshes on demand.
	h.updates = append(h.updates, followRotation(t, a, 0, b))
	if _, err := f.AuthorizePromotion(ctx, promotion(), signedPromotion(t, b, 1)); err != nil {
		t.Fatalf("new key after rotation: %v", err)
	}
	if epoch, addrs, _ := f.Current(); epoch != 1 || addrs[0] != strings.ToLower(b.Address()) {
		t.Fatalf("current = %d %v", epoch, addrs)
	}
	// The rotated-out key no longer authorizes anything, at either epoch.
	if _, err := f.AuthorizePromotion(ctx, promotion(), signedPromotion(t, a, 1)); err == nil {
		t.Fatal("rotated-out key accepted at the new epoch")
	}
	if _, err := f.AuthorizePromotion(ctx, promotion(), signedPromotion(t, a, 0)); err == nil {
		t.Fatal("old-epoch token accepted")
	}
	// A token claiming an epoch nobody committed is refused after the refresh.
	if _, err := f.AuthorizePromotion(ctx, promotion(), signedPromotion(t, b, 2)); err == nil {
		t.Fatal("future-epoch token accepted")
	}
}

func TestAuthorityFollowerPersistsAndNeverRollsBack(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	a, b := followSigner(t), followSigner(t)
	h := &fakeHistory{updates: []*pb.UpdateConsensusParamsCmd{followRotation(t, a, 0, b)}}
	f := newFollower(t, dir, 0, a, h)
	if err := f.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	// Restart against an unreachable arbiter: the persisted epoch stays in force.
	down := &fakeHistory{err: status.Error(codes.Unavailable, "down")}
	g := newFollower(t, dir, 0, a, down)
	if epoch, _, _ := g.Current(); epoch != 1 {
		t.Fatalf("restored epoch = %d", epoch)
	}
	if err := g.Refresh(ctx); err == nil {
		t.Fatal("unavailable refresh succeeded")
	}
	if _, err := g.AuthorizePromotion(ctx, promotion(), signedPromotion(t, b, 1)); err != nil {
		t.Fatalf("persisted set while unreachable: %v", err)
	}
	// A lagging voter answering epoch 0 does not roll the chain back.
	zero := uint64(0)
	lag := &fakeHistory{updates: h.updates, current: &zero}
	g.fetch = lag.fetch
	if err := g.Refresh(ctx); err != nil {
		t.Fatalf("lagging answer: %v", err)
	}
	if epoch, _, _ := g.Current(); epoch != 1 {
		t.Fatalf("rolled back to %d", epoch)
	}
	if _, err := g.AuthorizePromotion(ctx, promotion(), signedPromotion(t, a, 0)); err == nil {
		t.Fatal("old set accepted after a lagging answer")
	}
}

func TestAuthorityFollowerForkFailsClosedUntilReanchored(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	a, b, x := followSigner(t), followSigner(t), followSigner(t)
	h := &fakeHistory{updates: []*pb.UpdateConsensusParamsCmd{followRotation(t, a, 0, b)}}
	f := newFollower(t, dir, 0, a, h)
	if err := f.Refresh(ctx); err != nil {
		t.Fatal(err)
	}
	// The leaked bootstrap key a signs an alternative epoch-0 update.
	h.updates = []*pb.UpdateConsensusParamsCmd{followRotation(t, a, 0, x)}
	if err := f.Refresh(ctx); !errors.Is(err, authority.ErrAuthorityFork) {
		t.Fatalf("refresh err = %v, want fork", err)
	}
	if _, err := f.AuthorizePromotion(ctx, promotion(), signedPromotion(t, b, 1)); !errors.Is(err, authority.ErrAuthorityFork) {
		t.Fatalf("authorize after fork = %v", err)
	}
	// The fork survives a restart with the same anchor.
	g := newFollower(t, dir, 0, a, &fakeHistory{})
	if _, _, fork := g.Current(); fork == "" {
		t.Fatal("fork report lost on restart")
	}
	// The operator re-anchors at the recovery epoch: following resumes.
	h2 := &fakeHistory{updates: []*pb.UpdateConsensusParamsCmd{followRotation(t, a, 0, b)}}
	r := newFollower(t, dir, 1, b, h2)
	if _, _, fork := r.Current(); fork != "" {
		t.Fatalf("re-anchor kept the fork: %s", fork)
	}
	if _, err := r.AuthorizePromotion(ctx, promotion(), signedPromotion(t, b, 1)); err != nil {
		t.Fatalf("after re-anchor: %v", err)
	}
}

func TestAuthorityFollowerRefreshErrors(t *testing.T) {
	ctx := context.Background()
	a := followSigner(t)
	f := newFollower(t, t.TempDir(), 0, a, &fakeHistory{err: status.Error(codes.Unimplemented, "unknown service")})
	if err := f.Refresh(ctx); !errors.Is(err, ErrAuthorityHistoryUnimplemented) {
		t.Fatalf("err = %v", err)
	}
	other := &fakeHistory{}
	f.fetch = func(ctx context.Context, from uint64) (*pb.ConsensusHistory, error) {
		h, _ := other.fetch(ctx, from)
		h.NetworkId = "other"
		return h, nil
	}
	if err := f.Refresh(ctx); err == nil || !strings.Contains(err.Error(), "serves network") {
		t.Fatalf("network err = %v", err)
	}
	// An outsider's update is refused and changes nothing.
	x := followSigner(t)
	bad := &fakeHistory{updates: []*pb.UpdateConsensusParamsCmd{followRotation(t, x, 0, x)}}
	f.fetch = bad.fetch
	if err := f.Refresh(ctx); err == nil {
		t.Fatal("outsider update accepted")
	}
	if epoch, _, fork := f.Current(); epoch != 0 || fork != "" {
		t.Fatalf("current = %d fork %q", epoch, fork)
	}
}

func TestNewAuthorityFollowerValidates(t *testing.T) {
	a := followSigner(t)
	anchor := authority.AuthorityAnchor{NetworkID: followNetwork, GenesisSnapshotID: followGenesis, Addresses: []string{a.Address()}}
	for name, cfg := range map[string]AuthorityFollowerConfig{
		"no dir": {Anchor: anchor, MaxTokenAge: time.Minute},
		"no age": {Anchor: anchor, StateDir: t.TempDir()},
		"anchor": {Anchor: authority.AuthorityAnchor{NetworkID: followNetwork}, StateDir: t.TempDir(), MaxTokenAge: time.Minute},
	} {
		if _, err := NewAuthorityFollower(nil, cfg, nil); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
