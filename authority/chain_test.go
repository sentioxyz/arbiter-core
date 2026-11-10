package authority

import (
	"encoding/hex"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/sentioxyz/arbiter-core"
)

const (
	chainNetwork = "devnet-chain"
	chainGenesis = "0xchaingenesis"
)

func chainSigner(t *testing.T) *Signer {
	t.Helper()
	key, err := crypto.GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	s, err := NewSignerFromHex(hex.EncodeToString(crypto.FromECDSA(key)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

// rotation signs an update moving epoch to epoch+1 with the given new set.
func rotation(t *testing.T, signer *Signer, epoch uint64, next ...*Signer) SignedConsensusUpdate {
	t.Helper()
	u := arbiter.ConsensusParamsUpdate{
		NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, ExpectedEpoch: epoch,
		PreviousParamsDigest: "0xprev", MaxWriters: 1, ExpectedPromotionSeq: epoch * 10,
	}
	for _, n := range next {
		u.AuthorityAddresses = append(u.AuthorityAddresses, n.Address())
	}
	jws, err := signer.SignConsensusParamsUpdateAt(u, 1)
	if err != nil {
		t.Fatal(err)
	}
	return SignedConsensusUpdate{Update: u, AuthorityJWS: jws}
}

func newChain(t *testing.T, epoch uint64, set ...*Signer) AuthorityChain {
	t.Helper()
	anchor := AuthorityAnchor{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, Epoch: epoch}
	for _, s := range set {
		anchor.Addresses = append(anchor.Addresses, s.Address())
	}
	c, err := NewAuthorityChain(anchor)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestAuthorityChainFollowsRotations(t *testing.T) {
	a, b, c := chainSigner(t), chainSigner(t), chainSigner(t)
	chain := newChain(t, 0, a)
	history := []SignedConsensusUpdate{rotation(t, a, 0, a), rotation(t, a, 1, b), rotation(t, b, 2, c)}
	got, err := chain.Extend(0, history)
	if err != nil {
		t.Fatal(err)
	}
	if got.Epoch != 3 || len(got.UpdateHashes) != 3 || len(got.Addresses) != 1 || got.Addresses[0] != strings.ToLower(c.Address()) {
		t.Fatalf("chain = %+v", got)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
	// Re-serving the verified history is a no-op, and from a later epoch too.
	again, err := got.Extend(0, history)
	if err != nil || again.Epoch != 3 {
		t.Fatalf("re-extend = %+v, %v", again, err)
	}
	again, err = got.Extend(2, history[2:])
	if err != nil || again.Epoch != 3 {
		t.Fatalf("partial re-extend = %+v, %v", again, err)
	}
	// An empty answer at or above the chain's epoch changes nothing.
	again, err = got.Extend(5, nil)
	if err != nil || again.Epoch != 3 {
		t.Fatalf("empty = %+v, %v", again, err)
	}
}

func TestAuthorityChainRejectsUnendorsedUpdates(t *testing.T) {
	a, b, x := chainSigner(t), chainSigner(t), chainSigner(t)
	chain := newChain(t, 0, a)

	// Signed by a key outside the set in force.
	if _, err := chain.Extend(0, []SignedConsensusUpdate{rotation(t, x, 0, x)}); err == nil || !strings.Contains(err.Error(), "not in allowlist") {
		t.Fatalf("outsider err = %v", err)
	}
	// A rotated-out key signing the next epoch.
	rotated, err := chain.Extend(0, []SignedConsensusUpdate{rotation(t, a, 0, b)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rotated.Extend(1, []SignedConsensusUpdate{rotation(t, a, 1, a)}); err == nil {
		t.Fatal("rotated-out key accepted")
	}
	// Tampered content after signing.
	tampered := rotation(t, a, 0, a)
	tampered.Update.AuthorityAddresses = []string{x.Address()}
	if _, err := chain.Extend(0, []SignedConsensusUpdate{tampered}); err == nil {
		t.Fatal("tampered update accepted")
	}
	// Another network's update.
	other := rotation(t, a, 0, x)
	other.Update.NetworkID = "other"
	other.AuthorityJWS, _ = a.SignConsensusParamsUpdateAt(other.Update, 1)
	if _, err := chain.Extend(0, []SignedConsensusUpdate{other}); err == nil || !strings.Contains(err.Error(), "names network") {
		t.Fatalf("network err = %v", err)
	}
	// Non-contiguous epochs and a gap.
	if _, err := chain.Extend(0, []SignedConsensusUpdate{rotation(t, a, 1, a)}); err == nil {
		t.Fatal("wrong expected_epoch accepted")
	}
	if _, err := chain.Extend(1, []SignedConsensusUpdate{rotation(t, a, 1, a)}); err == nil || !strings.Contains(err.Error(), "gap") {
		t.Fatalf("gap err = %v", err)
	}
	// Below the anchor.
	anchored := newChain(t, 4, a)
	if _, err := anchored.Extend(3, nil); err == nil {
		t.Fatal("history below the anchor accepted")
	}
}

func TestAuthorityChainDetectsFork(t *testing.T) {
	a, b, x := chainSigner(t), chainSigner(t), chainSigner(t)
	chain := newChain(t, 0, a)
	genuine, err := chain.Extend(0, []SignedConsensusUpdate{rotation(t, a, 0, b)})
	if err != nil {
		t.Fatal(err)
	}
	// The leaked bootstrap key signs an alternative epoch-0 update.
	fork := rotation(t, a, 0, x)
	got, err := genuine.Extend(0, []SignedConsensusUpdate{fork})
	if !errors.Is(err, ErrAuthorityFork) {
		t.Fatalf("err = %v, want fork", err)
	}
	if got.Epoch != genuine.Epoch || got.Addresses[0] != genuine.Addresses[0] {
		t.Fatalf("chain changed on fork: %+v", got)
	}
}

func TestAuthorityChainAnchorCheckpoint(t *testing.T) {
	a, b, c := chainSigner(t), chainSigner(t), chainSigner(t)
	// Anchored at epoch 7 with set {b}: earlier history is never consulted.
	chain := newChain(t, 7, b)
	got, err := chain.Extend(7, []SignedConsensusUpdate{rotation(t, b, 7, c)})
	if err != nil || got.Epoch != 8 {
		t.Fatalf("checkpoint = %+v, %v", got, err)
	}
	if got.SameAnchor(AuthorityAnchor{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, Epoch: 7, Addresses: []string{strings.ToUpper(b.Address()[2:])}}) {
		t.Fatal("non-0x address matched the anchor")
	}
	if !got.SameAnchor(AuthorityAnchor{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, Epoch: 7, Addresses: []string{b.Address(), b.Address()}}) {
		t.Fatal("same anchor not recognized")
	}
	if got.SameAnchor(AuthorityAnchor{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, Epoch: 7, Addresses: []string{a.Address()}}) {
		t.Fatal("different anchor recognized")
	}
}

func TestNewAuthorityChainValidatesAnchor(t *testing.T) {
	for name, anchor := range map[string]AuthorityAnchor{
		"no network": {GenesisSnapshotID: chainGenesis, Addresses: []string{"0x1111111111111111111111111111111111111111"}},
		"no genesis": {NetworkID: chainNetwork, Addresses: []string{"0x1111111111111111111111111111111111111111"}},
		"no address": {NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis},
		"bad":        {NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, Addresses: []string{"0x1234"}},
	} {
		if _, err := NewAuthorityChain(anchor); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}

func TestAuthorizeInContextAndTokenEpoch(t *testing.T) {
	a := chainSigner(t)
	chain := newChain(t, 0, a)
	ctx := chain.Context()
	v := chain.Validator(time.Minute)
	cmd := testCmd()
	tok, err := a.SignPromotionWithContext(cmd, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if epoch, ok := TokenAuthorityEpoch(tok); !ok || epoch != 0 {
		t.Fatalf("token epoch = %d, %v", epoch, ok)
	}
	if _, err := v.AuthorizePromotionInContext(cmd, tok, ctx); err != nil {
		t.Fatal(err)
	}
	wrong := ctx
	wrong.AuthorityEpoch = 1
	if _, err := v.AuthorizePromotionInContext(cmd, tok, wrong); err == nil {
		t.Fatal("epoch mismatch accepted")
	}
	legacy, err := a.SignPromotion(cmd)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := TokenAuthorityEpoch(legacy); ok {
		t.Fatal("legacy token reported an epoch")
	}
	if _, err := v.AuthorizePromotionInContext(cmd, legacy, ctx); err == nil {
		t.Fatal("context-less token accepted")
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "t", PartitionID: "p", PromotionSeq: 1}
	ctok, err := a.SignCleanupWithContext(cleanup, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeCleanupInContext(cleanup, ctok, ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizePromotionInContext(cmd, ctok, ctx); err == nil {
		t.Fatal("cleanup token authorized a promotion")
	}
}
