package authority

import (
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func testQuarantineCommand() arbiter.QuarantineIndexerCommand {
	return arbiter.QuarantineIndexerCommand{IndexerID: 1, ExpectedGeneration: 0, Reason: "snode-2 stalled"}
}

func testReleaseCommand() arbiter.ReleaseIndexerCommand {
	return arbiter.ReleaseIndexerCommand{IndexerID: 1, ExpectedGeneration: 2, AcknowledgedRejectedBlocks: []uint64{7, 9}}
}

func testTracksContext() ConsensusContext {
	return ConsensusContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", AuthorityEpoch: 2}
}

// TestQuarantineIndexerIsContextBoundAndPurposeSeparated copies the eviction's
// matrix (TestEvictNodeIsContextBoundAndPurposeSeparated) for the quarantine
// (housegate spec 2026-10-10 §7.4; plan S2-A contract §2): Verify reads no
// clock and requires exactly the context, Authorize adds the token age, every
// signed field binds, and no other token family verifies.
func TestQuarantineIndexerIsContextBoundAndPurposeSeparated(t *testing.T) {
	s, v := newTestPair(t)
	ctx := testTracksContext()
	cmd := testQuarantineCommand()
	old, err := s.SignQuarantineIndexerWithContextAt(cmd, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if addr, err := v.VerifyQuarantineIndexer(cmd, old, ctx); err != nil || addr != s.Address() {
		t.Fatalf("deterministic verify: %q, %v", addr, err)
	}
	if _, err := v.AuthorizeQuarantineIndexer(cmd, old, ctx); err == nil {
		t.Fatal("the API boundary accepted a stale quarantine token")
	}
	fresh, err := s.SignQuarantineIndexerWithContext(cmd, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeQuarantineIndexer(cmd, fresh, ctx); err != nil {
		t.Fatalf("fresh token: %v", err)
	}
	hash, err := QuarantineIndexerHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	contextless, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: QuarantineIndexerPurpose, CmdHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	asEviction, err := s.signWithContextPurpose(hash, EvictNodePurpose, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	asRelease, err := s.signWithContextPurpose(hash, ReleaseIndexerPurpose, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	promotion, err := s.SignPromotionWithContextAt(testCmd(), ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	nextEpoch, otherNetwork, otherGenesis := ctx, ctx, ctx
	nextEpoch.AuthorityEpoch = 3
	otherNetwork.NetworkID = "mainnet"
	otherGenesis.GenesisSnapshotID = "0xother"
	indexerZero, otherGeneration, otherReason := cmd, cmd, cmd
	indexerZero.IndexerID = 0
	otherGeneration.ExpectedGeneration = 1
	otherReason.Reason = "other"
	unlisted := &Validator{AllowedAddresses: map[string]bool{"0x0000000000000000000000000000000000000001": true}}
	verify := func(c arbiter.QuarantineIndexerCommand, token string, cc ConsensusContext) error {
		_, err := v.VerifyQuarantineIndexer(c, token, cc)
		return err
	}
	_, unlistedErr := unlisted.VerifyQuarantineIndexer(cmd, old, ctx)
	for name, err := range map[string]error{
		"next authority epoch":        verify(cmd, old, nextEpoch),
		"other network":               verify(cmd, old, otherNetwork),
		"other genesis":               verify(cmd, old, otherGenesis),
		"indexer 0 instead of 1":      verify(indexerZero, old, ctx),
		"other expected generation":   verify(otherGeneration, old, ctx),
		"other reason":                verify(otherReason, old, ctx),
		"unlisted signer":             unlistedErr,
		"an eviction-purpose token":   verify(cmd, asEviction, ctx),
		"a release-purpose token":     verify(cmd, asRelease, ctx),
		"a promotion token":           verify(cmd, promotion, ctx),
		"a token without its context": verify(cmd, contextless, ctx),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	if _, err := v.VerifyEvictNode(testEvictCommand(), fresh, ctx); err == nil {
		t.Fatal("a quarantine token verified as an eviction")
	}
	if _, err := v.AuthorizePromotion(testCmd(), fresh); err == nil {
		t.Fatal("a quarantine token authorized a promotion")
	}
	for _, reason := range []string{"", "  "} {
		bad := cmd
		bad.Reason = reason
		if _, err := QuarantineIndexerHash(bad); err == nil || err.Error() != "quarantine indexer: reason must be non-empty" {
			t.Fatalf("reason %q: err = %v", reason, err)
		}
	}
	if _, err := v.VerifyQuarantineIndexer(cmd, old, ConsensusContext{GenesisSnapshotID: "0xgenesis", AuthorityEpoch: 2}); err == nil ||
		err.Error() != "quarantine indexer: network ID and genesis snapshot ID must be non-empty" {
		t.Fatalf("empty network: %v", err)
	}
	if _, err := v.VerifyQuarantineIndexer(cmd, old, nextEpoch); err == nil || err.Error() != "quarantine indexer token: consensus context mismatch" {
		t.Fatalf("next epoch: %v", err)
	}
}

// TestReleaseIndexerIsContextBoundAndPurposeSeparated is the same matrix for
// the release, whose acknowledged list binds too (spec §7.4, §7.5).
func TestReleaseIndexerIsContextBoundAndPurposeSeparated(t *testing.T) {
	s, v := newTestPair(t)
	ctx := testTracksContext()
	cmd := testReleaseCommand()
	old, err := s.SignReleaseIndexerWithContextAt(cmd, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if addr, err := v.VerifyReleaseIndexer(cmd, old, ctx); err != nil || addr != s.Address() {
		t.Fatalf("deterministic verify: %q, %v", addr, err)
	}
	if _, err := v.AuthorizeReleaseIndexer(cmd, old, ctx); err == nil {
		t.Fatal("the API boundary accepted a stale release token")
	}
	fresh, err := s.SignReleaseIndexerWithContext(cmd, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeReleaseIndexer(cmd, fresh, ctx); err != nil {
		t.Fatalf("fresh token: %v", err)
	}
	hash, err := ReleaseIndexerHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	contextless, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: ReleaseIndexerPurpose, CmdHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	asQuarantine, err := s.signWithContextPurpose(hash, QuarantineIndexerPurpose, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	nextEpoch, otherNetwork, otherGenesis := ctx, ctx, ctx
	nextEpoch.AuthorityEpoch = 3
	otherNetwork.NetworkID = "mainnet"
	otherGenesis.GenesisSnapshotID = "0xother"
	otherIndexer, otherGeneration, fewerBlocks, noBlocks := cmd, cmd, cmd, cmd
	otherIndexer.IndexerID = 0
	otherGeneration.ExpectedGeneration = 1
	fewerBlocks.AcknowledgedRejectedBlocks = []uint64{7}
	noBlocks.AcknowledgedRejectedBlocks = nil
	unlisted := &Validator{AllowedAddresses: map[string]bool{"0x0000000000000000000000000000000000000001": true}}
	verify := func(c arbiter.ReleaseIndexerCommand, token string, cc ConsensusContext) error {
		_, err := v.VerifyReleaseIndexer(c, token, cc)
		return err
	}
	_, unlistedErr := unlisted.VerifyReleaseIndexer(cmd, old, ctx)
	for name, err := range map[string]error{
		"next authority epoch":        verify(cmd, old, nextEpoch),
		"other network":               verify(cmd, old, otherNetwork),
		"other genesis":               verify(cmd, old, otherGenesis),
		"other indexer":               verify(otherIndexer, old, ctx),
		"other expected generation":   verify(otherGeneration, old, ctx),
		"fewer acknowledged blocks":   verify(fewerBlocks, old, ctx),
		"no acknowledged blocks":      verify(noBlocks, old, ctx),
		"unlisted signer":             unlistedErr,
		"a quarantine-purpose token":  verify(cmd, asQuarantine, ctx),
		"a token without its context": verify(cmd, contextless, ctx),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	if _, err := v.VerifyQuarantineIndexer(testQuarantineCommand(), fresh, ctx); err == nil {
		t.Fatal("a release token verified as a quarantine")
	}
	if _, err := v.VerifyReleaseIndexer(cmd, old, otherGenesis); err == nil || err.Error() != "release indexer token: consensus context mismatch" {
		t.Fatalf("other genesis: %v", err)
	}
	if _, err := v.VerifyReleaseIndexer(cmd, old, ConsensusContext{NetworkID: "devnet2", AuthorityEpoch: 2}); err == nil ||
		err.Error() != "release indexer: network ID and genesis snapshot ID must be non-empty" {
		t.Fatalf("empty genesis: %v", err)
	}
}

// TestReleaseIndexerHashCanonicalizesTheAcknowledgedList: nil and [] hash
// alike, as the canonical form without the member (computed outside Go:
// sha256 over "housegate-replay-mvp-v0:arbiter-release-indexer-command-v1" +
// "\x00" + {"indexer_id":1,"expected_generation":1}); an unsorted or
// duplicated list is refused, never sorted silently.
func TestReleaseIndexerHashCanonicalizesTheAcknowledgedList(t *testing.T) {
	const withoutBlocks = "0xa50ed35ee7822662c068f060dceb140d63877d14b0bb8377372231f3034b5e91"
	for _, blocks := range [][]uint64{nil, {}} {
		h, err := ReleaseIndexerHash(arbiter.ReleaseIndexerCommand{IndexerID: 1, ExpectedGeneration: 1, AcknowledgedRejectedBlocks: blocks})
		if err != nil || h != withoutBlocks {
			t.Fatalf("%#v: hash = %s, %v; want %s", blocks, h, err, withoutBlocks)
		}
	}
	for _, blocks := range [][]uint64{{9, 7}, {7, 7}} {
		_, err := ReleaseIndexerHash(arbiter.ReleaseIndexerCommand{IndexerID: 1, ExpectedGeneration: 2, AcknowledgedRejectedBlocks: blocks})
		if err == nil || !strings.Contains(err.Error(), "acknowledged_rejected_blocks must be sorted ascending without duplicates") {
			t.Fatalf("%v: err = %v", blocks, err)
		}
	}
}
