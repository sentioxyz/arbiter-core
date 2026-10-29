package authority

import (
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
)

func testSettleCommand() arbiter.SettleTrackCommand {
	return arbiter.SettleTrackCommand{IndexerID: 2, ExpectedGeneration: 3, PromotionSeqs: []uint64{41, 42}}
}

// TestAuthorizeSettleTrackRefusesAnotherPurposeAndAStaleToken: the settle
// token is context-bound like the quarantine's (plan S3-D contract §34): a
// fresh token by a listed authority authorizes exactly its command and
// context, a stale one, another purpose's, a context-less one and one for
// another list, generation or indexer do not, and a settle token authorizes
// no other command.
func TestAuthorizeSettleTrackRefusesAnotherPurposeAndAStaleToken(t *testing.T) {
	s, v := newTestPair(t)
	ctx := testTracksContext()
	cmd := testSettleCommand()
	fresh, err := s.SignSettleTrackWithContext(cmd, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if addr, err := v.AuthorizeSettleTrack(cmd, fresh, ctx); err != nil || addr != s.Address() {
		t.Fatalf("fresh token: %q, %v", addr, err)
	}
	stale, err := s.SignSettleTrackWithContextAt(cmd, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeSettleTrack(cmd, stale, ctx); err == nil || err.Error() != "authority token expired" {
		t.Fatalf("stale token: %v", err)
	}
	hash, err := SettleTrackHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	asQuarantine, err := s.signWithContextPurpose(hash, QuarantineIndexerPurpose, ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	asRelease, err := s.signWithContextPurpose(hash, ReleaseIndexerPurpose, ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	asPromotion, err := s.signWithContextPurpose(hash, PromotionPurpose, ctx, now)
	if err != nil {
		t.Fatal(err)
	}
	contextless, err := s.signPayload(JWSCommandPayload{Iat: now, Purpose: SettleTrackPurpose, CmdHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	nextEpoch, otherNetwork := ctx, ctx
	nextEpoch.AuthorityEpoch = 3
	otherNetwork.NetworkID = "mainnet"
	otherList, otherGeneration, otherIndexer := cmd, cmd, cmd
	otherList.PromotionSeqs = []uint64{41}
	otherGeneration.ExpectedGeneration = 4
	otherIndexer.IndexerID = 0
	unlisted := &Validator{AllowedAddresses: map[string]bool{"0x0000000000000000000000000000000000000001": true}, MaxTokenAge: v.MaxTokenAge}
	authorize := func(c arbiter.SettleTrackCommand, token string, cc ConsensusContext) error {
		_, err := v.AuthorizeSettleTrack(c, token, cc)
		return err
	}
	_, unlistedErr := unlisted.AuthorizeSettleTrack(cmd, fresh, ctx)
	for name, err := range map[string]error{
		"a quarantine-purpose token":  authorize(cmd, asQuarantine, ctx),
		"a release-purpose token":     authorize(cmd, asRelease, ctx),
		"a promotion-purpose token":   authorize(cmd, asPromotion, ctx),
		"a token without its context": authorize(cmd, contextless, ctx),
		"the next authority epoch":    authorize(cmd, fresh, nextEpoch),
		"another network":             authorize(cmd, fresh, otherNetwork),
		"another promotion list":      authorize(otherList, fresh, ctx),
		"another expected generation": authorize(otherGeneration, fresh, ctx),
		"another indexer":             authorize(otherIndexer, fresh, ctx),
		"an unlisted signer":          unlistedErr,
	} {
		if err == nil {
			t.Fatalf("%s: authorized", name)
		}
	}
	if _, err := v.AuthorizeQuarantineIndexer(arbiter.QuarantineIndexerCommand{IndexerID: 2, ExpectedGeneration: 3, Reason: "x"}, fresh, ctx); err == nil {
		t.Fatal("a settle token authorized a quarantine")
	}
	if _, err := v.AuthorizeReleaseIndexer(arbiter.ReleaseIndexerCommand{IndexerID: 2, ExpectedGeneration: 3}, fresh, ctx); err == nil {
		t.Fatal("a settle token authorized a release")
	}
	if _, err := v.AuthorizePromotion(testCmd(), fresh); err == nil {
		t.Fatal("a settle token authorized a promotion")
	}
	for _, bad := range [][]uint64{nil, {42, 41}} {
		c := cmd
		c.PromotionSeqs = bad
		if _, err := SettleTrackHash(c); err == nil || err.Error() != "settle track: promotion_seqs must be non-empty and strictly ascending" {
			t.Fatalf("%v: hash err = %v", bad, err)
		}
		if _, err := s.SignSettleTrackWithContext(c, ctx); err == nil {
			t.Fatalf("%v: signed", bad)
		}
	}
	if _, err := v.AuthorizeSettleTrack(cmd, fresh, ConsensusContext{GenesisSnapshotID: "0xgenesis", AuthorityEpoch: 2}); err == nil ||
		err.Error() != "settle track: network ID and genesis snapshot ID must be non-empty" {
		t.Fatalf("empty network: %v", err)
	}
	if _, err := v.AuthorizeSettleTrack(cmd, fresh, nextEpoch); err == nil || err.Error() != "settle track token: consensus context mismatch" {
		t.Fatalf("next epoch: %v", err)
	}
}
