package authority

import (
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// voterChangeTestUpdate is anchorFinalityTestUpdate with the table registry,
// k = 2 and the authority change delay set, and a pending voter change that
// enrols arbiter-3 (callers' order unsorted).
func voterChangeTestUpdate(t *testing.T) arbiter.ConsensusParamsUpdate {
	t.Helper()
	u := anchorFinalityTestUpdate(t)
	u.TableRegistry = validRegistryParams()
	u.AuthorityThreshold = 2
	u.AuthorityChangeDelayL2Blocks = arbiter.RecommendedAuthorityChangeDelayL2Blocks
	u.PendingVoterChange = &arbiter.PendingVoterChange{
		Additions:        []arbiter.VoterEntry{testVoterEntry(t, u, 4, "arbiter-4"), testVoterEntry(t, u, 3, "arbiter-3")},
		NotBeforeL2Block: 5_100_000,
	}
	return u
}

// TestNormalizeConsensusParamsUpdateVoterChangeDelay (arbiter#223): a
// pending voter change needs the delay and anchor_finality in the same
// update, additions normalized like voters (sorted, consent verified, ids and
// keys unique), something to add or a lowered quorum, and a not_before
// block. The hash binds it; unset, the hash is unchanged.
func TestNormalizeConsensusParamsUpdateVoterChangeDelay(t *testing.T) {
	u := voterChangeTestUpdate(t)
	base := u
	base.PendingVoterChange = nil
	before, err := ConsensusParamsUpdateHash(base)
	if err != nil {
		t.Fatal(err)
	}
	if h, err := ConsensusParamsUpdateHash(u); err != nil || h == before {
		t.Fatalf("the pending voter change is not bound by the hash: %s, %v", h, err)
	}
	callerFirst := u.PendingVoterChange.Additions[0].VoterID
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	if a := got.PendingVoterChange.Additions; len(a) != 2 || a[0].VoterID != "arbiter-3" || a[1].VoterID != "arbiter-4" {
		t.Fatalf("normalized additions = %+v", a)
	}
	if u.PendingVoterChange.Additions[0].VoterID != callerFirst {
		t.Fatal("normalization mutated the caller's pending voter change")
	}
	lowerOnly := voterChangeTestUpdate(t)
	lowerOnly.PendingVoterChange.Additions = nil
	lowerOnly.PendingVoterChange.AnchorFinalityQuorum = 2
	if got, err := NormalizeConsensusParamsUpdate(lowerOnly); err != nil || got.PendingVoterChange.AnchorFinalityQuorum != 2 || got.PendingVoterChange.Additions != nil {
		t.Fatalf("a quorum-only pending change: %+v, %v", got.PendingVoterChange, err)
	}

	for name, tc := range map[string]struct {
		mutate func(*arbiter.ConsensusParamsUpdate)
		want   string
	}{
		"without the delay": {func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityChangeDelayL2Blocks = 0 }, "pending_voter_change requires authority_change_delay_l2_blocks"},
		"without anchor_finality": {func(c *arbiter.ConsensusParamsUpdate) {
			c.AnchorFinality = nil
		}, "pending_voter_change requires anchor_finality"},
		"nothing to do": {func(c *arbiter.ConsensusParamsUpdate) { c.PendingVoterChange.Additions = nil }, "must add a voter or name a lowered anchor_finality_quorum"},
		"no not_before": {func(c *arbiter.ConsensusParamsUpdate) { c.PendingVoterChange.NotBeforeL2Block = 0 }, "needs a not_before_l2_block"},
		"duplicate id": {func(c *arbiter.ConsensusParamsUpdate) {
			c.PendingVoterChange.Additions[1] = testVoterEntry(t, *c, 5, "arbiter-4")
		}, `duplicate voter_id "arbiter-4"`},
		"shared key": {func(c *arbiter.ConsensusParamsUpdate) {
			c.PendingVoterChange.Additions[1] = testVoterEntry(t, *c, 4, "arbiter-5")
		}, "reuses another voter's ed25519_pubkey"},
		"bad consent": {func(c *arbiter.ConsensusParamsUpdate) {
			e := c.PendingVoterChange.Additions[0]
			e.ConsentSig = strings.Repeat("00", 64)
			c.PendingVoterChange.Additions[0] = e
		}, "consent_sig"},
	} {
		t.Run(name, func(t *testing.T) {
			c := voterChangeTestUpdate(t)
			c.PendingVoterChange.Additions = append([]arbiter.VoterEntry(nil), c.PendingVoterChange.Additions...)
			tc.mutate(&c)
			if _, err := NormalizeConsensusParamsUpdate(c); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	// The additions are verified in the update's own message context: a
	// consent signed for another network does not verify.
	c := voterChangeTestUpdate(t)
	other := c
	other.NetworkID = "other"
	c.PendingVoterChange.Additions = []arbiter.VoterEntry{testVoterEntry(t, other, 3, "arbiter-3")}
	if _, err := NormalizeConsensusParamsUpdate(c); err == nil || !strings.Contains(err.Error(), "consent_sig") {
		t.Fatalf("a consent from another network: %v", err)
	}
}
