package authority

import (
	"slices"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// delayTestUpdate is testConsensusUpdate with the table registry, k = 2 and
// the authority change delay set.
func delayTestUpdate() arbiter.ConsensusParamsUpdate {
	u := testConsensusUpdate()
	u.TableRegistry = validRegistryParams()
	u.AuthorityThreshold = 2
	u.AuthorityChangeDelayL2Blocks = arbiter.RecommendedAuthorityChangeDelayL2Blocks
	return u
}

func pendingChange() *arbiter.PendingAuthorityChange {
	return &arbiter.PendingAuthorityChange{
		AuthorityAddresses:           []string{"0xDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDDD", "0xcccccccccccccccccccccccccccccccccccccccc", "0xcccccccccccccccccccccccccccccccccccccccc"},
		AuthorityThreshold:           2,
		AuthorityChangeDelayL2Blocks: arbiter.MinAuthorityChangeDelayL2Blocks,
		NotBeforeL2Block:             5_100_000,
	}
}

// TestNormalizeConsensusParamsUpdateAuthorityChangeDelay (arbiter#173 Slice
// 3): the delay needs the floor, k >= 2 and the table registry in the same
// update; a pending change needs the delay, a complete normalized target
// with 2 <= k <= its set, a target delay at the floor, a not_before block,
// and no target address in the operations set. Unset, the hash is unchanged.
func TestNormalizeConsensusParamsUpdateAuthorityChangeDelay(t *testing.T) {
	base := testConsensusUpdate()
	base.TableRegistry = validRegistryParams()
	base.AuthorityThreshold = 2
	before, err := ConsensusParamsUpdateHash(base)
	if err != nil {
		t.Fatal(err)
	}
	u := delayTestUpdate()
	if h, err := ConsensusParamsUpdateHash(u); err != nil || h == before {
		t.Fatalf("the delay is not bound by the hash: %s, %v", h, err)
	}
	u.PendingAuthorityChange = pendingChange()
	original := slices.Clone(u.PendingAuthorityChange.AuthorityAddresses)
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"0xcccccccccccccccccccccccccccccccccccccccc", "0xdddddddddddddddddddddddddddddddddddddddd"}; !slices.Equal(got.PendingAuthorityChange.AuthorityAddresses, want) {
		t.Fatalf("normalized target = %v", got.PendingAuthorityChange.AuthorityAddresses)
	}
	if !slices.Equal(u.PendingAuthorityChange.AuthorityAddresses, original) {
		t.Fatal("normalization mutated the caller's pending change")
	}

	for name, tc := range map[string]struct {
		mutate func(*arbiter.ConsensusParamsUpdate)
		want   string
	}{
		"below the floor": {func(c *arbiter.ConsensusParamsUpdate) {
			c.AuthorityChangeDelayL2Blocks = arbiter.MinAuthorityChangeDelayL2Blocks - 1
		}, "below the floor of 3600"},
		"k = 1":                 {func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityThreshold = 1 }, "authority_threshold of at least 2"},
		"no table registry":     {func(c *arbiter.ConsensusParamsUpdate) { c.TableRegistry = nil }, "requires table_registry"},
		"pending without delay": {func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityChangeDelayL2Blocks = 0 }, "requires authority_change_delay_l2_blocks"},
		"empty target":          {func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange.AuthorityAddresses = nil }, "complete target"},
		"target k = 1":          {func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange.AuthorityThreshold = 1 }, "at least 2"},
		"target k above set":    {func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange.AuthorityThreshold = 3 }, "at most its 2"},
		"target delay low":      {func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange.AuthorityChangeDelayL2Blocks = 1 }, "below the floor"},
		"no not_before":         {func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange.NotBeforeL2Block = 0 }, "not_before_l2_block"},
		"malformed target":      {func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange.AuthorityAddresses = []string{"0x12"} }, "pending authority change address"},
		"target is an operations key": {func(c *arbiter.ConsensusParamsUpdate) {
			c.OperationsAuthorityAddresses = []string{"0xcccccccccccccccccccccccccccccccccccccccc"}
		}, "disjoint"},
	} {
		t.Run(name, func(t *testing.T) {
			bad := u
			p := *u.PendingAuthorityChange
			p.AuthorityAddresses = slices.Clone(p.AuthorityAddresses)
			bad.PendingAuthorityChange = &p
			tc.mutate(&bad)
			if _, err := NormalizeConsensusParamsUpdate(bad); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestConsensusUpdateBindsThePendingChange: a token does not verify once any
// field of the pending change, or the delay, changes.
func TestConsensusUpdateBindsThePendingChange(t *testing.T) {
	s, v := newTestPair(t)
	cmd := delayTestUpdate()
	cmd.PendingAuthorityChange = pendingChange()
	token, err := s.SignConsensusParamsUpdateAt(cmd, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyConsensusParamsUpdate(cmd, token); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*arbiter.ConsensusParamsUpdate){
		"cancelled": func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange = nil },
		"target": func(c *arbiter.ConsensusParamsUpdate) {
			c.PendingAuthorityChange.AuthorityAddresses[0] = "0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
		},
		"k":          func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange.AuthorityThreshold = 1 },
		"not_before": func(c *arbiter.ConsensusParamsUpdate) { c.PendingAuthorityChange.NotBeforeL2Block++ },
		"delay":      func(c *arbiter.ConsensusParamsUpdate) { c.AuthorityChangeDelayL2Blocks++ },
	} {
		mutated := cmd
		p := *cmd.PendingAuthorityChange
		p.AuthorityAddresses = slices.Clone(p.AuthorityAddresses)
		mutated.PendingAuthorityChange = &p
		mutate(&mutated)
		if _, err := v.VerifyConsensusParamsUpdate(mutated, token); err == nil {
			t.Fatalf("%s change was accepted", name)
		}
	}
}
