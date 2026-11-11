package authority

import (
	"slices"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

const (
	opsAddrC = "0xcccccccccccccccccccccccccccccccccccccccc"
	opsAddrD = "0xdddddddddddddddddddddddddddddddddddddddd"
)

// TestNormalizeConsensusParamsUpdateOperationsAuthority (arbiter#173 Slice
// 2): the operations set is normalized like the governance set, an empty set
// stays nil (off the canonical form, so every earlier hash is unchanged), and
// an address in both sets is refused.
func TestNormalizeConsensusParamsUpdateOperationsAuthority(t *testing.T) {
	u := testConsensusUpdate()
	base, err := ConsensusParamsUpdateHash(u)
	if err != nil {
		t.Fatal(err)
	}
	u.OperationsAuthorityAddresses = []string{}
	if h, err := ConsensusParamsUpdateHash(u); err != nil || h != base {
		t.Fatalf("an empty operations set changed the hash: %s, %v", h, err)
	}
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.OperationsAuthorityAddresses != nil {
		t.Fatalf("empty set normalized to %#v, %v", got.OperationsAuthorityAddresses, err)
	}

	u.OperationsAuthorityAddresses = []string{strings.ToUpper("0x" + opsAddrD[2:]), opsAddrC, opsAddrD}
	original := slices.Clone(u.OperationsAuthorityAddresses)
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || !slices.Equal(got.OperationsAuthorityAddresses, []string{opsAddrC, opsAddrD}) {
		t.Fatalf("normalized = %v, %v", got.OperationsAuthorityAddresses, err)
	}
	if !slices.Equal(u.OperationsAuthorityAddresses, original) {
		t.Fatal("normalization mutated the caller's slice")
	}
	h, err := ConsensusParamsUpdateHash(u)
	if err != nil || h == base {
		t.Fatalf("the operations set is not bound by the hash: %s, %v", h, err)
	}
	reordered := u
	reordered.OperationsAuthorityAddresses = []string{opsAddrD, opsAddrC}
	if h2, err := ConsensusParamsUpdateHash(reordered); err != nil || h2 != h {
		t.Fatalf("hash depends on order or case: %s vs %s, %v", h2, h, err)
	}

	for name, tc := range map[string]struct {
		addrs []string
		want  string
	}{
		"overlap": {[]string{opsAddrC, strings.ToUpper("0x" + u.AuthorityAddresses[0][2:])},
			"consensus params update: operations authority address " + u.AuthorityAddresses[0] + " is also an authority address; the operations and governance sets must be disjoint"},
		"malformed": {[]string{"0x1234"}, `consensus params update: operations authority address "0x1234" must be a 0x-prefixed 20-byte hex address`},
		"zero":      {[]string{"0x0000000000000000000000000000000000000000"}, "consensus params update: zero operations authority address is not allowed"},
	} {
		t.Run(name, func(t *testing.T) {
			bad := u
			bad.OperationsAuthorityAddresses = tc.addrs
			if _, err := NormalizeConsensusParamsUpdate(bad); err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestConsensusUpdateBindsOperationsAuthority: a token over an update does
// not verify once its operations set changes.
func TestConsensusUpdateBindsOperationsAuthority(t *testing.T) {
	s, v := newTestPair(t)
	cmd := testConsensusUpdate()
	cmd.OperationsAuthorityAddresses = []string{opsAddrC}
	token, err := s.SignConsensusParamsUpdateAt(cmd, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyConsensusParamsUpdate(cmd, token); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*arbiter.ConsensusParamsUpdate){
		"dropped":  func(c *arbiter.ConsensusParamsUpdate) { c.OperationsAuthorityAddresses = nil },
		"replaced": func(c *arbiter.ConsensusParamsUpdate) { c.OperationsAuthorityAddresses = []string{opsAddrD} },
		"added":    func(c *arbiter.ConsensusParamsUpdate) { c.OperationsAuthorityAddresses = []string{opsAddrC, opsAddrD} },
	} {
		mutated := cmd
		mutate(&mutated)
		if _, err := v.VerifyConsensusParamsUpdate(mutated, token); err == nil {
			t.Fatalf("%s operations set was accepted", name)
		}
	}
}
