package authority

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
)

func testRaftAdd() arbiter.RaftMembershipChange {
	return arbiter.RaftMembershipChange{Op: arbiter.RaftMembershipAddVoter, ServerID: "arbiter-3", RaftAddress: "arbiter-3.arbiter:7090"}
}

// TestRaftMembershipHashGolden pins the signed form: the canonical JSON and
// its digest. A change here voids every token an operator signed.
func TestRaftMembershipHashGolden(t *testing.T) {
	for _, tc := range []struct {
		cmd  arbiter.RaftMembershipChange
		json string
		hash string
	}{
		{testRaftAdd(), `{"op":"add_voter","server_id":"arbiter-3","raft_address":"arbiter-3.arbiter:7090"}`, raftAddGoldenHash},
		{arbiter.RaftMembershipChange{Op: arbiter.RaftMembershipRemoveServer, ServerID: "arbiter-1"}, `{"op":"remove_server","server_id":"arbiter-1","raft_address":""}`, raftRemoveGoldenHash},
	} {
		raw, err := json.Marshal(tc.cmd)
		if err != nil {
			t.Fatal(err)
		}
		if string(raw) != tc.json {
			t.Fatalf("canonical JSON = %s, want %s", raw, tc.json)
		}
		h, err := RaftMembershipHash(tc.cmd)
		if err != nil {
			t.Fatal(err)
		}
		if h != tc.hash {
			t.Fatalf("%s hash = %s, want %s", tc.cmd.Op, h, tc.hash)
		}
	}
}

// The golden digests are sha256("housegate-replay-mvp-v0:" +
// "arbiter-raft-membership-command-v1" + "\x00" + canonical JSON), computed
// independently of this package.
const (
	raftAddGoldenHash    = "0xfcbddece3622c464948e96d7d92dd063a743da920f2d307bef08dd581e46a02f"
	raftRemoveGoldenHash = "0xb633aaa0c8faef306b1107f9ddd1de52e2880cf490d851f057149471eaa3f166"
)

func TestRaftMembershipIsContextBoundAndPurposeSeparated(t *testing.T) {
	s, v := newTestPair(t)
	ctx := ConsensusContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", AuthorityEpoch: 7}
	cmd := testRaftAdd()
	old, err := s.SignRaftMembershipWithContextAt(cmd, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if addr, err := v.VerifyRaftMembership(cmd, old, ctx); err != nil || addr != s.Address() {
		t.Fatalf("deterministic verify: %q, %v", addr, err)
	}
	if _, err := v.AuthorizeRaftMembership(cmd, old, ctx); err == nil {
		t.Fatal("the API boundary accepted a stale membership token")
	}
	fresh, err := s.SignRaftMembershipWithContext(cmd, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeRaftMembership(cmd, fresh, ctx); err != nil {
		t.Fatalf("fresh token: %v", err)
	}
	h, err := RaftMembershipHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	contextless, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: RaftMembershipPurpose, CmdHash: h})
	if err != nil {
		t.Fatal(err)
	}
	asEviction, err := s.signWithContextPurpose(h, EvictNodePurpose, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	asUpdate, err := s.signWithContextPurpose(h, ConsensusParamsUpdatePurpose, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	nextEpoch, otherNetwork, otherGenesis := ctx, ctx, ctx
	nextEpoch.AuthorityEpoch++
	otherNetwork.NetworkID = "mainnet"
	otherGenesis.GenesisSnapshotID = "0xother"
	otherID, otherAddr, asRemove := cmd, cmd, cmd
	otherID.ServerID = "arbiter-4"
	otherAddr.RaftAddress = "evil:7090"
	asRemove.Op, asRemove.RaftAddress = arbiter.RaftMembershipRemoveServer, ""
	unlisted := &Validator{AllowedAddresses: map[string]bool{"0x0000000000000000000000000000000000000001": true}}
	verify := func(c arbiter.RaftMembershipChange, token string, cc ConsensusContext) error {
		_, err := v.VerifyRaftMembership(c, token, cc)
		return err
	}
	_, unlistedErr := unlisted.VerifyRaftMembership(cmd, old, ctx)
	for name, err := range map[string]error{
		"next authority epoch":        verify(cmd, old, nextEpoch),
		"other network":               verify(cmd, old, otherNetwork),
		"other genesis":               verify(cmd, old, otherGenesis),
		"other server":                verify(otherID, old, ctx),
		"other address":               verify(otherAddr, old, ctx),
		"the opposite op":             verify(asRemove, old, ctx),
		"unlisted signer":             unlistedErr,
		"an eviction token":           verify(cmd, asEviction, ctx),
		"an update token":             verify(cmd, asUpdate, ctx),
		"a token without its context": verify(cmd, contextless, ctx),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	evict := arbiter.EvictNodeCommand{NodeID: "arbiter-3", ExpectedRegistrationSeq: 1, Reason: "r"}
	if _, err := v.VerifyEvictNode(evict, old, ctx); err == nil {
		t.Fatal("a membership token authorized an eviction")
	}
}

func TestRaftMembershipHashRefusesMalformedChanges(t *testing.T) {
	for name, bad := range map[string]arbiter.RaftMembershipChange{
		"no op":            {ServerID: "a", RaftAddress: "a:1"},
		"unknown op":       {Op: "demote_voter", ServerID: "a"},
		"no server":        {Op: arbiter.RaftMembershipRemoveServer},
		"padded server":    {Op: arbiter.RaftMembershipRemoveServer, ServerID: " a"},
		"add no address":   {Op: arbiter.RaftMembershipAddVoter, ServerID: "a"},
		"add padded addr":  {Op: arbiter.RaftMembershipAddVoter, ServerID: "a", RaftAddress: "a:1 "},
		"remove with addr": {Op: arbiter.RaftMembershipRemoveServer, ServerID: "a", RaftAddress: "a:1"},
	} {
		if _, err := RaftMembershipHash(bad); err == nil {
			t.Fatalf("%s: hashed %+v", name, bad)
		}
	}
}

// TestRaftMembershipThreshold checks a k-of-n token: every component bound to
// the same context, k distinct allowlisted signers.
func TestRaftMembershipThreshold(t *testing.T) {
	s := sortedSigners(t, 3)
	cmd := testRaftAdd()
	ctx := ConsensusContext{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, AuthorityEpoch: 3}
	sign := func(s *Signer) string {
		tok, err := s.SignRaftMembershipWithContext(cmd, ctx)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	v := Validator{AllowedAddresses: allowOf(s...), Threshold: 2, MaxTokenAge: time.Minute}
	if _, err := v.AuthorizeRaftMembership(cmd, sign(s[0]), ctx); err == nil {
		t.Fatal("one signature met a threshold of 2")
	}
	two, err := CombineTokens(sign(s[2]), sign(s[0]))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeRaftMembership(cmd, two, ctx); err != nil {
		t.Fatalf("2 of 3: %v", err)
	}
	if !strings.Contains(two, TokenSetSeparator) {
		t.Fatal("combined token is not a token set")
	}
}
