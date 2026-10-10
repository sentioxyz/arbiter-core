package authority

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func testEvictCommand() arbiter.EvictNodeCommand {
	return arbiter.EvictNodeCommand{NodeID: "snode-2", ExpectedRegistrationSeq: 1760054400000, Reason: "key compromised"}
}

func TestEvictNodeIsContextBoundAndPurposeSeparated(t *testing.T) {
	s, v := newTestPair(t)
	ctx := ConsensusContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", AuthorityEpoch: 2}
	cmd := testEvictCommand()
	old, err := s.SignEvictNodeWithContextAt(cmd, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	if addr, err := v.VerifyEvictNode(cmd, old, ctx); err != nil || addr != s.Address() {
		t.Fatalf("deterministic verify: %q, %v", addr, err)
	}
	if _, err := v.AuthorizeEvictNode(cmd, old, ctx); err == nil {
		t.Fatal("the API boundary accepted a stale eviction token")
	}
	fresh, err := s.SignEvictNodeWithContext(cmd, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeEvictNode(cmd, fresh, ctx); err != nil {
		t.Fatalf("fresh token: %v", err)
	}
	promotion, err := s.SignPromotionWithContextAt(testCmd(), ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	evictHash, err := EvictNodeHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	contextless, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: EvictNodePurpose, CmdHash: evictHash})
	if err != nil {
		t.Fatal(err)
	}
	nextEpoch, otherNetwork, otherGenesis := ctx, ctx, ctx
	nextEpoch.AuthorityEpoch = 3
	otherNetwork.NetworkID = "mainnet"
	otherGenesis.GenesisSnapshotID = "0xother"
	otherSeq, otherNode, otherReason := cmd, cmd, cmd
	otherSeq.ExpectedRegistrationSeq++
	otherNode.NodeID = "snode-1"
	otherReason.Reason = "other"
	unlisted := &Validator{AllowedAddresses: map[string]bool{"0x0000000000000000000000000000000000000001": true}}
	verify := func(c arbiter.EvictNodeCommand, token string, cc ConsensusContext) error {
		_, err := v.VerifyEvictNode(c, token, cc)
		return err
	}
	_, unlistedErr := unlisted.VerifyEvictNode(cmd, old, ctx)
	for name, err := range map[string]error{
		"next authority epoch":        verify(cmd, old, nextEpoch),
		"other network":               verify(cmd, old, otherNetwork),
		"other genesis":               verify(cmd, old, otherGenesis),
		"other expected seq":          verify(otherSeq, old, ctx),
		"other node":                  verify(otherNode, old, ctx),
		"other reason":                verify(otherReason, old, ctx),
		"unlisted signer":             unlistedErr,
		"a promotion token":           verify(cmd, promotion, ctx),
		"a token without its context": verify(cmd, contextless, ctx),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	if _, err := v.AuthorizePromotion(testCmd(), fresh); err == nil {
		t.Fatal("an eviction token authorized a promotion")
	}
	for _, bad := range []arbiter.EvictNodeCommand{{Reason: "r"}, {NodeID: "n"}, {NodeID: " ", Reason: "r"}} {
		if _, err := EvictNodeHash(bad); err == nil {
			t.Fatalf("hashed %+v", bad)
		}
	}
}
