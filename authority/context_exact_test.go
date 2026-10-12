package authority

import "testing"

// TestEvictNodeErrorsAreUnchangedByTheSharedContextCheck pins the eviction's
// context refusals word for word as arbiter-core v0.13.0 returns them, before
// and after the exact-context check moves into the helper the quarantine and
// release commands share (plan S2-A contract §2).
func TestEvictNodeErrorsAreUnchangedByTheSharedContextCheck(t *testing.T) {
	s, v := newTestPair(t)
	ctx := ConsensusContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", AuthorityEpoch: 2}
	cmd := testEvictCommand()
	token, err := s.SignEvictNodeWithContextAt(cmd, ctx, 1)
	if err != nil {
		t.Fatal(err)
	}
	hash, err := EvictNodeHash(cmd)
	if err != nil {
		t.Fatal(err)
	}
	contextless, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: EvictNodePurpose, CmdHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	nextEpoch := ctx
	nextEpoch.AuthorityEpoch = 3
	for name, tc := range map[string]struct {
		token string
		ctx   ConsensusContext
		want  string
	}{
		"empty network":     {token, ConsensusContext{GenesisSnapshotID: "0xgenesis", AuthorityEpoch: 2}, "evict node: network ID and genesis snapshot ID must be non-empty"},
		"blank genesis":     {token, ConsensusContext{NetworkID: "devnet2", GenesisSnapshotID: " ", AuthorityEpoch: 2}, "evict node: network ID and genesis snapshot ID must be non-empty"},
		"next epoch":        {token, nextEpoch, "evict node token: consensus context mismatch"},
		"contextless token": {contextless, ctx, "evict node token: consensus context mismatch"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := v.VerifyEvictNode(cmd, tc.token, tc.ctx); err == nil || err.Error() != tc.want {
				t.Fatalf("VerifyEvictNode = %v, want %q", err, tc.want)
			}
		})
	}
	if addr, err := v.VerifyEvictNode(cmd, token, ctx); err != nil || addr != s.Address() {
		t.Fatalf("VerifyEvictNode = %q, %v", addr, err)
	}
}
