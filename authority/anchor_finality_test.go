package authority

import (
	"crypto/ed25519"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// testVoterKey is a deterministic ed25519 key for voter i.
func testVoterKey(i byte) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	for j := range seed {
		seed[j] = i + 1
	}
	return ed25519.NewKeyFromSeed(seed)
}

func testVoterEntry(t *testing.T, u arbiter.ConsensusParamsUpdate, i byte, id string) arbiter.VoterEntry {
	t.Helper()
	priv := testVoterKey(i)
	sig, err := SignVoterConsent(priv, MessageContext{NetworkID: u.NetworkID, GenesisSnapshotID: u.GenesisSnapshotID}, id)
	if err != nil {
		t.Fatal(err)
	}
	return arbiter.VoterEntry{VoterID: id, Ed25519Pubkey: priv.Public().(ed25519.PublicKey), ConsentSig: sig}
}

// anchorFinalityTestUpdate is stage3TestUpdate with the promotion finality
// gate, three enrolled voters (unsorted) and a 2-of-3 anchor finality quorum.
func anchorFinalityTestUpdate(t *testing.T) arbiter.ConsensusParamsUpdate {
	t.Helper()
	u := stage3TestUpdate()
	u.PromotionFinalityGate = true
	u.Voters = []arbiter.VoterEntry{testVoterEntry(t, u, 2, "arbiter-2"), testVoterEntry(t, u, 0, "arbiter-0"), testVoterEntry(t, u, 1, "arbiter-1")}
	u.AnchorFinality = &arbiter.AnchorFinalityParams{Quorum: 2, FinalityMode: arbiter.AnchorFinalityModeSafe,
		LastMergeableConfirmations: 1, L2ChainID: 8453, RegistryContract: "0x" + strings.Repeat("AB", 20)}
	return u
}

func TestNormalizeConsensusParamsUpdatePartClaimUniqueness(t *testing.T) {
	u := stage3TestUpdate()
	u.PartClaimUniqueness = true
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || !got.PartClaimUniqueness {
		t.Fatalf("with tracks: %v, %v", err, got.PartClaimUniqueness)
	}
	u = testConsensusUpdate()
	u.PartClaimUniqueness = true
	const want = "consensus params update: part_claim_uniqueness requires tracks_enabled in the same update"
	if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != want {
		t.Fatalf("without tracks: err = %v, want %q", err, want)
	}
}

func TestNormalizeConsensusParamsUpdateAnchorFinality(t *testing.T) {
	u := anchorFinalityTestUpdate(t)
	callerFirst := u.Voters[0].VoterID
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	if u.Voters[0].VoterID != callerFirst {
		t.Fatal("normalization reordered the caller's voters")
	}
	for i, id := range []string{"arbiter-0", "arbiter-1", "arbiter-2"} {
		if got.Voters[i].VoterID != id {
			t.Fatalf("voters not sorted: %+v", got.Voters)
		}
	}
	if got.AnchorFinality.RegistryContract != "0x"+strings.Repeat("ab", 20) {
		t.Fatalf("registry contract not lowercased: %s", got.AnchorFinality.RegistryContract)
	}
	// Voters alone, without the quorum, is an enrolment.
	u.AnchorFinality = nil
	if _, err := NormalizeConsensusParamsUpdate(u); err != nil {
		t.Fatalf("voters without anchor_finality: %v", err)
	}
	for name, tc := range map[string]struct {
		mutate func(*arbiter.ConsensusParamsUpdate)
		want   string
	}{
		"no promotion finality gate": {func(c *arbiter.ConsensusParamsUpdate) { c.PromotionFinalityGate = false }, "anchor_finality requires promotion_finality_gate"},
		"no voters":                  {func(c *arbiter.ConsensusParamsUpdate) { c.Voters = nil }, "needs a non-empty voters set"},
		"minority quorum":            {func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.Quorum = 1 }, "quorum 1 is not a majority of the 3 enrolled voters (2 to 3)"},
		"quorum above n":             {func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.Quorum = 4 }, "quorum 4 is not a majority"},
		"unknown mode":               {func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.FinalityMode = "latest" }, `finality_mode "latest" is not`},
		"confirmations without count": {func(c *arbiter.ConsensusParamsUpdate) {
			c.AnchorFinality.FinalityMode = arbiter.AnchorFinalityModeConfirmations
		}, "needs finality_confirmations above 0"},
		"zero chain":        {func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.L2ChainID = 0 }, "l2_chain_id must be above 0"},
		"bad contract":      {func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.RegistryContract = "0x01" }, "registry_contract must be"},
		"zero contract":     {func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.RegistryContract = zeroAddressForTest }, "registry_contract must be"},
		"duplicate voter":   {func(c *arbiter.ConsensusParamsUpdate) { c.Voters[1].VoterID = c.Voters[0].VoterID }, "consent_sig"},
		"blank voter id":    {func(c *arbiter.ConsensusParamsUpdate) { c.Voters[0].VoterID = "a b" }, "must be non-empty and free of whitespace"},
		"short key":         {func(c *arbiter.ConsensusParamsUpdate) { c.Voters[0].Ed25519Pubkey = []byte{1} }, "ed25519_pubkey must be 32 bytes"},
		"missing consent":   {func(c *arbiter.ConsensusParamsUpdate) { c.Voters[0].ConsentSig = "" }, "consent_sig is required"},
		"consent elsewhere": {func(c *arbiter.ConsensusParamsUpdate) { c.NetworkID += "-other" }, "consent_sig: voter message: consent signature does not verify"},
	} {
		t.Run(name, func(t *testing.T) {
			c := anchorFinalityTestUpdate(t)
			tc.mutate(&c)
			if _, err := NormalizeConsensusParamsUpdate(c); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
	t.Run("duplicate id with its own consent", func(t *testing.T) {
		c := anchorFinalityTestUpdate(t)
		c.Voters[1] = testVoterEntry(t, c, 1, c.Voters[0].VoterID)
		if _, err := NormalizeConsensusParamsUpdate(c); err == nil || !strings.Contains(err.Error(), "duplicate voter_id") {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("shared key", func(t *testing.T) {
		c := anchorFinalityTestUpdate(t)
		c.Voters[1] = testVoterEntry(t, c, 2, "arbiter-x")
		if _, err := NormalizeConsensusParamsUpdate(c); err == nil || !strings.Contains(err.Error(), "reuses another voter's ed25519_pubkey") {
			t.Fatalf("err = %v", err)
		}
	})
}

const zeroAddressForTest = "0x0000000000000000000000000000000000000000"

// TestConsensusUpdateBindsVotersAndAnchorFinality: the authority signature
// covers every new field.
func TestConsensusUpdateBindsVotersAndAnchorFinality(t *testing.T) {
	s, v := newTestPair(t)
	cmd, err := NormalizeConsensusParamsUpdate(anchorFinalityTestUpdate(t))
	if err != nil {
		t.Fatal(err)
	}
	token, err := s.SignConsensusParamsUpdateAt(cmd, 1)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyConsensusParamsUpdate(cmd, token); err != nil {
		t.Fatalf("unchanged update refused: %v", err)
	}
	for name, mutate := range map[string]func(*arbiter.ConsensusParamsUpdate){
		"quorum":  func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.Quorum = 3 },
		"mode":    func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.FinalityMode = arbiter.AnchorFinalityModeFinalized },
		"chain":   func(c *arbiter.ConsensusParamsUpdate) { c.AnchorFinality.L2ChainID++ },
		"voters":  func(c *arbiter.ConsensusParamsUpdate) { c.Voters = c.Voters[:2]; c.AnchorFinality.Quorum = 2 },
		"part uq": func(c *arbiter.ConsensusParamsUpdate) { c.PartClaimUniqueness = true },
	} {
		t.Run(name, func(t *testing.T) {
			c := cmd
			c.Voters = append([]arbiter.VoterEntry(nil), cmd.Voters...)
			af := *cmd.AnchorFinality
			c.AnchorFinality = &af
			mutate(&c)
			if _, err := v.VerifyConsensusParamsUpdate(c, token); err == nil {
				t.Fatal("modified signed field was accepted")
			}
		})
	}
}

func TestAnchorFinalityAttestationSignatures(t *testing.T) {
	ctx := MessageContext{NetworkID: "testnet", GenesisSnapshotID: "snapshot:genesis"}
	policy := arbiter.AnchorFinalityPolicy{FinalityMode: "safe", LastMergeableConfirmations: 1, L2ChainID: 8453, RegistryContract: "0x" + strings.Repeat("ab", 20)}
	priv := testVoterKey(0)
	pub := priv.Public().(ed25519.PublicKey)
	att, err := SignAnchorFinality(priv, ctx, policy, arbiter.AnchorFinalityAttestation{VoterID: "arbiter-0", L3BlockSeq: 7,
		L3BlockHash: "0x" + strings.Repeat("11", 32), StateRoot: "0x" + strings.Repeat("22", 32), L2BlockNumber: 100, Final: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyAnchorFinality(pub, ctx, policy, att); err != nil {
		t.Fatalf("own signature: %v", err)
	}
	// Golden: the hash a voter signs is a consensus constant.
	h, err := VoterMessageHash(VoterMessageAnchorFinality, ctx, NewAnchorFinalityBody(att, policy))
	if err != nil {
		t.Fatal(err)
	}
	const golden = "0x7392e25bed0085f9c563d44a0c00d042e547246c9d45f024421e1e9e9fc0b753"
	if h != golden {
		t.Fatalf("anchor finality hash = %s, want %s", h, golden)
	}
	for name, mutate := range map[string]func(*arbiter.AnchorFinalityAttestation, *arbiter.AnchorFinalityPolicy, *MessageContext){
		"voter":          func(a *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, _ *MessageContext) { a.VoterID = "arbiter-1" },
		"block":          func(a *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, _ *MessageContext) { a.L3BlockSeq++ },
		"hash":           func(a *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, _ *MessageContext) { a.L3BlockHash += "0" },
		"root":           func(a *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, _ *MessageContext) { a.StateRoot += "0" },
		"l2 block":       func(a *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, _ *MessageContext) { a.L2BlockNumber++ },
		"final":          func(a *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, _ *MessageContext) { a.Final = false },
		"last mergeable": func(a *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, _ *MessageContext) { a.LastMergeable = true },
		"mode":           func(_ *arbiter.AnchorFinalityAttestation, p *arbiter.AnchorFinalityPolicy, _ *MessageContext) { p.FinalityMode = "finalized" },
		"confirmations":  func(_ *arbiter.AnchorFinalityAttestation, p *arbiter.AnchorFinalityPolicy, _ *MessageContext) { p.LastMergeableConfirmations++ },
		"chain":          func(_ *arbiter.AnchorFinalityAttestation, p *arbiter.AnchorFinalityPolicy, _ *MessageContext) { p.L2ChainID++ },
		"contract":       func(_ *arbiter.AnchorFinalityAttestation, p *arbiter.AnchorFinalityPolicy, _ *MessageContext) { p.RegistryContract = "0x" + strings.Repeat("cd", 20) },
		"network":        func(_ *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, c *MessageContext) { c.NetworkID += "-other" },
		"genesis":        func(_ *arbiter.AnchorFinalityAttestation, _ *arbiter.AnchorFinalityPolicy, c *MessageContext) { c.GenesisSnapshotID += "-other" },
	} {
		t.Run(name, func(t *testing.T) {
			a, p, c := att, policy, ctx
			mutate(&a, &p, &c)
			if err := VerifyAnchorFinality(pub, c, p, a); err == nil {
				t.Fatal("modified attestation verified")
			}
		})
	}
	if err := VerifyAnchorFinality(testVoterKey(1).Public().(ed25519.PublicKey), ctx, policy, att); err == nil {
		t.Fatal("another key verified the attestation")
	}
	// A consent signature never verifies as an attestation, nor the reverse:
	// the kind is bound.
	consent, err := SignVoterConsent(priv, ctx, "arbiter-0")
	if err != nil {
		t.Fatal(err)
	}
	att.Signature = consent
	if err := VerifyAnchorFinality(pub, ctx, policy, att); err == nil {
		t.Fatal("a consent signature verified as an attestation")
	}
	if _, err := VoterMessageHash(VoterMessageConsent, ctx, NewAnchorFinalityBody(att, policy)); err == nil {
		t.Fatal("a consent hash took an attestation body")
	}
}
