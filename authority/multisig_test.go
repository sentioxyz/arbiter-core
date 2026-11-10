package authority

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// sortedSigners returns n fresh signers ordered by ascending address, the
// canonical component order of a multi-signature token.
func sortedSigners(t *testing.T, n int) []*Signer {
	t.Helper()
	out := make([]*Signer, n)
	for i := range out {
		out[i] = chainSigner(t)
	}
	slices.SortFunc(out, func(a, b *Signer) int {
		return strings.Compare(strings.ToLower(a.Address()), strings.ToLower(b.Address()))
	})
	return out
}

func allowOf(signers ...*Signer) map[string]bool {
	m := map[string]bool{}
	for _, s := range signers {
		m[strings.ToLower(s.Address())] = true
	}
	return m
}

func thresholdUpdate() arbiter.ConsensusParamsUpdate {
	return arbiter.ConsensusParamsUpdate{
		NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, ExpectedEpoch: 4,
		PreviousParamsDigest: "0xprev", MaxWriters: 1, ExpectedPromotionSeq: 40,
		AuthorityAddresses: []string{"0x00000000000000000000000000000000000000aa"},
	}
}

func signUpdate(t *testing.T, u arbiter.ConsensusParamsUpdate, signers ...*Signer) []string {
	t.Helper()
	out := make([]string, len(signers))
	for i, s := range signers {
		tok, err := s.SignConsensusParamsUpdate(u)
		if err != nil {
			t.Fatal(err)
		}
		out[i] = tok
	}
	return out
}

func TestThresholdOneKeepsTheSingleTokenForm(t *testing.T) {
	s := sortedSigners(t, 2)
	u := thresholdUpdate()
	toks := signUpdate(t, u, s...)
	for _, threshold := range []uint32{0, 1} {
		v := Validator{AllowedAddresses: allowOf(s...), Threshold: threshold}
		got, err := v.VerifyConsensusParamsUpdate(u, toks[1])
		if err != nil || got != strings.ToLower(s[1].Address()) {
			t.Fatalf("threshold %d single token: %q, %v", threshold, got, err)
		}
		// A multi-signature token is refused at threshold 0 and 1 even though
		// each component would verify: a k = 1 network accepts exactly what a
		// pre-threshold binary accepts.
		if _, err := v.VerifyConsensusParamsUpdate(u, toks[0]+TokenSetSeparator+toks[1]); err == nil || !strings.Contains(err.Error(), "not accepted at authority threshold 1") {
			t.Fatalf("threshold %d multi token: %v", threshold, err)
		}
	}
}

func TestThresholdMultiSignatureRules(t *testing.T) {
	s := sortedSigners(t, 4)
	outsider := chainSigner(t)
	set := allowOf(s[:3]...)
	u := thresholdUpdate()
	toks := signUpdate(t, u, s...)
	join := func(parts ...string) string { return strings.Join(parts, TokenSetSeparator) }
	v := Validator{AllowedAddresses: set, Threshold: 2}

	got, err := v.VerifyConsensusParamsUpdate(u, join(toks[0], toks[2]))
	if err != nil || got != strings.ToLower(s[0].Address()) {
		t.Fatalf("2 of 3: %q, %v", got, err)
	}
	if _, err := v.VerifyConsensusParamsUpdate(u, join(toks[0], toks[1], toks[2])); err != nil {
		t.Fatalf("3 of 3 at k=2: %v", err)
	}
	other := thresholdUpdate()
	other.MaxWriters = 2
	otherTok := signUpdate(t, other, s[1])[0]
	outsiderTok := signUpdate(t, u, outsider)[0]
	for name, token := range map[string]string{
		"one signature":         toks[0],
		"descending order":      join(toks[2], toks[0]),
		"repeated signer":       join(toks[0], toks[0]),
		"non-member":            join(toks[0], outsiderTok),
		"other command":         join(toks[0], otherTok),
		"empty component":       join(toks[0], ""),
		"more than the set":     join(toks[0], toks[1], toks[2], toks[3]),
		"garbage component":     join(toks[0], "a.b.c"),
		"trailing separator":    join(toks[0], toks[1]) + TokenSetSeparator,
		"single, member, k=2":   toks[1],
		"non-member, one sig":   outsiderTok,
		"two, one non-member":   join(toks[1], toks[3]),
		"two, other command":    join(toks[0], signUpdate(t, other, s[2])[0]),
		"distinct but unsorted": join(toks[1], toks[0]),
	} {
		if _, err := v.VerifyConsensusParamsUpdate(u, token); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	// A threshold above the allowlist size fails closed.
	if _, err := (&Validator{AllowedAddresses: allowOf(s[0]), Threshold: 2}).VerifyConsensusParamsUpdate(u, join(toks[0], toks[1])); err == nil {
		t.Fatal("threshold above the allowlist accepted")
	}
}

func TestThresholdContextBoundComponents(t *testing.T) {
	s := sortedSigners(t, 2)
	cmd := arbiter.EvictNodeCommand{NodeID: "snode-9", ExpectedRegistrationSeq: 7, Reason: "test"}
	ctx := ConsensusContext{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, AuthorityEpoch: 3}
	sign := func(s *Signer, c ConsensusContext) string {
		tok, err := s.SignEvictNodeWithContext(cmd, c)
		if err != nil {
			t.Fatal(err)
		}
		return tok
	}
	v := Validator{AllowedAddresses: allowOf(s...), Threshold: 2, MaxTokenAge: time.Minute}
	good := sign(s[0], ctx) + TokenSetSeparator + sign(s[1], ctx)
	if _, err := v.VerifyEvictNode(cmd, good, ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := v.AuthorizeEvictNode(cmd, good, ctx); err != nil {
		t.Fatal(err)
	}
	stale := ctx
	stale.AuthorityEpoch = 2
	bad := sign(s[0], ctx) + TokenSetSeparator + sign(s[1], stale)
	if _, err := v.VerifyEvictNode(cmd, bad, ctx); err == nil || !strings.Contains(err.Error(), "context mismatch") {
		t.Fatalf("one component of another epoch: %v", err)
	}
}

func TestThresholdQueryProfileActivation(t *testing.T) {
	s := sortedSigners(t, 2)
	p := testActivation()
	var toks []string
	for _, signer := range s {
		tok, err := signer.SignQueryProfileActivation(p)
		if err != nil {
			t.Fatal(err)
		}
		toks = append(toks, tok)
	}
	v := Validator{AllowedAddresses: allowOf(s...), Threshold: 2}
	if _, err := v.VerifyQueryProfileActivation(p, strings.Join(toks, TokenSetSeparator)); err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyQueryProfileActivation(p, toks[0]); err == nil {
		t.Fatal("one signature accepted at k=2")
	}
}

func TestArtifactDispositionRefusesAThreshold(t *testing.T) {
	s := chainSigner(t)
	v := Validator{AllowedAddresses: allowOf(s), Threshold: 2}
	if _, err := v.VerifyArtifactDispositionCommand(wire.ArtifactDispositionCommandV1{}, "x.y.z"); err == nil || !strings.Contains(err.Error(), "single-actor") {
		t.Fatalf("err = %v", err)
	}
}

func TestCombineTokensAndTokenSigners(t *testing.T) {
	s := sortedSigners(t, 3)
	u := thresholdUpdate()
	toks := signUpdate(t, u, s...)
	combined, err := CombineTokens(toks[2], toks[0], toks[1])
	if err != nil {
		t.Fatal(err)
	}
	if combined != strings.Join(toks, TokenSetSeparator) {
		t.Fatal("CombineTokens did not order by ascending signer")
	}
	signers, err := TokenSigners(combined)
	if err != nil {
		t.Fatal(err)
	}
	for i := range s {
		if signers[i] != strings.ToLower(s[i].Address()) {
			t.Fatalf("signers = %v", signers)
		}
	}
	if one, err := CombineTokens(toks[1]); err != nil || one != toks[1] {
		t.Fatalf("one token: %q, %v", one, err)
	}
	if _, err := CombineTokens(toks[0], toks[0]); err == nil {
		t.Fatal("repeated signer combined")
	}
	if _, err := CombineTokens(combined); err == nil {
		t.Fatal("an already combined token was accepted as one component")
	}
	if _, err := CombineTokens(); err == nil {
		t.Fatal("no tokens combined")
	}
	if epoch, ok := TokenAuthorityEpoch(combined); ok {
		t.Fatalf("a context-free update token reported epoch %d", epoch)
	}
}

func TestConsensusUpdateAuthorityThreshold(t *testing.T) {
	u := thresholdUpdate()
	base, err := ConsensusParamsUpdateHash(u)
	if err != nil {
		t.Fatal(err)
	}
	// Zero is omitted from the canonical form: the hash is unchanged.
	u.AuthorityThreshold = 0
	if again, _ := ConsensusParamsUpdateHash(u); again != base {
		t.Fatal("a zero authority_threshold changed the update hash")
	}
	u.AuthorityThreshold = 1
	if one, err := ConsensusParamsUpdateHash(u); err != nil || one == base {
		t.Fatalf("authority_threshold 1 must be signed: %v", err)
	}
	u.AuthorityThreshold = 2
	if _, err := NormalizeConsensusParamsUpdate(u); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("threshold above the set: %v", err)
	}
	u.AuthorityAddresses = append(u.AuthorityAddresses, "0x00000000000000000000000000000000000000bb")
	if n, err := NormalizeConsensusParamsUpdate(u); err != nil || n.AuthorityThreshold != 2 {
		t.Fatalf("2 of 2: %+v, %v", n, err)
	}
	// Duplicates collapse before the size check.
	u.AuthorityAddresses = []string{"0x00000000000000000000000000000000000000BB", "0x00000000000000000000000000000000000000bb"}
	if _, err := NormalizeConsensusParamsUpdate(u); err == nil {
		t.Fatal("a duplicated address counted twice toward the threshold")
	}
}

func TestAuthorityChainFollowsThreshold(t *testing.T) {
	s := sortedSigners(t, 3)
	chain := newChain(t, 0, s...)
	sign := func(u arbiter.ConsensusParamsUpdate, by ...*Signer) SignedConsensusUpdate {
		toks := signUpdate(t, u, by...)
		tok, err := CombineTokens(toks...)
		if err != nil {
			t.Fatal(err)
		}
		return SignedConsensusUpdate{Update: u, AuthorityJWS: tok}
	}
	upd := func(epoch uint64, threshold uint32) arbiter.ConsensusParamsUpdate {
		u := arbiter.ConsensusParamsUpdate{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, ExpectedEpoch: epoch,
			PreviousParamsDigest: "0xprev", MaxWriters: 1, ExpectedPromotionSeq: epoch * 10, AuthorityThreshold: threshold}
		for _, x := range s {
			u.AuthorityAddresses = append(u.AuthorityAddresses, x.Address())
		}
		return u
	}
	// Epoch 0 -> 1 sets k = 2 under the old k = 1 (one signature).
	activation := sign(upd(0, 2), s[1])
	got, err := chain.Extend(0, []SignedConsensusUpdate{activation})
	if err != nil || got.Threshold != 2 || got.Epoch != 1 {
		t.Fatalf("activation: %+v, %v", got, err)
	}
	// The next update needs two signatures.
	if _, err := got.Extend(1, []SignedConsensusUpdate{sign(upd(1, 2), s[0])}); err == nil {
		t.Fatal("one signature accepted after k = 2")
	}
	// Omitting the threshold after it was set is refused.
	if _, err := got.Extend(1, []SignedConsensusUpdate{sign(upd(1, 0), s[0], s[2])}); err == nil || !strings.Contains(err.Error(), "omits authority_threshold") {
		t.Fatalf("carry: %v", err)
	}
	// Lowering to 1 needs the old k = 2.
	lowered, err := got.Extend(1, []SignedConsensusUpdate{sign(upd(1, 1), s[0], s[2])})
	if err != nil || lowered.Threshold != 1 {
		t.Fatalf("lower: %+v, %v", lowered, err)
	}
	if err := lowered.Validate(); err != nil {
		t.Fatal(err)
	}
	// Promotions on the SNode stay single-signature.
	if v := lowered.Validator(time.Minute); v.Threshold != 1 {
		t.Fatalf("operational validator threshold = %d", v.Threshold)
	}
	// An anchor after the activation names its threshold and changes the anchor.
	anchor := AuthorityAnchor{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, Epoch: 1, Addresses: []string{s[0].Address(), s[1].Address(), s[2].Address()}, Threshold: 2}
	c2, err := NewAuthorityChain(anchor)
	if err != nil || c2.Threshold != 2 || chain.SameAnchor(anchor) {
		t.Fatalf("anchor: %+v, %v", c2, err)
	}
	if _, err := c2.Extend(1, []SignedConsensusUpdate{sign(upd(1, 2), s[1])}); err == nil {
		t.Fatal("anchored chain accepted one signature at k = 2")
	}
	anchor.Threshold = 4
	if _, err := NewAuthorityChain(anchor); err == nil {
		t.Fatal("anchor threshold above its set accepted")
	}
}
