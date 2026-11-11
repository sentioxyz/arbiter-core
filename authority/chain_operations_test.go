package authority

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"
)

// opsRotation is rotation with an operations authority set.
func opsRotation(t *testing.T, signer *Signer, epoch uint64, governance []*Signer, ops ...*Signer) SignedConsensusUpdate {
	t.Helper()
	su := rotation(t, signer, epoch, governance...)
	for _, o := range ops {
		su.Update.OperationsAuthorityAddresses = append(su.Update.OperationsAuthorityAddresses, o.Address())
	}
	jws, err := signer.SignConsensusParamsUpdateAt(su.Update, 1)
	if err != nil {
		t.Fatal(err)
	}
	su.AuthorityJWS = jws
	return su
}

func lower(signers ...*Signer) []string {
	out := make([]string, len(signers))
	for i, s := range signers {
		out[i] = strings.ToLower(s.Address())
	}
	slices.Sort(out)
	return out
}

// TestAuthorityChainFollowsOperationsAuthority (arbiter#173 Slice 2): before
// the operations set, operational tokens verify against the authority set;
// the update that sets it moves operational verification to it alone; the
// set is one-way, and governance alone changes or revokes it.
func TestAuthorityChainFollowsOperationsAuthority(t *testing.T) {
	g, g2, o1, o2 := chainSigner(t), chainSigner(t), chainSigner(t), chainSigner(t)
	chain := newChain(t, 0, g)
	if chain.OperationsAddresses != nil || !slices.Equal(chain.OperationalAddresses(), lower(g)) {
		t.Fatalf("before the set: %+v", chain)
	}
	promote := func(c AuthorityChain, s *Signer) error {
		t.Helper()
		tok, err := s.SignPromotionWithContext(testCmd(), c.Context())
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Validator(time.Minute).AuthorizePromotionInContext(testCmd(), tok, c.Context())
		return err
	}
	if err := promote(chain, g); err != nil {
		t.Fatalf("governance key before the set: %v", err)
	}

	// The online keys o1, o2 move into the operations set; governance is g2.
	activated, err := chain.Extend(0, []SignedConsensusUpdate{opsRotation(t, g, 0, []*Signer{g2}, o1, o2)})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(activated.OperationsAddresses, lower(o1, o2)) || !slices.Equal(activated.Addresses, lower(g2)) {
		t.Fatalf("activated = %+v", activated)
	}
	if err := activated.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := promote(activated, o1); err != nil {
		t.Fatalf("operations key: %v", err)
	}
	if err := promote(activated, g2); err == nil || !strings.Contains(err.Error(), "not in allowlist") {
		t.Fatalf("governance key after the set: %v", err)
	}

	// One-way: an update that omits the set is refused.
	if _, err := activated.Extend(1, []SignedConsensusUpdate{rotation(t, g2, 1, g2)}); err == nil || !strings.Contains(err.Error(), "omits operations_authority_addresses") {
		t.Fatalf("omitted set: %v", err)
	}
	// An operations key cannot sign a consensus update.
	if _, err := activated.Extend(1, []SignedConsensusUpdate{opsRotation(t, o1, 1, []*Signer{g2}, o1)}); err == nil {
		t.Fatal("an operations key signed a consensus update")
	}
	// Revocation by governance: o2 leaves the set.
	revoked, err := activated.Extend(1, []SignedConsensusUpdate{opsRotation(t, g2, 1, []*Signer{g2}, o1)})
	if err != nil {
		t.Fatal(err)
	}
	if err := promote(revoked, o2); err == nil {
		t.Fatal("a revoked operations key still signs")
	}
	if err := promote(revoked, o1); err != nil {
		t.Fatal(err)
	}
	// An overlapping update cannot even be signed (the normalizer refuses
	// it), and the chain refuses it when an Arbiter serves it.
	overlap := rotation(t, g2, 2, g2)
	overlap.Update.OperationsAuthorityAddresses = []string{g2.Address()}
	if _, err := g2.SignConsensusParamsUpdateAt(overlap.Update, 1); err == nil || !strings.Contains(err.Error(), "disjoint") {
		t.Fatalf("signing an overlap: %v", err)
	}
	if _, err := revoked.Extend(2, []SignedConsensusUpdate{overlap}); err == nil || !strings.Contains(err.Error(), "disjoint") {
		t.Fatalf("overlap: %v", err)
	}
}

// TestAuthorityChainOperationsAnchorAndPersistence: an anchor may name the
// operations set in force at its epoch (disjoint, normalized), SameAnchor
// compares it, and a chain persisted before the field existed reads as nil.
func TestAuthorityChainOperationsAnchorAndPersistence(t *testing.T) {
	g, o := chainSigner(t), chainSigner(t)
	anchor := AuthorityAnchor{NetworkID: chainNetwork, GenesisSnapshotID: chainGenesis, Epoch: 7,
		Addresses: []string{g.Address()}, OperationsAddresses: []string{strings.ToUpper("0x" + o.Address()[2:])}}
	c, err := NewAuthorityChain(anchor)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.OperationsAddresses, lower(o)) || !slices.Equal(c.OperationalAddresses(), lower(o)) || !c.SameAnchor(anchor) {
		t.Fatalf("chain = %+v", c)
	}
	plain := anchor
	plain.OperationsAddresses = nil
	if c.SameAnchor(plain) {
		t.Fatal("an anchor without the operations set is the same anchor")
	}
	overlap := anchor
	overlap.OperationsAddresses = []string{g.Address()}
	if _, err := NewAuthorityChain(overlap); err == nil || !strings.Contains(err.Error(), "disjoint") {
		t.Fatalf("overlapping anchor: %v", err)
	}
	b, err := json.Marshal(newChain(t, 0, g))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "operations_addresses") {
		t.Fatalf("a chain without the set persists it: %s", b)
	}
	var old AuthorityChain
	if err := json.Unmarshal(b, &old); err != nil || old.OperationsAddresses != nil || old.Validate() != nil {
		t.Fatalf("pre-slice chain: %+v, %v", old, err)
	}
}
