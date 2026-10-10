package authoritytest_test

import (
	"crypto/ed25519"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
)

func TestFixtureKeysMatchTheirAddresses(t *testing.T) {
	for key, want := range map[string]string{
		authoritytest.AuthorityKeyHex: authoritytest.AuthorityAddr,
		authoritytest.IndexerKeyHex0:  authoritytest.IndexerAddr0,
		authoritytest.IndexerKeyHex1:  authoritytest.IndexerAddr1,
		authoritytest.StrangerKeyHex:  authoritytest.StrangerAddr,
	} {
		if got := authoritytest.MustSigner(t, key).Address(); got != want {
			t.Fatalf("address of %s = %s, want %s", key, got, want)
		}
	}
	for i, want := range []string{authoritytest.Verifier1PubkeyHex, authoritytest.Verifier2PubkeyHex, authoritytest.Verifier3PubkeyHex} {
		if got := hex.EncodeToString(authoritytest.VerifierKey(i + 1).Public().(ed25519.PublicKey)); got != want {
			t.Fatalf("verifier %d public key = %s, want %s", i+1, got, want)
		}
	}
}

func TestSNodeEnrollmentVectors(t *testing.T) {
	for i, v := range authoritytest.SNodeEnrollmentVectors {
		if v.Statement.IndexerID != uint64(i) {
			t.Fatalf("vector %d is indexer %d", i, v.Statement.IndexerID)
		}
		if h, err := authority.SNodeEnrollmentHash(v.Statement); err != nil || h != v.Hash {
			t.Fatalf("indexer %d hash = %s, %v; want %s", i, h, err, v.Hash)
		}
		if got, err := authoritytest.MustSigner(t, v.KeyHex).SignSNodeEnrollmentAt(v.Statement, v.Iat); err != nil || got != v.JWS {
			t.Fatalf("indexer %d token = %s, %v; want the pinned token", i, got, err)
		}
		if err := authority.VerifySNodeEnrollment(v.Statement, v.JWS, v.Signer); err != nil {
			t.Fatalf("indexer %d: %v", i, err)
		}
		if err := authority.VerifySNodeEnrollment(v.Statement, v.JWS, authoritytest.StrangerAddr); err == nil {
			t.Fatalf("indexer %d verified under a stranger", i)
		}
	}
}

func TestSNodeMessageVectors(t *testing.T) {
	signer := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
	for _, tc := range []struct {
		kind      authority.SNodeMessageKind
		body      any
		hash, jws string
	}{
		{authority.SNodeMessageRegistration, authoritytest.SNodeRegistration(), authoritytest.SNodeRegistrationHash, authoritytest.SNodeRegistrationJWS},
		{authority.SNodeMessageMarkActive, authoritytest.SNodeMarkActive(), authoritytest.SNodeMarkActiveHash, authoritytest.SNodeMarkActiveJWS},
		{authority.SNodeMessageResultClaim, authoritytest.ResultClaim(), authoritytest.ResultClaimHash, authoritytest.ResultClaimJWS},
		{authority.SNodeMessagePromotionAck, authoritytest.PromotionAck(), authoritytest.PromotionAckHash, authoritytest.PromotionAckJWS},
		{authority.SNodeMessageCleanupAck, authoritytest.CleanupAck(), authoritytest.CleanupAckHash, authoritytest.CleanupAckJWS},
		{authority.SNodeMessageTablePurged, authoritytest.SNodeTablePurged(), authoritytest.SNodeTablePurgedHash, authoritytest.SNodeTablePurgedJWS},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			if h, err := authority.SNodeMessageHash(tc.kind, authoritytest.Context(), tc.body); err != nil || h != tc.hash {
				t.Fatalf("hash = %s, %v; want %s", h, err, tc.hash)
			}
			if got, err := signer.SignSNodeMessageAt(tc.kind, authoritytest.Context(), tc.body, authoritytest.Iat); err != nil || got != tc.jws {
				t.Fatalf("token = %s, %v; want the pinned token", got, err)
			}
			if err := authority.VerifySNodeMessage(tc.kind, authoritytest.Context(), tc.body, tc.jws, authoritytest.IndexerAddr1); err != nil {
				t.Fatal(err)
			}
			if err := authority.VerifySNodeMessage(tc.kind, authoritytest.Context(), tc.body, tc.jws, authoritytest.IndexerAddr0); err == nil {
				t.Fatal("verified under indexer 0's signer")
			}
		})
	}
}

func TestVerifierMessageVectors(t *testing.T) {
	priv := authoritytest.VerifierKey(1)
	pub := priv.Public().(ed25519.PublicKey)
	for _, tc := range []struct {
		kind      authority.VerifierMessageKind
		body      any
		hash, sig string
	}{
		{authority.VerifierMessageRegistration, authoritytest.VerifierRegistration(), authoritytest.VerifierRegistrationHash, authoritytest.VerifierRegistrationSignature},
		{authority.VerifierMessageMarkActive, authoritytest.VerifierMarkActive(), authoritytest.VerifierMarkActiveHash, authoritytest.VerifierMarkActiveSignature},
		{authority.VerifierMessageTablePurged, authoritytest.VerifierTablePurged(), authoritytest.VerifierTablePurgedHash, authoritytest.VerifierTablePurgedSignature},
	} {
		t.Run(string(tc.kind), func(t *testing.T) {
			if h, err := authority.VerifierMessageHash(tc.kind, authoritytest.Context(), tc.body); err != nil || h != tc.hash {
				t.Fatalf("hash = %s, %v; want %s", h, err, tc.hash)
			}
			if got, err := authority.SignVerifierMessage(priv, tc.kind, authoritytest.Context(), tc.body); err != nil || got != tc.sig {
				t.Fatalf("signature = %s, %v; want the pinned signature", got, err)
			}
			if err := authority.VerifyVerifierMessage(pub, tc.kind, authoritytest.Context(), tc.body, tc.sig); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestEvictNodeVector(t *testing.T) {
	if h, err := authority.EvictNodeHash(authoritytest.EvictCommand()); err != nil || h != authoritytest.EvictHash {
		t.Fatalf("hash = %s, %v", h, err)
	}
	s := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	if got, err := s.SignEvictNodeWithContextAt(authoritytest.EvictCommand(), authoritytest.ConsensusContext(2), authoritytest.Iat); err != nil || got != authoritytest.EvictJWS {
		t.Fatalf("token = %s, %v; want the pinned token", got, err)
	}
	v := &authority.Validator{AllowedAddresses: map[string]bool{authoritytest.AuthorityAddr: true}}
	if addr, err := v.VerifyEvictNode(authoritytest.EvictCommand(), authoritytest.EvictJWS, authoritytest.ConsensusContext(2)); err != nil || addr != authoritytest.AuthorityAddr {
		t.Fatalf("verify = %s, %v", addr, err)
	}
}

func TestActivationUpdateVector(t *testing.T) {
	u := authoritytest.ActivationUpdate()
	if h, err := authority.ConsensusParamsUpdateHash(u); err != nil || h != authoritytest.ActivationUpdateHash {
		t.Fatalf("hash = %s, %v; want %s", h, err, authoritytest.ActivationUpdateHash)
	}
	if got, err := authority.NormalizeConsensusParamsUpdate(u); err != nil || !reflect.DeepEqual(got, u) {
		t.Fatalf("ActivationUpdate is not in normalized form: %+v, %v", got, err)
	}
	e0, e1 := authoritytest.SIIndexerEntry0(), authoritytest.SIIndexerEntry1(6_000_000)
	if err := e0.Validate(); err != nil {
		t.Fatal(err)
	}
	if err := e1.Validate(); err != nil {
		t.Fatal(err)
	}
	for _, e := range []struct {
		stmt        authority.SNodeEnrollmentStatement
		jws, signer string
	}{{authoritytest.SNodeEnrollmentVectors[0].Statement, e0.EnrollmentJWS, e0.Signer}, {authoritytest.SNodeEnrollmentVectors[1].Statement, e1.EnrollmentJWS, e1.Signer}} {
		if err := authority.VerifySNodeEnrollment(e.stmt, e.jws, e.signer); err != nil {
			t.Fatal(err)
		}
	}
}
