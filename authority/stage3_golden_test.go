package authority_test

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
)

// TestPoolVerifierEntryKeepsItsV20CanonicalForm: a pool-only update hashes
// exactly as under every earlier arbiter-core (authoritytest.ActivationUpdateHash
// and TestConsensusParamsUpdateHashIsFrozen stay unedited), a pool entry's
// canonical JSON is its two v20 members, and no stage-3 member appears in the
// update (plan S3-E1 K58).
func TestPoolVerifierEntryKeepsItsV20CanonicalForm(t *testing.T) {
	u := authoritytest.ActivationUpdate()
	if h, err := authority.ConsensusParamsUpdateHash(u); err != nil || h != authoritytest.ActivationUpdateHash {
		t.Fatalf("pool-only update hash = %s, %v; want %s", h, err, authoritytest.ActivationUpdateHash)
	}
	got, err := authority.NormalizeConsensusParamsUpdate(u)
	if err != nil {
		t.Fatal(err)
	}
	for i, v := range got.Verifiers {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		want := `{"node_id":"` + authoritytest.VerifierNodeID(i+1) + `","ed25519_pubkey":"` +
			base64.StdEncoding.EncodeToString(authoritytest.VerifierKey(i+1).Public().(ed25519.PublicKey)) + `"}`
		if string(b) != want {
			t.Fatalf("pool entry %d = %s, want %s", i+1, b, want)
		}
	}
	b, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range []string{`"stage3"`, `"host_indexer_id"`, `"replica_signer"`, `"consent_jws"`, `"weight"`, `"failure_domain"`, `"verify"`, `"replicate"`, `"counts"`, `"shared_rmt"`} {
		if strings.Contains(string(b), member) {
			t.Fatalf("a pool-only update carries %s: %s", member, b)
		}
	}
}

// TestStage3UpdateHashIsFrozen pins the canonical hash of the v21 activation
// shape (authoritytest.Stage3Update, Stage3UpdateHash) and of the same update
// listing member-b (Stage3MemberUpdateHash), which freezes a member entry's
// canonical form: every field, in proto order. Both were computed outside Go
// over the preimages authoritytest documents; never regenerate either value.
func TestStage3UpdateHashIsFrozen(t *testing.T) {
	u := authoritytest.Stage3Update()
	if got, err := authority.NormalizeConsensusParamsUpdate(u); err != nil || !reflect.DeepEqual(got, u) {
		t.Fatalf("Stage3Update is not in normalized form: %+v, %v", got, err)
	}
	if h, err := authority.ConsensusParamsUpdateHash(u); err != nil || h != authoritytest.Stage3UpdateHash {
		t.Fatalf("hash = %s, %v; want %s", h, err, authoritytest.Stage3UpdateHash)
	}
	withMember := authoritytest.Stage3Update()
	withMember.Verifiers = append([]arbiter.VerifierEntry{authoritytest.MemberVerifierEntryB()}, withMember.Verifiers...)
	if got, err := authority.NormalizeConsensusParamsUpdate(withMember); err != nil || !reflect.DeepEqual(got, withMember) {
		t.Fatalf("Stage3Update with member-b is not in normalized form: %+v, %v", got, err)
	}
	if h, err := authority.ConsensusParamsUpdateHash(withMember); err != nil || h != authoritytest.Stage3MemberUpdateHash {
		t.Fatalf("hash with member-b = %s, %v; want %s", h, err, authoritytest.Stage3MemberUpdateHash)
	}
}

// TestStage3AuthorityCommandVectorsAreFrozen reproduces the three authority
// commands' pinned hashes and tokens (AuthorityKeyHex, ConsensusContext(2),
// Iat), verifies each in its own context, and refuses it in the next epoch.
func TestStage3AuthorityCommandVectorsAreFrozen(t *testing.T) {
	s := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	v := &authority.Validator{AllowedAddresses: map[string]bool{authoritytest.AuthorityAddr: true}}
	ctx := authoritytest.ConsensusContext(2)
	rebalance, reselect, lagging := authoritytest.RebalanceGroupCommand(), authoritytest.ReselectVerifiersCommand(), authoritytest.MarkMemberLaggingCommand()
	for _, tc := range []struct {
		name              string
		hash              func() (string, error)
		sign              func() (string, error)
		verify            func(token string, c authority.ConsensusContext) (string, error)
		wantHash, wantJWS string
	}{
		{"rebalance group", func() (string, error) { return authority.RebalanceGroupHash(rebalance) },
			func() (string, error) { return s.SignRebalanceGroupWithContextAt(rebalance, ctx, authoritytest.Iat) },
			func(tok string, c authority.ConsensusContext) (string, error) {
				return v.VerifyRebalanceGroup(rebalance, tok, c)
			},
			authoritytest.RebalanceGroupHash, authoritytest.RebalanceGroupJWS},
		{"reselect verifiers", func() (string, error) { return authority.ReselectVerifiersHash(reselect) },
			func() (string, error) { return s.SignReselectVerifiersWithContextAt(reselect, ctx, authoritytest.Iat) },
			func(tok string, c authority.ConsensusContext) (string, error) {
				return v.VerifyReselectVerifiers(reselect, tok, c)
			},
			authoritytest.ReselectVerifiersHash, authoritytest.ReselectVerifiersJWS},
		{"mark member lagging", func() (string, error) { return authority.MarkMemberLaggingHash(lagging) },
			func() (string, error) { return s.SignMarkMemberLaggingWithContextAt(lagging, ctx, authoritytest.Iat) },
			func(tok string, c authority.ConsensusContext) (string, error) {
				return v.VerifyMarkMemberLagging(lagging, tok, c)
			},
			authoritytest.MarkMemberLaggingHash, authoritytest.MarkMemberLaggingJWS},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if h, err := tc.hash(); err != nil || h != tc.wantHash {
				t.Fatalf("hash = %s, %v; want %s", h, err, tc.wantHash)
			}
			if tok, err := tc.sign(); err != nil || tok != tc.wantJWS {
				t.Fatalf("token = %s, %v; want the pinned token", tok, err)
			}
			if addr, err := tc.verify(tc.wantJWS, ctx); err != nil || addr != authoritytest.AuthorityAddr {
				t.Fatalf("verify = %s, %v", addr, err)
			}
			if _, err := tc.verify(tc.wantJWS, authoritytest.ConsensusContext(3)); err == nil {
				t.Fatal("the epoch-2 token verified in epoch 3")
			}
		})
	}
}

// TestGroupMemberConsentVectorIsFrozen reproduces indexer 1's pinned consent
// to member-b, verifies it under indexer 1's signer only, and checks that the
// fixture keys are the ones the vectors name.
func TestGroupMemberConsentVectorIsFrozen(t *testing.T) {
	stmt := authoritytest.GroupMemberConsent()
	if h, err := authority.GroupMemberConsentHash(stmt); err != nil || h != authoritytest.GroupMemberConsentHash {
		t.Fatalf("hash = %s, %v; want %s", h, err, authoritytest.GroupMemberConsentHash)
	}
	if got, err := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1).SignGroupMemberConsentAt(stmt, authoritytest.Iat); err != nil || got != authoritytest.GroupMemberConsentJWS {
		t.Fatalf("token = %s, %v; want the pinned token", got, err)
	}
	if err := authority.VerifyGroupMemberConsent(stmt, authoritytest.GroupMemberConsentJWS, authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
	if err := authority.VerifyGroupMemberConsent(stmt, authoritytest.GroupMemberConsentJWS, authoritytest.IndexerAddr0); err == nil {
		t.Fatal("indexer 1's consent verified under indexer 0's signer")
	}
	if got := hex.EncodeToString(authoritytest.MemberKeyB().Public().(ed25519.PublicKey)); got != "66be7e332c7a453332bd9d0a7f7db055f5c5ef1a06ada66d98b39fb6810c473a" {
		t.Fatalf("MemberKeyB public key = %s", got)
	}
	if got := authoritytest.MustSigner(t, authoritytest.ReplicaKeyHexB).Address(); got != authoritytest.ReplicaAddrB {
		t.Fatalf("address of ReplicaKeyHexB = %s, want %s", got, authoritytest.ReplicaAddrB)
	}
	if e := authoritytest.MemberVerifierEntryB(); e.ConsentJWS != authoritytest.GroupMemberConsentJWS || *e.HostIndexerID != stmt.HostIndexerID ||
		e.ReplicaSigner != stmt.ReplicaSigner || !e.IsMember() {
		t.Fatalf("MemberVerifierEntryB %+v does not carry the pinned consent", e)
	}
}

// TestMemberReadyVectorIsFrozen reproduces member-b's pinned MemberReady
// signature and verifies it under member-b's key only.
func TestMemberReadyVectorIsFrozen(t *testing.T) {
	body := authoritytest.MemberReadyBody()
	if h, err := authority.MemberReadyHash(authoritytest.Context(), body); err != nil || h != authoritytest.MemberReadyHash {
		t.Fatalf("hash = %s, %v; want %s", h, err, authoritytest.MemberReadyHash)
	}
	if got, err := authority.SignMemberReady(authoritytest.MemberKeyB(), authoritytest.Context(), body); err != nil || got != authoritytest.MemberReadySignature {
		t.Fatalf("signature = %s, %v; want the pinned signature", got, err)
	}
	if err := authority.VerifyMemberReady(authoritytest.MemberKeyB().Public().(ed25519.PublicKey), authoritytest.Context(), body, authoritytest.MemberReadySignature); err != nil {
		t.Fatal(err)
	}
	if err := authority.VerifyMemberReady(authoritytest.VerifierKey(1).Public().(ed25519.PublicKey), authoritytest.Context(), body, authoritytest.MemberReadySignature); err == nil {
		t.Fatal("member-b's MemberReady verified under verifier-1's key")
	}
}

// TestMemberCheckpointVectorIsFrozen reproduces member-b's pinned checkpoint
// token and verifies it under ReplicaAddrB only.
func TestMemberCheckpointVectorIsFrozen(t *testing.T) {
	cp := authoritytest.MemberCheckpoint()
	if h, err := authority.MemberCheckpointHash(authoritytest.Context(), cp); err != nil || h != authoritytest.MemberCheckpointHash {
		t.Fatalf("hash = %s, %v; want %s", h, err, authoritytest.MemberCheckpointHash)
	}
	if got, err := authoritytest.MustSigner(t, authoritytest.ReplicaKeyHexB).SignMemberCheckpointAt(authoritytest.Context(), cp, authoritytest.Iat); err != nil || got != authoritytest.MemberCheckpointJWS {
		t.Fatalf("token = %s, %v; want the pinned token", got, err)
	}
	if err := authority.VerifyMemberCheckpoint(authoritytest.Context(), cp, authoritytest.MemberCheckpointJWS, authoritytest.ReplicaAddrB); err != nil {
		t.Fatal(err)
	}
	if err := authority.VerifyMemberCheckpoint(authoritytest.Context(), cp, authoritytest.MemberCheckpointJWS, authoritytest.IndexerAddr1); err == nil {
		t.Fatal("member-b's checkpoint verified under its host's signer")
	}
}
