package authority

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
)

// Throwaway keys (never provision): an indexer signer and a stranger.
const (
	indexerTestKeyHex  = "0000000000000000000000000000000000000000000000000000000000000b22"
	strangerTestKeyHex = "0000000000000000000000000000000000000000000000000000000000000c33"
)

func testMessageContext() MessageContext {
	return MessageContext{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis"}
}

func mustTestSigner(t *testing.T, keyHex string) *Signer {
	t.Helper()
	s, err := NewSignerFromHex(keyHex)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func testRegistration() arbiter.NodeRegistration {
	return arbiter.NodeRegistration{NodeID: "snode-2", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 1760054400000}
}

func TestSNodeEnrollmentSignVerify(t *testing.T) {
	s := mustTestSigner(t, indexerTestKeyHex)
	stmt := SNodeEnrollmentStatement{NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", IndexerID: 1, SNodeNodeID: "snode-2"}
	token, err := s.SignSNodeEnrollment(stmt)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySNodeEnrollment(stmt, token, "0x"+strings.ToUpper(s.Address()[2:])); err != nil {
		t.Fatalf("verify with a mixed-case signer: %v", err)
	}
	for name, mutate := range map[string]func(*SNodeEnrollmentStatement){
		"other network":       func(s *SNodeEnrollmentStatement) { s.NetworkID = "mainnet" },
		"other genesis":       func(s *SNodeEnrollmentStatement) { s.GenesisSnapshotID = "0xother" },
		"other indexer":       func(s *SNodeEnrollmentStatement) { s.IndexerID = 2 },
		"other snode node id": func(s *SNodeEnrollmentStatement) { s.SNodeNodeID = "snode-3" },
	} {
		t.Run(name, func(t *testing.T) {
			other := stmt
			mutate(&other)
			if err := VerifySNodeEnrollment(other, token, s.Address()); err == nil {
				t.Fatal("a statement signed for another identity verified")
			}
		})
	}
	if err := VerifySNodeEnrollment(stmt, token, mustTestSigner(t, strangerTestKeyHex).Address()); err == nil {
		t.Fatal("verified under a signer that did not sign")
	}
	for _, bad := range []string{"", "0x1234", "0x0000000000000000000000000000000000000000", "0xzz00000000000000000000000000000000000000"} {
		if err := VerifySNodeEnrollment(stmt, token, bad); err == nil {
			t.Fatalf("verified under malformed signer %q", bad)
		}
	}
	hash, err := SNodeEnrollmentHash(stmt)
	if err != nil {
		t.Fatal(err)
	}
	asMessage, err := s.signPayload(JWSCommandPayload{Iat: 1, Purpose: SNodeMessagePurpose, CmdHash: hash})
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifySNodeEnrollment(stmt, asMessage, s.Address()); err == nil {
		t.Fatal("a token of another purpose verified as an enrolment")
	}
	for _, missing := range []SNodeEnrollmentStatement{{GenesisSnapshotID: "g", SNodeNodeID: "s"}, {NetworkID: "n", SNodeNodeID: "s"}, {NetworkID: "n", GenesisSnapshotID: "g"}} {
		if _, err := s.SignSNodeEnrollment(missing); err == nil {
			t.Fatalf("signed an enrolment without its identity: %+v", missing)
		}
	}
}

// TestSignedMessageVerificationReadsNoClock is the FSM's verification path:
// VerifySNodeMessage hands the token to Validator.verify with a one-address
// allowlist and enforceAge=false, so the ES256K recovery alone decides,
// whatever the token's iat.
func TestSignedMessageVerificationReadsNoClock(t *testing.T) {
	s := mustTestSigner(t, indexerTestKeyHex)
	for _, iat := range []int64{1, time.Now().Add(24 * time.Hour).Unix(), time.Now().Add(-365 * 24 * time.Hour).Unix()} {
		token, err := s.SignSNodeMessageAt(SNodeMessageRegistration, testMessageContext(), testRegistration(), iat)
		if err != nil {
			t.Fatal(err)
		}
		if err := VerifySNodeMessage(SNodeMessageRegistration, testMessageContext(), testRegistration(), token, s.Address()); err != nil {
			t.Fatalf("iat %d: %v", iat, err)
		}
	}
}

func TestSNodeMessageBindsKindContextBodyAndSigner(t *testing.T) {
	s := mustTestSigner(t, indexerTestKeyHex)
	ctx, reg := testMessageContext(), testRegistration()
	token, err := s.SignSNodeMessage(SNodeMessageRegistration, ctx, reg)
	if err != nil {
		t.Fatal(err)
	}
	replayed := reg
	replayed.RegistrationSeq--
	otherNetwork, otherGenesis := ctx, ctx
	otherNetwork.NetworkID = "mainnet"
	otherGenesis.GenesisSnapshotID = "0xother"
	stranger := mustTestSigner(t, strangerTestKeyHex).Address()
	for name, err := range map[string]error{
		"another registration_seq": VerifySNodeMessage(SNodeMessageRegistration, ctx, replayed, token, s.Address()),
		"another network":          VerifySNodeMessage(SNodeMessageRegistration, otherNetwork, reg, token, s.Address()),
		"another genesis":          VerifySNodeMessage(SNodeMessageRegistration, otherGenesis, reg, token, s.Address()),
		"another signer":           VerifySNodeMessage(SNodeMessageRegistration, ctx, reg, token, stranger),
		"another kind":             VerifySNodeMessage(SNodeMessageMarkActive, ctx, MarkActiveBody{NodeID: reg.NodeID, RegistrationSeq: reg.RegistrationSeq}, token, s.Address()),
		"tampered signature":       VerifySNodeMessage(SNodeMessageRegistration, ctx, reg, token[:len(token)-4]+"AAAA", s.Address()),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	if _, err := SNodeMessageHash(SNodeMessageMarkActive, ctx, reg); err == nil {
		t.Fatal("a registration body was accepted as mark_active")
	}
	if _, err := SNodeMessageHash(SNodeMessageResultClaim, ctx, &arbiter.RCRecord{}); err == nil {
		t.Fatal("a pointer body was accepted")
	}
	if _, err := SNodeMessageHash("result_claims", ctx, arbiter.RCRecord{}); err == nil {
		t.Fatal("an unknown kind was accepted")
	}
	if _, err := SNodeMessageHash(SNodeMessageCleanupAck, MessageContext{NetworkID: "devnet2"}, arbiter.CleanupAck{}); err == nil {
		t.Fatal("a message without a genesis snapshot id was hashed")
	}
}

// TestSNodeMessageHashCanonicalizesEmptyLists: wire converters decode every
// empty repeated field as nil, so the FSM hashes what it decodes; a sender
// that built [] must sign the same digest (the ByteSideScanMsg.Body rule,
// types.go:249-267).
func TestSNodeMessageHashCanonicalizesEmptyLists(t *testing.T) {
	ctx := testMessageContext()
	for _, tc := range []struct {
		kind           SNodeMessageKind
		nilBody, empty any
	}{
		{SNodeMessageRegistration, arbiter.NodeRegistration{NodeID: "s"}, arbiter.NodeRegistration{NodeID: "s", Roles: []arbiter.NodeRole{}, Ed25519Pubkey: []byte{}}},
		{SNodeMessageResultClaim, arbiter.RCRecord{SourceNode: "s"}, arbiter.RCRecord{SourceNode: "s", CandidateParts: []arbiter.CandidatePart{}, PartitionNewPartSums: []arbiter.PartitionLtHashSum{}}},
		{SNodeMessagePromotionAck, arbiter.PromotionAck{NodeID: "s"}, arbiter.PromotionAck{NodeID: "s", Parts: []arbiter.SafePartMapping{}, SafePartitionParts: []arbiter.SafePartMapping{}}},
	} {
		a, errA := SNodeMessageHash(tc.kind, ctx, tc.nilBody)
		b, errB := SNodeMessageHash(tc.kind, ctx, tc.empty)
		if errA != nil || errB != nil || a != b {
			t.Fatalf("%s: nil and [] hash differently (%s vs %s; %v, %v)", tc.kind, a, b, errA, errB)
		}
	}
}

func TestVerifierMessageSignVerify(t *testing.T) {
	seed := func(b byte) []byte { s := make([]byte, ed25519.SeedSize); s[0] = b; return s }
	priv := ed25519.NewKeyFromSeed(seed(7))
	pub := priv.Public().(ed25519.PublicKey)
	ctx := testMessageContext()
	reg := arbiter.NodeRegistration{NodeID: "verifier-1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: pub, RegistrationSeq: 1760054400000}
	sig, err := SignVerifierMessage(priv, VerifierMessageRegistration, ctx, reg)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyVerifierMessage(pub, VerifierMessageRegistration, ctx, reg, sig); err != nil {
		t.Fatal(err)
	}
	other := ed25519.NewKeyFromSeed(seed(8)).Public().(ed25519.PublicKey)
	replayed := reg
	replayed.RegistrationSeq--
	otherNetwork := ctx
	otherNetwork.NetworkID = "mainnet"
	for name, err := range map[string]error{
		"another key":      VerifyVerifierMessage(other, VerifierMessageRegistration, ctx, reg, sig),
		"another seq":      VerifyVerifierMessage(pub, VerifierMessageRegistration, ctx, replayed, sig),
		"another network":  VerifyVerifierMessage(pub, VerifierMessageRegistration, otherNetwork, reg, sig),
		"another kind":     VerifyVerifierMessage(pub, VerifierMessageMarkActive, ctx, MarkActiveBody{NodeID: reg.NodeID, RegistrationSeq: reg.RegistrationSeq}, sig),
		"not hex":          VerifyVerifierMessage(pub, VerifierMessageRegistration, ctx, reg, "zz"+sig[2:]),
		"short signature":  VerifyVerifierMessage(pub, VerifierMessageRegistration, ctx, reg, sig[:126]),
		"short public key": VerifyVerifierMessage(pub[:31], VerifierMessageRegistration, ctx, reg, sig),
	} {
		if err == nil {
			t.Fatalf("%s: verified", name)
		}
	}
	if _, err := SignVerifierMessage(priv[:32], VerifierMessageRegistration, ctx, reg); err == nil {
		t.Fatal("signed with a malformed private key instead of refusing")
	}
	snodeHash, _ := SNodeMessageHash(SNodeMessageRegistration, ctx, reg)
	verifierHash, _ := VerifierMessageHash(VerifierMessageRegistration, ctx, reg)
	if snodeHash == verifierHash {
		t.Fatal("SNode and verifier messages must use separate digest domains")
	}
}
