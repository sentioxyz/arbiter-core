package authority

import (
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
)

// Throwaway test key (never provision): secp256k1.
const testKeyHex = "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"

func testCmd() arbiter.PromoteSafePartition {
	return arbiter.PromoteSafePartition{
		TableID:            "tbl-1",
		PartitionID:        "202607",
		PromotionSeq:       9,
		BaseSafeSnapshotID: "snap-1",
		BasePartitionRoot:  "0xdead",
		CandidateParts: []arbiter.PartRef{
			{TableID: "tbl-1", PartitionID: "202607", PartRowLtHash: "0xbeef"},
		},
	}
}

func newTestPair(t *testing.T) (*Signer, *Validator) {
	t.Helper()
	s, err := NewSignerFromHex(testKeyHex)
	if err != nil {
		t.Fatalf("NewSignerFromHex: %v", err)
	}
	v := &Validator{
		AllowedAddresses: map[string]bool{strings.ToLower(s.Address()): true},
		MaxTokenAge:      time.Minute,
	}
	return s, v
}

func TestPromotionSignAuthorizeRoundTrip(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.SignPromotion(testCmd())
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	addr, err := v.AuthorizePromotion(testCmd(), token)
	if err != nil {
		t.Fatalf("AuthorizePromotion: %v", err)
	}
	if !strings.EqualFold(addr, s.Address()) {
		t.Fatalf("recovered %s, want signer %s", addr, s.Address())
	}
}

func TestTamperedCommandRejected(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.SignPromotion(testCmd())
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	evil := testCmd()
	evil.CandidateParts = append(evil.CandidateParts, arbiter.PartRef{
		TableID: "tbl-1", PartitionID: "202607", PartRowLtHash: "0xevil",
	})
	if _, err := v.AuthorizePromotion(evil, token); err == nil {
		t.Fatal("tampered command accepted")
	}
}

func TestCleanupTokenCannotAuthorizePromotion(t *testing.T) {
	// Domain separation: a cleanup signature over field-identical content
	// must not authorize a promotion (distinct CanonicalDigest domains).
	s, v := newTestPair(t)
	cleanup := arbiter.UnsafeCleanup{
		TableID: "tbl-1", PartitionID: "202607", PromotionSeq: 9,
		Parts: []arbiter.PartRef{{TableID: "tbl-1", PartitionID: "202607", PartRowLtHash: "0xbeef"}},
	}
	token, err := s.SignCleanup(cleanup)
	if err != nil {
		t.Fatalf("SignCleanup: %v", err)
	}
	promote := arbiter.PromoteSafePartition{
		TableID: "tbl-1", PartitionID: "202607", PromotionSeq: 9,
		CandidateParts: []arbiter.PartRef{{TableID: "tbl-1", PartitionID: "202607", PartRowLtHash: "0xbeef"}},
	}
	if _, err := v.AuthorizePromotion(promote, token); err == nil {
		t.Fatal("cleanup token authorized a promotion")
	}
}

func TestNonAllowlistedSignerRejected(t *testing.T) {
	s, _ := newTestPair(t)
	v := &Validator{
		AllowedAddresses: map[string]bool{"0x0000000000000000000000000000000000000001": true},
		MaxTokenAge:      time.Minute,
	}
	token, err := s.SignPromotion(testCmd())
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	if _, err := v.AuthorizePromotion(testCmd(), token); err == nil {
		t.Fatal("non-allowlisted signer accepted")
	}
}

func TestStaleTokenRejected(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.signPromotionAt(testCmd(), time.Now().Add(-10*time.Minute).Unix())
	if err != nil {
		t.Fatalf("signPromotionAt: %v", err)
	}
	if _, err := v.AuthorizePromotion(testCmd(), token); err == nil {
		t.Fatal("stale token accepted")
	}
}

func TestEmptyAllowlistRejected(t *testing.T) {
	// The authority gate must fail closed: a zero-value Validator (no
	// allowlist configured) rejects every command instead of trusting
	// any recovered signer.
	s, _ := newTestPair(t)
	token, err := s.SignPromotion(testCmd())
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	v := &Validator{MaxTokenAge: time.Minute}
	if _, err := v.AuthorizePromotion(testCmd(), token); err == nil {
		t.Fatal("empty allowlist authorized a command")
	}
}

func TestWrongPurposeRejected(t *testing.T) {
	s, v := newTestPair(t)
	token, err := s.signWithPurpose(testCmd(), "peer-relay")
	if err != nil {
		t.Fatalf("signWithPurpose: %v", err)
	}
	if _, err := v.AuthorizePromotion(testCmd(), token); err == nil {
		t.Fatal("wrong-purpose token accepted")
	}
}

func TestAuthorize_ZeroMaxTokenAgeFailsClosed(t *testing.T) {
	s, v := newTestPair(t)
	v.MaxTokenAge = 0
	cmd := testCmd()
	token, err := s.SignPromotion(cmd)
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	if _, err := v.AuthorizePromotion(cmd, token); err == nil {
		t.Fatal("MaxTokenAge=0 must fail closed (never-expiring tokens are the empty-allowlist zero-value trap)")
	}
}

func TestAuthorize_NegativeMaxTokenAgeFailsClosed(t *testing.T) {
	s, v := newTestPair(t)
	v.MaxTokenAge = -time.Minute
	cmd := testCmd()
	token, err := s.SignPromotion(cmd)
	if err != nil {
		t.Fatalf("SignPromotion: %v", err)
	}
	if _, err := v.AuthorizePromotion(cmd, token); err == nil {
		t.Fatal("negative MaxTokenAge must fail closed")
	}
}
