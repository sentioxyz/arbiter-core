package authority

import (
	"encoding/base64"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"

	"github.com/housegate/housegate/pkg/replay"
)

func testActivation() replay.ActiveQueryPolicy {
	return replay.ActiveQueryPolicy{
		ActivationID: "act-1", NetworkID: "net", ActivationBlockSeq: 7,
		ExecutorProfileID: "prof", QueryProfileID: "q1", Enabled: true,
	}
}

func TestQueryProfileActivationSignVerifyAuthorizeRoundTrip(t *testing.T) {
	s := testSigner(t)
	p := testActivation()
	token, err := s.SignQueryProfileActivation(p)
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	for name, fn := range map[string]func(replay.ActiveQueryPolicy, string) (string, error){"verify": v.VerifyQueryProfileActivation, "authorize": v.AuthorizeQueryProfileActivation} {
		if got, err := fn(p, token); err != nil || got != s.Address() {
			t.Fatalf("%s = %q, %v", name, got, err)
		}
	}
}

func TestQueryProfileActivationRefusesTamperedActivationWrongPurposeAndUnlistedSigner(t *testing.T) {
	s := testSigner(t)
	p := testActivation()
	token, err := s.SignQueryProfileActivation(p)
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	tampered := p
	tampered.QueryProfileID = "q2"
	if _, err := v.VerifyQueryProfileActivation(tampered, token); err == nil {
		t.Fatal("tampered activation accepted")
	}
	// An abort token over an unrelated record must not verify as an
	// activation, and an activation token must not verify as an abort:
	// the two token families are purpose- and shape-separated.
	abortRecord := testAbortRecord()
	abortToken, err := s.SignSnapshotQueryAbort(abortRecord)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyQueryProfileActivation(p, abortToken); err == nil {
		t.Fatal("abort token authorized an activation")
	}
	if _, err := v.VerifySnapshotQueryAbort(abortRecord, token); err == nil {
		t.Fatal("activation token authorized an abort")
	}
	other := Validator{AllowedAddresses: map[string]bool{"0x" + strings.Repeat("0f", 20): true}, MaxTokenAge: time.Minute}
	if _, err := other.VerifyQueryProfileActivation(p, token); err == nil {
		t.Fatal("unlisted signer accepted")
	}
	if _, err := (&Validator{MaxTokenAge: time.Minute}).VerifyQueryProfileActivation(p, token); err == nil {
		t.Fatal("empty allowlist accepted")
	}
}

// TestQueryProfileActivationRejectsSameShapeWrongPurposeOrVersion isolates
// the purpose/version comparison in parseQueryProfileActivationPayload from
// the strictJSONObject key-set check exercised above: the abort-token
// cross-purpose case is rejected by the key set (different field names)
// before it ever reaches the purpose/version comparison. Both tokens here
// keep the correct 4-key {purpose,version,iat,activation} shape, so a wrong
// Purpose or wrong Version can only be caught by the explicit
// "purpose != ... || version != ... || iat <= 0" check.
func TestQueryProfileActivationRejectsSameShapeWrongPurposeOrVersion(t *testing.T) {
	s := testSigner(t)
	p := testActivation()
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}

	wrongPurpose := QueryProfileActivationPayloadV1{Purpose: SnapshotQueryAbortPurpose, Version: QueryProfileActivationVersion, Iat: time.Now().Unix(), Activation: p}
	tokenA, err := s.signQueryProfileActivationPayload(wrongPurpose)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyQueryProfileActivation(p, tokenA); err == nil || !strings.Contains(err.Error(), "unexpected purpose") {
		t.Fatalf("wrong purpose (same shape) = %v, want the purpose/version comparison to refuse it", err)
	}

	wrongVersion := QueryProfileActivationPayloadV1{Purpose: QueryProfileActivationPurpose, Version: QueryProfileActivationVersion + 1, Iat: time.Now().Unix(), Activation: p}
	tokenB, err := s.signQueryProfileActivationPayload(wrongVersion)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifyQueryProfileActivation(p, tokenB); err == nil || !strings.Contains(err.Error(), "unexpected purpose") {
		t.Fatalf("wrong version (same shape) = %v, want the purpose/version comparison to refuse it", err)
	}
}

// TestQueryProfileActivationTamperedSignatureRejected rewrites the first
// character of the token's signature segment and confirms the mutated
// signature no longer recovers an allow-listed address.
func TestQueryProfileActivationTamperedSignatureRejected(t *testing.T) {
	s := testSigner(t)
	p := testActivation()
	token, err := s.SignQueryProfileActivation(p)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("malformed token: %q", token)
	}
	sig := []byte(parts[2])
	if sig[0] == 'A' {
		sig[0] = 'B'
	} else {
		sig[0] = 'A'
	}
	parts[2] = string(sig)
	tampered := strings.Join(parts, ".")
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	if _, err := v.VerifyQueryProfileActivation(p, tampered); err == nil {
		t.Fatal("tampered signature accepted")
	}
}

// TestQueryProfileActivationRejectsNonCanonicalSignatures proves the
// signature-canonicalization fix: a high-S malleation (r, n-s, v^1) of a
// validly-signed token's signature, and a raw-V (0/1, not the canonical
// 27/28) re-encoding of one, are each a distinct byte string that would
// otherwise recover the very same allow-listed address as the original —
// both must be refused by validateQueryProfileActivationSignature, and the
// original, canonically-signed token must be unaffected.
func TestQueryProfileActivationRejectsNonCanonicalSignatures(t *testing.T) {
	s := testSigner(t)
	p := testActivation()
	token, err := s.SignQueryProfileActivation(p)
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("malformed token: %q", token)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil || len(sig) != 65 {
		t.Fatalf("decode signature: %v (len=%d)", err, len(sig))
	}

	// High-S malleation: (r, n-s, v^1) recovers the same address as (r, s, v)
	// but is a distinct, non-canonical byte string (go-ethereum's crypto.Sign
	// always produces the canonical low-S form, so s here starts low).
	n := crypto.S256().Params().N
	sVal := new(big.Int).SetBytes(sig[32:64])
	malleated := append([]byte(nil), sig...)
	copy(malleated[32:64], new(big.Int).Sub(n, sVal).FillBytes(make([]byte, 32)))
	if malleated[64] == 27 {
		malleated[64] = 28
	} else {
		malleated[64] = 27
	}
	malleatedParts := append([]string(nil), parts...)
	malleatedParts[2] = base64.RawURLEncoding.EncodeToString(malleated)
	if _, err := v.VerifyQueryProfileActivation(p, strings.Join(malleatedParts, ".")); err == nil || !strings.Contains(err.Error(), "canonical low-S") {
		t.Fatalf("high-S malleation: got err=%v, want the canonical low-S check to refuse it", err)
	}

	// Raw-V re-encoding: sig[64] in {0,1} (as SigToPub itself expects) instead
	// of the canonical Ethereum {27,28}.
	rawV := append([]byte(nil), sig...)
	rawV[64] -= 27
	rawVParts := append([]string(nil), parts...)
	rawVParts[2] = base64.RawURLEncoding.EncodeToString(rawV)
	if _, err := v.VerifyQueryProfileActivation(p, strings.Join(rawVParts, ".")); err == nil || !strings.Contains(err.Error(), "recovery V") {
		t.Fatalf("raw-V signature: got err=%v, want the recovery-V check to refuse it", err)
	}

	// The original, canonically-signed token is unaffected by the new checks.
	if _, err := v.VerifyQueryProfileActivation(p, token); err != nil {
		t.Fatalf("original token must still verify: %v", err)
	}
}

func TestQueryProfileActivationVerifyIgnoresAgeAndAuthorizeEnforcesIt(t *testing.T) {
	s := testSigner(t)
	p := testActivation()
	stale, err := s.SignQueryProfileActivationAt(p, time.Now().Add(-48*time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	if _, err := v.VerifyQueryProfileActivation(p, stale); err != nil {
		t.Fatalf("deterministic verify read the clock: %v", err)
	}
	if _, err := v.AuthorizeQueryProfileActivation(p, stale); err == nil {
		t.Fatal("stale token authorized")
	}
	if _, err := (&Validator{AllowedAddresses: map[string]bool{s.Address(): true}}).AuthorizeQueryProfileActivation(p, stale); err == nil {
		t.Fatal("zero MaxTokenAge must fail closed")
	}
}

func TestQueryProfileActivationValidation(t *testing.T) {
	cases := map[string]func(*replay.ActiveQueryPolicy){
		"empty activation id":  func(p *replay.ActiveQueryPolicy) { p.ActivationID = " " },
		"empty network":        func(p *replay.ActiveQueryPolicy) { p.NetworkID = "" },
		"nul network":          func(p *replay.ActiveQueryPolicy) { p.NetworkID = "n\x00et" },
		"nonzero keeper shard": func(p *replay.ActiveQueryPolicy) { p.KeeperShardID = 1 },
		"empty executor":       func(p *replay.ActiveQueryPolicy) { p.ExecutorProfileID = "" },
		"empty query profile":  func(p *replay.ActiveQueryPolicy) { p.QueryProfileID = "" },
		"disabled":             func(p *replay.ActiveQueryPolicy) { p.Enabled = false },
	}
	for name, mutate := range cases {
		p := testActivation()
		mutate(&p)
		if err := ValidateQueryProfileActivation(p); err == nil {
			t.Fatalf("%s accepted", name)
		}
		if _, err := QueryProfileActivationHash(p); err == nil {
			t.Fatalf("%s hashed", name)
		}
	}
	if err := ValidateQueryProfileActivation(testActivation()); err != nil {
		t.Fatal(err)
	}
	// activation_block_seq=0 is explicitly allowed (unlike the abort
	// record's required block_seq).
	zeroBlock := testActivation()
	zeroBlock.ActivationBlockSeq = 0
	if err := ValidateQueryProfileActivation(zeroBlock); err != nil {
		t.Fatalf("activation_block_seq=0 must be accepted: %v", err)
	}
}

// TestQueryProfileActivationHashGolden freezes the domain-separated command
// digest for one canonical activation record
// (replay.ActiveQueryPolicy{ActivationID: "act-1", NetworkID: "net",
// ActivationBlockSeq: 7, ExecutorProfileID: "prof", QueryProfileID: "q1",
// Enabled: true}). Frozen vector: computed once from
// queryProfileActivationCommandDomain + replay.CanonicalDigest and pinned as
// a literal; a change here means the digest moved and downstream verifiers
// must be re-coordinated.
func TestQueryProfileActivationHashGolden(t *testing.T) {
	got, err := QueryProfileActivationHash(testActivation())
	if err != nil {
		t.Fatal(err)
	}
	const want = "0x963ed07aa8ca3551bde14e2266cf93d78ada7efb8e3c20db4ba514b5e58c1e30"
	if got != want {
		t.Fatalf("golden mismatch: got %s, want %s", got, want)
	}
}
