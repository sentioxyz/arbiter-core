package authority

import (
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

// testSigner mints a Signer from the package's shared throwaway test key.
func testSigner(t *testing.T) *Signer {
	t.Helper()
	s, err := NewSignerFromHex(testKeyHex)
	if err != nil {
		t.Fatalf("NewSignerFromHex: %v", err)
	}
	return s
}

func testAbortRecord() replay.SnapshotQueryAbortRecord {
	return replay.SnapshotQueryAbortRecord{
		BlockSeq: 13, StatementID: "0x1234:1:fixture", InputRoot: "0x" + strings.Repeat("ab", 32),
		ReservationID: "reservation-1", FencingGeneration: 9, ReasonCode: "resource_exhausted",
		PrevSnapshotID: "0x" + strings.Repeat("11", 32), NextSnapshotID: "0x" + strings.Repeat("22", 32),
	}
}

func TestSnapshotQueryAbortSignVerifyAuthorizeRoundTrip(t *testing.T) {
	s := testSigner(t)
	rec := testAbortRecord()
	token, err := s.SignSnapshotQueryAbort(rec)
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	for name, fn := range map[string]func(replay.SnapshotQueryAbortRecord, string) (string, error){"verify": v.VerifySnapshotQueryAbort, "authorize": v.AuthorizeSnapshotQueryAbort} {
		if got, err := fn(rec, token); err != nil || got != s.Address() {
			t.Fatalf("%s = %q, %v", name, got, err)
		}
	}
}

func TestSnapshotQueryAbortHashIsNotTheRecordDigest(t *testing.T) {
	rec := testAbortRecord()
	cmd, err := SnapshotQueryAbortHash(rec)
	if err != nil {
		t.Fatal(err)
	}
	record, err := rec.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if cmd == record {
		t.Fatal("command digest must live in its own domain, not snapshot-query-abort-v1")
	}
}

func TestSnapshotQueryAbortRefusesTamperedRecordWrongPurposeAndUnlistedSigner(t *testing.T) {
	s := testSigner(t)
	rec := testAbortRecord()
	token, err := s.SignSnapshotQueryAbort(rec)
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	tampered := rec
	tampered.ReasonCode = "operator_requested"
	if _, err := v.VerifySnapshotQueryAbort(tampered, token); err == nil {
		t.Fatal("tampered record accepted")
	}
	// A consensus-update token over the same bytes cannot authorize an abort,
	// and an abort token cannot authorize a consensus update (vice versa).
	foreign, err := s.signPayload(JWSCommandPayload{Iat: time.Now().Unix(), Purpose: ConsensusParamsUpdatePurpose, CmdHash: mustAbortHash(t, rec)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := v.VerifySnapshotQueryAbort(rec, foreign); err == nil {
		t.Fatal("consensus-purpose token authorized an abort")
	}
	if _, err := v.VerifyConsensusParamsUpdate(testConsensusUpdate(), token); err == nil {
		t.Fatal("abort-purpose token authorized a consensus update")
	}
	other := Validator{AllowedAddresses: map[string]bool{"0x" + strings.Repeat("0f", 20): true}, MaxTokenAge: time.Minute}
	if _, err := other.VerifySnapshotQueryAbort(rec, token); err == nil {
		t.Fatal("unlisted signer accepted")
	}
	if _, err := (&Validator{MaxTokenAge: time.Minute}).VerifySnapshotQueryAbort(rec, token); err == nil {
		t.Fatal("empty allowlist accepted")
	}
}

func TestSnapshotQueryAbortVerifyIgnoresAgeAndAuthorizeEnforcesIt(t *testing.T) {
	s := testSigner(t)
	rec := testAbortRecord()
	stale, err := s.SignSnapshotQueryAbortAt(rec, time.Now().Add(-48*time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	v := Validator{AllowedAddresses: map[string]bool{s.Address(): true}, MaxTokenAge: time.Minute}
	if _, err := v.VerifySnapshotQueryAbort(rec, stale); err != nil {
		t.Fatalf("deterministic verify read the clock: %v", err)
	}
	if _, err := v.AuthorizeSnapshotQueryAbort(rec, stale); err == nil {
		t.Fatal("stale token authorized")
	}
	if _, err := (&Validator{AllowedAddresses: map[string]bool{s.Address(): true}}).AuthorizeSnapshotQueryAbort(rec, stale); err == nil {
		t.Fatal("zero MaxTokenAge must fail closed")
	}
}

func TestSnapshotQueryAbortRecordValidation(t *testing.T) {
	cases := map[string]func(*replay.SnapshotQueryAbortRecord){
		"zero block":        func(r *replay.SnapshotQueryAbortRecord) { r.BlockSeq = 0 },
		"zero generation":   func(r *replay.SnapshotQueryAbortRecord) { r.FencingGeneration = 0 },
		"empty statement":   func(r *replay.SnapshotQueryAbortRecord) { r.StatementID = " " },
		"empty input root":  func(r *replay.SnapshotQueryAbortRecord) { r.InputRoot = "" },
		"empty reservation": func(r *replay.SnapshotQueryAbortRecord) { r.ReservationID = "" },
		"empty reason":      func(r *replay.SnapshotQueryAbortRecord) { r.ReasonCode = "" },
		"nul reason":        func(r *replay.SnapshotQueryAbortRecord) { r.ReasonCode = "a\x00b" },
		"empty prev":        func(r *replay.SnapshotQueryAbortRecord) { r.PrevSnapshotID = "" },
		"empty next":        func(r *replay.SnapshotQueryAbortRecord) { r.NextSnapshotID = "" },
		"prev equals next":  func(r *replay.SnapshotQueryAbortRecord) { r.NextSnapshotID = r.PrevSnapshotID },
		"nul cleanup root":  func(r *replay.SnapshotQueryAbortRecord) { r.CleanupAuthorizationRoot = "a\x00b" },
	}
	for name, mutate := range cases {
		rec := testAbortRecord()
		mutate(&rec)
		if err := ValidateSnapshotQueryAbortRecord(rec); err == nil {
			t.Fatalf("%s accepted", name)
		}
		if _, err := SnapshotQueryAbortHash(rec); err == nil {
			t.Fatalf("%s hashed", name)
		}
	}
	if err := ValidateSnapshotQueryAbortRecord(testAbortRecord()); err != nil {
		t.Fatal(err)
	}
}

// TestSnapshotQueryAbortRecordValidationErrorNamesEarliestField pins that the
// field checks run in a fixed order (an ordered slice, not a map) so the
// error naming multiple invalid fields is deterministic rather than
// depending on Go's randomized map iteration.
func TestSnapshotQueryAbortRecordValidationErrorNamesEarliestField(t *testing.T) {
	rec := testAbortRecord()
	rec.StatementID = ""
	rec.ReasonCode = ""
	err := ValidateSnapshotQueryAbortRecord(rec)
	if err == nil {
		t.Fatal("record with two invalid fields accepted")
	}
	if !strings.Contains(err.Error(), "statement_id") {
		t.Fatalf("error %q does not name statement_id, the earlier field in the ordered table", err)
	}
}

func mustAbortHash(t *testing.T, r replay.SnapshotQueryAbortRecord) string {
	t.Helper()
	h, err := SnapshotQueryAbortHash(r)
	if err != nil {
		t.Fatal(err)
	}
	return h
}
