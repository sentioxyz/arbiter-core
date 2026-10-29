package authority

import "testing"

// TestSettleTrackHashGolden pins the settle's cmd_hash and one token signed at
// a fixed issue time with the test authority key (plan S3-D contract §34).
// The leader recomputes the hash from the request and checks the token
// against it, and arbiter-admin signs it, so the canonical form is frozen.
// The hash was computed outside Go as sha256 over
// "housegate-replay-mvp-v0:arbiter-settle-track-command-v1" + "\x00" +
// {"indexer_id":2,"expected_generation":3,"promotion_seqs":[41,42]}.
// ES256K signing is deterministic (RFC 6979), so the token is frozen too.
// Never regenerate either value.
func TestSettleTrackHashGolden(t *testing.T) {
	got, err := SettleTrackHash(testSettleCommand())
	if err != nil {
		t.Fatal(err)
	}
	const wantHash = "0x4e41eb27307ca6ee29036dbaece5392c12f661e9dbfc5db488881720b5508ea8"
	if got != wantHash {
		t.Fatalf("SettleTrackHash = %s, want %s", got, wantHash)
	}
	s, v := newTestPair(t)
	token, err := s.SignSettleTrackWithContextAt(testSettleCommand(), testTracksContext(), 1_760_000_000)
	if err != nil {
		t.Fatal(err)
	}
	const wantToken = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ." +
		"eyJpYXQiOjE3NjAwMDAwMDAsInB1cnBvc2UiOiJhcmJpdGVyLXNldHRsZS10cmFjay12MSIsImNtZF9oYXNoIjoiMHg0ZTQxZWIyNzMwN2NhNmVlMjkwMzZkYmFlY2U1MzkyYzEyZjY2MWU5ZGJmYzVkYjQ4ODg4MTcyMGI1NTA4ZWE4IiwibmV0d29ya19pZCI6ImRldm5ldDIiLCJnZW5lc2lzX3NuYXBzaG90X2lkIjoiMHhnZW5lc2lzIiwiYXV0aG9yaXR5X2Vwb2NoIjoyfQ." +
		"Law3sKsY099UMUmQ2J1MQbusfcC1lY4Y2WrcSgNYDFULDIU6LiyzaLuJlUuMzzw14grBvDQFI-h2k5SEco2p1Rs"
	if token != wantToken {
		t.Fatalf("settle token = %s, want %s", token, wantToken)
	}
	if _, err := v.verifyExactContext("settle track", wantHash, SettleTrackPurpose, wantToken, testTracksContext(), false); err != nil {
		t.Fatalf("the frozen token does not verify: %v", err)
	}
}
