package authority

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core"
)

func TestContextSigningPreservesSNodeCompatibility(t *testing.T) {
	s, v := newTestPair(t)
	context := ConsensusContext{NetworkID: "testnet", GenesisSnapshotID: "snapshot:genesis", AuthorityEpoch: 0}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p", PromotionSeq: 7}
	for _, tt := range []struct {
		name      string
		sign      func() (string, error)
		authorize func(string) (string, error)
		hash      func() (string, error)
	}{
		{"promotion", func() (string, error) { return s.SignPromotionWithContext(testCmd(), context) }, func(token string) (string, error) { return v.AuthorizePromotion(testCmd(), token) }, func() (string, error) { return PromoteCommandHash(testCmd()) }},
		{"cleanup", func() (string, error) { return s.SignCleanupWithContext(cleanup, context) }, func(token string) (string, error) { return v.AuthorizeCleanup(cleanup, token) }, func() (string, error) { return CleanupCommandHash(cleanup) }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			token, err := tt.sign()
			if err != nil {
				t.Fatal(err)
			}
			if address, err := tt.authorize(token); err != nil || address != s.Address() {
				t.Fatalf("existing SNode validator rejected context token: %q, %v", address, err)
			}
			payloadJSON, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
			if err != nil {
				t.Fatal(err)
			}
			var payload JWSCommandPayload
			if err := json.Unmarshal(payloadJSON, &payload); err != nil {
				t.Fatal(err)
			}
			hash, err := tt.hash()
			if err != nil {
				t.Fatal(err)
			}
			if payload.Purpose != PromotionPurpose || payload.CmdHash != hash || payload.NetworkID != context.NetworkID || payload.GenesisSnapshotID != context.GenesisSnapshotID || payload.AuthorityEpoch == nil || *payload.AuthorityEpoch != 0 {
				t.Fatalf("context token payload = %s", payloadJSON)
			}
			var fields map[string]json.RawMessage
			if err := json.Unmarshal(payloadJSON, &fields); err != nil || len(fields) != 6 || string(fields["authority_epoch"]) != "0" {
				t.Fatalf("context fields = %s, err = %v", payloadJSON, err)
			}
			for _, field := range []string{"network_id", "genesis_snapshot_id", "authority_epoch"} {
				changed := map[string]json.RawMessage{}
				for key, value := range fields {
					changed[key] = value
				}
				if field == "authority_epoch" {
					changed[field] = json.RawMessage("1")
				} else {
					changed[field] = json.RawMessage(`"other"`)
				}
				body, err := json.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
				parts := strings.Split(token, ".")
				parts[1] = base64.RawURLEncoding.EncodeToString(body)
				if _, err := tt.authorize(strings.Join(parts, ".")); err == nil {
					t.Fatalf("unsigned mutation of %s was accepted", field)
				}
			}
		})
	}
}

func TestContextSigningExplicitTimeAndLegacyPayload(t *testing.T) {
	s, _ := newTestPair(t)
	context := ConsensusContext{NetworkID: "testnet", GenesisSnapshotID: "snapshot:genesis", AuthorityEpoch: 42}
	for _, sign := range []func() (string, error){
		func() (string, error) { return s.SignPromotionWithContextAt(testCmd(), context, 123) },
		func() (string, error) { return s.SignCleanupWithContextAt(arbiter.UnsafeCleanup{}, context, 123) },
	} {
		token, err := sign()
		if err != nil {
			t.Fatal(err)
		}
		body, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
		if err != nil {
			t.Fatal(err)
		}
		var payload JWSCommandPayload
		if err := json.Unmarshal(body, &payload); err != nil {
			t.Fatal(err)
		}
		if payload.Iat != 123 || payload.AuthorityEpoch == nil || *payload.AuthorityEpoch != 42 {
			t.Fatalf("explicit signing context = %s", body)
		}
	}
	for _, sign := range []func() (string, error){
		func() (string, error) { return s.SignPromotion(testCmd()) },
		func() (string, error) { return s.SignCleanup(arbiter.UnsafeCleanup{}) },
	} {
		token, err := sign()
		if err != nil {
			t.Fatal(err)
		}
		body, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]json.RawMessage
		if err := json.Unmarshal(body, &fields); err != nil || len(fields) != 3 || fields["network_id"] != nil || fields["genesis_snapshot_id"] != nil || fields["authority_epoch"] != nil {
			t.Fatalf("legacy payload changed: %s, err = %v", body, err)
		}
	}
}

func TestContextSigningRejectsMissingIdentity(t *testing.T) {
	s, _ := newTestPair(t)
	for _, context := range []ConsensusContext{{}, {NetworkID: "testnet"}, {GenesisSnapshotID: "snapshot:genesis"}, {NetworkID: " ", GenesisSnapshotID: "snapshot:genesis"}} {
		if _, err := s.SignPromotionWithContextAt(testCmd(), context, time.Now().Unix()); err == nil {
			t.Fatal("promotion signed with missing identity")
		}
		if _, err := s.SignCleanupWithContextAt(arbiter.UnsafeCleanup{}, context, time.Now().Unix()); err == nil {
			t.Fatal("cleanup signed with missing identity")
		}
	}
}
