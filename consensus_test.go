package arbiter

import (
	"encoding/json"
	"slices"
	"strings"
	"testing"
)

func TestClientLaneParamsValidate(t *testing.T) {
	if err := (ClientLaneParams{}).Validate(); err == nil {
		t.Fatal("max_lanes_per_account 0 must be refused: it would admit no lane")
	}
	if err := (ClientLaneParams{MaxLanesPerAccount: 256}).Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestConsensusParamsUpdateOmitsAbsentClientLanes(t *testing.T) {
	b, err := json.Marshal(ConsensusParamsUpdate{NetworkID: "n"})
	if err != nil {
		t.Fatal(err)
	}
	var absent map[string]any
	if err := json.Unmarshal(b, &absent); err != nil {
		t.Fatal(err)
	}
	if _, ok := absent["client_lanes"]; ok {
		t.Fatalf("absent client_lanes must be omitted from the canonical form: %s", b)
	}
	b, err = json.Marshal(ConsensusParamsUpdate{NetworkID: "n", ClientLanes: &ClientLaneParams{MaxLanesPerAccount: 256}})
	if err != nil {
		t.Fatal(err)
	}
	var present map[string]any
	if err := json.Unmarshal(b, &present); err != nil {
		t.Fatal(err)
	}
	if got, ok := present["client_lanes"].(map[string]any); !ok || got["max_lanes_per_account"] != float64(256) {
		t.Fatalf("client_lanes JSON = %s", b)
	}
}

func TestLocalNodeFeatures(t *testing.T) {
	got := LocalNodeFeatures()
	if !slices.Equal(got, []string{"client_lanes_v1", "signed_claims_v1"}) || ClientLanesFeature != "client_lanes_v1" || SignedClaimsFeature != "signed_claims_v1" {
		t.Fatalf("LocalNodeFeatures = %v", got)
	}
	got[0] = "mutated"
	if LocalNodeFeatures()[0] != ClientLanesFeature {
		t.Fatal("LocalNodeFeatures must return a fresh slice")
	}
}

func validSIIndexerEntry() SIIndexerEntry {
	return SIIndexerEntry{IndexerID: 1, ActivationBlock: 5_510_731, Signer: "0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3",
		SNodeNodeID: "snode-2", EnrollmentJWS: "h.p.s"}
}

func TestSIIndexerEntryValidate(t *testing.T) {
	if err := validSIIndexerEntry().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*SIIndexerEntry){
		"zero activation block": func(e *SIIndexerEntry) { e.ActivationBlock = 0 },
		"uppercase signer":      func(e *SIIndexerEntry) { e.Signer = "0x20C87974E9AD8113BC6C71F3B6ADB2B472A616F3" },
		"short signer":          func(e *SIIndexerEntry) { e.Signer = "0x20c87974" },
		"unprefixed signer":     func(e *SIIndexerEntry) { e.Signer = "20c87974e9ad8113bc6c71f3b6adb2b472a616f3aa" },
		"zero signer":           func(e *SIIndexerEntry) { e.Signer = "0x0000000000000000000000000000000000000000" },
		"empty snode node id":   func(e *SIIndexerEntry) { e.SNodeNodeID = "" },
		"snode node id space":   func(e *SIIndexerEntry) { e.SNodeNodeID = "snode 2" },
		"snode node id newline": func(e *SIIndexerEntry) { e.SNodeNodeID = "snode-2\n" },
		"snode node id NUL":     func(e *SIIndexerEntry) { e.SNodeNodeID = "snode\x002" },
		"blank enrolment":       func(e *SIIndexerEntry) { e.EnrollmentJWS = " " },
	} {
		t.Run(name, func(t *testing.T) {
			e := validSIIndexerEntry()
			mutate(&e)
			if err := e.Validate(); err == nil || !strings.Contains(err.Error(), "si_indexers") {
				t.Fatalf("%+v: err = %v, want an si_indexers refusal", e, err)
			}
		})
	}
}

func TestVerifierEntryValidate(t *testing.T) {
	if err := (VerifierEntry{NodeID: "verifier-1", Ed25519Pubkey: make([]byte, 32)}).Validate(); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []VerifierEntry{
		{Ed25519Pubkey: make([]byte, 32)}, {NodeID: " ", Ed25519Pubkey: make([]byte, 32)},
		{NodeID: "verifier-1"}, {NodeID: "verifier-1", Ed25519Pubkey: make([]byte, 31)}, {NodeID: "verifier-1", Ed25519Pubkey: make([]byte, 33)},
	} {
		if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "verifiers") {
			t.Fatalf("%+v: err = %v, want a verifiers refusal", bad, err)
		}
	}
}

func TestSignedClaimsFieldsAreOmittedWhileAbsent(t *testing.T) {
	b, err := json.Marshal(ConsensusParamsUpdate{NetworkID: "n", SIIndexers: []SIIndexerEntry{}, Verifiers: []VerifierEntry{}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "si_indexers") || strings.Contains(string(b), "verifiers") {
		t.Fatalf("absent signed-claims lists leaked into the canonical form: %s", b)
	}
	b, err = json.Marshal(NodeRegistration{NodeID: "s1", Roles: []NodeRole{NodeRoleSNode}})
	if err != nil || string(b) != `{"node_id":"s1","roles":[2],"ed25519_pubkey":null}` {
		t.Fatalf("pre-activation registration JSON = %s (%v): a zero registration_seq must stay omitted", b, err)
	}
	b, _ = json.Marshal(NodeRegistration{NodeID: "s1", Roles: []NodeRole{NodeRoleSNode}, RegistrationSeq: 7})
	if string(b) != `{"node_id":"s1","roles":[2],"ed25519_pubkey":null,"registration_seq":7}` {
		t.Fatalf("registration JSON = %s", b)
	}
}
