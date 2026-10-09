package arbiter

import (
	"encoding/json"
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
	b, _ := json.Marshal(ConsensusParamsUpdate{NetworkID: "n"})
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if _, ok := m["client_lanes"]; ok {
		t.Fatalf("absent client_lanes must be omitted from the canonical form: %s", b)
	}
	b, _ = json.Marshal(ConsensusParamsUpdate{NetworkID: "n", ClientLanes: &ClientLaneParams{MaxLanesPerAccount: 256}})
	_ = json.Unmarshal(b, &m)
	if got, ok := m["client_lanes"].(map[string]any); !ok || got["max_lanes_per_account"] != float64(256) {
		t.Fatalf("client_lanes JSON = %s", b)
	}
}

func TestLocalNodeFeatures(t *testing.T) {
	got := LocalNodeFeatures()
	if len(got) != 1 || got[0] != "client_lanes_v1" || ClientLanesFeature != "client_lanes_v1" {
		t.Fatalf("LocalNodeFeatures = %v", got)
	}
	got[0] = "mutated"
	if LocalNodeFeatures()[0] != ClientLanesFeature {
		t.Fatal("LocalNodeFeatures must return a fresh slice")
	}
}
