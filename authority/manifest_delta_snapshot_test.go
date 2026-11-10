package authority

import (
	"testing"
)

// TestNormalizeConsensusParamsUpdateManifestDeltaSnapshot: the switch needs
// stage3 in the same update, and it survives normalization.
func TestNormalizeConsensusParamsUpdateManifestDeltaSnapshot(t *testing.T) {
	u := stage3TestUpdate()
	u.ManifestDeltaSnapshot = true
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || !got.ManifestDeltaSnapshot {
		t.Fatalf("switch with stage3: %v, switch %v", err, got.ManifestDeltaSnapshot)
	}
	u = testConsensusUpdate()
	u.ManifestDeltaSnapshot = true
	const want = "consensus params update: manifest_delta_snapshot requires stage3 in the same update"
	if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != want {
		t.Fatalf("switch without stage3: err = %v, want %q", err, want)
	}
	u.ManifestDeltaSnapshot = false
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.ManifestDeltaSnapshot {
		t.Fatalf("no switch: %v, switch %v", err, got.ManifestDeltaSnapshot)
	}
}
