package authority

import (
	"testing"
)

// TestNormalizeConsensusParamsUpdatePromotionAckDeltaSnapshot: the switch
// needs manifest_delta_snapshot in the same update, and it survives
// normalization.
func TestNormalizeConsensusParamsUpdatePromotionAckDeltaSnapshot(t *testing.T) {
	u := stage3TestUpdate()
	u.ManifestDeltaSnapshot, u.PromotionAckDeltaSnapshot = true, true
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || !got.PromotionAckDeltaSnapshot {
		t.Fatalf("switch with manifest_delta_snapshot: %v, switch %v", err, got.PromotionAckDeltaSnapshot)
	}
	u.ManifestDeltaSnapshot = false
	const want = "consensus params update: promotion_ack_delta_snapshot requires manifest_delta_snapshot in the same update"
	if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != want {
		t.Fatalf("switch without manifest_delta_snapshot: err = %v, want %q", err, want)
	}
	u.PromotionAckDeltaSnapshot = false
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.PromotionAckDeltaSnapshot {
		t.Fatalf("no switch: %v, switch %v", err, got.PromotionAckDeltaSnapshot)
	}
}
