package authority

import (
	"testing"
)

// TestNormalizeConsensusParamsUpdatePromotionFinalityGate: the gate needs
// tracks_enabled in the same update, and it survives normalization.
func TestNormalizeConsensusParamsUpdatePromotionFinalityGate(t *testing.T) {
	u := stage3TestUpdate()
	u.PromotionFinalityGate = true
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || !got.PromotionFinalityGate {
		t.Fatalf("gate with tracks: %v, gate %v", err, got.PromotionFinalityGate)
	}
	u = testConsensusUpdate()
	u.PromotionFinalityGate = true
	const want = "consensus params update: promotion_finality_gate requires tracks_enabled in the same update"
	if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != want {
		t.Fatalf("gate without tracks: err = %v, want %q", err, want)
	}
	u.PromotionFinalityGate = false
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.PromotionFinalityGate {
		t.Fatalf("no gate: %v, gate %v", err, got.PromotionFinalityGate)
	}
}
