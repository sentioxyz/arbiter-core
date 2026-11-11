package authority

import (
	"testing"
)

// TestNormalizeConsensusParamsUpdateHistoryRetainBlocks: the parameter needs
// custody_frontier in the same update (every update carries it once it is
// set), and it survives normalization.
func TestNormalizeConsensusParamsUpdateHistoryRetainBlocks(t *testing.T) {
	u := stage3TestUpdate()
	u.ManifestDeltaSnapshot, u.PromotionAckDeltaSnapshot, u.SnapshotQueryTracks, u.CustodyFrontier, u.HistoryRetainBlocks = true, true, true, true, 64
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || got.HistoryRetainBlocks != 64 {
		t.Fatalf("parameter with its prerequisite: %v, parameter %d", err, got.HistoryRetainBlocks)
	}
	const want = "consensus params update: history_retain_blocks requires custody_frontier in the same update"
	noCustody := u
	noCustody.CustodyFrontier = false
	if _, err := NormalizeConsensusParamsUpdate(noCustody); err == nil || err.Error() != want {
		t.Fatalf("parameter without custody_frontier: err = %v, want %q", err, want)
	}
	u.HistoryRetainBlocks = 0
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.HistoryRetainBlocks != 0 {
		t.Fatalf("no parameter: %v, parameter %d", err, got.HistoryRetainBlocks)
	}
}
