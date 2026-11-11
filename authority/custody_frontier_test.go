package authority

import (
	"testing"
)

// TestNormalizeConsensusParamsUpdateCustodyFrontier: the parameter needs
// snapshot_query_tracks in the same update (every update carries it once it
// is set), and it survives normalization.
func TestNormalizeConsensusParamsUpdateCustodyFrontier(t *testing.T) {
	u := stage3TestUpdate()
	u.ManifestDeltaSnapshot, u.PromotionAckDeltaSnapshot, u.SnapshotQueryTracks, u.CustodyFrontier = true, true, true, true
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || !got.CustodyFrontier {
		t.Fatalf("parameter with its prerequisite: %v, parameter %v", err, got.CustodyFrontier)
	}
	const want = "consensus params update: custody_frontier requires snapshot_query_tracks in the same update"
	noTracks := u
	noTracks.SnapshotQueryTracks = false
	if _, err := NormalizeConsensusParamsUpdate(noTracks); err == nil || err.Error() != want {
		t.Fatalf("parameter without snapshot_query_tracks: err = %v, want %q", err, want)
	}
	u.CustodyFrontier = false
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.CustodyFrontier {
		t.Fatalf("no parameter: %v, parameter %v", err, got.CustodyFrontier)
	}
}
