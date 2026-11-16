package authority

import (
	"testing"
)

// TestNormalizeConsensusParamsUpdateCustodyFrontier: the parameter needs
// promotion_ack_delta_snapshot in the same update (every update carries it
// once it is set), not snapshot_query_tracks, and it survives normalization.
func TestNormalizeConsensusParamsUpdateCustodyFrontier(t *testing.T) {
	u := stage3TestUpdate()
	u.ManifestDeltaSnapshot, u.PromotionAckDeltaSnapshot, u.SnapshotQueryTracks, u.CustodyFrontier = true, true, true, true
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || !got.CustodyFrontier {
		t.Fatalf("parameter with its prerequisite: %v, parameter %v", err, got.CustodyFrontier)
	}
	noTracks := u
	noTracks.SnapshotQueryTracks = false
	if got, err := NormalizeConsensusParamsUpdate(noTracks); err != nil || !got.CustodyFrontier || got.SnapshotQueryTracks {
		t.Fatalf("parameter without snapshot_query_tracks: %v, parameter %v, tracks %v", err, got.CustodyFrontier, got.SnapshotQueryTracks)
	}
	const want = "consensus params update: custody_frontier requires promotion_ack_delta_snapshot in the same update"
	noAckDelta := noTracks
	noAckDelta.PromotionAckDeltaSnapshot = false
	if _, err := NormalizeConsensusParamsUpdate(noAckDelta); err == nil || err.Error() != want {
		t.Fatalf("parameter without promotion_ack_delta_snapshot: err = %v, want %q", err, want)
	}
	u.CustodyFrontier = false
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.CustodyFrontier {
		t.Fatalf("no parameter: %v, parameter %v", err, got.CustodyFrontier)
	}
}
