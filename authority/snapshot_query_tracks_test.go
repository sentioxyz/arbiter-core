package authority

import (
	"testing"
)

// TestNormalizeConsensusParamsUpdateSnapshotQueryTracks: the parameter needs
// tracks_enabled and promotion_ack_delta_snapshot in the same update (every
// update carries both once they are set), and it survives normalization.
func TestNormalizeConsensusParamsUpdateSnapshotQueryTracks(t *testing.T) {
	u := stage3TestUpdate()
	u.ManifestDeltaSnapshot, u.PromotionAckDeltaSnapshot, u.SnapshotQueryTracks = true, true, true
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || !got.SnapshotQueryTracks {
		t.Fatalf("parameter with its prerequisites: %v, parameter %v", err, got.SnapshotQueryTracks)
	}
	const want = "consensus params update: snapshot_query_tracks requires tracks_enabled and promotion_ack_delta_snapshot in the same update"
	noAck := u
	noAck.PromotionAckDeltaSnapshot = false
	if _, err := NormalizeConsensusParamsUpdate(noAck); err == nil || err.Error() != want {
		t.Fatalf("parameter without promotion_ack_delta_snapshot: err = %v, want %q", err, want)
	}
	noTracks := u
	noTracks.TracksEnabled, noTracks.Stage3 = false, nil
	noTracks.ManifestDeltaSnapshot, noTracks.PromotionAckDeltaSnapshot = false, false
	if _, err := NormalizeConsensusParamsUpdate(noTracks); err == nil || err.Error() != want {
		t.Fatalf("parameter without tracks_enabled: err = %v, want %q", err, want)
	}
	u.SnapshotQueryTracks = false
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.SnapshotQueryTracks {
		t.Fatalf("no parameter: %v, parameter %v", err, got.SnapshotQueryTracks)
	}
}
