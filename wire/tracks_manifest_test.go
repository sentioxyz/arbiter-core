package wire

import (
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"
)

// sealedTrackAwareManifest is a sealed manifest published after the tracks
// activation: one track_frontiers entry per enrolled indexer, indexer 0
// included, sorted by indexer id (housegate spec 2026-10-10 §7.3).
func sealedTrackAwareManifest(t *testing.T) replay.SafeSnapshotManifest {
	t.Helper()
	m, err := (replay.SafeSnapshotManifest{
		ParentSnapshotID: "0xparent", SafeBlockSeq: 7, SchemaSnapshotID: "schema-genesis", SchemaRoot: "0xschema",
		ExecutorProfileID: "housegate-replay-mvp-v0",
		Tables: []replay.TableManifest{{TableID: "db1.t", SchemaHash: "0xh",
			PartitionRoots: []replay.PartitionCommitment{{TableID: "db1.t", PartitionID: "all", Root: "0xr"}}}},
		TrackFrontiers: []replay.TrackFrontier{
			{IndexerID: 0, SafeBlockSeq: 7, AppliedPromotionSeq: 3},
			{IndexerID: 1, SafeBlockSeq: 9, AppliedPromotionSeq: 5},
		},
	}).Seal()
	if err != nil {
		t.Fatalf("seal track-aware manifest: %v", err)
	}
	return m
}

// TestManifestConvertersCarryTrackFrontiers: ManifestFromPB / ManifestToPB
// copy track_frontiers, indexer 0 and zero values included; an empty list
// decodes to nil, the canonical absent form, so a manifest without frontiers
// keeps its root (TestSnapshotQueryPairedGolden) and its Raft bytes
// (TestPreTracksCommandBytesAreFrozen).
func TestManifestConvertersCarryTrackFrontiers(t *testing.T) {
	m := sealedTrackAwareManifest(t)
	pbm := ManifestToPB(m)
	if f := pbm.GetTrackFrontiers(); len(f) != 2 || f[0].GetIndexerId() != 0 || f[0].GetSafeBlockSeq() != 7 || f[1].GetAppliedPromotionSeq() != 5 {
		t.Fatalf("ManifestToPB track_frontiers = %v", f)
	}
	if got := ManifestFromPB(pbm); !reflect.DeepEqual(got, m) {
		t.Fatalf("manifest round trip\n got %+v\nwant %+v", got, m)
	}
	zero := replay.TrackFrontier{}
	if got := TrackFrontierFromPB(TrackFrontierToPB(zero)); got != zero {
		t.Fatalf("zero frontier round trip = %+v", got)
	}
	if got := ManifestFromPB(&pb.SafeSnapshotManifest{SnapshotId: "s", TrackFrontiers: []*pb.TrackFrontier{}}); got.TrackFrontiers != nil {
		t.Fatal("an empty track_frontiers list must decode to nil")
	}
	if got := ManifestToPB(replay.SafeSnapshotManifest{SnapshotID: "s"}).GetTrackFrontiers(); got != nil {
		t.Fatalf("a manifest without frontiers encoded %v", got)
	}
}

// TestTrackAwareManifestSurvivesPublishSafeSnapshot: a track-aware manifest
// round-trips through the PublishSafeSnapshot Raft command and still
// validates, because its manifest_root covers the frontiers; the same
// manifest stripped of them fails validation, which is what a converter that
// dropped the field would do to every verifier.
func TestTrackAwareManifestSurvivesPublishSafeSnapshot(t *testing.T) {
	m := sealedTrackAwareManifest(t)
	out := mustRoundTrip(t, Command{PublishSafeSnapshot: &PublishSafeSnapshot{Manifest: m}})
	if err := out.PublishSafeSnapshot.Manifest.Validate(); err != nil {
		t.Fatalf("decoded track-aware manifest: %v", err)
	}
	stripped := out.PublishSafeSnapshot.Manifest
	stripped.TrackFrontiers = nil
	if err := stripped.Validate(); err == nil || !strings.Contains(err.Error(), "manifest_root mismatch") {
		t.Fatalf("manifest stripped of its frontiers: Validate = %v, want a manifest_root mismatch", err)
	}
}

// TestArtifactDispositionRootOmitsTrackFrontiers: the C1 canonical manifest DTO
// has no track_frontiers member, so rejectNilSlices skips the raw field
// (canonicalProjectionOmits) and the command root is the one every earlier
// release computes, whether the raw manifest carries no frontiers (nil, the
// decoded form) or any (plan S2-A contract §2).
func TestArtifactDispositionRootOmitsTrackFrontiers(t *testing.T) {
	if _, ok := reflect.TypeOf(canonicalSafeSnapshotManifest{}).FieldByName("TrackFrontiers"); ok {
		t.Fatal("canonical manifest DTO carries TrackFrontiers; drop the canonicalProjectionOmits entry")
	}
	register := func(frontiers []replay.TrackFrontier) ArtifactDispositionCommandV1 {
		return dispositionCommand(ArtifactDispositionActionV1{RegisterCandidate: &ArtifactDispositionRegisterCandidateV1{
			Manifest: replay.SafeSnapshotManifest{SnapshotID: "snapshot", Tables: []replay.TableManifest{}, TrackFrontiers: frontiers},
		}})
	}
	want, err := ArtifactDispositionCommandRoot(register(nil))
	if err != nil {
		t.Fatalf("nil track frontiers rejected: %v", err)
	}
	for name, frontiers := range map[string][]replay.TrackFrontier{
		"empty":     {},
		"populated": {{IndexerID: 0, SafeBlockSeq: 5, AppliedPromotionSeq: 2}},
	} {
		if got, err := ArtifactDispositionCommandRoot(register(frontiers)); err != nil || got != want {
			t.Fatalf("%s track frontiers: root %q, %v; want %q", name, got, err, want)
		}
	}
}
