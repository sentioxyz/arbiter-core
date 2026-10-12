package wire

import (
	"encoding/hex"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// preTracksGolden lists, for every Raft command the tracks stage extends that
// no earlier golden pins, the shape a pre-activation leader proposes and its
// exact bytes, computed from arbiter-proto dece588 (v0.10.0) with
// protoc --encode=arbiter.RaftCommand. The other extended commands are pinned
// elsewhere: UpdateConsensusParams by TestLegacyCommandBytesAreFrozen and
// TestPreSignedClaimsCommandBytesAreFrozen, RecordCleanupAck by the latter.
func preTracksGolden() []signedClaimsGolden {
	return []signedClaimsGolden{
		{"schedule_unsafe_cleanup", Command{ScheduleUnsafeCleanup: &ScheduleUnsafeCleanup{
			Cleanup: arbiter.UnsafeCleanup{TableID: "db1.t", PartitionID: "all", PromotionSeq: 7,
				Parts: []arbiter.PartRef{{TableID: "db1.t", PartitionID: "all", PartRowLtHash: "0xaa", PartName: "all_1_1_0"}}},
			AuthorityJWS: "h.p.s"}},
			"5a360a2d0a056462312e741203616c6c1807221d0a056462312e741203616c6c1a04307861612209616c6c5f315f315f301205682e702e73"},
		{"publish_safe_snapshot", Command{PublishSafeSnapshot: &PublishSafeSnapshot{Manifest: replay.SafeSnapshotManifest{
			SnapshotID: "0xsnap", ParentSnapshotID: "0xparent", SafeBlockSeq: 7, StateRoot: "0xstate",
			SchemaSnapshotID: "schema-genesis", SchemaRoot: "0xschema", ExecutorProfileID: "housegate-replay-mvp-v0",
			DataRoot: "0xdata", ManifestRoot: "0xmanifest",
			Tables: []replay.TableManifest{{TableID: "db1.t", SchemaHash: "0xh",
				PartitionRoots: []replay.PartitionCommitment{{TableID: "db1.t", PartitionID: "all", Root: "0xr"}}}}}}},
			"5288010a85010a063078736e617012083078706172656e7418072207307873746174652a0e736368656d612d67656e6573697332083078736368656d613a17686f757365676174652d7265706c61792d6d76702d763042063078646174614a0a30786d616e6966657374521f0a056462312e7412033078681a110a056462312e741203616c6c1a03307872"},
	}
}

// TestPreTracksCommandBytesAreFrozen pins the RaftCommand bytes of the cleanup
// and publication commands in their pre-tracks shape (housegate spec
// 2026-10-10 §7; plan S2-A contract §0). Until the tracks activation every
// voter, upgraded or not, must decode exactly these bytes: a
// rejected_block_seq or track_frontiers leaking into one of them forks an
// un-upgraded voter. Never regenerate these values: a diff here is a
// consensus break.
func TestPreTracksCommandBytesAreFrozen(t *testing.T) {
	for _, tc := range preTracksGolden() {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Encode(tc.cmd)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if hex.EncodeToString(got) != tc.want {
				t.Fatalf("encoded bytes changed:\n got %s\nwant %s", hex.EncodeToString(got), tc.want)
			}
			if _, err := Decode(got); err != nil {
				t.Fatalf("Decode(own bytes): %v", err)
			}
		})
	}
}
