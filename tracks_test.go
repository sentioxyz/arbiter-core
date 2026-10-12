package arbiter

import (
	"encoding/json"
	"strings"
	"testing"
)

// TestTracksCanonicalForms pins the canonical JSON every signed or hashed form
// of the tracks stage produces (housegate spec 2026-10-10 §7; plan S2-A
// contract §2): each new member is omitted while zero, so every digest, Raft
// payload and signed body computed before the tracks activation is unchanged,
// and it is the last member when set, which fixes the new preimages. Indexer 0
// and generation 0 are signed values, never omitted.
func TestTracksCanonicalForms(t *testing.T) {
	const update = `{"network_id":"n","genesis_snapshot_id":"","expected_epoch":0,"previous_params_digest":"","authority_addresses":null,"max_writers":0,"expected_promotion_seq":0`
	for name, tc := range map[string]struct {
		v    any
		want string
	}{
		"update before the activation": {ConsensusParamsUpdate{NetworkID: "n"}, update + `}`},
		"tracks activation update":     {ConsensusParamsUpdate{NetworkID: "n", TracksEnabled: true}, update + `,"tracks_enabled":true}`},
		"promotion cleanup": {UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 7},
			`{"table_id":"db.t","partition_id":"all","promotion_seq":7,"parts":null}`},
		"rejected-part cleanup": {UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12},
			`{"table_id":"db.t","partition_id":"all","promotion_seq":0,"parts":null,"rejected_block_seq":12}`},
		"promotion cleanup ack": {CleanupAck{NodeID: "snode-2", PromotionSeq: 7, TableID: "db.t", PartitionID: "all"},
			`{"node_id":"snode-2","promotion_seq":7,"table_id":"db.t","partition_id":"all"}`},
		"rejected-part cleanup ack": {CleanupAck{NodeID: "snode-2", TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12},
			`{"node_id":"snode-2","promotion_seq":0,"table_id":"db.t","partition_id":"all","rejected_block_seq":12}`},
		"quarantine of indexer 0 at generation 0": {QuarantineIndexerCommand{Reason: "stalled"},
			`{"indexer_id":0,"expected_generation":0,"reason":"stalled"}`},
		"release without rejected blocks": {ReleaseIndexerCommand{IndexerID: 1, ExpectedGeneration: 1},
			`{"indexer_id":1,"expected_generation":1}`},
		"release with an empty list": {ReleaseIndexerCommand{IndexerID: 1, ExpectedGeneration: 1, AcknowledgedRejectedBlocks: []uint64{}},
			`{"indexer_id":1,"expected_generation":1}`},
		"release acknowledging blocks": {ReleaseIndexerCommand{IndexerID: 1, ExpectedGeneration: 2, AcknowledgedRejectedBlocks: []uint64{7, 9}},
			`{"indexer_id":1,"expected_generation":2,"acknowledged_rejected_blocks":[7,9]}`},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := json.Marshal(tc.v)
			if err != nil || string(b) != tc.want {
				t.Fatalf("canonical JSON = %s (%v), want %s", b, err, tc.want)
			}
		})
	}
}

// TestReleaseIndexerCommandValidate: a release names an exact set of rejected
// blocks, sorted ascending without duplicates (spec §7.5).
func TestReleaseIndexerCommandValidate(t *testing.T) {
	for _, ok := range [][]uint64{nil, {}, {7}, {7, 9}} {
		if err := (ReleaseIndexerCommand{IndexerID: 1, AcknowledgedRejectedBlocks: ok}).Validate(); err != nil {
			t.Fatalf("%v: %v", ok, err)
		}
	}
	for _, bad := range [][]uint64{{9, 7}, {7, 7}, {7, 9, 9}} {
		err := (ReleaseIndexerCommand{IndexerID: 1, AcknowledgedRejectedBlocks: bad}).Validate()
		if err == nil || !strings.Contains(err.Error(), "release indexer 1: acknowledged_rejected_blocks must be sorted ascending without duplicates") {
			t.Fatalf("%v: err = %v", bad, err)
		}
	}
}
