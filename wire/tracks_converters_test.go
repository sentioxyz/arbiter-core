package wire

import (
	"reflect"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestConsensusParamsUpdateCarriesTracksEnabled: the converters copy the tracks
// activation flag both ways; an update without it is the pre-tracks message,
// whose bytes TestLegacyCommandBytesAreFrozen and
// TestPreSignedClaimsCommandBytesAreFrozen keep pinned.
func TestConsensusParamsUpdateCarriesTracksEnabled(t *testing.T) {
	u := legacyGoldenUpdate()
	if ConsensusParamsUpdateToPB(u).GetTracksEnabled() {
		t.Fatal("an update without the activation set tracks_enabled")
	}
	u.TracksEnabled = true
	m := ConsensusParamsUpdateToPB(u)
	if !m.GetTracksEnabled() {
		t.Fatal("ConsensusParamsUpdateToPB dropped tracks_enabled")
	}
	if got := ConsensusParamsUpdateFromPB(m); !reflect.DeepEqual(got, u) {
		t.Fatalf("update round trip\n got %+v\nwant %+v", got, u)
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
}

// TestCleanupConvertersCarryRejectedBlockSeq: a rejected-part cleanup and its
// acknowledgement keep rejected_block_seq through every converter on their
// path: Raft (ScheduleUnsafeCleanup, RecordCleanupAck), the SNode dispatch
// stream (CleanupCommandPB) and the AckCleanup request (plan S2-A
// clarification C4). A promotion cleanup carries none.
func TestCleanupConvertersCarryRejectedBlockSeq(t *testing.T) {
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12,
		Parts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "all", PartRowLtHash: "0xaa", PartName: "all_1_1_0"}}}
	if got := CleanupFromPB(CleanupToPB(cleanup)); !reflect.DeepEqual(got, cleanup) {
		t.Fatalf("cleanup round trip = %+v", got)
	}
	if got := CleanupCommandPB(cleanup, "h.p.s").GetCleanup().GetRejectedBlockSeq(); got != 12 {
		t.Fatalf("dispatched cleanup rejected_block_seq = %d, want 12", got)
	}
	mustRoundTrip(t, Command{ScheduleUnsafeCleanup: &ScheduleUnsafeCleanup{Cleanup: cleanup, AuthorityJWS: "h.p.s"}})
	ack := arbiter.CleanupAck{NodeID: "snode-2", TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12}
	if got := CleanupAckFromPB(CleanupAckToPB(ack)); got != ack {
		t.Fatalf("ack round trip = %+v", got)
	}
	signed := RecordCleanupAck{Ack: ack, SourceJWS: "h.p.s"}
	if got := RecordCleanupAckFromRequest(RecordCleanupAckToRequest(signed)); got != signed {
		t.Fatalf("ack request round trip = %+v", got)
	}
	mustRoundTrip(t, Command{RecordCleanupAck: &signed})
	if got := CleanupToPB(arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 7}).GetRejectedBlockSeq(); got != 0 {
		t.Fatalf("a promotion cleanup carries rejected_block_seq %d", got)
	}
}

// TestTableRegistrySnapshotCarriesQuarantinedIndexers: the registry view
// carries the quarantined set ascending (plan S2-A contract §2); absent and
// empty decode to nil, and the decoder refuses an unsorted or duplicated list
// and (per amendment A3 (proposed as G2-2)) an id without an si_indexers entry, like
// seeded_indexers.
func TestTableRegistrySnapshotCarriesQuarantinedIndexers(t *testing.T) {
	s := registrySnapshotFixture(t)
	s.SIIndexers = []arbiter.SIIndexerEntry{siEntryW(0, 100, "snode-1"), siEntryW(1, 100, "snode-2"), siEntryW(2, 300, "snode-3")}
	got, err := TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || got.QuarantinedIndexers != nil {
		t.Fatalf("no quarantine: %v, %v", got.QuarantinedIndexers, err)
	}
	s.QuarantinedIndexers = []uint64{0, 2}
	got, err = TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip (%v)\n got %+v\nwant %+v", err, got, s)
	}
	for name, ids := range map[string][]uint64{
		"unsorted":      {2, 0},
		"duplicated":    {0, 0},
		"without entry": {0, 9},
	} {
		t.Run(name, func(t *testing.T) {
			m := TableRegistrySnapshotToPB(s)
			m.QuarantinedIndexers = ids
			if _, err := TableRegistrySnapshotFromPB(m); err == nil || !strings.Contains(err.Error(), "quarantined") {
				t.Fatalf("decoder accepted quarantined_indexers %v: %v", ids, err)
			}
		})
	}
}
