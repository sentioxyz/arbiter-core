package wire

import (
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// assertEveryFieldPopulated fails for every field of m, at any depth, that is
// unset. A converter that forgets a field leaves it at its zero value, so a
// value seeded with non-zero data everywhere must reach the message with every
// field set. skip names, by full name, the fields that stay unset by design.
func assertEveryFieldPopulated(t *testing.T, m protoreflect.Message, skip map[protoreflect.FullName]bool) {
	t.Helper()
	fields := m.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		if skip[fd.FullName()] {
			continue
		}
		if !m.Has(fd) {
			t.Errorf("%s is unset", fd.FullName())
			continue
		}
		switch {
		case fd.IsList() && fd.Kind() == protoreflect.MessageKind:
			list := m.Get(fd).List()
			for j := 0; j < list.Len(); j++ {
				assertEveryFieldPopulated(t, list.Get(j).Message(), skip)
			}
		case !fd.IsList() && !fd.IsMap() && fd.Kind() == protoreflect.MessageKind:
			assertEveryFieldPopulated(t, m.Get(fd).Message(), skip)
		}
	}
}

func stage3PartitionStatesFixture() PartitionStates {
	return PartitionStates{
		PromotionSeq:  41,
		SafeWatermark: arbiter.SafeWatermark{SnapshotID: "0xsnap", SafeBlockSeq: 17, ManifestRoot: "0xmanifest"},
		Partitions: []PartitionState{
			{
				TableID: "db.t", PartitionID: "p_a", BaseSafeSnapshotID: "0xbase-a", BasePartitionRoot: "0xroot-a", PublishSeq: 40,
				SafeParts: []replay.PartManifestEntry{
					{TableID: "db.t", PartitionID: "p_a", PartName: "a_3_3_0", PartPhysHash: "0xphys-1", PartRowLtHash: "0xrow-1", RowCount: 2, Bytes: 300, StorageRefs: []string{"gs://bucket/a_3_3_0"}},
					{TableID: "db.t", PartitionID: "p_a", PartName: "a_4_4_0", PartPhysHash: "0xphys-2", PartRowLtHash: "0xrow-2", RowCount: 5, Bytes: 700, StorageRefs: []string{"gs://bucket/a_4_4_0", "gs://mirror/a_4_4_0"}},
				},
				PromotedUnsafePartRowLtHashes: []string{"0xrow-1", "0xrow-2"},
				UnackedPromotionSeq:           41,
				LastAckedPromotionSeq:         40,
				InventoryUnrefreshed:          true,
			},
			{
				TableID: "db.t", PartitionID: "p_b", BaseSafeSnapshotID: "0xbase-b", BasePartitionRoot: "0xroot-b", PublishSeq: 12,
				SafeParts: []replay.PartManifestEntry{
					{TableID: "db.t", PartitionID: "p_b", PartName: "b_1_1_0", PartPhysHash: "0xphys-3", PartRowLtHash: "0xrow-3", RowCount: 1, Bytes: 90, StorageRefs: []string{"gs://bucket/b_1_1_0"}},
				},
				PromotedUnsafePartRowLtHashes: []string{"0xrow-3"},
				UnackedPromotionSeq:           13,
				LastAckedPromotionSeq:         12,
				InventoryUnrefreshed:          true,
			},
		},
	}
}

func stage3PromotionOutcomesFixture() PromotionOutcomes {
	track := uint64(2)
	ack := arbiter.PromotionAck{
		NodeID: "snode-3", PromotionSeq: 9, TableID: "db.t", PartitionID: "p_a", PostPartitionCommitment: "0xpost",
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xrow-9", SafePartName: "a_9_9_0", PartPhysHash: "0xphys-9"}},
		SafePartitionParts: []arbiter.SafePartMapping{
			{PartRowLtHash: "0xrow-1", SafePartName: "a_10_10_0", PartPhysHash: "0xphys-1"},
			{PartRowLtHash: "0xrow-9", SafePartName: "a_9_9_0", PartPhysHash: "0xphys-9"},
		},
		Applied: true, Detail: "applied",
	}
	return PromotionOutcomes{
		PromotionSeq: 12,
		Outcomes: []PromotionOutcome{{
			PromotionSeq: 9,
			Promote: arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p_a", PromotionSeq: 9,
				BaseSafeSnapshotID: "0xbase", BasePartitionRoot: "0xroot",
				CandidateParts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p_a", PartRowLtHash: "0xrow-9", PartName: "a_9_9_0"}}},
			StatementSeqs: []uint64{30, 31}, BlockSeq: 8, TrackIndexerID: &track, SourceNode: "snode-3",
			AuthorityJWS: "h.p.s", AuthorityEpoch: 4, Acked: true, Ack: &ack, AckJWS: "a.b.c", AckEpoch: 5, CleanupPending: true,
		}},
		NextAfterPromotionSeq: 9,
	}
}

// TestPartitionStatesConvertersCopyEveryField: GetPartitionStates' query and
// answer survive both converters and the wire bytes with every field (plan
// S3-A contract §3).
func TestPartitionStatesConvertersCopyEveryField(t *testing.T) {
	v := stage3PartitionStatesFixture()
	m := PartitionStatesToPB(v)
	assertEveryFieldPopulated(t, m.ProtoReflect(), nil)
	if got := PartitionStatesFromPB(m); !reflect.DeepEqual(got, v) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, v)
	}
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pb.PartitionStates
	if err := proto.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := PartitionStatesFromPB(&decoded); !reflect.DeepEqual(got, v) {
		t.Fatalf("round trip through the wire bytes\n got %+v\nwant %+v", got, v)
	}
	q := PartitionStatesQuery{TableID: "db.t", PartitionID: "p_a"}
	qm := PartitionStatesQueryToPB(q)
	assertEveryFieldPopulated(t, qm.ProtoReflect(), nil)
	if got := PartitionStatesQueryFromPB(qm); got != q {
		t.Fatalf("query round trip = %+v, want %+v", got, q)
	}
}

// TestPromotionOutcomesConvertersCopyEveryField: the same for
// GetPromotionOutcomes. The stored acknowledgement is the body the source
// signed without its request-only source_jws; its signature travels in ack_jws.
func TestPromotionOutcomesConvertersCopyEveryField(t *testing.T) {
	v := stage3PromotionOutcomesFixture()
	m := PromotionOutcomesToPB(v)
	assertEveryFieldPopulated(t, m.ProtoReflect(), map[protoreflect.FullName]bool{"arbiter.PromotionAck.source_jws": true})
	if got := PromotionOutcomesFromPB(m); !reflect.DeepEqual(got, v) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, v)
	}
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	var decoded pb.PromotionOutcomes
	if err := proto.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if got := PromotionOutcomesFromPB(&decoded); !reflect.DeepEqual(got, v) {
		t.Fatalf("round trip through the wire bytes\n got %+v\nwant %+v", got, v)
	}
	q := PromotionOutcomesQuery{TableID: "db.t", PartitionID: "p_a", AfterPromotionSeq: 8, Limit: 64}
	qm := PromotionOutcomesQueryToPB(q)
	assertEveryFieldPopulated(t, qm.ProtoReflect(), nil)
	if got := PromotionOutcomesQueryFromPB(qm); got != q {
		t.Fatalf("query round trip = %+v, want %+v", got, q)
	}
}

// TestStage3ReadsKeepAbsenceAndIndexerZero: an outcome issued before the
// tracks activation has no track and, until acknowledged, no ack; both stay
// absent both ways. A track of indexer 0 is present (message presence keeps
// indexer 0 distinct from absent), and the converter copies the id rather
// than the caller's pointer. Empty lists are absent both ways, and an absent
// watermark is the zero watermark.
func TestStage3ReadsKeepAbsenceAndIndexerZero(t *testing.T) {
	o := PromotionOutcome{PromotionSeq: 3, Promote: arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "all", PromotionSeq: 3}, StatementSeqs: []uint64{}}
	m := PromotionOutcomesToPB(PromotionOutcomes{Outcomes: []PromotionOutcome{o}}).GetOutcomes()[0]
	if m.GetTrack() != nil || m.GetAck() != nil || m.GetStatementSeqs() != nil {
		t.Fatalf("absent track, ack and statement seqs encoded as %v, %v, %v", m.GetTrack(), m.GetAck(), m.GetStatementSeqs())
	}
	back := PromotionOutcomesFromPB(&pb.PromotionOutcomes{Outcomes: []*pb.PromotionOutcome{m}}).Outcomes[0]
	o.StatementSeqs = nil
	if !reflect.DeepEqual(back, o) || back.TrackIndexerID != nil || back.Ack != nil {
		t.Fatalf("absent fields decoded as %+v, want %+v", back, o)
	}
	zero := uint64(0)
	o.TrackIndexerID = &zero
	m = PromotionOutcomesToPB(PromotionOutcomes{Outcomes: []PromotionOutcome{o}}).GetOutcomes()[0]
	if m.GetTrack() == nil || m.GetTrack().GetIndexerId() != 0 {
		t.Fatalf("track of indexer 0 encoded as %v, want a present track", m.GetTrack())
	}
	back = PromotionOutcomesFromPB(&pb.PromotionOutcomes{Outcomes: []*pb.PromotionOutcome{m}}).Outcomes[0]
	if back.TrackIndexerID == nil || *back.TrackIndexerID != 0 || back.TrackIndexerID == o.TrackIndexerID {
		t.Fatalf("track of indexer 0 decoded as %v (the caller's pointer %p)", back.TrackIndexerID, o.TrackIndexerID)
	}
	s := PartitionStates{Partitions: []PartitionState{{TableID: "db.t", PartitionID: "all", SafeParts: []replay.PartManifestEntry{}, PromotedUnsafePartRowLtHashes: []string{}}}}
	ms := PartitionStatesToPB(s)
	if e := ms.GetPartitions()[0]; e.GetSafeParts() != nil || e.GetPromotedUnsafePartRowLthashes() != nil {
		t.Fatalf("empty lists encoded as %v, %v", e.GetSafeParts(), e.GetPromotedUnsafePartRowLthashes())
	}
	if got := PartitionStatesFromPB(ms).Partitions[0]; got.SafeParts != nil || got.PromotedUnsafePartRowLtHashes != nil {
		t.Fatalf("empty lists decoded as %#v, %#v; want nil", got.SafeParts, got.PromotedUnsafePartRowLtHashes)
	}
	if got := PartitionStatesFromPB(&pb.PartitionStates{}); !reflect.DeepEqual(got, PartitionStates{}) {
		t.Fatalf("an empty answer decoded as %+v", got)
	}
}

// TestPartManifestEntryConvertersRoundTrip: the exported pair is the manifest's
// own converter (the FSM's SafeParts and a manifest's active parts share
// PartManifestEntry), every field included.
func TestPartManifestEntryConvertersRoundTrip(t *testing.T) {
	e := replay.PartManifestEntry{TableID: "db.t", PartitionID: "p_a", PartName: "a_3_3_0", PartPhysHash: "0xphys", PartRowLtHash: "0xrow", RowCount: 2, Bytes: 300, StorageRefs: []string{"gs://bucket/a_3_3_0"}}
	m := PartManifestEntryToPB(e)
	assertEveryFieldPopulated(t, m.ProtoReflect(), nil)
	if got := PartManifestEntryFromPB(m); !reflect.DeepEqual(got, e) {
		t.Fatalf("round trip = %+v, want %+v", got, e)
	}
	if got := tableManifestFromPB(tableManifestToPB(replay.TableManifest{TableID: "db.t", ActiveParts: []replay.PartManifestEntry{e}})).ActiveParts[0]; !reflect.DeepEqual(got, PartManifestEntryFromPB(m)) {
		t.Fatalf("the manifest converter and the exported one disagree: %+v vs %+v", got, e)
	}
}
