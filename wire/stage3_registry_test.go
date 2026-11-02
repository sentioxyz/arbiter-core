package wire

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// stage3RegistryFixture is registrySnapshotFixture after the placement and
// checkpoints switches: three enrolled indexers, incarnation 4 on a scoped
// Keeper path with its purge members, group 0/dex (a ready member with a
// stored checkpoint, a delisted Lagging member, a released Leaving member)
// and group 1/dex (a Behind member hosted by indexer 0, an unreleased Leaving
// member), three published checkpoint sequences and the stage3 parameter.
func stage3RegistryFixture(t *testing.T) TableRegistrySnapshot {
	t.Helper()
	s := registrySnapshotFixture(t)
	s.SIIndexers = []arbiter.SIIndexerEntry{siEntryW(0, 100, "snode-1"), siEntryW(1, 100, "snode-2"), siEntryW(2, 300, "snode-3")}
	s.Incarnations[3].ScopedKeeperPath = true
	s.Incarnations[3].PurgeMembers = []string{"member-a", "member-b"}
	host0, host1 := uint64(0), uint64(1)
	entry := replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 90, AppliedPromotionSeq: 40}
	observed := replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 70, AppliedPromotionSeq: 31}
	s.Groups = []DatabaseGroup{
		{OwnerIndexerID: 0, DatabaseID: "dex", NetworkFill: true, Generation: 3, GroupKey: "0xgroup-0-dex", PlacementSalt: "0xsalt-0-dex",
			Members: []GroupMember{
				{NodeID: "member-b", JoinedGeneration: 1, ReadyGeneration: 2, Class: MemberClassCaughtUp, HostIndexerID: &host1,
					Checkpoint: &MemberCheckpointRecord{ReplicaJWS: "r.j.s", Epoch: 6, Checkpoint: arbiter.MemberCheckpoint{
						NodeID: "member-b", OwnerIndexerID: 0, CheckpointSeq: 9, SnapshotID: "0xsnap", Entry: entry,
						Groups: []arbiter.CheckpointGroup{{DatabaseID: "dex", GroupGeneration: 3,
							Excluded: []arbiter.ExcludedTable{{TableID: "dex.pools", Reason: arbiter.ExclusionAuditPending}}}}}}},
				{NodeID: "member-c", JoinedGeneration: 3, Class: MemberClassLagging, ObservedEntry: &observed, LagAdvances: 5},
			},
			PastMembers: []string{"member-a"},
			Leaving: []LeavingMember{{NodeID: "member-a", RemovedGeneration: 3, ReleaseEntry: entry,
				FrozenMembers: []string{"member-b", "member-c"}, FrozenDomains: 2, Released: true}}},
		{OwnerIndexerID: 1, DatabaseID: "dex", Generation: 1, GroupKey: "0xgroup-1-dex", PlacementSalt: "0xsalt-1-dex",
			Members: []GroupMember{{NodeID: "member-a", JoinedGeneration: 1, ReadyGeneration: 1, Class: MemberClassBehind, HostIndexerID: &host0}},
			Leaving: []LeavingMember{{NodeID: "member-d", RemovedGeneration: 1,
				ReleaseEntry: replay.TrackFrontier{IndexerID: 1, SafeBlockSeq: 12, AppliedPromotionSeq: 4}}}},
	}
	s.MemberCheckpointSeqs = []MemberCheckpointSeq{
		{NodeID: "member-a", OwnerIndexerID: 1, CheckpointSeq: 2, ReplicaSigner: "0x00000000000000000000000000000000000000ea"},
		{NodeID: "member-b", OwnerIndexerID: 0, CheckpointSeq: 9, ReplicaSigner: "0x00000000000000000000000000000000000000eb"},
		{NodeID: "member-b", OwnerIndexerID: 2, CheckpointSeq: 4, ReplicaSigner: "0x00000000000000000000000000000000000000eb"},
	}
	s.Stage3 = stage3ParamsFixture()
	return s
}

// TestTableRegistrySnapshotCarriesGroupsSeqsScopedPathsAndStage3: the
// registry view of groups, checkpoint sequences, scoped Keeper paths, purge
// members and the stage3 parameter survives both converters and the wire
// bytes (plan S3-E1 contract §41, K64), and the lookups answer from it. A
// snapshot from before the v21 activation has none of these fields on the
// wire, and empty lists decode to nil.
func TestTableRegistrySnapshotCarriesGroupsSeqsScopedPathsAndStage3(t *testing.T) {
	s := stage3RegistryFixture(t)
	m := TableRegistrySnapshotToPB(s)
	if inc := m.GetIncarnations()[3]; !inc.GetScopedKeeperPath() || !reflect.DeepEqual(inc.GetPurgeMembers(), []string{"member-a", "member-b"}) {
		t.Fatalf("incarnation 4 encoded as scoped %v, purge members %v", inc.GetScopedKeeperPath(), inc.GetPurgeMembers())
	}
	if len(m.GetGroups()) != 2 || len(m.GetMemberCheckpointSeqs()) != 3 || m.GetStage3().GetGroupSize() != 3 {
		t.Fatalf("stage-3 view encoded as %d groups, %d seqs, stage3 %v", len(m.GetGroups()), len(m.GetMemberCheckpointSeqs()), m.GetStage3())
	}
	var decoded pb.TableRegistrySnapshot
	if err := proto.Unmarshal(mustMarshal(t, m), &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := TableRegistrySnapshotFromPB(&decoded)
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip (%v)\n got %+v\nwant %+v", err, got, s)
	}

	if g, ok := got.Group(1, "dex"); !ok || g.Members[0].NodeID != "member-a" || g.GroupKey != "0xgroup-1-dex" {
		t.Fatalf("Group(1, dex) = %+v, %v", g, ok)
	}
	if _, ok := got.Group(2, "dex"); ok {
		t.Fatal("Group(2, dex) found a group that does not exist")
	}
	for node, want := range map[string][]string{
		"member-a": {"1/dex"}, // released from 0/dex, a member of 1/dex
		"member-b": {"0/dex"}, // a current member
		"member-c": {"0/dex"}, // delisted, still a member
		"member-d": {"1/dex"}, // Leaving and not yet released
		"member-x": nil,       // never a member
	} {
		var keys []string
		for _, g := range got.GroupsWithMember(node) {
			keys = append(keys, fmt.Sprintf("%d/%s", g.OwnerIndexerID, g.DatabaseID))
		}
		if !reflect.DeepEqual(keys, want) {
			t.Errorf("GroupsWithMember(%s) = %v, want %v", node, keys, want)
		}
	}
	if seq, ok := got.PublishedCheckpointSeq("member-b", 2); !ok || seq.CheckpointSeq != 4 {
		t.Fatalf("PublishedCheckpointSeq(member-b, 2) = %+v, %v", seq, ok)
	}
	if _, ok := got.PublishedCheckpointSeq("member-b", 1); ok {
		t.Fatal("PublishedCheckpointSeq(member-b, 1) found a sequence that was never published")
	}

	before := TableRegistrySnapshotToPB(registrySnapshotFixture(t))
	for _, num := range wireFieldNumbers(t, mustMarshal(t, before)) {
		if num >= 10 && num <= 12 {
			t.Fatalf("a pre-stage-3 snapshot carries field %d", num)
		}
	}
	for _, inc := range before.GetIncarnations() {
		for _, num := range wireFieldNumbers(t, mustMarshal(t, inc)) {
			if num == 19 || num == 20 {
				t.Fatalf("a pre-stage-3 incarnation carries field %d", num)
			}
		}
	}
	plain, err := TableRegistrySnapshotFromPB(before)
	if err != nil || plain.Groups != nil || plain.MemberCheckpointSeqs != nil || plain.Stage3 != nil || plain.Incarnations[3].ScopedKeeperPath || plain.Incarnations[3].PurgeMembers != nil {
		t.Fatalf("a pre-stage-3 snapshot decoded with stage-3 fields: %+v, %v", plain, err)
	}
	m.Groups[1].PastMembers, m.Groups[1].Leaving[0].FrozenMembers, m.Incarnations[0].PurgeMembers = []string{}, []string{}, []string{}
	if got, err = TableRegistrySnapshotFromPB(m); err != nil || got.Groups[1].PastMembers != nil || got.Groups[1].Leaving[0].FrozenMembers != nil || got.Incarnations[0].PurgeMembers != nil {
		t.Fatalf("empty lists must decode to nil: %+v, %v", got.Groups[1], err)
	}
}

// TestTableRegistrySnapshotEveryStage3FieldRoundTrips: every stage-3 field of
// the registry view, seeded by reflection with its own value (the
// seedSnapshot precedent) and then given the few values the decoder requires,
// reaches the message and comes back unchanged through the wire bytes. Each
// Go view type has its message's field count, so a field added to either
// fails here until both converters carry it.
func TestTableRegistrySnapshotEveryStage3FieldRoundTrips(t *testing.T) {
	for _, tc := range []struct {
		goValue any
		msg     proto.Message
	}{
		{DatabaseGroup{}, &pb.DatabaseGroup{}},
		{GroupMember{}, &pb.GroupMember{}},
		{LeavingMember{}, &pb.LeavingMember{}},
		{MemberCheckpointRecord{}, &pb.MemberCheckpointRecord{}},
		{MemberCheckpointSeq{}, &pb.MemberCheckpointSeq{}},
		{arbiter.Stage3Params{}, &pb.Stage3Params{}},
		{arbiter.IndexerFailureDomain{}, &pb.IndexerFailureDomain{}},
		{arbiter.MemberCheckpoint{}, &pb.MemberCheckpoint{}},
		{arbiter.CheckpointGroup{}, &pb.CheckpointGroup{}},
		{arbiter.ExcludedTable{}, &pb.ExcludedTable{}},
	} {
		if got, want := reflect.TypeOf(tc.goValue).NumField(), tc.msg.ProtoReflect().Descriptor().Fields().Len(); got != want {
			t.Errorf("%T has %d fields, %s has %d", tc.goValue, got, tc.msg.ProtoReflect().Descriptor().FullName(), want)
		}
	}

	s := registrySnapshotFixture(t)
	s.SIIndexers = []arbiter.SIIndexerEntry{siEntryW(1, 100, "snode-1"), siEntryW(2, 100, "snode-2")}
	var serial uint64
	seedSnapshot(reflect.ValueOf(&s.Groups).Elem(), "Groups", &serial)
	seedSnapshot(reflect.ValueOf(&s.MemberCheckpointSeqs).Elem(), "Seqs", &serial)
	seedSnapshot(reflect.ValueOf(&s.Stage3).Elem(), "Stage3", &serial)
	for i := range s.Incarnations {
		seedSnapshot(reflect.ValueOf(&s.Incarnations[i].ScopedKeeperPath).Elem(), "Scoped", &serial)
		seedSnapshot(reflect.ValueOf(&s.Incarnations[i].PurgeMembers).Elem(), fmt.Sprintf("Incarnations[%d].PurgeMembers", i), &serial)
	}
	// The decoder accepts only a view the arbiter can serve: enrolled owners,
	// known classes and reasons, and lag_low below max_lag_publications. Every
	// field stays non-zero.
	classes := []MemberClass{MemberClassCaughtUp, MemberClassLagging}
	reasons := []arbiter.CheckpointExclusionReason{arbiter.ExclusionDiverged, arbiter.ExclusionForeign}
	for gi := range s.Groups {
		g := &s.Groups[gi]
		g.OwnerIndexerID = uint64(gi) + 1
		for mi := range g.Members {
			g.Members[mi].Class = classes[mi]
			for ci := range g.Members[mi].Checkpoint.Checkpoint.Groups {
				for ei := range g.Members[mi].Checkpoint.Checkpoint.Groups[ci].Excluded {
					g.Members[mi].Checkpoint.Checkpoint.Groups[ci].Excluded[ei].Reason = reasons[ei]
				}
			}
		}
	}
	s.Stage3.LagLow = s.Stage3.MaxLagPublications - 1

	m := TableRegistrySnapshotToPB(s)
	for _, g := range m.GetGroups() {
		assertEveryFieldPopulated(t, g.ProtoReflect(), nil)
	}
	for _, seq := range m.GetMemberCheckpointSeqs() {
		assertEveryFieldPopulated(t, seq.ProtoReflect(), nil)
	}
	assertEveryFieldPopulated(t, m.GetStage3().ProtoReflect(), nil)
	for _, inc := range m.GetIncarnations() {
		if !inc.GetScopedKeeperPath() || len(inc.GetPurgeMembers()) != 2 {
			t.Fatalf("incarnation %d encoded as scoped %v, purge members %v", inc.GetSeq(), inc.GetScopedKeeperPath(), inc.GetPurgeMembers())
		}
	}
	var decoded pb.TableRegistrySnapshot
	if err := proto.Unmarshal(mustMarshal(t, m), &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := TableRegistrySnapshotFromPB(&decoded)
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip (%v)\n got %#v\nwant %#v", err, got, s)
	}
}

// TestTableRegistrySnapshotRefusesCorruptStage3Views: the decoder refuses,
// word for word, every stage-3 view the arbiter never serves (plan S3-E1
// contract §41), like an unknown status.
func TestTableRegistrySnapshotRefusesCorruptStage3Views(t *testing.T) {
	const (
		groupsOrder = "table registry snapshot: groups must be sorted by (owner_indexer_id, database_id) without duplicates"
		listsOrder  = "table registry snapshot: group 0/dex: members, past_members and leaving must each be sorted by node id without duplicates"
		seqsOrder   = "table registry snapshot: member_checkpoint_seqs must be sorted by (node_id, owner_indexer_id) without duplicates"
		purgeOrder  = "table registry snapshot: incarnation 4: purge_members must be sorted ascending without duplicates"
	)
	type snapshot = pb.TableRegistrySnapshot
	for _, tc := range []struct {
		name   string
		mutate func(m *snapshot)
		want   string
	}{
		{"absent stored checkpoint body", func(m *snapshot) { m.Groups[0].Members[0].Checkpoint.Checkpoint = nil },
			"table registry snapshot: group 0/dex: member member-b: wire: member checkpoint body is required"},
		{"absent stored checkpoint entry", func(m *snapshot) { m.Groups[0].Members[0].Checkpoint.Checkpoint.Entry = nil },
			"table registry snapshot: group 0/dex: member member-b: wire: member checkpoint entry is required"},
		{"unsorted purge members", func(m *snapshot) { m.Incarnations[3].PurgeMembers = []string{"member-b", "member-a"} }, purgeOrder},
		{"duplicate purge member", func(m *snapshot) { m.Incarnations[3].PurgeMembers = []string{"member-a", "member-a"} }, purgeOrder},
		{"unsorted groups", func(m *snapshot) { m.Groups[0], m.Groups[1] = m.Groups[1], m.Groups[0] }, groupsOrder},
		{"duplicate group", func(m *snapshot) { m.Groups[1].OwnerIndexerId = 0 }, groupsOrder},
		{"owner without an entry", func(m *snapshot) { m.Groups[1].OwnerIndexerId = 9 },
			"table registry snapshot: group 9/dex is owned by indexer 9, which has no si_indexers entry"},
		{"unsorted members", func(m *snapshot) { ms := m.Groups[0].Members; ms[0], ms[1] = ms[1], ms[0] }, listsOrder},
		{"duplicate past member", func(m *snapshot) { m.Groups[0].PastMembers = []string{"member-a", "member-a"} }, listsOrder},
		{"unsorted leaving", func(m *snapshot) {
			m.Groups[0].Leaving = append(m.Groups[0].Leaving, &pb.LeavingMember{NodeId: "member-0"})
		}, listsOrder},
		{"unspecified class", func(m *snapshot) { m.Groups[0].Members[0].MemberClass = pb.MemberClass_MEMBER_CLASS_UNSPECIFIED },
			"table registry snapshot: group 0/dex: member member-b has unknown class MEMBER_CLASS_UNSPECIFIED"},
		{"future class", func(m *snapshot) { m.Groups[0].Members[0].MemberClass = pb.MemberClass(9) },
			"table registry snapshot: group 0/dex: member member-b has unknown class 9"},
		{"unknown exclusion reason", func(m *snapshot) {
			m.Groups[0].Members[0].Checkpoint.Checkpoint.Groups[0].Excluded[0].Reason = pb.CheckpointExclusionReason(7)
		}, "table registry snapshot: group 0/dex: member member-b: wire: member checkpoint excludes dex.pools with reason 7, which is not a known CheckpointExclusionReason"},
		{"unsorted seqs", func(m *snapshot) { ss := m.MemberCheckpointSeqs; ss[0], ss[1] = ss[1], ss[0] }, seqsOrder},
		{"duplicate seq", func(m *snapshot) { m.MemberCheckpointSeqs[2].OwnerIndexerId = 0 }, seqsOrder},
		{"invalid stage3", func(m *snapshot) { m.Stage3.GroupSize = 0 }, "table registry snapshot: stage3: group_size must be at least 1"},
		{"stage3 switch out of order", func(m *snapshot) { m.Stage3.Purge = false }, "table registry snapshot: stage3: switch eviction_voiding requires purge"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := TableRegistrySnapshotToPB(stage3RegistryFixture(t))
			tc.mutate(m)
			if _, err := TableRegistrySnapshotFromPB(m); err == nil || err.Error() != tc.want {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestGroupMemberHostIndexerKeepsAbsenceAndIndexerZero: the served host of a
// member (cross-plan decision D27) keeps indexer 0 distinct from absent: a
// member hosted by indexer 0, one hosted by indexer 1 and a delisted one
// served without a host survive FromPB, ToPB and proto.Marshal unchanged, no
// decoder refuses the absent host, and the pointers are the converter's own.
func TestGroupMemberHostIndexerKeepsAbsenceAndIndexerZero(t *testing.T) {
	zero, one := uint64(0), uint64(1)
	s := stage3RegistryFixture(t)
	s.Groups = s.Groups[:1]
	s.Groups[0].Members = []GroupMember{
		{NodeID: "member-a", JoinedGeneration: 1, ReadyGeneration: 1, Class: MemberClassCaughtUp, HostIndexerID: &zero},
		{NodeID: "member-b", JoinedGeneration: 1, ReadyGeneration: 1, Class: MemberClassBehind, HostIndexerID: &one},
		{NodeID: "member-c", JoinedGeneration: 2, Class: MemberClassBootstrapping},
	}
	m := TableRegistrySnapshotToPB(s)
	members := m.GetGroups()[0].GetMembers()
	for i, want := range []*uint64{&zero, &one, nil} {
		got := members[i].HostIndexerId
		if (got == nil) != (want == nil) || (got != nil && *got != *want) {
			t.Fatalf("member %s encoded host %v, want %v", members[i].GetNodeId(), got, want)
		}
		hasField8 := false
		for _, num := range wireFieldNumbers(t, mustMarshal(t, members[i])) {
			hasField8 = hasField8 || num == 8
		}
		if hasField8 != (want != nil) {
			t.Fatalf("member %s: host_indexer_id on the wire = %v, want %v", members[i].GetNodeId(), hasField8, want != nil)
		}
	}
	raw := mustMarshal(t, m)
	var decoded pb.TableRegistrySnapshot
	if err := proto.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	got, err := TableRegistrySnapshotFromPB(&decoded)
	if err != nil || !reflect.DeepEqual(got, s) {
		t.Fatalf("round trip (%v)\n got %+v\nwant %+v", err, got.Groups, s.Groups)
	}
	if got.Groups[0].Members[0].HostIndexerID == &zero || members[0].HostIndexerId == &zero {
		t.Fatal("a converter shares the caller's host pointer")
	}
	if again := mustMarshal(t, TableRegistrySnapshotToPB(got)); !bytes.Equal(again, raw) {
		t.Fatalf("re-encoded bytes differ:\n got %x\nwant %x", again, raw)
	}
}

// TestGroupMemberReady: a member is ready once it reported its replicas at or
// after the generation it joined at; ReadyGeneration 0 means never.
func TestGroupMemberReady(t *testing.T) {
	for _, tc := range []struct {
		joined, ready uint64
		want          bool
	}{
		{0, 0, false}, {1, 0, false}, {2, 1, false}, {1, 1, true}, {2, 3, true},
	} {
		if got := (GroupMember{JoinedGeneration: tc.joined, ReadyGeneration: tc.ready}).Ready(); got != tc.want {
			t.Errorf("joined %d, ready %d: Ready() = %v, want %v", tc.joined, tc.ready, got, tc.want)
		}
	}
}

// The transport converters own every mutable value they return. Exercise both
// directions independently so a round trip cannot hide a shared slice/pointer.
func TestStage3RegistryConvertersDetachMutableFields(t *testing.T) {
	s := stage3RegistryFixture(t)
	m := TableRegistrySnapshotToPB(s)
	before := proto.Clone(m)
	s.Incarnations[3].PurgeMembers[0] = "changed"
	s.Groups[0].PastMembers[0] = "changed"
	s.Groups[0].Leaving[0].FrozenMembers[0] = "changed"
	*s.Groups[0].Members[0].HostIndexerID = 99
	s.Groups[0].Members[0].Checkpoint.Checkpoint.Groups[0].Excluded[0].TableID = "changed"
	s.Groups[0].Members[1].ObservedEntry.SafeBlockSeq = 99
	s.MemberCheckpointSeqs[0].NodeID = "changed"
	s.Stage3.IndexerFailureDomains[0].FailureDomain = "changed"
	if !proto.Equal(m, before) {
		t.Fatal("ToPB aliases the source registry view")
	}
	got, err := TableRegistrySnapshotFromPB(m)
	if err != nil {
		t.Fatal(err)
	}
	want := stage3RegistryFixture(t)
	m.Incarnations[3].PurgeMembers[0] = "changed"
	m.Groups[0].PastMembers[0] = "changed"
	m.Groups[0].Leaving[0].FrozenMembers[0] = "changed"
	*m.Groups[0].Members[0].HostIndexerId = 99
	m.Groups[0].Members[0].Checkpoint.Checkpoint.Groups[0].Excluded[0].TableId = "changed"
	m.Groups[0].Members[1].ObservedEntry.SafeBlockSeq = 99
	m.MemberCheckpointSeqs[0].NodeId = "changed"
	m.Stage3.IndexerFailureDomains[0].FailureDomain = "changed"
	if !reflect.DeepEqual(got, want) {
		t.Fatal("FromPB aliases the protobuf registry view")
	}
}
