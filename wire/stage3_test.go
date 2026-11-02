package wire

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// stage3ParamsFixture sets every Stage3Params field and passes Validate: every
// switch is on, so strict copies and min_safe_copies 2 are allowed.
func stage3ParamsFixture() *arbiter.Stage3Params {
	return &arbiter.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 2, StrictCopies: true,
		IndexerFailureDomains: []arbiter.IndexerFailureDomain{{IndexerID: 0, FailureDomain: "node:test-a"}, {IndexerID: 1, FailureDomain: "node:test-b"}},
		MaxLagPublications:    4, LagLow: 2, EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2400, MaxUncleanedPartsPerTable: 20000,
		ExcludeQuarantinedHosts: true, Purge: true, EvictionVoiding: true, OwnerAwareSelection: true, Placement: true,
		Reselection: true, Checkpoints: true, NarrowedDrain: true}
}

// memberVerifierEntry is a member entry with every field explicit (plan S3-E1
// K58); key fills the 32 key bytes.
func memberVerifierEntry(nodeID string, host uint64, domain string, key byte) arbiter.VerifierEntry {
	return arbiter.VerifierEntry{NodeID: nodeID, Ed25519Pubkey: bytes.Repeat([]byte{key}, 32), HostIndexerID: &host,
		ReplicaSigner: fmt.Sprintf("0x%040x", 0xe0+uint64(key)), ConsentJWS: "c.o.n", Weight: 2, FailureDomain: domain,
		Verify: true, Replicate: true, Counts: true, SharedRMT: true}
}

// wireFieldNumbers lists the top-level field numbers of an encoded message.
func wireFieldNumbers(t *testing.T, b []byte) []protowire.Number {
	t.Helper()
	var out []protowire.Number
	for len(b) > 0 {
		num, typ, n := protowire.ConsumeTag(b)
		if n < 0 {
			t.Fatalf("invalid tag in %x", b)
		}
		b = b[n:]
		n = protowire.ConsumeFieldValue(num, typ, b)
		if n < 0 {
			t.Fatalf("invalid field %d", num)
		}
		b = b[n:]
		out = append(out, num)
	}
	return out
}

// eachFlagAlone calls check with a copy of base for every bool field of its
// type, that field true and every other bool false, so a converter that swaps
// two flags fails even where a seeded value sets them all.
func eachFlagAlone[T any](base T, check func(name string, v T)) {
	rt := reflect.TypeOf(base)
	for i := 0; i < rt.NumField(); i++ {
		if rt.Field(i).Type.Kind() != reflect.Bool {
			continue
		}
		v := base
		rv := reflect.ValueOf(&v).Elem()
		for j := 0; j < rt.NumField(); j++ {
			if rt.Field(j).Type.Kind() == reflect.Bool {
				rv.Field(j).SetBool(j == i)
			}
		}
		check(rt.Field(i).Name, v)
	}
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// TestVerifierEntryEveryFieldSurvivesThePBRoundTrip: every field of
// arbiter.VerifierEntry, seeded by reflection with its own value, reaches the
// message, survives the wire bytes and decodes back (plan S3-E1 contract §41;
// the stage-2 converters copied only node_id and ed25519_pubkey). The Go
// type and the message have the same field count, so a field added to either
// fails here until both converters carry it. A pool entry encodes exactly as
// under arbiter-proto v0.11.0, host 0 stays distinct from no host, and no
// slice or pointer is shared with the caller.
func TestVerifierEntryEveryFieldSurvivesThePBRoundTrip(t *testing.T) {
	if got, want := reflect.TypeOf(arbiter.VerifierEntry{}).NumField(), (&pb.VerifierEntry{}).ProtoReflect().Descriptor().Fields().Len(); got != want {
		t.Fatalf("arbiter.VerifierEntry has %d fields, pb.VerifierEntry %d", got, want)
	}
	var seeded arbiter.VerifierEntry
	var serial uint64
	seedSnapshot(reflect.ValueOf(&seeded).Elem(), "entry", &serial)
	pool := arbiter.VerifierEntry{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{7}, 32)}
	hostZero := memberVerifierEntry("member-a", 0, "node:test-a", 0x0a)
	entries := []arbiter.VerifierEntry{seeded, pool, hostZero}

	ms := VerifierEntriesToPB(entries)
	assertEveryFieldPopulated(t, ms[0].ProtoReflect(), nil)
	if got, want := hex.EncodeToString(mustMarshal(t, ms[1])), "0a0a76657269666965722d311220"+strings.Repeat("07", 32); got != want {
		t.Fatalf("pool entry bytes = %s, want arbiter-proto v0.11.0's %s", got, want)
	}
	if ms[1].HostIndexerId != nil || ms[2].HostIndexerId == nil || *ms[2].HostIndexerId != 0 {
		t.Fatalf("host_indexer_id presence: pool %v, host 0 %v", ms[1].HostIndexerId, ms[2].HostIndexerId)
	}
	var decoded pb.ConsensusMutableParams
	if err := proto.Unmarshal(mustMarshal(t, &pb.ConsensusMutableParams{Verifiers: ms}), &decoded); err != nil {
		t.Fatal(err)
	}
	got := VerifierEntriesFromPB(decoded.GetVerifiers())
	if !reflect.DeepEqual(got, entries) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, entries)
	}
	if got[1].HostIndexerID != nil || got[2].HostIndexerID == nil || *got[2].HostIndexerID != 0 {
		t.Fatalf("decoded host: pool %v, host 0 %v", got[1].HostIndexerID, got[2].HostIndexerID)
	}
	*ms[0].HostIndexerId, ms[0].Ed25519Pubkey[0] = 999, 0xff
	if *seeded.HostIndexerID == 999 || seeded.Ed25519Pubkey[0] == 0xff {
		t.Fatal("VerifierEntriesToPB shares the caller's host pointer or key bytes")
	}
	if back := VerifierEntriesFromPB(ms); back[0].HostIndexerID == ms[0].HostIndexerId {
		t.Fatal("VerifierEntriesFromPB shares the message's host pointer")
	}
	eachFlagAlone(hostZero, func(name string, e arbiter.VerifierEntry) {
		if got := VerifierEntriesFromPB(VerifierEntriesToPB([]arbiter.VerifierEntry{e})); !reflect.DeepEqual(got[0], e) {
			t.Errorf("%s alone: round trip = %+v", name, got[0])
		}
	})
}

// TestConsensusParamsUpdateWithStage3RoundTrips: the stage-3 parameter with
// every field set, and member entries beside a pool entry, survive both
// converters and an UpdateConsensusParams Raft command; an absent parameter
// stays absent (nil both ways, no field 14 on the wire), and an empty
// failure-domain list decodes to nil.
func TestConsensusParamsUpdateWithStage3RoundTrips(t *testing.T) {
	u := legacyGoldenUpdate()
	u.TracksEnabled = true
	u.SIIndexers = []arbiter.SIIndexerEntry{siEntryW(0, 100, "snode-1"), siEntryW(1, 200, "snode-2")}
	u.Verifiers = []arbiter.VerifierEntry{memberVerifierEntry("member-a", 1, "node:test-b", 0x0a), memberVerifierEntry("member-b", 0, "node:test-a", 0x0b),
		{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{7}, 32)}}
	u.Stage3 = stage3ParamsFixture()

	m := ConsensusParamsUpdateToPB(u)
	// Indexer 0's failure domain has indexer_id 0, a value proto3 leaves off the wire.
	assertEveryFieldPopulated(t, m.GetStage3().ProtoReflect(), map[protoreflect.FullName]bool{"arbiter.IndexerFailureDomain.indexer_id": true})
	if got := ConsensusParamsUpdateFromPB(m); !reflect.DeepEqual(got, u) {
		t.Fatalf("update round trip\n got %+v\nwant %+v", got, u)
	}
	m.Stage3.IndexerFailureDomains[0].FailureDomain = "changed"
	if u.Stage3.IndexerFailureDomains[0].FailureDomain != "node:test-a" {
		t.Fatal("ToPB shares the caller's failure domains")
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
	mutable := &pb.ConsensusMutableParams{Stage3: Stage3ParamsToPB(u.Stage3)}
	if got := Stage3ParamsFromPB(mutable.GetStage3()); !reflect.DeepEqual(got, u.Stage3) {
		t.Fatalf("ConsensusMutableParams.stage3 round trip = %+v", got)
	}

	without := u
	without.Stage3 = nil
	if ConsensusParamsUpdateToPB(without).GetStage3() != nil || ConsensusParamsUpdateFromPB(ConsensusParamsUpdateToPB(without)).Stage3 != nil {
		t.Fatal("an absent stage3 parameter must stay absent both ways")
	}
	for _, num := range wireFieldNumbers(t, mustMarshal(t, ConsensusParamsUpdateToPB(without))) {
		if num == 14 {
			t.Fatal("an update without stage3 carries field 14")
		}
	}
	eachFlagAlone(*stage3ParamsFixture(), func(name string, p arbiter.Stage3Params) {
		if got := Stage3ParamsFromPB(Stage3ParamsToPB(&p)); !reflect.DeepEqual(*got, p) {
			t.Errorf("%s alone: round trip = %+v", name, *got)
		}
	})
	if Stage3ParamsFromPB(nil) != nil || Stage3ParamsToPB(nil) != nil {
		t.Fatal("nil must convert to nil")
	}
	if got := Stage3ParamsFromPB(&pb.Stage3Params{GroupSize: 3, IndexerFailureDomains: []*pb.IndexerFailureDomain{}}); got == nil || got.IndexerFailureDomains != nil {
		t.Fatalf("an empty failure-domain list decoded to %+v; want nil", got)
	}
}

// TestStage3CommandsRoundTripThroughEncodeAndDecode: each stage-3 command
// encodes at its Raft tag (38-42) and decodes to the identical command, owner
// 0 and generation 0 included; every field of each command, seeded by
// reflection with its own value, reaches the request message and survives the
// Raft bytes, and each Go type has its message's field count, so a field
// added to either fails here until both converters carry it; empty lists
// decode to nil; two commands never encode together; and the strict decoder
// refuses an unknown field inside every new message at any depth (plan S3-E1
// contract §39, §41).
func TestStage3CommandsRoundTripThroughEncodeAndDecode(t *testing.T) {
	entry := replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 90, AppliedPromotionSeq: 40}
	checkpoint := arbiter.MemberCheckpoint{NodeID: "member-b", OwnerIndexerID: 0, CheckpointSeq: 7,
		Groups: []arbiter.CheckpointGroup{
			{DatabaseID: "dex", GroupGeneration: 1, Excluded: []arbiter.ExcludedTable{
				{TableID: "dex.pools", Reason: arbiter.ExclusionNotInstalled}, {TableID: "dex.swaps", Reason: arbiter.ExclusionRetiring}}},
			{DatabaseID: "lend", GroupGeneration: 3},
		},
		SnapshotID: "0xsnap", Entry: entry}
	for _, tc := range []struct {
		tag protowire.Number
		cmd Command
	}{
		{38, Command{RebalanceGroup: &RebalanceGroup{OwnerIndexerID: 0, DatabaseID: "dex", ExpectedGeneration: 0,
			Add: []string{"member-c"}, Remove: []string{"member-b"}, NetworkFill: true, AuthorityJWS: "h.p.s"}}},
		{38, Command{RebalanceGroup: &RebalanceGroup{OwnerIndexerID: 1, DatabaseID: "lend", ExpectedGeneration: 4, AuthorityJWS: "h.p.s"}}},
		{39, Command{ReselectVerifiers: &ReselectVerifiers{L3BlockSeq: 100, ExpectedSet: []string{"member-b", "verifier-1", "verifier-2"}, AuthorityJWS: "h.p.s"}}},
		{40, Command{MemberReady: &MemberReady{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex", Generation: 1, Ed25519Signature: "ab"}}},
		{41, Command{MemberCheckpoint: &MemberCheckpoint{Checkpoint: checkpoint, ReplicaJWS: "r.j.s"}}},
		{42, Command{MarkMemberLagging: &MarkMemberLagging{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex", ObservedEntry: entry, AuthorityJWS: "h.p.s"}}},
	} {
		b, err := Encode(tc.cmd)
		if err != nil {
			t.Fatal(err)
		}
		if num, typ, n := protowire.ConsumeTag(b); n < 0 || num != tc.tag || typ != protowire.BytesType {
			t.Fatalf("outer tag = (%d, %d), want (%d, bytes)", num, typ, tc.tag)
		}
		mustRoundTrip(t, tc.cmd)
	}

	var (
		serial    uint64
		rebalance RebalanceGroup
		reselect  ReselectVerifiers
		ready     MemberReady
		submitted MemberCheckpoint
		lagging   MarkMemberLagging
	)
	for _, v := range []any{&rebalance, &reselect, &ready, &submitted, &lagging} {
		seedSnapshot(reflect.ValueOf(v).Elem(), reflect.TypeOf(v).Elem().Name(), &serial)
	}
	// A seeded reason is no CheckpointExclusionReason; give each a known one.
	for gi := range submitted.Checkpoint.Groups {
		for ei, reason := range []arbiter.CheckpointExclusionReason{arbiter.ExclusionDiskThreshold, arbiter.ExclusionAuditPending} {
			submitted.Checkpoint.Groups[gi].Excluded[ei].Reason = reason
		}
	}
	for _, tc := range []struct {
		goValue any
		request proto.Message
		cmd     Command
	}{
		{rebalance, RebalanceGroupToRequest(rebalance), Command{RebalanceGroup: &rebalance}},
		{reselect, ReselectVerifiersToRequest(reselect), Command{ReselectVerifiers: &reselect}},
		{ready, MemberReadyToRequest(ready), Command{MemberReady: &ready}},
		{submitted, MemberCheckpointToRequest(submitted), Command{MemberCheckpoint: &submitted}},
		{lagging, MarkMemberLaggingToRequest(lagging), Command{MarkMemberLagging: &lagging}},
	} {
		desc := tc.request.ProtoReflect().Descriptor()
		if got, want := reflect.TypeOf(tc.goValue).NumField(), desc.Fields().Len(); got != want {
			t.Errorf("%T has %d fields, %s has %d", tc.goValue, got, desc.FullName(), want)
		}
		assertEveryFieldPopulated(t, tc.request.ProtoReflect(), nil)
		mustRoundTrip(t, tc.cmd)
	}

	got := mustRoundTrip(t, Command{RebalanceGroup: &RebalanceGroup{DatabaseID: "dex", AuthorityJWS: "h.p.s"}})
	b, err := Encode(Command{RebalanceGroup: &RebalanceGroup{DatabaseID: "dex", Add: []string{}, Remove: []string{}, AuthorityJWS: "h.p.s"}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err = Decode(b); err != nil || got.RebalanceGroup.Add != nil || got.RebalanceGroup.Remove != nil {
		t.Fatalf("empty add and remove must decode to nil: %+v, %v", got.RebalanceGroup, err)
	}
	b, err = Encode(Command{MemberCheckpoint: &MemberCheckpoint{Checkpoint: arbiter.MemberCheckpoint{NodeID: "member-b",
		Groups: []arbiter.CheckpointGroup{{DatabaseID: "dex", Excluded: []arbiter.ExcludedTable{}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	if got, err = Decode(b); err != nil || got.MemberCheckpoint.Checkpoint.Groups[0].Excluded != nil {
		t.Fatalf("an empty excluded list must decode to nil: %+v, %v", got.MemberCheckpoint, err)
	}
	if _, err := Encode(Command{RebalanceGroup: &RebalanceGroup{}, MemberReady: &MemberReady{}}); err == nil {
		t.Fatal("two commands encoded together")
	}

	unknown := protowire.AppendVarint(protowire.AppendTag(nil, 9, protowire.VarintType), 1)
	excluded := mustMarshal(t, &pb.ExcludedTable{TableId: "dex.pools", Reason: pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_DIVERGED})
	frontier := mustMarshal(t, &pb.TrackFrontier{IndexerId: 0, SafeBlockSeq: 90})
	for name, tc := range map[string]struct {
		raw  []byte
		want string
	}{
		"RebalanceGroupCmd":    {unionMessage(38, append(mustMarshal(t, &pb.RebalanceGroupCmd{DatabaseId: "dex"}), unknown...)), "wire: unknown arbiter.RebalanceGroupCmd field 9"},
		"ReselectVerifiersCmd": {unionMessage(39, append(mustMarshal(t, &pb.ReselectVerifiersCmd{L3BlockSeq: 1}), unknown...)), "wire: unknown arbiter.ReselectVerifiersCmd field 9"},
		"MemberReadyCmd":       {unionMessage(40, append(mustMarshal(t, &pb.MemberReadyCmd{NodeId: "member-b"}), unknown...)), "wire: unknown arbiter.MemberReadyCmd field 9"},
		"MemberCheckpointCmd":  {unionMessage(41, append(mustMarshal(t, &pb.MemberCheckpointCmd{ReplicaJws: "r.j.s"}), unknown...)), "wire: unknown arbiter.MemberCheckpointCmd field 9"},
		"MemberCheckpoint":     {unionMessage(41, unionMessage(1, unknown)), "wire: unknown arbiter.MemberCheckpoint field 9"},
		"CheckpointGroup":      {unionMessage(41, unionMessage(1, unionMessage(4, unknown))), "wire: unknown arbiter.CheckpointGroup field 9"},
		"ExcludedTable":        {unionMessage(41, unionMessage(1, unionMessage(4, unionMessage(3, append(excluded, unknown...))))), "wire: unknown arbiter.ExcludedTable field 9"},
		"MarkMemberLaggingCmd": {unionMessage(42, append(mustMarshal(t, &pb.MarkMemberLaggingCmd{NodeId: "member-b"}), unknown...)), "wire: unknown arbiter.MarkMemberLaggingCmd field 9"},
		"TrackFrontier":        {unionMessage(42, unionMessage(4, append(frontier, unknown...))), "wire: unknown arbiter.TrackFrontier field 9"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(tc.raw); err == nil || err.Error() != tc.want {
				t.Fatalf("Decode = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestStage3CanonicalizeSortsAndDeduplicates: Canonical drops the token and
// sorts and deduplicates the node lists into fresh slices, nil when empty, so
// a signer and the FSM hash one form; Body drops the member-ready signature.
func TestStage3CanonicalizeSortsAndDeduplicates(t *testing.T) {
	add, remove := []string{"member-d", "member-c", "member-d"}, []string{"member-b", "member-a", "member-b"}
	r := RebalanceGroup{OwnerIndexerID: 2, DatabaseID: "dex", ExpectedGeneration: 5, Add: add, Remove: remove, NetworkFill: true, AuthorityJWS: "h.p.s"}
	want := arbiter.RebalanceGroupCommand{OwnerIndexerID: 2, DatabaseID: "dex", ExpectedGeneration: 5,
		Add: []string{"member-c", "member-d"}, Remove: []string{"member-a", "member-b"}, NetworkFill: true}
	got := r.Canonical()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("rebalance Canonical = %+v, want %+v", got, want)
	}
	got.Add[0], got.Remove[0] = "changed", "changed"
	if !reflect.DeepEqual(add, []string{"member-d", "member-c", "member-d"}) || !reflect.DeepEqual(remove, []string{"member-b", "member-a", "member-b"}) {
		t.Fatal("Canonical modified or shares the command's lists")
	}
	for _, empty := range [][]string{nil, {}} {
		if c := (RebalanceGroup{DatabaseID: "dex", Add: empty, Remove: empty}).Canonical(); c.Add != nil || c.Remove != nil {
			t.Fatalf("%#v: Canonical lists = %#v, %#v; want nil", empty, c.Add, c.Remove)
		}
	}
	set := []string{"verifier-2", "member-b", "verifier-1", "member-b"}
	s := ReselectVerifiers{L3BlockSeq: 100, ExpectedSet: set, AuthorityJWS: "h.p.s"}
	if c := s.Canonical(); !reflect.DeepEqual(c, arbiter.ReselectVerifiersCommand{L3BlockSeq: 100, ExpectedSet: []string{"member-b", "verifier-1", "verifier-2"}}) {
		t.Fatalf("reselect Canonical = %+v", c)
	}
	if !reflect.DeepEqual(set, []string{"verifier-2", "member-b", "verifier-1", "member-b"}) {
		t.Fatal("Canonical modified the expected set")
	}
	entry := replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 90, AppliedPromotionSeq: 40}
	l := MarkMemberLagging{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex", ObservedEntry: entry, AuthorityJWS: "h.p.s"}
	if c := l.Canonical(); c != (arbiter.MarkMemberLaggingCommand{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex", ObservedEntry: entry}) {
		t.Fatalf("mark lagging Canonical = %+v", c)
	}
	ready := MemberReady{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex", Generation: 1, Ed25519Signature: "ab"}
	if b := ready.Body(); b != (arbiter.MemberReadyBody{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex", Generation: 1}) {
		t.Fatalf("member ready Body = %+v", b)
	}
}

// TestDecodeRefusesAnUnknownExclusionReason: an exclusion reason outside the
// closed CheckpointExclusionReason set, UNSPECIFIED or any future number, is
// refused at decode, on every voter alike, in the body, the request and the
// Raft command (a stored checkpoint of the registry view:
// TestTableRegistrySnapshotRefusesCorruptStage3Views); the six known reasons
// map to their arbiter spellings both ways, and a Go reason outside the set
// encodes as UNSPECIFIED, which Decode refuses (plan S3-E1 K63).
func TestDecodeRefusesAnUnknownExclusionReason(t *testing.T) {
	known := map[pb.CheckpointExclusionReason]arbiter.CheckpointExclusionReason{
		pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_NOT_INSTALLED:  arbiter.ExclusionNotInstalled,
		pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_DIVERGED:       arbiter.ExclusionDiverged,
		pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_FOREIGN:        arbiter.ExclusionForeign,
		pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_DISK_THRESHOLD: arbiter.ExclusionDiskThreshold,
		pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_AUDIT_PENDING:  arbiter.ExclusionAuditPending,
		pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_RETIRING:       arbiter.ExclusionRetiring,
	}
	body := func(r pb.CheckpointExclusionReason) *pb.MemberCheckpoint {
		return &pb.MemberCheckpoint{NodeId: "member-b", CheckpointSeq: 1, SnapshotId: "0xsnap", Entry: &pb.TrackFrontier{SafeBlockSeq: 90},
			Groups: []*pb.CheckpointGroup{{DatabaseId: "dex", GroupGeneration: 1, Excluded: []*pb.ExcludedTable{{TableId: "dex.pools", Reason: r}}}}}
	}
	for r, want := range known {
		got, err := MemberCheckpointBodyFromPB(body(r))
		if err != nil || got.Groups[0].Excluded[0].Reason != want {
			t.Fatalf("%v decoded as %+v, %v; want %q", r, got, err, want)
		}
		if back := MemberCheckpointBodyToPB(got).GetGroups()[0].GetExcluded()[0].GetReason(); back != r {
			t.Fatalf("%q encoded as %v, want %v", want, back, r)
		}
	}
	for _, r := range []pb.CheckpointExclusionReason{pb.CheckpointExclusionReason_CHECKPOINT_EXCLUSION_REASON_UNSPECIFIED, pb.CheckpointExclusionReason(7)} {
		t.Run(r.String(), func(t *testing.T) {
			want := fmt.Sprintf("wire: member checkpoint excludes dex.pools with reason %v, which is not a known CheckpointExclusionReason", r)
			if _, err := MemberCheckpointBodyFromPB(body(r)); err == nil || err.Error() != want {
				t.Fatalf("MemberCheckpointBodyFromPB = %v, want %q", err, want)
			}
			req := &pb.MemberCheckpointCmd{Checkpoint: body(r), ReplicaJws: "r.j.s"}
			if _, err := MemberCheckpointFromRequest(req); err == nil || err.Error() != want {
				t.Fatalf("MemberCheckpointFromRequest = %v, want %q", err, want)
			}
			raw := mustMarshal(t, &pb.RaftCommand{Cmd: &pb.RaftCommand_MemberCheckpoint{MemberCheckpoint: req}})
			if _, err := Decode(raw); err == nil || err.Error() != want {
				t.Fatalf("Decode = %v, want %q", err, want)
			}
		})
	}
	made := Command{MemberCheckpoint: &MemberCheckpoint{Checkpoint: arbiter.MemberCheckpoint{NodeID: "member-b", CheckpointSeq: 1,
		Groups: []arbiter.CheckpointGroup{{DatabaseID: "dex", Excluded: []arbiter.ExcludedTable{{TableID: "dex.pools", Reason: "made_up"}}}}}}}
	raw, err := Encode(made)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(raw); err == nil || !strings.Contains(err.Error(), "with reason CHECKPOINT_EXCLUSION_REASON_UNSPECIFIED") {
		t.Fatalf("a reason outside the set must encode as UNSPECIFIED and be refused, got %v", err)
	}
}

// TestMemberCheckpointRequiresExplicitBodyAndEntry distinguishes missing
// transport fields from a present owner-0 zero frontier. The canonical Go
// value cannot retain protobuf presence, so the decoder owns this check.
func TestMemberCheckpointRequiresExplicitBodyAndEntry(t *testing.T) {
	base := &pb.MemberCheckpoint{NodeId: "member-b", CheckpointSeq: 1, SnapshotId: "0xsnap",
		Groups: []*pb.CheckpointGroup{{DatabaseId: "dex", GroupGeneration: 1}}, Entry: &pb.TrackFrontier{}}
	without := proto.Clone(base).(*pb.MemberCheckpoint)
	without.Entry = nil
	for _, tc := range []struct {
		name string
		body *pb.MemberCheckpoint
		want string
	}{
		{"absent body", nil, "wire: member checkpoint body is required"},
		{"absent owner-0 entry", without, "wire: member checkpoint entry is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := MemberCheckpointBodyFromPB(tc.body); err == nil || err.Error() != tc.want {
				t.Fatalf("body error = %v, want %q", err, tc.want)
			}
			req := &pb.MemberCheckpointCmd{Checkpoint: tc.body, ReplicaJws: "r.j.s"}
			if _, err := MemberCheckpointFromRequest(req); err == nil || err.Error() != tc.want {
				t.Fatalf("request error = %v, want %q", err, tc.want)
			}
			raw := mustMarshal(t, &pb.RaftCommand{Cmd: &pb.RaftCommand_MemberCheckpoint{MemberCheckpoint: req}})
			if _, err := Decode(raw); err == nil || err.Error() != tc.want {
				t.Fatalf("Raft error = %v, want %q", err, tc.want)
			}
		})
	}
	if _, err := MemberCheckpointFromRequest(nil); err == nil || err.Error() != "wire: member checkpoint body is required" {
		t.Fatalf("nil request error = %v", err)
	}
	cp, err := MemberCheckpointBodyFromPB(base)
	if err != nil || cp.OwnerIndexerID != 0 || cp.Entry != (replay.TrackFrontier{}) {
		t.Fatalf("present owner-0 zero frontier: %+v, %v", cp, err)
	}
	if back := MemberCheckpointBodyToPB(cp); back.Entry == nil {
		t.Fatal("a present zero frontier lost its presence")
	}
	mustRoundTrip(t, Command{MemberCheckpoint: &MemberCheckpoint{Checkpoint: cp, ReplicaJWS: "r.j.s"}})
}

// TestVerifierEntriesNilPBElementKeepsItsZeroValue preserves the previous
// getter-only converter's nil-element behavior. Validation rejects the zero
// entry separately; adding optional-pointer copying must not introduce panic.
func TestVerifierEntriesNilPBElementKeepsItsZeroValue(t *testing.T) {
	got := VerifierEntriesFromPB([]*pb.VerifierEntry{nil})
	if want := []arbiter.VerifierEntry{{}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("nil element = %+v, want %+v", got, want)
	}
}
