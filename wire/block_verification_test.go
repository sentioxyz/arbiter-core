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

// blockHeaderViewFields names the pb.L3BlockHeader fields BlockHeaderView
// reads, one per BlockHeaderView field (plan S3-C2 contract §21.2).
var blockHeaderViewFields = map[protoreflect.Name]bool{
	"l3_block_seq": true, "statement_seq_start": true, "statement_count": true,
	"prev_safe_snapshot_id": true, "prev_state_root": true, "table_set_transition": true, "track": true,
}

func blockAttestationFixture(replica string, seq uint64) replay.ReplayAttestation {
	return replay.ReplayAttestation{
		ReplicaID: replica,
		Receipt: replay.ExecutionReceipt{
			BlockSeq: seq, PrevSafeSnapshotID: "0xprev-snap", PrevStateRoot: "0xprev-state", SchemaSnapshotID: "schema-genesis",
			ExecutorProfileID: "housegate-replay-mvp-v0", StatementRoot: "0xstatements", PayloadRoot: "0xpayloads",
			SourceClaimRoot: "0xclaims", ComputedStateRoot: "0xstate-" + replica, MatchSourceRoot: true,
			PartitionCommitmentsAfter: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p_a", Root: "0xroot-a"}},
			AffectedParts:             []replay.PartManifestEntry{{TableID: "db.t", PartitionID: "p_a", PartName: "p_a_7_7_0", PartPhysHash: "0xphys-a", PartRowLtHash: "0xrow-a", RowCount: 3, Bytes: 900}},
			ReplayLogHash:             "0xlog-" + replica,
		},
		ReceiptHash: "0xreceipt-" + replica, Signature: "sig-" + replica, MatchSourceRoot: true,
	}
}

func blockScanFixture(replica string, seq uint64) arbiter.ByteSideScanMsg {
	return arbiter.ByteSideScanMsg{ReplicaID: replica, BlockSeq: seq,
		Parts:    []arbiter.PartScan{{TableID: "db.t", PartitionID: "p_a", ClaimedPartRowLtHash: "0xrow-a", ScannedPartRowLtHash: "0xrow-a", LivePartName: "p_a_7_7_0"}},
		ScanHash: "0xscan-" + replica, Signature: "scan-sig-" + replica}
}

// blockVerificationFixture sets every field. Across its statements and its
// evidence each bool has its own pattern of true and false, so a converter
// that swaps two of them fails the comparison.
func blockVerificationFixture() BlockVerification {
	track := uint64(2)
	att := func(r string) *replay.ReplayAttestation { a := blockAttestationFixture(r, 41); return &a }
	scan := func(r string) *arbiter.ByteSideScanMsg { s := blockScanFixture(r, 41); return &s }
	return BlockVerification{
		Header: BlockHeaderView{L3BlockSeq: 41, StatementSeqStart: 300, StatementCount: 2, PrevSafeSnapshotID: "0xprev-snap",
			PrevStateRoot: "0xprev-state", IsTableSetTransition: true, TrackIndexerID: &track},
		ChainHash: "0xchain-41",
		ReplayJob: replay.ReplayJob{BlockSeq: 41, PrevSafeSnapshotID: "0xprev-snap", PrevStateRoot: "0xprev-state",
			SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0", SourceClaimRoot: "0xclaims",
			Statements: []replay.Statement{{StatementID: "stmt-300", StatementSeq: 300, SQL: "INSERT INTO db.t FORMAT Native",
				SQLHash: "0xsql", PayloadRef: "da://300", PayloadHash: "0xpayload", PayloadLength: 64, TargetTableID: "db.t"}}},
		Statements: []StatementCandidates{
			{StatementSeq: 300, StatementID: "stmt-300", Status: "pending", SourceNode: "snode-3", RCBound: true, SourceClaimRoot: "0xclaim-300",
				CandidateParts:            []arbiter.CandidatePart{{TableID: "db.t", PartitionID: "p_a", PartName: "p_a_7_7_0", PartRowLtHash: "0xrow-a", PartPhysHash: "0xphys-a", RowCount: 3, Bytes: 900}},
				PartitionNewPartSums:      []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p_a", NewPartsLtHashSum: "0xrow-a"}},
				UnpromotedPartRowLtHashes: []string{"0xrow-a"}},
			{StatementSeq: 301, StatementID: "stmt-301", Status: "rejected", SourceNode: "snode-4", SourceClaimRoot: "0xclaim-301",
				CandidateParts: []arbiter.CandidatePart{
					{TableID: "db.t", PartitionID: "p_b", PartName: "p_b_8_8_0", PartRowLtHash: "0xrow-b", PartPhysHash: "0xphys-b", RowCount: 1, Bytes: 300},
					{TableID: "db.t", PartitionID: "p_b", PartName: "p_b_9_9_0", PartRowLtHash: "0xrow-c", PartPhysHash: "0xphys-c", RowCount: 2, Bytes: 600},
				},
				PartitionNewPartSums:      []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p_b", NewPartsLtHashSum: "0xsum-bc"}},
				UnpromotedPartRowLtHashes: []string{"0xrow-b", "0xrow-c"}},
		},
		ScanParts:   []arbiter.PartRef{{TableID: "db.t", PartitionID: "p_a", PartRowLtHash: "0xrow-a", PartName: "p_a_7_7_0"}},
		SourceNodes: []string{"snode-3", "snode-4"},
		VerifierSet: []string{"verifier-1", "verifier-2", "verifier-3"},
		Evidence: []VerifierEvidence{
			{ReplicaID: "verifier-1", Attestation: att("verifier-1"), ByteSideScan: scan("verifier-1"), Evaluated: true, Check3ByteSideMatch: true, Pass: true},
			{ReplicaID: "verifier-2", Attestation: att("verifier-2"), ByteSideScan: scan("verifier-2"), Check1RootMatch: true, Check3ByteSideMatch: true},
			{ReplicaID: "verifier-3", Attestation: att("verifier-3"), ByteSideScan: scan("verifier-3"), Check2PartitionMatch: true, Pass: true},
		},
		VerdictEvaluated: true, Quorum: false, Finality: false, LastMergeable: true,
		Anchor: &arbiter.AnchorRef{L3BlockHash: "0xl3-41", StateRoot: "0xstate-41", L2TxRef: "0xtx", L2BlockNumber: 9001, DARef: "da://anchor"},
	}
}

func appendMessages[M proto.Message](r protoreflect.Message, fd protoreflect.FieldDescriptor, ms []M) {
	list := r.Mutable(fd).List()
	for _, m := range ms {
		list.Append(protoreflect.ValueOfMessage(m.ProtoReflect()))
	}
}

func appendStrings(r protoreflect.Message, fd protoreflect.FieldDescriptor, ss []string) {
	list := r.Mutable(fd).List()
	for _, s := range ss {
		list.Append(protoreflect.ValueOfString(s))
	}
}

// headerThroughReflection sets every pb.L3BlockHeader field: those of the view
// from v, every other one to a value the view must not read.
func headerThroughReflection(t *testing.T, v BlockHeaderView) *pb.L3BlockHeader {
	t.Helper()
	m := &pb.L3BlockHeader{}
	r := m.ProtoReflect()
	fields := r.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch fd.Name() {
		case "l3_block_seq":
			r.Set(fd, protoreflect.ValueOfUint64(v.L3BlockSeq))
		case "statement_seq_start":
			r.Set(fd, protoreflect.ValueOfUint64(v.StatementSeqStart))
		case "statement_count":
			r.Set(fd, protoreflect.ValueOfUint32(v.StatementCount))
		case "prev_safe_snapshot_id":
			r.Set(fd, protoreflect.ValueOfString(v.PrevSafeSnapshotID))
		case "prev_state_root":
			r.Set(fd, protoreflect.ValueOfString(v.PrevStateRoot))
		case "table_set_transition":
			if v.IsTableSetTransition {
				r.Set(fd, protoreflect.ValueOfMessage((&pb.TableSetTransition{Retires: []string{"db.old"}, NewSchemaRoot: "0xschema-next"}).ProtoReflect()))
			}
		case "track":
			if v.TrackIndexerID != nil {
				r.Set(fd, protoreflect.ValueOfMessage((&pb.L3BlockTrack{IndexerId: *v.TrackIndexerID}).ProtoReflect()))
			}
		case "l2_anchor_ref":
			r.Set(fd, protoreflect.ValueOfMessage(AnchorRefToPB(arbiter.AnchorRef{L3BlockHash: "0xnot-read", StateRoot: "0xnot-read"}).ProtoReflect()))
		default:
			if fd.Kind() != protoreflect.StringKind {
				t.Fatalf("%s has no case: decide whether BlockHeaderView reads it", fd.FullName())
			}
			r.Set(fd, protoreflect.ValueOfString("not-read-"+string(fd.Name())))
		}
	}
	return m
}

func statementThroughReflection(t *testing.T, v StatementCandidates) *pb.StatementCandidates {
	t.Helper()
	m := &pb.StatementCandidates{}
	r := m.ProtoReflect()
	fields := r.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch fd.Name() {
		case "statement_seq":
			r.Set(fd, protoreflect.ValueOfUint64(v.StatementSeq))
		case "statement_id":
			r.Set(fd, protoreflect.ValueOfString(v.StatementID))
		case "status":
			r.Set(fd, protoreflect.ValueOfString(v.Status))
		case "source_node":
			r.Set(fd, protoreflect.ValueOfString(v.SourceNode))
		case "rc_bound":
			r.Set(fd, protoreflect.ValueOfBool(v.RCBound))
		case "source_claim_root":
			r.Set(fd, protoreflect.ValueOfString(v.SourceClaimRoot))
		case "candidate_parts":
			appendMessages(r, fd, mapSlice(v.CandidateParts, CandidatePartToPB))
		case "partition_new_part_sums":
			appendMessages(r, fd, mapSlice(v.PartitionNewPartSums, PartitionLtHashSumToPB))
		case "unpromoted_part_row_lthashes":
			appendStrings(r, fd, v.UnpromotedPartRowLtHashes)
		default:
			t.Fatalf("%s has no case: StatementCandidates and this test must carry it", fd.FullName())
		}
	}
	return m
}

func evidenceThroughReflection(t *testing.T, v VerifierEvidence) *pb.VerifierEvidence {
	t.Helper()
	m := &pb.VerifierEvidence{}
	r := m.ProtoReflect()
	fields := r.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch fd.Name() {
		case "replica_id":
			r.Set(fd, protoreflect.ValueOfString(v.ReplicaID))
		case "attestation":
			r.Set(fd, protoreflect.ValueOfMessage(AttestationToPB(*v.Attestation).ProtoReflect()))
		case "byte_side_scan":
			r.Set(fd, protoreflect.ValueOfMessage(ScanToPB(*v.ByteSideScan).ProtoReflect()))
		case "evaluated":
			r.Set(fd, protoreflect.ValueOfBool(v.Evaluated))
		case "check1_root_match":
			r.Set(fd, protoreflect.ValueOfBool(v.Check1RootMatch))
		case "check2_partition_match":
			r.Set(fd, protoreflect.ValueOfBool(v.Check2PartitionMatch))
		case "check3_byte_side_match":
			r.Set(fd, protoreflect.ValueOfBool(v.Check3ByteSideMatch))
		case "pass":
			r.Set(fd, protoreflect.ValueOfBool(v.Pass))
		default:
			t.Fatalf("%s has no case: VerifierEvidence and this test must carry it", fd.FullName())
		}
	}
	return m
}

// blockVerificationThroughReflection builds v's message field by field
// through protoreflect, never through BlockVerificationToPB. Every field of
// pb.BlockVerification, pb.StatementCandidates and pb.VerifierEvidence needs a
// case, so a field added to the proto fails here until the Go type carries it.
func blockVerificationThroughReflection(t *testing.T, v BlockVerification) *pb.BlockVerification {
	t.Helper()
	m := &pb.BlockVerification{}
	r := m.ProtoReflect()
	fields := r.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		fd := fields.Get(i)
		switch fd.Name() {
		case "header":
			r.Set(fd, protoreflect.ValueOfMessage(headerThroughReflection(t, v.Header).ProtoReflect()))
		case "chain_hash":
			r.Set(fd, protoreflect.ValueOfString(v.ChainHash))
		case "replay_job":
			r.Set(fd, protoreflect.ValueOfMessage(ReplayJobToPB(v.ReplayJob).ProtoReflect()))
		case "statements":
			list := r.Mutable(fd).List()
			for _, s := range v.Statements {
				list.Append(protoreflect.ValueOfMessage(statementThroughReflection(t, s).ProtoReflect()))
			}
		case "scan_parts":
			appendMessages(r, fd, mapSlice(v.ScanParts, partRefToPB))
		case "source_nodes":
			appendStrings(r, fd, v.SourceNodes)
		case "verifier_set":
			appendStrings(r, fd, v.VerifierSet)
		case "evidence":
			list := r.Mutable(fd).List()
			for _, e := range v.Evidence {
				list.Append(protoreflect.ValueOfMessage(evidenceThroughReflection(t, e).ProtoReflect()))
			}
		case "verdict_evaluated":
			r.Set(fd, protoreflect.ValueOfBool(v.VerdictEvaluated))
		case "quorum":
			r.Set(fd, protoreflect.ValueOfBool(v.Quorum))
		case "anchor":
			r.Set(fd, protoreflect.ValueOfMessage(AnchorRefToPB(*v.Anchor).ProtoReflect()))
		case "finality":
			r.Set(fd, protoreflect.ValueOfBool(v.Finality))
		case "last_mergeable":
			r.Set(fd, protoreflect.ValueOfBool(v.LastMergeable))
		default:
			t.Fatalf("%s has no case: BlockVerification and this test must carry it", fd.FullName())
		}
	}
	return m
}

// TestBlockVerificationFromPBReadsEveryField: every field of
// pb.BlockVerification, pb.StatementCandidates and pb.VerifierEvidence is set
// through protoreflect and read back, and of the header exactly the fields of
// BlockHeaderView are read (plan S3-C2 contract §21.2). The four top-level
// bools and the transition flag take three patterns, so a swap of any two of
// them, or a flag read from the track, is caught.
func TestBlockVerificationFromPBReadsEveryField(t *testing.T) {
	if n := reflect.TypeOf(BlockHeaderView{}).NumField(); n != len(blockHeaderViewFields) {
		t.Fatalf("BlockHeaderView has %d fields, the test names %d header fields", n, len(blockHeaderViewFields))
	}
	for _, flags := range [][5]bool{{true, false, false, true, true}, {false, true, false, true, false}, {false, false, true, false, true}} {
		want := blockVerificationFixture()
		want.VerdictEvaluated, want.Quorum, want.Finality, want.LastMergeable = flags[0], flags[1], flags[2], flags[3]
		want.Header.IsTableSetTransition = flags[4]

		got, err := BlockVerificationFromPB(blockVerificationThroughReflection(t, want))

		if err != nil {
			t.Fatalf("BlockVerificationFromPB: %v", err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("flags %v: read back\n got %+v\nwant %+v", flags, got, want)
		}
	}
	back := BlockVerificationToPB(blockVerificationFixture()).GetHeader().ProtoReflect()
	fields := back.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		if fd := fields.Get(i); !blockHeaderViewFields[fd.Name()] && back.Has(fd) {
			t.Errorf("BlockVerificationToPB set header field %s, which BlockHeaderView does not carry", fd.FullName())
		}
	}
}

// TestBlockVerificationRoundTrip: ToPB then FromPB, also through
// proto.Marshal, gives the value back, with every optional message absent as
// well as present.
func TestBlockVerificationRoundTrip(t *testing.T) {
	sparse := blockVerificationFixture()
	sparse.Header.IsTableSetTransition, sparse.Header.TrackIndexerID = false, nil
	sparse.Anchor = nil
	sparse.ScanParts, sparse.SourceNodes = nil, nil
	sparse.Evidence = []VerifierEvidence{{ReplicaID: "verifier-1"}, {ReplicaID: "verifier-2", Evaluated: true}}
	sparse.Statements = []StatementCandidates{{StatementSeq: 300, StatementID: "stmt-300", Status: "pending"}}
	for name, want := range map[string]BlockVerification{"every field": blockVerificationFixture(), "absent messages and lists": sparse, "header only": {Header: BlockHeaderView{L3BlockSeq: 1}}} {
		t.Run(name, func(t *testing.T) {
			raw, err := proto.Marshal(BlockVerificationToPB(want))
			if err != nil {
				t.Fatal(err)
			}
			var m pb.BlockVerification
			if err := proto.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			for via, msg := range map[string]*pb.BlockVerification{"direct": BlockVerificationToPB(want), "marshalled": &m} {
				got, err := BlockVerificationFromPB(msg)
				if err != nil {
					t.Fatalf("%s: %v", via, err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%s round trip\n got %+v\nwant %+v", via, got, want)
				}
			}
		})
	}
}

// TestBlockVerificationWithoutAHeaderIsRefused: an answer that names no block
// is refused, never read as block 0.
func TestBlockVerificationWithoutAHeaderIsRefused(t *testing.T) {
	for name, m := range map[string]*pb.BlockVerification{"nil": nil, "no header": {ChainHash: "0xchain", Quorum: true}} {
		if _, err := BlockVerificationFromPB(m); err == nil || err.Error() != "wire: block verification has no header" {
			t.Fatalf("%s: BlockVerificationFromPB = %v, want the no-header refusal", name, err)
		}
	}
}

// TestBlockVerificationTrackKeepsIndexerZeroDistinctFromAbsent: a block of
// indexer 0's track has a present track with indexer_id 0, a block sealed
// before the tracks activation has none, and both survive the wire.
func TestBlockVerificationTrackKeepsIndexerZeroDistinctFromAbsent(t *testing.T) {
	zero := uint64(0)
	for name, track := range map[string]*uint64{"absent": nil, "indexer 0": &zero} {
		t.Run(name, func(t *testing.T) {
			raw, err := proto.Marshal(BlockVerificationToPB(BlockVerification{Header: BlockHeaderView{L3BlockSeq: 5, TrackIndexerID: track}}))
			if err != nil {
				t.Fatal(err)
			}
			var m pb.BlockVerification
			if err := proto.Unmarshal(raw, &m); err != nil {
				t.Fatal(err)
			}
			if (m.GetHeader().GetTrack() != nil) != (track != nil) {
				t.Fatalf("wire track present = %v, want %v", m.GetHeader().GetTrack() != nil, track != nil)
			}
			got, err := BlockVerificationFromPB(&m)
			if err != nil {
				t.Fatal(err)
			}
			switch {
			case track == nil && got.Header.TrackIndexerID != nil:
				t.Fatalf("absent track read as indexer %d", *got.Header.TrackIndexerID)
			case track != nil && (got.Header.TrackIndexerID == nil || *got.Header.TrackIndexerID != 0):
				t.Fatalf("indexer 0's track read as %v", got.Header.TrackIndexerID)
			case got.Header.IsTableSetTransition:
				t.Fatal("a block with a track and no table_set_transition read as a transition block")
			}
		})
	}
}
