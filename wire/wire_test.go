package wire

import (
	"reflect"
	"testing"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

func mustRoundTrip(t *testing.T, in Command) Command {
	t.Helper()
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip diverged:\n in=%+v\nout=%+v", in, out)
	}
	return out
}

func TestRoundTrip_SubmitStatement(t *testing.T) {
	mustRoundTrip(t, Command{SubmitStatement: &SubmitStatement{
		Envelope: arbiter.StatementEnvelope{
			StatementID:   arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"},
			StatementKind: arbiter.StatementKindInsert,
			SQL:           "INSERT INTO t VALUES (1)",
			SQLHash:       "0x11",
			SettingsHash:  "0x22",
			PayloadRef:    "ref",
			PayloadHash:   "0x33",
			PayloadLength: 9,
			TargetTableID: "db.t",
			UserJWS:       "a.b.c",
		},
	}})
}

func TestRoundTrip_AllScalarCommands(t *testing.T) {
	for _, c := range []Command{
		{SealL3Block: &SealL3Block{}},
		{MarkReplaying: &MarkReplaying{BlockSeq: 4}},
		{RecordAnchorFinality: &RecordAnchorFinality{L3BlockSeq: 4, Anchor: arbiter.AnchorRef{L3BlockHash: "0xaa", StateRoot: "0xbb", L2TxRef: "tx", L2BlockNumber: 9}, FinalityReached: true, LastMergeableReached: true}},
		{OpenChallenge: &OpenChallenge{BlockSeq: 4, Reason: "mismatch", OpenedBy: "r2"}},
		{ResolveChallenge: &ResolveChallenge{BlockSeq: 4, Verdict: ChallengeVerdictRejected}},
		{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "n1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier, arbiter.NodeRoleSNode}, Ed25519Pubkey: []byte{1, 2, 3}, DialAddr: "addr"}}},
		{MarkActive: &MarkActive{NodeID: "n1"}},
		{EvictNode: &EvictNode{NodeID: "n1", Reason: "audit"}},
	} {
		mustRoundTrip(t, c)
	}
}

func TestRoundTrip_RCAndEvidence(t *testing.T) {
	rc := arbiter.RCRecord{
		StatementID:          arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"},
		SourceNode:           "s1",
		CandidateParts:       []arbiter.CandidatePart{{TableID: "db.t", PartitionID: "p0", PartName: "all_1_1_0", PartRowLtHash: "0xffee", PartPhysHash: "0x99", RowCount: 2, Bytes: 64}},
		SourceClaimRoot:      "0xr00t",
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: "p0", NewPartsLtHashSum: "0xffee"}},
	}
	mustRoundTrip(t, Command{RegisterRC: &RegisterRC{RC: rc}})

	att := replay.ReplayAttestation{
		ReplicaID: "r1",
		Receipt: replay.ExecutionReceipt{
			BlockSeq: 4, PrevSafeSnapshotID: "s0", PrevStateRoot: "0x0", SchemaSnapshotID: "sch", ExecutorProfileID: "prof",
			StatementRoot: "0xs", PayloadRoot: "0xp", SourceClaimRoot: "0xr00t", ComputedStateRoot: "0xr00t", MatchSourceRoot: true,
			PartitionCommitmentsAfter: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: "0xcc"}},
			AffectedParts:             []replay.PartManifestEntry{{TableID: "db.t", PartitionID: "p0", PartName: "all_1_1_0", PartPhysHash: "0x99", PartRowLtHash: "0xffee", RowCount: 2, Bytes: 64}},
		},
		ReceiptHash: "0xrh", Signature: "aabb", MatchSourceRoot: true,
	}
	mustRoundTrip(t, Command{RecordAttestation: &RecordAttestation{Attestation: att}})

	scan := arbiter.ByteSideScanMsg{ReplicaID: "r1", BlockSeq: 4,
		Parts:    []arbiter.PartScan{{TableID: "db.t", PartitionID: "p0", ClaimedPartRowLtHash: "0xffee", ScannedPartRowLtHash: "0xffee", LivePartName: "all_1_1_0"}},
		ScanHash: "0xsh", Signature: "ccdd"}
	mustRoundTrip(t, Command{RecordByteSideScan: &RecordByteSideScan{Scan: scan}})
}

func TestRoundTrip_PromotionAndManifest(t *testing.T) {
	mustRoundTrip(t, Command{RecordPromotionIssued: &RecordPromotionIssued{
		Promote:      arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1, BaseSafeSnapshotID: "s0", BasePartitionRoot: "0x00", CandidateParts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: "0xffee", PartName: "all_1_1_0"}}},
		AuthorityJWS: "x.y.z"}})
	mustRoundTrip(t, Command{RecordPromotionAck: &RecordPromotionAck{Ack: arbiter.PromotionAck{
		NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", PostPartitionCommitment: "0xpost",
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xffee", SafePartName: "all_2_2_0", PartPhysHash: "0x99"}}, Applied: true, Detail: "ok"}}})
	mustRoundTrip(t, Command{ScheduleUnsafeCleanup: &ScheduleUnsafeCleanup{
		Cleanup: arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1, Parts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: "0xffee"}}}, AuthorityJWS: "x.y.z"}})
	mustRoundTrip(t, Command{RecordCleanupAck: &RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0"}}})
	mustRoundTrip(t, Command{PublishSafeSnapshot: &PublishSafeSnapshot{Manifest: replay.SafeSnapshotManifest{
		SnapshotID: "s1", ParentSnapshotID: "s0", SafeBlockSeq: 4, StateRoot: "0xsr", SchemaSnapshotID: "sch", SchemaRoot: "0xschr", ExecutorProfileID: "prof", DataRoot: "0xdr", ManifestRoot: "0xmr",
		Tables: []replay.TableManifest{{TableID: "db.t", SchemaHash: "0xsh",
			PartitionRoots: []replay.PartitionCommitment{{TableID: "db.t", PartitionID: "p0", Root: "0xcc"}},
			ActiveParts:    []replay.PartManifestEntry{{TableID: "db.t", PartitionID: "p0", PartName: "all_2_2_0", PartPhysHash: "0x99", PartRowLtHash: "0xffee", RowCount: 2, Bytes: 64, StorageRefs: []string{"s3://x"}}}}}}}})
}

// The frozen nil/empty rule: empty repeated fields decode to nil, absent
// messages to nil pointers — the canonical Go form (design §2).
func TestNormalization_EmptyRepeatedDecodesNil(t *testing.T) {
	in := Command{RegisterRC: &RegisterRC{RC: arbiter.RCRecord{
		StatementID:          arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"},
		SourceNode:           "s1",
		CandidateParts:       []arbiter.CandidatePart{}, // empty NON-nil in
		SourceClaimRoot:      "0xr",
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{}}}} // empty NON-nil in
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.RegisterRC.RC.CandidateParts != nil || out.RegisterRC.RC.PartitionNewPartSums != nil {
		t.Fatal("empty repeated fields must normalize to nil on decode")
	}
}

func TestEncode_RejectsZeroOrMultipleCommands(t *testing.T) {
	if _, err := Encode(Command{}); err == nil {
		t.Fatal("empty command must be rejected")
	}
	if _, err := Encode(Command{SealL3Block: &SealL3Block{}, MarkActive: &MarkActive{NodeID: "n"}}); err == nil {
		t.Fatal("two set commands must be rejected")
	}
}

func TestDecode_RejectsGarbageAndEmptyOneof(t *testing.T) {
	if _, err := Decode([]byte{0xff, 0x01, 0x02}); err == nil {
		t.Fatal("garbage bytes must error")
	}
	if _, err := Decode(nil); err == nil {
		t.Fatal("empty RaftCommand (no oneof) must error")
	}
}

func TestDispatchBuilders(t *testing.T) {
	job := replay.ReplayJob{
		BlockSeq:           4,
		PrevSafeSnapshotID: "s0",
		PrevStateRoot:      "0xps",
		SchemaSnapshotID:   "sch",
		ExecutorProfileID:  "prof",
		SourceClaimRoot:    "0xr",
		Statements: []replay.Statement{{
			StatementID:   "0xa:1:n",
			StatementSeq:  7,
			SQL:           "INSERT",
			SQLHash:       "0xh",
			SettingsHash:  "0xsettings",
			PayloadRef:    "s3://payload",
			PayloadHash:   "0xpayload",
			PayloadLength: 64,
			TargetTableID: "db.t",
			UserJWS:       "a.b.c",
		}},
	}
	d := ReplayJobDispatch(job)
	pj := d.GetReplayJob()
	if pj == nil || pj.GetBlockSeq() != job.BlockSeq || pj.GetPrevSafeSnapshotId() != job.PrevSafeSnapshotID ||
		pj.GetPrevStateRoot() != job.PrevStateRoot || pj.GetSchemaSnapshotId() != job.SchemaSnapshotID ||
		pj.GetExecutorProfileId() != job.ExecutorProfileID || pj.GetSourceClaimRoot() != job.SourceClaimRoot ||
		len(pj.GetStatements()) != 1 {
		t.Fatalf("replay job dispatch: %+v", d)
	}
	wantStatement := job.Statements[0]
	gotStatement := pj.GetStatements()[0]
	if gotStatement.GetStatementId() != wantStatement.StatementID ||
		gotStatement.GetStatementSeq() != wantStatement.StatementSeq ||
		gotStatement.GetSql() != wantStatement.SQL ||
		gotStatement.GetSqlHash() != wantStatement.SQLHash ||
		gotStatement.GetSettingsHash() != wantStatement.SettingsHash ||
		gotStatement.GetPayloadRef() != wantStatement.PayloadRef ||
		gotStatement.GetPayloadHash() != wantStatement.PayloadHash ||
		gotStatement.GetPayloadLength() != wantStatement.PayloadLength ||
		gotStatement.GetTargetTableId() != wantStatement.TargetTableID ||
		gotStatement.GetUserJws() != wantStatement.UserJWS {
		t.Fatalf("replay statement dispatch: %+v", gotStatement)
	}
	sd := ByteSideScanDispatch(4, []arbiter.PartRef{{TableID: "db.t", PartitionID: "p0", PartRowLtHash: "0xffee"}})
	sr := sd.GetByteSideScan()
	if sr == nil || sr.GetBlockSeq() != 4 || len(sr.GetParts()) != 1 || sr.GetParts()[0].GetPartRowLthash() != "0xffee" {
		t.Fatalf("scan dispatch: %+v", sd)
	}
	pc := PromotionCommandPB(arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1}, "x.y.z")
	if pc.GetPromote() == nil || pc.GetPromote().GetPromotionSeq() != 1 || pc.GetAuthorityJws() != "x.y.z" {
		t.Fatalf("promotion command: %+v", pc)
	}
	cc := CleanupCommandPB(arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p0", PromotionSeq: 1}, "x.y.z")
	if cc.GetCleanup() == nil || cc.GetCleanup().GetPromotionSeq() != 1 {
		t.Fatalf("cleanup command: %+v", cc)
	}
}

func TestExportedConverterRoundTrip(t *testing.T) {
	env := arbiter.StatementEnvelope{StatementID: arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"},
		StatementKind: arbiter.StatementKindInsert, SQL: "INSERT", SQLHash: "0xh", UserJWS: "a.b.c"}
	if got := EnvelopeFromPB(EnvelopeToPB(env)); got != env {
		t.Fatalf("envelope round trip: %+v", got)
	}
}
