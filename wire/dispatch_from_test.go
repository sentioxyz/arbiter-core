package wire

import (
	"reflect"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

func TestReplayJobFromPB_roundTripsReplayJob(t *testing.T) {
	// Given
	want := replay.ReplayJob{
		BlockSeq:           41,
		PrevSafeSnapshotID: "snap-40",
		PrevStateRoot:      "0xprev",
		SchemaSnapshotID:   "schema-7",
		ExecutorProfileID:  "ch-26.x-pinned",
		SourceClaimRoot:    "0xsource",
		Statements: []replay.Statement{{
			StatementID:   "0xabc:9:nonce",
			StatementSeq:  9,
			SQL:           "INSERT INTO db.t FORMAT CSVWithNames",
			SQLHash:       "0xsql",
			SettingsHash:  "0xsettings",
			PayloadRef:    "s3://payloads/9.csv",
			PayloadHash:   "0xpayload",
			PayloadLength: 123,
			TargetTableID: "db.t",
			UserJWS:       "a.b.c",
		}},
	}
	msg := ReplayJobToPB(want)

	// When
	got := ReplayJobFromPB(msg)

	// Then
	if !reflect.DeepEqual(want, got) {
		t.Fatalf("replay job round trip diverged:\nwant=%+v\n got=%+v", want, got)
	}
}

func TestStatementFromPB_roundTripsStatement(t *testing.T) {
	// Given
	want := replay.Statement{
		StatementID:   "0xabc:10:nonce",
		StatementSeq:  10,
		SQL:           "INSERT INTO db.t VALUES (1)",
		SQLHash:       "0xsql",
		SettingsHash:  "0xsettings",
		PayloadRef:    "s3://payloads/10.csv",
		PayloadHash:   "0xpayload",
		PayloadLength: 64,
		TargetTableID: "db.t",
		UserJWS:       "a.b.c",
	}
	msg := statementToPB(want)

	// When
	got := StatementFromPB(msg)

	// Then
	if got != want {
		t.Fatalf("statement round trip diverged:\nwant=%+v\n got=%+v", want, got)
	}
}

func TestReceiveSideSigningPayloads_roundTrip(t *testing.T) {
	// Given
	promote := arbiter.PromoteSafePartition{
		TableID:            "db.t",
		PartitionID:        "202607",
		PromotionSeq:       4,
		BaseSafeSnapshotID: "snap-3",
		BasePartitionRoot:  "0xbase",
		CandidateParts:     []arbiter.PartRef{{TableID: "db.t", PartitionID: "202607", PartRowLtHash: "0xrow", PartName: "all_1_1_0"}},
	}
	cleanup := arbiter.UnsafeCleanup{
		TableID:      "db.t",
		PartitionID:  "202607",
		PromotionSeq: 4,
		Parts:        []arbiter.PartRef{{TableID: "db.t", PartitionID: "202607", PartRowLtHash: "0xrow", PartName: "all_1_1_0"}},
	}

	// When
	gotPromote := PromoteFromPB(PromoteToPB(promote))
	gotCleanup := CleanupFromPB(CleanupToPB(cleanup))

	// Then
	if !reflect.DeepEqual(promote, gotPromote) {
		t.Fatalf("promote round trip diverged:\nwant=%+v\n got=%+v", promote, gotPromote)
	}
	if !reflect.DeepEqual(cleanup, gotCleanup) {
		t.Fatalf("cleanup round trip diverged:\nwant=%+v\n got=%+v", cleanup, gotCleanup)
	}
}

func TestPartRefsFromPB_emptyRepeatedNormalizesNil(t *testing.T) {
	// When
	got := PartRefsFromPB(nil)

	// Then
	if got != nil {
		t.Fatalf("nil repeated part refs must stay nil, got %+v", got)
	}
}
