package snode

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func testRecord(id string) intakeRecord {
	return intakeRecord{
		StatementID: id,
		Lifecycle:   LifecyclePreparing,
		Envelope: arbiter.StatementEnvelope{
			StatementID: arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 7, ClientNonce: "n"},
			SQL:         "INSERT INTO t VALUES",
		},
		PayloadEncoding:   "csv-with-names-v1",
		Revision:          54460,
		TouchedPartitions: []string{"p_p0"},
		PreWriteInventory: map[string][]string{"p_p0": {"p0_1_1_0"}},
		ExpectedRowCount:  2,
	}
}

func TestIntakeJournal_SaveLoadRoundTrip(t *testing.T) {
	j, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	id := "0xabc:7:n"
	if _, ok, err := j.load(id); err != nil || ok {
		t.Fatalf("empty journal must miss: ok=%v err=%v", ok, err)
	}
	rec := testRecord(id)
	if err := j.save(rec); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := j.load(id)
	if err != nil || !ok {
		t.Fatalf("load: ok=%v err=%v", ok, err)
	}
	if got.Lifecycle != LifecyclePreparing || got.ExpectedRowCount != 2 || got.PreWriteInventory["p_p0"][0] != "p0_1_1_0" {
		t.Fatalf("round trip mismatch: %+v", got)
	}

	rec.Lifecycle = LifecycleAbortPending
	rec.Abort = &intakeAbort{Reason: "test transition"}
	if err := j.save(rec); err != nil {
		t.Fatalf("save transition: %v", err)
	}
	got, _, _ = j.load(id)
	if got.Lifecycle != LifecycleAbortPending {
		t.Fatalf("transition not persisted: %+v", got)
	}
}

func TestIntakeJournal_ListAndCorruptFileFails(t *testing.T) {
	dir := t.TempDir()
	j, err := openIntakeJournal(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := j.save(testRecord("a:1:x")); err != nil {
		t.Fatalf("save first: %v", err)
	}
	if err := j.save(testRecord("b:2:y")); err != nil {
		t.Fatalf("save second: %v", err)
	}
	recs, err := j.list()
	if err != nil || len(recs) != 2 {
		t.Fatalf("list: %d %v", len(recs), err)
	}
	if err := os.WriteFile(filepath.Join(dir, "intake", "deadbeef.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := j.list(); err == nil {
		t.Fatal("corrupt journal file must fail list")
	}
}

func TestIntakeJournal_FilenameEncodesColons(t *testing.T) {
	j, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	id := "0xAbC:9:no/nce"
	if err := j.save(testRecord(id)); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, ok, err := j.load(id)
	if err != nil || !ok || got.StatementID != id {
		t.Fatalf("hex filename round trip: ok=%v err=%v got=%+v", ok, err, got)
	}
}

func TestIntakeJournal_RejectsIllegalTransition(t *testing.T) {
	j, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rec := testRecord("a:1:x")
	if err := j.save(rec); err != nil {
		t.Fatalf("save preparing: %v", err)
	}
	rec.Lifecycle = LifecycleCleaned
	rec.Abort = &intakeAbort{Reason: "skip required states"}
	if err := j.save(rec); err == nil {
		t.Fatal("Preparing -> Cleaned must be rejected")
	}
}

func TestIntakeJournal_CrashTempFileLeavesOldRecordAuthoritative(t *testing.T) {
	j, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	rec := testRecord("a:1:x")
	if err := j.save(rec); err != nil {
		t.Fatalf("save preparing: %v", err)
	}
	if err := os.WriteFile(j.recordPath(rec.StatementID)+".tmp", []byte("{"), 0o600); err != nil {
		t.Fatalf("write simulated crash temp: %v", err)
	}
	got, ok, err := j.load(rec.StatementID)
	if err != nil || !ok || got.Lifecycle != LifecyclePreparing {
		t.Fatalf("old record must survive pre-rename crash: ok=%v err=%v got=%+v", ok, err, got)
	}
	records, err := j.list()
	if err != nil || len(records) != 1 || records[0].StatementID != rec.StatementID {
		t.Fatalf("temp file must be ignored by startup listing: records=%+v err=%v", records, err)
	}
}

func TestConvergeStartup_CorruptRecordFailsClosed(t *testing.T) {
	j, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := os.WriteFile(filepath.Join(j.dir, "deadbeef.json"), []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}
	role := &Role{journal: j}
	if err := role.convergeStartup(t.Context()); err == nil {
		t.Fatal("startup must fail closed on a corrupt intake record")
	}
}

func overwriteIntakeRecordForTest(t *testing.T, journal *intakeJournal, rec intakeRecord) {
	t.Helper()
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		t.Fatalf("marshal intake record: %v", err)
	}
	if err := os.WriteFile(journal.recordPath(rec.StatementID), data, 0o600); err != nil {
		t.Fatalf("overwrite intake record: %v", err)
	}
}
