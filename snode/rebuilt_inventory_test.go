package snode

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The owner audit's rebuilt reference and the rebuild's journal writer
// (housegate spec 2026-10-14 §14 owner audit, §16 P4; plan S3-D contract §34,
// clarification K52). A rebuild restores hg_safe through RESTORE and ATTACH
// PARTITION FROM, which give the parts new names; the journal then holds the
// new names at the arbiter's PublishSeq, marked rebuilt, and the audit takes
// it as the reference when it equals the arbiter's SafeParts by row LtHash and
// hash_of_all_files.

// rebuiltCase is one partition acknowledged at promotion 5 under the names
// p0_3_3_0 and p0_4_4_0 and rebuilt under p0_1_1_0 and p0_2_2_0.
func rebuiltCase() (local *ackedSafeInventory, fsm *wire.PartitionState) {
	local = &ackedSafeInventory{PromotionSeq: 5, Rebuilt: true, Parts: []arbiter.SafePartMapping{
		{PartRowLtHash: "0xrowA", SafePartName: "p0_1_1_0", PartPhysHash: "hA"},
		{PartRowLtHash: "0xrowB", SafePartName: "p0_2_2_0", PartPhysHash: "hB"},
	}}
	fsm = &wire.PartitionState{TableID: "db.t", PartitionID: "p_p0", PublishSeq: 5, SafeParts: []replay.PartManifestEntry{
		{TableID: "db.t", PartitionID: "p_p0", PartName: "p0_4_4_0", PartPhysHash: "hB", PartRowLtHash: "0xrowB"},
		{TableID: "db.t", PartitionID: "p_p0", PartName: "p0_3_3_0", PartPhysHash: "hA", PartRowLtHash: "0xrowA"},
	}}
	return local, fsm
}

func TestOwnerAuditAcceptsARebuiltInventoryThatMatchesTheArbiterByHash(t *testing.T) {
	local, fsm := rebuiltCase()
	ref, ok, refusal, detail := ownerAuditReferenceFor(ownerAuditInputs{Watermark: 5, Local: local, FSMAvailable: true, FSM: fsm})
	want := []ownerAuditPart{{"p0_1_1_0", "hA"}, {"p0_2_2_0", "hB"}}
	if !ok || refusal != "" || detail != "" || ref.Source != "rebuilt" || ref.Seq != 5 || !samePartSet(ref.Parts, want) {
		t.Fatalf("reference = %+v ok %v refusal %q %q; want the rebuilt inventory at 5 with its own names", ref, ok, refusal, detail)
	}
	active := []partInfo{
		{Name: "p0_1_1_0", PartitionValue: "p0", PhysHash: "hA", Rows: 2},
		{Name: "p0_2_2_0", PartitionValue: "p0", PhysHash: "hB", Rows: 2},
	}
	if plan := planOwnerAudit(ref, ok, active); !plan.clean() {
		t.Fatalf("the rebuilt hg_safe is unclean against its reference: %+v", plan)
	}
	ref, ok, refusal, _ = ownerAuditReferenceFor(ownerAuditInputs{Watermark: 5, Local: local})
	if !ok || refusal != "" || ref.Source != "local" || !samePartSet(ref.Parts, want) {
		t.Fatalf("with the arbiter unavailable the rebuilt inventory is the local reference; got %+v ok %v %q", ref, ok, refusal)
	}
	plain := *local
	plain.Rebuilt = false
	if _, ok, refusal, _ := ownerAuditReferenceFor(ownerAuditInputs{Watermark: 5, Local: &plain, FSMAvailable: true, FSM: fsm}); ok || refusal != OwnerAuditFSMDisagrees {
		t.Fatalf("an ordinary inventory under other names is still fsm_disagrees; got ok %v %q", ok, refusal)
	}
}

func TestOwnerAuditRefusesARebuiltInventoryWithAnotherHash(t *testing.T) {
	for name, edit := range map[string]func(*ackedSafeInventory){
		"another row hash":          func(l *ackedSafeInventory) { l.Parts[0].PartRowLtHash = "0xrowX" },
		"another hash_of_all_files": func(l *ackedSafeInventory) { l.Parts[1].PartPhysHash = "hX" },
		"a part missing":            func(l *ackedSafeInventory) { l.Parts = l.Parts[:1] },
		"a part twice": func(l *ackedSafeInventory) {
			l.Parts = append(l.Parts, arbiter.SafePartMapping{PartRowLtHash: "0xrowA", SafePartName: "p0_5_5_0", PartPhysHash: "hA"})
		},
	} {
		t.Run(name, func(t *testing.T) {
			local, fsm := rebuiltCase()
			edit(local)
			_, ok, refusal, detail := ownerAuditReferenceFor(ownerAuditInputs{Watermark: 5, Local: local, FSMAvailable: true, FSM: fsm})
			if ok || refusal != OwnerAuditFSMDisagrees ||
				detail != "the rebuilt inventory of promotion 5 differs from the arbiter's by row hash or hash_of_all_files" {
				t.Fatalf("ok %v refusal %q detail %q", ok, refusal, detail)
			}
		})
	}
}

func TestOwnerAuditKeepsFSMAheadForARebuiltInventory(t *testing.T) {
	local, fsm := rebuiltCase()
	fsm.PublishSeq = 6
	_, ok, refusal, detail := ownerAuditReferenceFor(ownerAuditInputs{Watermark: 5, Local: local, FSMAvailable: true, FSM: fsm})
	if ok || refusal != OwnerAuditFSMAhead || !strings.Contains(detail, "applied promotion 6") {
		t.Fatalf("ok %v refusal %q detail %q, want fsm_ahead", ok, refusal, detail)
	}
	fsm.PublishSeq = 4
	ref, ok, refusal, _ := ownerAuditReferenceFor(ownerAuditInputs{Watermark: 5, Local: local, FSMAvailable: true, FSM: fsm})
	if !ok || refusal != "" || ref.Source != "local" || ref.Seq != 5 {
		t.Fatalf("a rebuilt inventory newer than the arbiter's is the local reference by the ordinary rule; got %+v ok %v %q", ref, ok, refusal)
	}
}

// rebuildJournalS is a journal at promotion 5 of db.t/p_p0 whose last
// acknowledgement was applied.
func rebuildJournalS(t *testing.T, watermark uint64) string {
	t.Helper()
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	k := partitionKey{Table: "db.t", Partition: "p_p0"}
	ack := arbiter.PromotionAck{NodeID: "snode-3", PromotionSeq: watermark, TableID: "db.t", PartitionID: "p_p0", PostPartitionCommitment: "0xpost",
		Applied: true, SafePartitionParts: []arbiter.SafePartMapping{{PartRowLtHash: "0xrowA", SafePartName: "p0_3_3_0", PartPhysHash: "hA"}}}
	if err := st.RecordAppliedPromotion(k, watermark, ack, "0xpost", "snap", nil, nil); err != nil {
		t.Fatal(err)
	}
	return dir
}

func rebuiltInventoryS() RebuiltSafeInventory {
	return RebuiltSafeInventory{TableID: "db.t", PartitionID: "p_p0", PublishSeq: 5, Parts: []arbiter.SafePartMapping{
		{PartRowLtHash: "0xrowA", SafePartName: "p0_1_1_0", PartPhysHash: "hA"}}}
}

func TestTheFirstPromotionAfterARebuildReplacesTheRebuiltReference(t *testing.T) {
	dir := rebuildJournalS(t, 5)
	if err := RecordRebuiltSafeInventories(dir, []RebuiltSafeInventory{rebuiltInventoryS()}); err != nil {
		t.Fatal(err)
	}
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	k := partitionKey{Table: "db.t", Partition: "p_p0"}
	want := ackedSafeInventory{PromotionSeq: 5, Rebuilt: true, Parts: rebuiltInventoryS().Parts}
	if got, ok := st.AckedSafeInventory(k); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("rebuilt inventory = %+v, %v; want %+v", got, ok, want)
	}
	if done, err := CheckRebuildJournal(dir, []RebuiltSafeInventory{rebuiltInventoryS()}); err != nil || len(done) != 1 || !done[0] {
		t.Fatalf("CheckRebuildJournal = %v, %v; want the partition reported rebuilt", done, err)
	}
	refused := arbiter.PromotionAck{NodeID: "snode-3", PromotionSeq: 6, TableID: "db.t", PartitionID: "p_p0", Detail: "base CAS mismatch"}
	if err := st.RecordAck(k, 6, refused, "0xpost", "snap"); err != nil {
		t.Fatal(err)
	}
	if got, ok := st.AckedSafeInventory(k); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("a not-applied acknowledgement must keep the rebuilt reference; got %+v, %v", got, ok)
	}
	next := []arbiter.SafePartMapping{{PartRowLtHash: "0xrowA", SafePartName: "p0_9_9_0", PartPhysHash: "hA"}, {PartRowLtHash: "0xrowC", SafePartName: "p0_10_10_0", PartPhysHash: "hC"}}
	applied := arbiter.PromotionAck{NodeID: "snode-3", PromotionSeq: 7, TableID: "db.t", PartitionID: "p_p0", PostPartitionCommitment: "0xpost7", Applied: true, SafePartitionParts: next}
	if err := st.RecordAppliedPromotion(k, 7, applied, "0xpost7", "snap-7", nil, nil); err != nil {
		t.Fatal(err)
	}
	if got, ok := st.AckedSafeInventory(k); !ok || !reflect.DeepEqual(got, ackedSafeInventory{PromotionSeq: 7, Parts: next}) {
		t.Fatalf("the first applied promotion after the rebuild must replace the rebuilt reference; got %+v, %v", got, ok)
	}
	if done, err := CheckRebuildJournal(dir, []RebuiltSafeInventory{{TableID: "db.t", PartitionID: "p_p0", PublishSeq: 7}}); err != nil || done[0] {
		t.Fatalf("CheckRebuildJournal after the promotion = %v, %v; want not rebuilt", done, err)
	}
}

func TestRecordRebuiltSafeInventoriesRefusesAJournalBehindTheArbiter(t *testing.T) {
	dir := rebuildJournalS(t, 4)
	before, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	inv := rebuiltInventoryS()
	for name, err := range map[string]error{
		"record": RecordRebuiltSafeInventories(dir, []RebuiltSafeInventory{inv}),
		"check":  func() error { _, err := CheckRebuildJournal(dir, []RebuiltSafeInventory{inv}); return err }(),
	} {
		if !errors.Is(err, ErrRebuildJournalBehind) || !strings.Contains(err.Error(), "db.t/p_p0 is at promotion 4, the arbiter acknowledged promotion 5") {
			t.Fatalf("%s: err = %v, want ErrRebuildJournalBehind naming the partition", name, err)
		}
	}
	if after, err := os.ReadFile(filepath.Join(dir, "state.json")); err != nil || string(after) != string(before) {
		t.Fatal("a refused rebuild record must write nothing")
	}
	missing := filepath.Join(t.TempDir(), "lost")
	if _, err := CheckRebuildJournal(missing, []RebuiltSafeInventory{inv}); !errors.Is(err, ErrRebuildJournalBehind) {
		t.Fatalf("a lost journal: err = %v, want ErrRebuildJournalBehind", err)
	}
	if err := RecordRebuiltSafeInventories(missing, []RebuiltSafeInventory{inv}); !errors.Is(err, ErrRebuildJournalBehind) {
		t.Fatalf("a lost journal: record err = %v", err)
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatalf("checking a lost journal must not create its directory: %v", err)
	}
}

func TestRecordRebuiltSafeInventoriesRefusesAPendingIntent(t *testing.T) {
	dir := rebuildJournalS(t, 5)
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	other := partitionKey{Table: "db.t", Partition: "p_p1"}
	if err := st.BeginPromotion(other, promotionIntent{PromotionSeq: 6, PostPartitionRoot: "0xpost"}); err != nil {
		t.Fatal(err)
	}
	err = RecordRebuiltSafeInventories(dir, []RebuiltSafeInventory{rebuiltInventoryS()})
	if !errors.Is(err, ErrRebuildIntentPending) || !strings.Contains(err.Error(), "db.t/p_p1 at promotion 6") {
		t.Fatalf("err = %v, want ErrRebuildIntentPending naming the intent", err)
	}
	if _, err := CheckRebuildJournal(dir, []RebuiltSafeInventory{rebuiltInventoryS()}); !errors.Is(err, ErrRebuildIntentPending) {
		t.Fatalf("check err = %v", err)
	}
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if inv, _ := reopened.AckedSafeInventory(partitionKey{Table: "db.t", Partition: "p_p0"}); inv.Rebuilt {
		t.Fatal("a refused rebuild record must write nothing")
	}
}

// TestStateFileWithoutRebuiltLoadsUnchanged: a CORE_TAG_P3 state file carries
// acked_safe_inventories without rebuilt. It loads with Rebuilt false, and a
// write that records no rebuilt inventory keeps that shape.
func TestStateFileWithoutRebuiltLoadsUnchanged(t *testing.T) {
	dir := t.TempDir()
	p3 := `{"watermarks":{"db.t\u0000p_p0":5},"last_acks":{},"base_roots":{"db.t\u0000p_p0":"0xpost"},"base_snapshot_ids":{},"unpromoted_sums":{},` +
		`"acked_safe_inventories":{"db.t\u0000p_p0":{"promotion_seq":5,"parts":[{"part_row_lthash":"0xrowA","safe_part_name":"p0_3_3_0","part_phys_hash":"hA"}]}}}`
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte(p3), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	k := partitionKey{Table: "db.t", Partition: "p_p0"}
	want := ackedSafeInventory{PromotionSeq: 5, Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xrowA", SafePartName: "p0_3_3_0", PartPhysHash: "hA"}}}
	if got, ok := st.AckedSafeInventory(k); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory = %+v, %v; want %+v", got, ok, want)
	}
	if err := st.RecordAck(k, 6, arbiter.PromotionAck{PromotionSeq: 6, TableID: "db.t", PartitionID: "p_p0"}, "0xpost", ""); err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "rebuilt") || !strings.Contains(string(b), `"acked_safe_inventories":{"db.t\u0000p_p0":{"promotion_seq":5,"parts":[`) {
		t.Fatalf("the state file changed shape: %s", b)
	}
}

// TestCheckRebuildJournalComparesTheJournaledNamesWhenGivenParts: a journal
// that holds a rebuilt inventory at the right seq but under other part names
// (a second rebuild that restored hg_safe and stopped before its journal
// write) is not "already rebuilt" for an inventory carrying the current
// names; without Parts the check is by seq only, as before.
func TestCheckRebuildJournalComparesTheJournaledNamesWhenGivenParts(t *testing.T) {
	dir := rebuildJournalS(t, 5)
	if err := RecordRebuiltSafeInventories(dir, []RebuiltSafeInventory{rebuiltInventoryS()}); err != nil {
		t.Fatal(err)
	}
	renamed := rebuiltInventoryS()
	renamed.Parts = []arbiter.SafePartMapping{{PartRowLtHash: "0xrowA", SafePartName: "p0_4_4_0", PartPhysHash: "hA"}}
	bySeq := rebuiltInventoryS()
	bySeq.Parts = nil
	done, err := CheckRebuildJournal(dir, []RebuiltSafeInventory{rebuiltInventoryS(), renamed, bySeq})
	if err != nil {
		t.Fatal(err)
	}
	if want := []bool{true, false, true}; !reflect.DeepEqual(done, want) {
		t.Fatalf("CheckRebuildJournal = %v, want %v", done, want)
	}
}
