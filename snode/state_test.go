package snode

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"

	"github.com/sentioxyz/arbiter-core"
)

func TestStateStore_RoundTripsDurableState(t *testing.T) {
	dir := t.TempDir()
	k := partitionKey{Table: "db.t", Partition: "p0"}
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 7, TableID: "db.t", PartitionID: "p0", Applied: true}
	part := testAccumulatorHexS(t, "part-a")

	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	if err := st.RecordAck(k, 7, ack, "0xbase", "snap-7"); err != nil {
		t.Fatalf("record ack: %v", err)
	}
	if err := st.AddUnpromoted(k, part); err != nil {
		t.Fatalf("add unpromoted: %v", err)
	}

	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen state: %v", err)
	}
	if got := reopened.Watermark(k); got != 7 {
		t.Fatalf("watermark: %d", got)
	}
	gotAck, ok := reopened.LastAck(k)
	if !ok || gotAck.PromotionSeq != 7 || !gotAck.Applied {
		t.Fatalf("ack: %+v ok=%v", gotAck, ok)
	}
	root, snap := reopened.BaseRoot(k)
	if root != "0xbase" || snap != "snap-7" {
		t.Fatalf("base: %s %s", root, snap)
	}
	if got := reopened.UnpromotedSum(k); got != part {
		t.Fatalf("unpromoted sum: got %s want %s", got, part)
	}
}

func TestStateStore_PromotedUnsafePartsLifecycle(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	k0 := partitionKey{Table: "db.t", Partition: "p0"}
	k1 := partitionKey{Table: "db.t", Partition: "p1"}
	other := partitionKey{Table: "db.u", Partition: "p0"}
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", Applied: true}
	if err := st.RecordAppliedPromotion(k0, 1, ack, "0xpost", "snap", nil, []string{"all_2_2_0", "all_1_1_0"}); err != nil {
		t.Fatalf("record p0: %v", err)
	}
	if err := st.RecordAppliedPromotion(k1, 1, ack, "0xpost", "snap", nil, []string{"all_9_9_0"}); err != nil {
		t.Fatalf("record p1: %v", err)
	}
	if err := st.RecordAppliedPromotion(other, 1, ack, "0xpost", "snap", nil, []string{"all_5_5_0"}); err != nil {
		t.Fatalf("record other: %v", err)
	}
	if got, err := st.PromotedUnsafeParts("db.t"); err != nil || !reflect.DeepEqual(got, []string{"all_1_1_0", "all_2_2_0", "all_9_9_0"}) {
		t.Fatalf("promoted (sorted, table-wide) = %v", got)
	}
	// Second promotion on the same partition appends; duplicates collapse.
	if err := st.RecordAppliedPromotion(k0, 2, ack, "0xpost2", "snap", nil, []string{"all_3_3_0", "all_1_1_0"}); err != nil {
		t.Fatalf("record p0 seq2: %v", err)
	}
	if got, err := st.PromotedUnsafeParts("db.t"); err != nil || len(got) != 4 {
		t.Fatalf("after second promotion = %v", got)
	}
	// Cleanup drops exactly the named parts; unknown names are ignored.
	if err := st.RecordCleanup(k0, []string{"all_1_1_0", "all_2_2_0", "ghost"}); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if got, err := st.PromotedUnsafeParts("db.t"); err != nil || !reflect.DeepEqual(got, []string{"all_3_3_0", "all_9_9_0"}) {
		t.Fatalf("after cleanup = %v", got)
	}
	// Durable across reopen.
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got, err := reopened.PromotedUnsafeParts("db.t"); err != nil || !reflect.DeepEqual(got, []string{"all_3_3_0", "all_9_9_0"}) {
		t.Fatalf("reopened = %v", got)
	}
	if got, err := reopened.PromotedUnsafeParts("db.u"); err != nil || !reflect.DeepEqual(got, []string{"all_5_5_0"}) {
		t.Fatalf("other table = %v", got)
	}
	if got, err := reopened.PromotedUnsafeParts("db.none"); err != nil || got != nil {
		t.Fatalf("unknown table = %v, want nil", got)
	}
}

func TestStateStore_PromotionIntentFailsReadsClosedUntilDurableFinalize(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	k := partitionKey{Table: "db.t", Partition: "p0"}
	intent := promotionIntent{
		PromotionSeq:       2,
		BasePartitionRoot:  "0xbase",
		PostPartitionRoot:  "0xpost",
		BaseSafeSnapshotID: "snap-1",
		CandidatePartHashes: []string{
			"0xpart",
		},
		UnsafePartNames: []string{"all_2_2_0"},
		SafePartsBefore: []promotionSafePart{{
			Name: "all_1_1_0", PhysHash: "hash-1",
		}},
	}
	if err := st.BeginPromotion(k, intent); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if _, err := st.PromotedUnsafeParts("db.t"); err == nil {
		t.Fatal("unresolved durable promotion intent must fail unsafe_latest reads closed")
	}

	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got, ok := reopened.PendingPromotion(k); !ok || !reflect.DeepEqual(got, intent) {
		t.Fatalf("pending after reopen = %+v ok=%v, want %+v", got, ok, intent)
	}
	if _, err := reopened.PromotedUnsafeParts("db.t"); err == nil {
		t.Fatal("reopened unresolved promotion intent must still fail reads closed")
	}

	// A failed final state-file replacement must not advance the in-memory
	// watermark or drop the fail-closed intent. Otherwise a same-process retry
	// could ACK state that was never made durable.
	reopened.path = dir // os.Rename(temp, existing directory) must fail.
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 2, TableID: "db.t", PartitionID: "p0", Applied: true}
	if err := reopened.RecordAppliedPromotion(k, 2, ack, "0xpost", "snap-2", nil, []string{"all_2_2_0"}); err == nil {
		t.Fatal("finalize with an unwritable state target unexpectedly succeeded")
	}
	if got := reopened.Watermark(k); got != 0 {
		t.Fatalf("watermark after failed finalize = %d, want 0", got)
	}
	if _, ok := reopened.PendingPromotion(k); !ok {
		t.Fatal("failed finalize dropped the in-memory promotion intent")
	}
}

func TestStateStore_BeginPromotionRefreshesSameCommandInventory(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	k := partitionKey{Table: "db.t", Partition: "p0"}
	intent := promotionIntent{
		PromotionSeq:        2,
		BasePartitionRoot:   "0xbase",
		PostPartitionRoot:   "0xpost",
		BaseSafeSnapshotID:  "snap-1",
		CandidatePartHashes: []string{"0xpart"},
		UnsafePartNames:     []string{"all_2_2_0"},
		SafePartsBefore:     []promotionSafePart{{Name: "all_1_1_0", PhysHash: "hash-1"}},
	}
	if err := st.BeginPromotion(k, intent); err != nil {
		t.Fatalf("begin: %v", err)
	}
	refreshed := clonePromotionIntent(intent)
	refreshed.SafePartsBefore = []promotionSafePart{{Name: "all_1_1_1", PhysHash: "hash-2"}}
	if err := st.BeginPromotion(k, refreshed); err != nil {
		t.Fatalf("refresh same command after content-neutral safe rewrite: %v", err)
	}

	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if got, ok := reopened.PendingPromotion(k); !ok || !reflect.DeepEqual(got, refreshed) {
		t.Fatalf("refreshed intent after reopen = %+v ok=%v, want %+v", got, ok, refreshed)
	}
}

func TestStateStore_RecordAckPersistFailureDoesNotAdvanceMemoryOrDisk(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	k := partitionKey{Table: "db.t", Partition: "p0"}
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 7, TableID: "db.t", PartitionID: "p0"}
	statePath := st.path
	st.path = dir // os.Rename(temp, existing directory) must fail.
	if err := st.RecordAck(k, 7, ack, "0xbase", "snap-7"); err == nil {
		t.Fatal("RecordAck with an unwritable state target unexpectedly succeeded")
	}
	if got := st.Watermark(k); got != 0 {
		t.Fatalf("in-memory watermark after failed persist = %d, want 0", got)
	}
	if _, ok := st.LastAck(k); ok {
		t.Fatal("in-memory ack advanced after failed persist")
	}

	st.path = statePath
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen after failed persist: %v", err)
	}
	if got := reopened.Watermark(k); got != 0 {
		t.Fatalf("disk watermark after failed persist = %d, want 0", got)
	}
	if err := st.RecordAck(k, 7, ack, "0xbase", "snap-7"); err != nil {
		t.Fatalf("same-process retry: %v", err)
	}
	reopened, err = openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen after retry: %v", err)
	}
	if got := reopened.Watermark(k); got != 7 {
		t.Fatalf("disk watermark after retry = %d, want 7", got)
	}
}

func TestStateStore_RecordCleanupPersistFailureRetainsExclusion(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	k := partitionKey{Table: "db.t", Partition: "p0"}
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: "db.t", PartitionID: "p0", Applied: true}
	if err := st.RecordAppliedPromotion(k, 1, ack, "0xpost", "snap-1", nil, []string{"all_1_1_0"}); err != nil {
		t.Fatalf("seed exclusion: %v", err)
	}
	statePath := st.path
	st.path = dir // os.Rename(temp, existing directory) must fail.
	if err := st.RecordCleanup(k, []string{"all_1_1_0"}); err == nil {
		t.Fatal("RecordCleanup with an unwritable state target unexpectedly succeeded")
	}
	if got, err := st.PromotedUnsafeParts("db.t"); err != nil || !reflect.DeepEqual(got, []string{"all_1_1_0"}) {
		t.Fatalf("in-memory exclusions after failed cleanup = %v %v", got, err)
	}

	st.path = statePath
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen after failed cleanup: %v", err)
	}
	if got, err := reopened.PromotedUnsafeParts("db.t"); err != nil || !reflect.DeepEqual(got, []string{"all_1_1_0"}) {
		t.Fatalf("disk exclusions after failed cleanup = %v %v", got, err)
	}
	if err := st.RecordCleanup(k, []string{"all_1_1_0"}); err != nil {
		t.Fatalf("same-process cleanup retry: %v", err)
	}
	reopened, err = openStateStore(dir)
	if err != nil {
		t.Fatalf("reopen after cleanup retry: %v", err)
	}
	if got, err := reopened.PromotedUnsafeParts("db.t"); err != nil || got != nil {
		t.Fatalf("disk exclusions after cleanup retry = %v %v, want empty", got, err)
	}
}

func TestStateStore_UnpromotedLtHashMath(t *testing.T) {
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatalf("open state: %v", err)
	}
	k := partitionKey{Table: "db.t", Partition: "p0"}
	partA := testAccumulatorHexS(t, "part-a")
	partB := testAccumulatorHexS(t, "part-b")
	if err := st.AddUnpromoted(k, partA); err != nil {
		t.Fatalf("add A: %v", err)
	}
	if err := st.AddUnpromoted(k, partB); err != nil {
		t.Fatalf("add B: %v", err)
	}
	if err := st.DrainUnpromoted(k, []string{partA}); err != nil {
		t.Fatalf("drain A: %v", err)
	}
	if got := st.UnpromotedSum(k); got != partB {
		t.Fatalf("remaining sum: got %s want %s", got, partB)
	}
}

func TestStateStore_CorruptJSONFailsToOpen(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "state.json"), []byte("{"), 0o600); err != nil {
		t.Fatalf("write corrupt state: %v", err)
	}
	if _, err := openStateStore(dir); err == nil {
		t.Fatal("corrupt state must fail to open")
	}
}

func testAccumulatorHexS(t *testing.T, seed string) string {
	t.Helper()
	h := lthash.New()
	h.Add([]byte(seed))
	return "0x" + hex.EncodeToString(h.Bytes())
}
