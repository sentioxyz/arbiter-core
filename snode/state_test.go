package snode

import (
	"encoding/hex"
	"os"
	"path/filepath"
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
