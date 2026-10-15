package snode

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func ackedInventoryS(names ...string) []arbiter.SafePartMapping {
	out := make([]arbiter.SafePartMapping, 0, len(names))
	for _, name := range names {
		out = append(out, arbiter.SafePartMapping{PartRowLtHash: "0xrow-" + name, SafePartName: name, PartPhysHash: "0xphys-" + name})
	}
	return out
}

// TestStateStore_AckedSafeInventorySurvivesANotAppliedAck: a base CAS refusal
// replaces LastAcks, but the owner audit's local reference stays the last
// applied inventory, across a reopen (housegate spec 2026-10-14 §14).
func TestStateStore_AckedSafeInventorySurvivesANotAppliedAck(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	k := partitionKey{Table: "db.t", Partition: "all"}
	inventory := ackedInventoryS("all_2_2_0", "all_3_3_0")
	applied := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 3, TableID: "db.t", PartitionID: "all", PostPartitionCommitment: "0xpost", Applied: true, SafePartitionParts: inventory}
	if err := st.RecordAppliedPromotion(k, 3, applied, "0xpost", "snap-3", nil, nil); err != nil {
		t.Fatal(err)
	}
	refused := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 4, TableID: "db.t", PartitionID: "all", Detail: "base CAS mismatch"}
	if err := st.RecordAck(k, 4, refused, "0xpost", "snap-3"); err != nil {
		t.Fatal(err)
	}
	want := ackedSafeInventory{PromotionSeq: 3, Parts: inventory}
	if got, ok := st.AckedSafeInventory(k); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory after a not-applied ack = %+v, %v; want %+v", got, ok, want)
	}
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := reopened.AckedSafeInventory(k); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory after a reopen = %+v, %v; want %+v", got, ok, want)
	}
	if last, _ := reopened.LastAck(k); last.PromotionSeq != 4 || last.Applied {
		t.Fatalf("LastAck = %+v, want the not-applied promotion 4", last)
	}
}

// TestStateStore_AckedSafeInventoriesIsAnOptionalJSONField: a state file written
// by v0.14.1 has no acked_safe_inventories. It loads unchanged, the inventory is
// derived from its applied LastAcks entry, a write that records no inventory
// keeps v0.14.1's shape, and a not-applied acknowledgement writes the derived
// inventory down before it replaces that entry.
func TestStateStore_AckedSafeInventoriesIsAnOptionalJSONField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	v0141 := `{"watermarks":{"db.t\u0000all":5},"last_acks":{"db.t\u0000all":{"node_id":"s1","promotion_seq":5,"table_id":"db.t","partition_id":"all",` +
		`"post_partition_commitment":"0xpost","parts":null,"safe_partition_parts":[{"part_row_lthash":"0xrow","safe_part_name":"all_7_7_0","part_phys_hash":"0xphys"}],"applied":true}},` +
		`"base_roots":{"db.t\u0000all":"0xpost"},"base_snapshot_ids":{"db.t\u0000all":"snap"},"unpromoted_sums":{}}`
	if err := os.WriteFile(path, []byte(v0141), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("a v0.14.1 state file: %v", err)
	}
	k := partitionKey{Table: "db.t", Partition: "all"}
	want := ackedSafeInventory{PromotionSeq: 5, Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xrow", SafePartName: "all_7_7_0", PartPhysHash: "0xphys"}}}
	if got, ok := st.AckedSafeInventory(k); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("derived inventory = %+v, %v; want %+v", got, ok, want)
	}
	if got := st.ackedInventoryPartitions("db.t"); !slices.Equal(got, []string{"all"}) {
		t.Fatalf("partitions with an inventory = %v, want [all]", got)
	}
	fields := func() map[string]json.RawMessage {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]json.RawMessage
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if _, err := st.NextRegistrationSeq(100); err != nil {
		t.Fatal(err)
	}
	if raw, ok := fields()["acked_safe_inventories"]; ok {
		t.Fatalf("a write that recorded no inventory wrote acked_safe_inventories %s", raw)
	}
	refused := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 6, TableID: "db.t", PartitionID: "all", Detail: "base CAS mismatch"}
	if err := st.RecordAck(k, 6, refused, "0xpost", "snap"); err != nil {
		t.Fatal(err)
	}
	var stored map[string]ackedSafeInventory
	if err := json.Unmarshal(fields()["acked_safe_inventories"], &stored); err != nil || !reflect.DeepEqual(stored, map[string]ackedSafeInventory{key("db.t", "all"): want}) {
		t.Fatalf("acked_safe_inventories = %v (%v), want the derived inventory", stored, err)
	}
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := reopened.AckedSafeInventory(k); !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("inventory after the not-applied ack and a reopen = %+v, %v; want %+v", got, ok, want)
	}
}

// TestStateStore_RecordRefreshedAckKeepsTheAppliedInventory: refreshing a
// legacy acknowledgement completes a missing inventory, but never replaces the
// one the applied promotion recorded for the same seq, which describes hg_safe
// as REPLACE left it.
func TestStateStore_RecordRefreshedAckKeepsTheAppliedInventory(t *testing.T) {
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	k := partitionKey{Table: "db.t", Partition: "all"}
	ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 4, TableID: "db.t", PartitionID: "all", PostPartitionCommitment: "0xpost", Applied: true,
		SafePartitionParts: ackedInventoryS("all_4_4_0")}
	if err := st.RecordAppliedPromotion(k, 4, ack, "0xpost", "snap-4", nil, nil); err != nil {
		t.Fatal(err)
	}
	refreshed := ack
	refreshed.SafePartitionParts = ackedInventoryS("all_4_4_1")
	if err := st.RecordRefreshedAck(k, refreshed); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.AckedSafeInventory(k); !reflect.DeepEqual(got.Parts, ackedInventoryS("all_4_4_0")) {
		t.Fatalf("inventory after a refresh = %+v, want the applied one", got)
	}
	legacyKey := partitionKey{Table: "db.u", Partition: "all"}
	legacy := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 5, TableID: "db.u", PartitionID: "all", PostPartitionCommitment: "0xpost-u", Applied: true}
	if err := st.RecordAppliedPromotion(legacyKey, 5, legacy, "0xpost-u", "snap-5", nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.AckedSafeInventory(legacyKey); ok {
		t.Fatal("an applied acknowledgement without an inventory recorded one")
	}
	legacy.SafePartitionParts = ackedInventoryS("all_5_5_0")
	if err := st.RecordRefreshedAck(legacyKey, legacy); err != nil {
		t.Fatal(err)
	}
	if got, ok := st.AckedSafeInventory(legacyKey); !ok || got.PromotionSeq != 5 || !reflect.DeepEqual(got.Parts, ackedInventoryS("all_5_5_0")) {
		t.Fatalf("inventory after refreshing a legacy acknowledgement = %+v, %v", got, ok)
	}
}

// TestStateStore_ForgetTableForgetsAckedSafeInventories: a dropped table's
// inventory goes with its other per-partition state; another table's stays.
func TestStateStore_ForgetTableForgetsAckedSafeInventories(t *testing.T) {
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	k, other := partitionKey{Table: "db.t", Partition: "all"}, partitionKey{Table: "db.u", Partition: "all"}
	for i, pk := range []partitionKey{k, other} {
		seq := uint64(i + 2)
		ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: seq, TableID: pk.Table, PartitionID: pk.Partition, PostPartitionCommitment: "0xpost", Applied: true, SafePartitionParts: ackedInventoryS("all_1_1_0")}
		if err := st.RecordAppliedPromotion(pk, seq, ack, "0xpost", "snap", nil, nil); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.ForgetTable("db.t"); err != nil {
		t.Fatal(err)
	}
	if _, ok := st.AckedSafeInventory(k); ok {
		t.Fatal("ForgetTable kept the dropped table's inventory")
	}
	if _, ok := st.AckedSafeInventory(other); !ok {
		t.Fatal("ForgetTable dropped another table's inventory")
	}
}
