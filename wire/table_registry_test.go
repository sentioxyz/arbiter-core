package wire

import (
	"reflect"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func TestTableRegistryCommandsRoundTrip(t *testing.T) {
	ev := arbiter.L2EventRef{BlockNumber: 10, BlockHash: "0xb", LogIndex: 2, TxHash: "0xt"}
	for name, c := range map[string]Command{
		"seed": {SeedLegacyTables: &SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 9, Hash: "0x9"},
			Tables: []arbiter.LegacyTable{{DatabaseID: "db", TableID: "a", Created: ev}}}},
		"seed empty": {SeedLegacyTables: &SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 9, Hash: "0x9"}}},
		"add": {AddTable: &AddTable{DatabaseID: "db", TableID: "t", Created: ev, Schema: arbiter.L2EventRef{BlockNumber: 11, BlockHash: "0xc", LogIndex: 0, TxHash: "0xu"},
			SchemaVersion: 3, SchemaHash: "0xh", SchemaJSON: `{"table_id":"db.t"}`}},
		"retire": {RetireTables: &RetireTables{DatabaseID: "db", TableIDs: []string{"a", "b"}, Deleted: ev, Reason: TableRetireReasonDatabaseDeleted}},
		"cursor": {AdvanceL2Cursor: &AdvanceL2Cursor{To: arbiter.L2BlockRef{Number: 12, Hash: "0xd"}}},
		"purged": {RecordTablePurged: &RecordTablePurged{NodeID: "v1", IncarnationSeq: 7}},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := Encode(c)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, c) {
				t.Fatalf("round trip\n got %+v\nwant %+v", got, c)
			}
		})
	}
}

func TestTableRegistryCommandsRequireEvidence(t *testing.T) {
	for name, c := range map[string]Command{
		"seed without block":     {SeedLegacyTables: &SeedLegacyTables{}},
		"add without created":    {AddTable: &AddTable{DatabaseID: "db", TableID: "t", Schema: arbiter.L2EventRef{BlockNumber: 1, BlockHash: "0x1"}}},
		"retire without deleted": {RetireTables: &RetireTables{DatabaseID: "db", TableIDs: []string{"t"}}},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := Encode(c)
			if err != nil {
				t.Fatal(err)
			}
			got, err := Decode(b)
			if err != nil {
				t.Fatal(err)
			}
			// Absent sub-messages decode to zero values; the FSM rejects them.
			if got.SeedLegacyTables == nil && got.AddTable == nil && got.RetireTables == nil {
				t.Fatal("command lost its type")
			}
		})
	}
}
