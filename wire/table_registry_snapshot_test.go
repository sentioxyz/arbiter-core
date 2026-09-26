package wire

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func registrySnapshotFixture(t *testing.T) TableRegistrySnapshot {
	t.Helper()
	schema := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "p",
		Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	ev := func(n uint64) *arbiter.L2EventRef {
		return &arbiter.L2EventRef{BlockNumber: n, BlockHash: "0xb", LogIndex: 1, TxHash: "0xt"}
	}
	return TableRegistrySnapshot{
		Params: arbiter.TableRegistryParams{ChainID: 7, DatabasesContract: "0x00000000000000000000000000000000000000d1",
			SIIndexerID: 1, ActivationBlock: 100, Confirmation: arbiter.TableRegistryConfirmationSafe},
		Version: 9, Seeded: true, Cursor: TableRegistryCursor{BlockNumber: 120, BlockHash: "0xc", LogIndex: 3, BlockComplete: true},
		Incarnations: []TableIncarnation{
			{Seq: 1, DatabaseID: "db", TableID: "g", Origin: TableOriginGenesis, Status: TableStatusActive, SchemaHash: "0xg"},
			{Seq: 2, DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusPurged, Created: ev(101), SchemaRef: ev(102),
				SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash("net", schema), SchemaJSON: string(js),
				Deleted: ev(103), RetireReason: TableRetireReasonTableDeleted, AddBlockSeq: 4, RetireBlockSeq: 6, PurgedBy: []string{"s1", "v1"}},
			{Seq: 3, DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusRefused, Created: ev(104), SchemaRef: ev(105),
				SchemaVersion: 1, RefusedCode: "column_type", RefusedReason: "column v has type UUID"},
			{Seq: 4, DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusPending, Created: ev(106), SchemaRef: ev(107),
				SchemaVersion: 2, SchemaHash: payloadexec.TableSchemaHash("net", schema), SchemaJSON: string(js)},
			{Seq: 5, DatabaseID: "db", TableID: "old", Origin: TableOriginLegacy, Status: TableStatusLegacy, Created: ev(50)},
		},
	}
}

func TestTableRegistrySnapshotRoundTrip(t *testing.T) {
	want := registrySnapshotFixture(t)
	b, err := proto.Marshal(TableRegistrySnapshotToPB(want))
	if err != nil {
		t.Fatal(err)
	}
	var m pb.TableRegistrySnapshot
	if err := proto.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	got, err := TableRegistrySnapshotFromPB(&m)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("round trip\n got %+v\nwant %+v", got, want)
	}
}

func TestTableRegistrySnapshotRefusesUnknownVocabulary(t *testing.T) {
	for name, mutate := range map[string]func(*pb.TableRegistrySnapshot){
		"unspecified status": func(m *pb.TableRegistrySnapshot) {
			m.Incarnations[0].Status = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_UNSPECIFIED
		},
		"future status": func(m *pb.TableRegistrySnapshot) { m.Incarnations[0].Status = pb.TableIncarnationStatus(99) },
		"unspecified origin": func(m *pb.TableRegistrySnapshot) {
			m.Incarnations[0].Origin = pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_UNSPECIFIED
		},
		"future origin": func(m *pb.TableRegistrySnapshot) { m.Incarnations[0].Origin = pb.TableIncarnationOrigin(9) },
		"future reason": func(m *pb.TableRegistrySnapshot) { m.Incarnations[1].RetireReason = pb.TableRetireReason(3) },
		"misnumbered":   func(m *pb.TableRegistrySnapshot) { m.Incarnations[2].Seq = 7 },
	} {
		t.Run(name, func(t *testing.T) {
			m := TableRegistrySnapshotToPB(registrySnapshotFixture(t))
			mutate(m)
			if _, err := TableRegistrySnapshotFromPB(m); err == nil {
				t.Fatal("decoder accepted an unknown value")
			}
		})
	}
	if _, err := TableRegistrySnapshotFromPB(nil); err == nil {
		t.Fatal("nil snapshot must be refused")
	}
}

func TestTableRegistrySnapshotLiveAndActive(t *testing.T) {
	s := registrySnapshotFixture(t)
	if live := s.Live("db.t"); live == nil || live.Seq != 4 {
		t.Fatalf("Live(db.t) = %+v, want the Pending incarnation 4 (Refused 3 and Purged 2 never shadow it)", live)
	}
	s.Incarnations[3].Status = TableStatusPurged
	if live := s.Live("db.t"); live == nil || live.Seq != 4 {
		t.Fatalf("Live(db.t) with every incarnation terminal = %+v, want the newest (4)", live)
	}
	if live := s.Live("db.none"); live != nil {
		t.Fatalf("Live(db.none) = %+v, want nil", live)
	}
	active := s.ActiveTables()
	if len(active) != 1 || active[0].Key() != "db.g" {
		t.Fatalf("ActiveTables() = %+v", active)
	}
}

func TestTableIncarnationSchema(t *testing.T) {
	s := registrySnapshotFixture(t)
	schema, err := s.Incarnations[3].Schema()
	if err != nil {
		t.Fatal(err)
	}
	if schema.TableID != "db.t" || schema.PartitionBy != "p" || len(schema.Columns) != 2 {
		t.Fatalf("schema = %+v", schema)
	}
	if payloadexec.TableSchemaHash("net", schema) != s.Incarnations[3].SchemaHash {
		t.Fatal("decoded schema must hash to the registry hash")
	}
	if _, err := s.Incarnations[0].Schema(); err == nil || !strings.Contains(err.Error(), "no schema_json") {
		t.Fatalf("genesis incarnation schema err = %v", err)
	}
	bad := s.Incarnations[3]
	bad.TableID = "other"
	if _, err := bad.Schema(); err == nil {
		t.Fatal("schema_json naming another table must be refused")
	}
}
