package wire

import (
	"reflect"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

func TestEnvelopeV2RoundTripsEveryField(t *testing.T) {
	in := arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: "0xabc", ClientSeq: 7, ClientNonce: "n7"},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             "INSERT INTO db.t FORMAT Native",
		SQLHash:         "0x11",
		SettingsHash:    "0x22",
		PayloadRef:      "ref",
		PayloadHash:     "0x33",
		PayloadLength:   9,
		TargetTableID:   "db.t",
		UserJWS:         "a.b.c",
		EnvelopeVersion: 2,
		NetworkID:       "net",
		KeeperShardID:   0,
		PayloadFormat:   "clickhouse-native-data-v1",
		ClientRevision:  54460,
		SchemaHash:      "0x44",
		RowIDProfileID:  "housegate-row-id-v1",
	}
	out := EnvelopeFromPB(EnvelopeToPB(in))
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip lost fields:\n in=%+v\nout=%+v", in, out)
	}
	st := replay.Statement{StatementID: "0xabc:7:n7", StatementSeq: 3, SQL: in.SQL, SQLHash: in.SQLHash, SettingsHash: in.SettingsHash, PayloadRef: "ref", PayloadHash: "0x33", PayloadLength: 9, TargetTableID: "db.t", UserJWS: "a.b.c", PayloadFormat: "clickhouse-native-data-v1", ClientRevision: 54460, SchemaHash: "0x44"}
	if got := StatementFromPB(statementToPB(st)); !reflect.DeepEqual(st, got) {
		t.Fatalf("replay statement round trip lost fields:\n in=%+v\nout=%+v", st, got)
	}
}
