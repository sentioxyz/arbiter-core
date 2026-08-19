package snode

import (
	"testing"

	"github.com/ClickHouse/ch-go/proto"
)

const testRevision = 54460

// nativePayload encodes one client Data packet with columns (p String,
// v UInt64) — the wire bytes an agent-side housegate would have signed.
func nativePayload(t *testing.T, rows ...struct {
	P string
	V uint64
}) []byte {
	t.Helper()
	p := proto.ColStr{}
	v := proto.ColUInt64{}
	for _, r := range rows {
		p.Append(r.P)
		v.Append(r.V)
	}
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	if err := (proto.Block{Rows: len(rows), Columns: 2}).EncodeBlock(&buf, testRevision, proto.Input{{Name: "p", Data: &p}, {Name: "v", Data: &v}}); err != nil {
		t.Fatalf("encode native payload: %v", err)
	}
	return append([]byte(nil), buf.Buf...)
}

type pv = struct {
	P string
	V uint64
}
