package arbiter

import "testing"

func TestStatementIDString(t *testing.T) {
	got := StatementIDString("0xAbCd000000000000000000000000000000000001", 42, "n-7")
	want := "0xabcd000000000000000000000000000000000001:42:n-7"
	if got != want {
		t.Fatalf("StatementIDString: got %q want %q", got, want)
	}
}

func TestStatementIDFlatAndCoord(t *testing.T) {
	id := StatementID{ClientAccount: "0xABcD", ClientSeq: 7, ClientNonce: "n1"}
	if got, want := id.Flat(), "0xabcd:7:n1"; got != want {
		t.Fatalf("Flat: got %q want %q", got, want)
	}
	if c := id.Coord(); c.Account != "0xabcd" || c.ClientSeq != 7 {
		t.Fatalf("Coord: got %+v", c)
	}
}

func TestByteSideScanBodyExcludesHashAndSignature(t *testing.T) {
	m := ByteSideScanMsg{ReplicaID: "r1", BlockSeq: 3, Parts: []PartScan{{TableID: "db.t"}}, ScanHash: "0xdead", Signature: "beef"}
	b := m.Body()
	if b.ReplicaID != "r1" || b.BlockSeq != 3 || len(b.Parts) != 1 {
		t.Fatalf("Body: got %+v", b)
	}
}

// An empty scan hashes in the form it has after the wire boundary (nil), not
// as a non-nil empty slice; a non-empty scan keeps its parts untouched.
func TestByteSideScanBodyCanonicalizesEmptyParts(t *testing.T) {
	if b := (ByteSideScanMsg{ReplicaID: "r1", BlockSeq: 15, Parts: []PartScan{}}).Body(); b.Parts != nil {
		t.Fatalf("empty parts body = %#v, want nil parts", b.Parts)
	}
	if b := (ByteSideScanMsg{ReplicaID: "r1", BlockSeq: 15}).Body(); b.Parts != nil {
		t.Fatalf("nil parts body = %#v, want nil parts", b.Parts)
	}
	parts := []PartScan{{TableID: "db.t", PartitionID: "p"}}
	if b := (ByteSideScanMsg{Parts: parts}).Body(); len(b.Parts) != 1 || &b.Parts[0] != &parts[0] {
		t.Fatalf("non-empty parts body = %#v, want the message's parts", b.Parts)
	}
}

func TestTablePartitionTextRoundTrip(t *testing.T) {
	in := TablePartition{TableID: "db.table", PartitionID: "2026-07"}
	b, err := in.MarshalText()
	if err != nil {
		t.Fatalf("MarshalText: %v", err)
	}
	var out TablePartition
	if err := out.UnmarshalText(b); err != nil {
		t.Fatalf("UnmarshalText: %v", err)
	}
	if out != in {
		t.Fatalf("round trip: got %+v want %+v", out, in)
	}
	if err := out.UnmarshalText([]byte("no-delimiter")); err == nil {
		t.Fatal("missing NUL must error")
	}
}
