package arbiter

import (
	"encoding/json"
	"testing"
)

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

func TestStatementIDLaneRendering(t *testing.T) {
	legacy := StatementID{ClientAccount: "0xAbC", ClientSeq: 42, ClientNonce: "9f1c"}
	if got := legacy.Flat(); got != "0xabc:42:9f1c" {
		t.Fatalf("legacy Flat = %q", got)
	}
	laned := legacy
	laned.ClientLane = "5e1f0a2b7c9d3e4f"
	if got := laned.Flat(); got != "0xabc:5e1f0a2b7c9d3e4f:42:9f1c" {
		t.Fatalf("laned Flat = %q", got)
	}
	if c := legacy.Coord(); c != (StatementCoord{Account: "0xabc", ClientSeq: 42}) || c.Subject() != "0xabc" {
		t.Fatalf("legacy coord %+v subject %q", c, c.Subject())
	}
	if c := laned.Coord(); c != (StatementCoord{Account: "0xabc", Lane: "5e1f0a2b7c9d3e4f", ClientSeq: 42}) || c.Subject() != "0xabc:5e1f0a2b7c9d3e4f" {
		t.Fatalf("laned coord %+v subject %q", c, c.Subject())
	}
	if got := StatementIDStringWithLane("0xABC", "", 1, "n"); got != StatementIDString("0xABC", 1, "n") {
		t.Fatalf("empty lane must render the legacy form, got %q", got)
	}
}

func TestStatementIDJSONOmitsEmptyLane(t *testing.T) {
	b, err := json.Marshal(StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n"})
	if err != nil || string(b) != `{"client_account":"0xabc","client_seq":1,"client_nonce":"n"}` {
		t.Fatalf("legacy JSON = %s (%v): an empty lane must not change any canonical preimage", b, err)
	}
	b, _ = json.Marshal(StatementID{ClientAccount: "0xabc", ClientSeq: 1, ClientNonce: "n", ClientLane: "00000000000000ff"})
	if string(b) != `{"client_account":"0xabc","client_seq":1,"client_nonce":"n","client_lane":"00000000000000ff"}` {
		t.Fatalf("laned JSON = %s", b)
	}
	b, _ = json.Marshal(StatementCoord{Account: "0xabc", ClientSeq: 1})
	if string(b) != `{"account":"0xabc","client_seq":1}` {
		t.Fatalf("legacy coord JSON = %s", b)
	}
}

func TestValidClientLane(t *testing.T) {
	for lane, want := range map[string]bool{
		"5e1f0a2b7c9d3e4f": true, "0000000000000000": true, "ffffffffffffffff": true,
		"": false, "5e1f0a2b7c9d3e4": false, "5e1f0a2b7c9d3e4f0": false,
		"5E1F0A2B7C9D3E4F": false, "5e1f0a2b7c9d3e4g": false, "5e1f0a2b:c9d3e4f": false, " 5e1f0a2b7c9d3e4": false,
	} {
		if got := ValidClientLane(lane); got != want {
			t.Errorf("ValidClientLane(%q) = %v, want %v", lane, got, want)
		}
	}
}
