package dataplane

import (
	"context"
	"errors"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/wire"
)

func TestSubmitTablePurgedReportCarriesSignatures(t *testing.T) {
	r := newFakeRegistry()
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c.SubmitTablePurgedReport(ctx, wire.RecordTablePurged{NodeID: "s1", IncarnationSeq: 4, SignerJWS: "h.p.s"}); err != nil {
		t.Fatal(err)
	}
	signed := PurgeReporter{Client: c, Sign: func(node string, seq uint64) (wire.RecordTablePurged, error) {
		return wire.RecordTablePurged{NodeID: node, IncarnationSeq: seq, Ed25519Signature: "ab"}, nil
	}}
	if err := signed.SubmitTablePurged(ctx, "v1", 9); err != nil {
		t.Fatal(err)
	}
	if err := (PurgeReporter{Client: c}).SubmitTablePurged(ctx, "v2", 9); err != nil {
		t.Fatal(err)
	}
	failing := PurgeReporter{Client: c, Sign: func(string, uint64) (wire.RecordTablePurged, error) {
		return wire.RecordTablePurged{}, errors.New("no key")
	}}
	if err := failing.SubmitTablePurged(ctx, "v1", 9); err == nil {
		t.Fatal("a signing failure must not send an unsigned report")
	}
	lying := PurgeReporter{Client: c, Sign: func(string, uint64) (wire.RecordTablePurged, error) {
		return wire.RecordTablePurged{NodeID: "other", IncarnationSeq: 9}, nil
	}}
	if err := lying.SubmitTablePurged(ctx, "v1", 9); err == nil {
		t.Fatal("a signed report naming another node must be refused")
	}
	r.mu.Lock()
	purges := append([]*pb.RecordTablePurgedCmd(nil), r.purges...)
	r.mu.Unlock()
	if len(purges) != 3 || purges[0].GetSignerJws() != "h.p.s" || purges[1].GetNodeId() != "v1" || purges[1].GetEd25519Signature() != "ab" ||
		purges[2].GetSignerJws() != "" || purges[2].GetEd25519Signature() != "" {
		t.Fatalf("purges = %v", purges)
	}
}

func TestGenesisSnapshotIDIsTheArbitersDerivation(t *testing.T) {
	tables := []payloadexec.TableSchema{{TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}}
	id, err := GenesisSnapshotID("devnet2", "schema-genesis", "housegate-replay-mvp-v0", tables)
	if err != nil || id == "" {
		t.Fatalf("GenesisSnapshotID = %q, %v", id, err)
	}
	// arbiter cmd/arbiter/genesis.go loadGenesisManifest derives exactly this.
	m, err := payloadexec.New("devnet2", tables...).GenesisSnapshot(0, "schema-genesis", "housegate-replay-mvp-v0")
	if err != nil || m.SnapshotID != id {
		t.Fatalf("arbiter derivation = %q (%v), helper = %q", m.SnapshotID, err, id)
	}
	if other, _ := GenesisSnapshotID("devnet2", "schema-genesis", "housegate-replay-mvp-v0", nil); other == id {
		t.Fatal("another genesis table set must derive another id")
	}
}
