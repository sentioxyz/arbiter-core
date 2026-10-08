package wire

import (
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// fsmScanHash is the Arbiter FSM's recompute in applyRecordByteSideScan: the
// canonical digest of the PB-decoded scan's body.
func fsmScanHash(t *testing.T, decoded arbiter.ByteSideScanMsg) string {
	t.Helper()
	h, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, decoded.Body())
	if err != nil {
		t.Fatalf("fsm recompute: %v", err)
	}
	return h
}

// signScan signs a scan exactly as verifier.handleScanRequest does.
func signScan(t *testing.T, priv ed25519.PrivateKey, msg arbiter.ByteSideScanMsg) arbiter.ByteSideScanMsg {
	t.Helper()
	h, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, msg.Body())
	if err != nil {
		t.Fatalf("scan hash: %v", err)
	}
	msg.ScanHash = h
	msg.Signature = hex.EncodeToString(ed25519.Sign(priv, []byte(h)))
	return msg
}

// decodeAsFSM carries a scan the way it reaches the FSM: ScanToPB on the
// verifier, gRPC bytes to the gateway, then a Raft command the FSM decodes.
func decodeAsFSM(t *testing.T, msg arbiter.ByteSideScanMsg) arbiter.ByteSideScanMsg {
	t.Helper()
	b, err := proto.Marshal(ScanToPB(msg))
	if err != nil {
		t.Fatalf("marshal scan: %v", err)
	}
	onWire := &pb.ByteSideScanMsg{}
	if err := proto.Unmarshal(b, onWire); err != nil {
		t.Fatalf("unmarshal scan: %v", err)
	}
	raft, err := Encode(Command{RecordByteSideScan: &RecordByteSideScan{Scan: ScanFromPB(onWire)}})
	if err != nil {
		t.Fatalf("encode raft command: %v", err)
	}
	cmd, err := Decode(raft)
	if err != nil {
		t.Fatalf("decode raft command: %v", err)
	}
	if cmd.RecordByteSideScan == nil {
		t.Fatalf("decoded command %+v is not a byte-side scan", cmd)
	}
	return cmd.RecordByteSideScan.Scan
}

// The empty scan of a table-set transition block, built the way
// CHScanner.Scan builds it (a non-nil, zero-length slice), must verify after
// the wire round trip: its signed scan_hash equals the FSM's recompute over
// the decoded message, whose Parts is nil. Before the Body() normalization
// the verifier signed "parts":[] and the FSM recomputed "parts":null
// (devnet2 L3 block 15).
func TestByteSideScan_EmptyScanVerifiesAfterWireRoundTrip(t *testing.T) {
	priv := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	for _, rid := range []string{"verifier-1", "verifier-2", "verifier-3"} {
		signed := signScan(t, priv, arbiter.ByteSideScanMsg{ReplicaID: rid, BlockSeq: 15, Parts: make([]arbiter.PartScan, 0, 0)})
		decoded := decodeAsFSM(t, signed)
		if decoded.Parts != nil {
			t.Fatalf("%s: decoded parts = %#v, want nil (the frozen wire normalization)", rid, decoded.Parts)
		}
		if got := fsmScanHash(t, decoded); got != signed.ScanHash {
			t.Fatalf("%s: FSM recompute %s != signed scan_hash %s", rid, got, signed.ScanHash)
		}
		sig, err := hex.DecodeString(decoded.Signature)
		if err != nil || !ed25519.Verify(priv.Public().(ed25519.PublicKey), []byte(decoded.ScanHash), sig) {
			t.Fatalf("%s: signature does not verify over the decoded scan_hash", rid)
		}
	}
}

// Golden pins computed with arbiter-core v0.10.1 (before the Body()
// normalization). They prove the fix changes no hash the v0.8.1 FSM computes
// for a decoded scan: decoded scans carry either nil Parts or at least one
// part, and both forms hash exactly as before.
func TestByteSideScan_HashGoldenUnchanged(t *testing.T) {
	// nil Parts: the FSM's recompute for devnet2 block 15, matching the
	// "recomputed(nil parts)" values decoded from the devnet2 leader Raft log.
	nilForm := map[string]string{
		"verifier-1": "0xea13acfa24fc517157f6419bf93cb22e347b12af3107261b8244618faf9a8e9a",
		"verifier-2": "0xc172fadb9ae7f81dd1a584fce97465c7208a2efa825a0ab1171560032056968b",
		"verifier-3": "0x2fbdf80761142d6eae98bac4f11387702ded81a1cf8258723f3245670be137b7",
	}
	for rid, want := range nilForm {
		for name, parts := range map[string][]arbiter.PartScan{"nil": nil, "empty": {}} {
			msg := arbiter.ByteSideScanMsg{ReplicaID: rid, BlockSeq: 15, Parts: parts}
			if got := fsmScanHash(t, msg); got != want {
				t.Fatalf("%s %s parts: hash %s, want the FSM's nil-form %s", rid, name, got, want)
			}
		}
	}

	nonEmpty := arbiter.ByteSideScanMsg{ReplicaID: "verifier-1", BlockSeq: 14, Parts: []arbiter.PartScan{
		{TableID: "devnet101.swap_new2", PartitionID: "202610", ClaimedPartRowLtHash: "0xaa", ScannedPartRowLtHash: "0xaa", LivePartName: "202610_3_3_0"},
		{TableID: "devnet101.swap_new2", PartitionID: "202611", ClaimedPartRowLtHash: "0xbb", ScannedPartRowLtHash: "0xbc"},
	}}
	const wantNonEmpty = "0x7cfc91caab81f80c1b8408bdfd80774a45cd157d436e64bcc29ce981c24d3f87"
	if got := fsmScanHash(t, nonEmpty); got != wantNonEmpty {
		t.Fatalf("non-empty scan hash %s, want pinned v0.10.1 value %s", got, wantNonEmpty)
	}
	if got := fsmScanHash(t, decodeAsFSM(t, nonEmpty)); got != wantNonEmpty {
		t.Fatalf("decoded non-empty scan hash %s, want pinned v0.10.1 value %s", got, wantNonEmpty)
	}
}
