package verifier

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

func newRoleHarnessWithScanner(t *testing.T, sc scanner, logger *slog.Logger) (*Role, *verifierFakeServer) {
	t.Helper()
	server := newVerifierFakeServer()
	addr := startVerifierFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	role, err := New(testConfigV(), Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: sc, Logger: logger})
	if err != nil {
		t.Fatalf("new role: %v", err)
	}
	return role, server
}

// The empty scan of a table-set transition block, produced by the real
// CHScanner (a non-nil empty slice), must reach the arbiter with a scan_hash
// that the FSM's recompute over the PB-decoded message reproduces (devnet2
// L3 block 15). The pre-fix verifier signed "parts":[] while the FSM
// recomputed "parts":null and rejected every submission.
func TestRun_EmptyTransitionScanHashMatchesFSMRecompute(t *testing.T) {
	// Given
	role, server := newRoleHarnessWithScanner(t, NewScanner(testConfigV(), nil), nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()

	// When
	server.push(wire.ByteSideScanDispatch(15, nil))

	// Then
	waitVerifier(t, "empty byte-side scan submission", func() bool {
		_, _, _, scans := server.snapshot()
		return len(scans) == 1
	})
	_, _, _, scans := server.snapshot()
	decoded := wire.ScanFromPB(scans[0])
	fsmHash, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, decoded.Body())
	if err != nil {
		t.Fatalf("fsm recompute: %v", err)
	}
	if decoded.Parts != nil || decoded.BlockSeq != 15 || decoded.ReplicaID != "v1" || decoded.ScanHash != fsmHash {
		t.Fatalf("empty scan %+v: FSM recompute %s", decoded, fsmHash)
	}
	sig, err := hex.DecodeString(decoded.Signature)
	if err != nil {
		t.Fatalf("signature hex: %v", err)
	}
	pub := ed25519.NewKeyFromSeed(testSeedV()).Public().(ed25519.PublicKey)
	if !ed25519.Verify(pub, []byte(decoded.ScanHash), sig) {
		t.Fatal("empty scan signature must verify over the FSM-recomputed scan hash")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run exit: %v", err)
	}
}

type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// A scan the arbiter rejects must be visible: the gateway logs nothing and
// the subscription loop keeps going, so the verifier warns with the code.
func TestRun_RejectedScanSubmissionIsLoggedAtWarn(t *testing.T) {
	// Given
	var logs syncBuffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelWarn}))
	role, server := newRoleHarnessWithScanner(t, NewScanner(testConfigV(), nil), logger)
	server.mu.Lock()
	server.scanErr = status.Error(codes.InvalidArgument, "scan_hash does not match the scan body")
	server.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()

	// When
	server.push(wire.ByteSideScanDispatch(15, nil))

	// Then
	waitVerifier(t, "rejection warning", func() bool {
		return strings.Contains(logs.String(), "arbiter did not accept verifier evidence")
	})
	for _, want := range []string{"level=WARN", "kind=\"byte-side scan\"", "block=15", "code=InvalidArgument", "scan_hash does not match the scan body"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("warning %q lacks %q", logs.String(), want)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run exit: %v", err)
	}
}
