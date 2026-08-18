package verifier

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

func newRoleHarnessV(t *testing.T, core *fakeReplayCore, scanner *fakeScanner) (*Role, *verifierFakeServer) {
	t.Helper()
	server := newVerifierFakeServer()
	addr := startVerifierFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	role, err := New(testConfigV(), Deps{Client: client, Replay: core, Scanner: scanner})
	if err != nil {
		t.Fatalf("new role: %v", err)
	}
	return role, server
}

func TestNew_AssertsSchemaRoot(t *testing.T) {
	// Given
	cfg := testConfigV()
	cfg.SchemaRoot = "0xwrong"
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	defer client.Close()

	// When
	_, err = New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}})

	// Then
	if err == nil || !strings.Contains(err.Error(), "schema_root") {
		t.Fatalf("schema_root mismatch must fail, got %v", err)
	}
}

func TestConfigRejectsPhysicalTableNameCollision(t *testing.T) {
	cfg := testConfigV()
	cfg.Tables = []payloadexec.TableSchema{
		{TableID: "a.b__c"},
		{TableID: "a__b.c"},
	}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "physical table name collision") {
		t.Fatalf("colliding schema set must fail, got %v", err)
	}
}

func TestRegister_SendsVerifierRegistration(t *testing.T) {
	// Given
	role, server := newRoleHarnessV(t, &fakeReplayCore{}, &fakeScanner{})
	pub := ed25519.NewKeyFromSeed(testSeedV()).Public().(ed25519.PublicKey)

	// When
	if err := role.Register(context.Background()); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Then
	regs, active, _, _ := server.snapshot()
	if len(regs) != 1 {
		t.Fatalf("registrations: %+v", regs)
	}
	if regs[0].GetNodeId() != "v1" || !reflect.DeepEqual(regs[0].GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_VERIFIER}) ||
		!reflect.DeepEqual(regs[0].GetEd25519Pubkey(), []byte(pub)) {
		t.Fatalf("registration: %+v", regs[0])
	}
	if !reflect.DeepEqual(active, []string{"v1"}) {
		t.Fatalf("active calls: %+v", active)
	}
}

func TestRun_ReplayJobFlowsToSubmitAttestation(t *testing.T) {
	// Given
	want := replay.ReplayJob{BlockSeq: 9, PrevSafeSnapshotID: "snap-8", PrevStateRoot: "0xprev", SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0", SourceClaimRoot: "0xsource"}
	att := replay.ReplayAttestation{ReplicaID: "v1", ReceiptHash: "0xreceipt", Signature: "sig"}
	core := &fakeReplayCore{results: []replay.ReplayAttestation{att}}
	role, server := newRoleHarnessV(t, core, &fakeScanner{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()

	// When
	server.push(wire.ReplayJobDispatch(want))

	// Then
	waitVerifier(t, "attestation submission", func() bool {
		_, _, atts, _ := server.snapshot()
		return len(atts) == 1
	})
	jobs := core.jobsSnapshot()
	if len(jobs) != 1 || !reflect.DeepEqual(jobs[0], want) {
		t.Fatalf("replay jobs: %+v", jobs)
	}
	_, _, atts, _ := server.snapshot()
	if got := wire.AttestationFromPB(atts[0]); !reflect.DeepEqual(got, att) {
		t.Fatalf("attestation: got %+v want %+v", got, att)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run exit: %v", err)
	}
}

func TestRun_ReplayCoreErrorMeansNoAttestationAndLoopSurvives(t *testing.T) {
	// Given
	core := &fakeReplayCore{
		errs:    []error{errors.New("boom")},
		results: []replay.ReplayAttestation{{ReplicaID: "v1", ReceiptHash: "0xreceipt2", Signature: "sig2"}},
	}
	role, server := newRoleHarnessV(t, core, &fakeScanner{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()

	// When
	server.push(wire.ReplayJobDispatch(replay.ReplayJob{BlockSeq: 1}))
	server.push(wire.ReplayJobDispatch(replay.ReplayJob{BlockSeq: 2}))

	// Then
	waitVerifier(t, "second replay job", func() bool { return core.jobCount() == 2 })
	waitVerifier(t, "single attestation submission", func() bool {
		_, _, atts, _ := server.snapshot()
		return len(atts) == 1
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run exit: %v", err)
	}
}

func TestRun_ScanRequestFlowsToSubmitByteSideScan(t *testing.T) {
	// Given
	scan := arbiter.PartScan{TableID: "db.t", PartitionID: "202607", ClaimedPartRowLtHash: "0xclaimed", ScannedPartRowLtHash: "0xscanned", LivePartName: "all_1_1_0"}
	scanner := &fakeScanner{results: [][]arbiter.PartScan{{scan}}}
	role, server := newRoleHarnessV(t, &fakeReplayCore{}, scanner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()

	// When
	server.push(wire.ByteSideScanDispatch(11, []arbiter.PartRef{{TableID: "db.t", PartitionID: "202607", PartRowLtHash: "0xclaimed", PartName: "all_1_1_0"}}))

	// Then
	waitVerifier(t, "byte-side scan submission", func() bool {
		_, _, _, scans := server.snapshot()
		return len(scans) == 1
	})
	_, _, _, scans := server.snapshot()
	got := wire.ScanFromPB(scans[0])
	wantHash, err := replay.CanonicalDigest(arbiter.DomainByteSideScan, got.Body())
	if err != nil {
		t.Fatalf("scan hash: %v", err)
	}
	if got.ScanHash != wantHash || got.ReplicaID != "v1" || got.BlockSeq != 11 || !reflect.DeepEqual(got.Parts, []arbiter.PartScan{scan}) {
		t.Fatalf("scan message: %+v want_hash=%s", got, wantHash)
	}
	sig, err := hex.DecodeString(got.Signature)
	if err != nil {
		t.Fatalf("signature hex: %v", err)
	}
	pub := ed25519.NewKeyFromSeed(testSeedV()).Public().(ed25519.PublicKey)
	if !ed25519.Verify(pub, []byte(got.ScanHash), sig) {
		t.Fatal("scan signature must verify over scan hash string bytes")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run exit: %v", err)
	}
}

func TestRun_ScannerErrorMeansNoScanSubmissionAndLoopSurvives(t *testing.T) {
	// Given
	scanner := &fakeScanner{
		errs:    []error{errors.New("scan failed")},
		results: [][]arbiter.PartScan{{{TableID: "db.t", PartitionID: "202607", ClaimedPartRowLtHash: "0xclaimed", ScannedPartRowLtHash: "0xscanned"}}},
	}
	role, server := newRoleHarnessV(t, &fakeReplayCore{}, scanner)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()
	req := []arbiter.PartRef{{TableID: "db.t", PartitionID: "202607", PartRowLtHash: "0xclaimed"}}

	// When
	server.push(wire.ByteSideScanDispatch(1, req))
	server.push(wire.ByteSideScanDispatch(2, req))

	// Then
	waitVerifier(t, "second scan request", func() bool { return scanner.callCount() == 2 })
	waitVerifier(t, "single scan submission", func() bool {
		_, _, _, scans := server.snapshot()
		return len(scans) == 1
	})
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run exit: %v", err)
	}
}

func TestNew_DefaultsUnsafeDatabase(t *testing.T) {
	cfg := testConfigV()
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	defer client.Close()
	role, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}})
	if err != nil {
		t.Fatalf("new role: %v", err)
	}
	if role.cfg.UnsafeDatabase != "hg_unsafe" {
		t.Fatalf("unsafe database default: %q", role.cfg.UnsafeDatabase)
	}
}

func TestNew_RequiresTables(t *testing.T) {
	cfg := testConfigV()
	cfg.Tables = nil
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, nil)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	defer client.Close()
	_, err = New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}})
	if err == nil || !strings.Contains(err.Error(), "table schema") {
		t.Fatalf("missing tables must fail, got %v", err)
	}
}
