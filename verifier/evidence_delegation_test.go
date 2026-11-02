package verifier

import (
	"context"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/evidence"
	"github.com/sentioxyz/arbiter-core/wire"
)

// What this verifier submitted, before dataplane/evidence existed, for the
// job and scan request below as verifier-1 with authoritytest.VerifierKey(1):
// the frozen values of dataplane/evidence's
// TestEvidenceHandlerSubmitsWhatTheVerifierSubmitted, never regenerated.
const (
	frozenAttestationHexV = "0a0a76657269666965722d3112001a0a3078726563656970743922063078736967392801"
	frozenScanHexV        = "0a0a76657269666965722d3110091a340a0464622e7412063230323630371a0a3078636c61696d656431220a3078636c61696d6564312a0c3230323630375f315f315f301a340a0464622e7412063230323630381a0a3078636c61696d656432220a30787363616e6e6564322a0c3230323630385f325f325f3022423078323731336266636230353561366138333637323163386235613066656561333164323864663463336564313933306431336638393363616635656239306634342a80013963316437313533303932366662353665366436333236623561376433623739666530333635616662623863646266623531613333353931623530663335333334346265306661306134313034626666656463333634326237646130376435663933313463383966393437353931336338316534343165373531373830653033"
)

// withEvidenceV gives a Role assembled field by field (newGateRoleV) the
// evidence handler New builds, over an unused client and an empty scanner.
func withEvidenceV(t *testing.T, r *Role) {
	t.Helper()
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	r.cfg.ReplicaID, r.cfg.Ed25519Seed = "v1", testSeedV()
	r.d.Client, r.d.Scanner = client, &fakeScanner{}
	if r.evidence, err = r.newEvidenceHandler(); err != nil {
		t.Fatal(err)
	}
}

func TestVerifierRoleDelegatesToTheEvidenceHandler(t *testing.T) {
	cfg := testConfigV()
	cfg.ReplicaID = authoritytest.VerifierNodeID(1)
	cfg.Ed25519Seed = authoritytest.VerifierKey(1).Seed()
	att := replay.ReplayAttestation{ReplicaID: cfg.ReplicaID, ReceiptHash: "0xreceipt9", Signature: "0xsig9", MatchSourceRoot: true}
	scans := []arbiter.PartScan{
		{TableID: "db.t", PartitionID: "202607", ClaimedPartRowLtHash: "0xclaimed1", ScannedPartRowLtHash: "0xclaimed1", LivePartName: "202607_1_1_0"},
		{TableID: "db.t", PartitionID: "202608", ClaimedPartRowLtHash: "0xclaimed2", ScannedPartRowLtHash: "0xscanned2", LivePartName: "202608_2_2_0"},
	}
	server := newVerifierFakeServer()
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startVerifierFakeServer(t, server)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	role, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{results: []replay.ReplayAttestation{att}},
		Scanner: &fakeScanner{results: [][]arbiter.PartScan{scans}}})
	if err != nil {
		t.Fatal(err)
	}
	if role.evidence == nil {
		t.Fatal("New built no evidence handler")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()
	server.push(wire.ReplayJobDispatch(replay.ReplayJob{BlockSeq: 9, PrevSafeSnapshotID: "snap-8", PrevStateRoot: "0xprev",
		SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0", SourceClaimRoot: "0xsource"}))
	server.push(wire.ByteSideScanDispatch(9, []arbiter.PartRef{
		{TableID: "db.t", PartitionID: "202607", PartRowLtHash: "0xclaimed1", PartName: "202607_1_1_0"},
		{TableID: "db.t", PartitionID: "202608", PartRowLtHash: "0xclaimed2", PartName: "202608_2_2_0"},
	}))
	waitVerifier(t, "evidence submissions", func() bool {
		_, _, atts, scans := server.snapshot()
		return len(atts) == 1 && len(scans) == 1
	})
	_, _, atts, submitted := server.snapshot()
	for _, c := range []struct {
		what string
		msg  proto.Message
		want string
	}{{"attestation", atts[0], frozenAttestationHexV}, {"scan", submitted[0], frozenScanHexV}} {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(c.msg)
		if err != nil {
			t.Fatal(err)
		}
		if got := hex.EncodeToString(b); got != c.want {
			t.Fatalf("%s = %s, want the pre-extraction verifier's %s", c.what, got, c.want)
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("run exit: %v", err)
	}
	// The add gate refuses through the handler, with the verifier's sentinel.
	r, core := newGateRoleV(t, nil, true, &syncBufferV{})
	gateCtx, gateCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer gateCancel()
	if err := r.handleReplayJob(gateCtx, addTransitionJob(7, "db.n")); !errors.Is(err, ErrAddedTableNotReady) || core.jobCount() != 0 {
		t.Fatalf("gate = %v with %d replays, want ErrAddedTableNotReady and none", err, core.jobCount())
	}
}

func TestVerifierRegistrationFileKeepsItsShape(t *testing.T) {
	if evidence.RegistrationStateFile != "registration.json" {
		t.Fatalf("RegistrationStateFile = %q", evidence.RegistrationStateFile)
	}
	fake := &membershipFakeV{}
	client := startMembershipFakeV(t, fake)
	cfg := signingConfigV(t)
	now := fixtureMillisV
	role := newSigningVerifier(t, cfg, client, &now)
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.StateDir, "registration.json")
	if b, err := os.ReadFile(path); err != nil || string(b) != `{"registration_seq":1760054400000}` {
		t.Fatalf("registration.json = %q, %v", b, err)
	}
	// A file a v0.15.0 verifier wrote is read back and stays its floor.
	if err := os.WriteFile(path, []byte(`{"registration_seq":1760054400099}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := newSigningVerifier(t, cfg, client, &now).Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if regs, _ := fake.snapshot(); len(regs) != 2 || regs[1].GetRegistrationSeq() != 1760054400100 {
		t.Fatalf("registrations %v, want the second at the file's seq + 1", regs)
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != `{"registration_seq":1760054400100}` {
		t.Fatalf("registration.json = %q, %v", b, err)
	}
	if entries, err := os.ReadDir(cfg.StateDir); err != nil || len(entries) != 1 {
		t.Fatalf("state dir holds %v (%v), want only registration.json", entries, err)
	}
}

// syncBufferV discards gate logs.
type syncBufferV struct{}

func (syncBufferV) Write(p []byte) (int, error) { return len(p), nil }
