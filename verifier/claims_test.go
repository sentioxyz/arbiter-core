package verifier

import (
	"context"
	"crypto/ed25519"
	"net"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// membershipFakeV records RegisterNode and MarkActive requests.
type membershipFakeV struct {
	pb.UnimplementedMembershipServer
	mu    sync.Mutex
	regs  []*pb.NodeRegistration
	marks []*pb.NodeRef
}

func (f *membershipFakeV) RegisterNode(_ context.Context, m *pb.NodeRegistration) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.regs = append(f.regs, m)
	return &pb.Ack{}, nil
}

func (f *membershipFakeV) MarkActive(_ context.Context, m *pb.NodeRef) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, m)
	return &pb.Ack{}, nil
}

func (f *membershipFakeV) snapshot() ([]*pb.NodeRegistration, []*pb.NodeRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.regs), slices.Clone(f.marks)
}

func startMembershipFakeV(t *testing.T, f *membershipFakeV) *dataplane.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterMembershipServer(srv, f)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

const fixtureMillisV = int64(1_760_054_400_000) // authoritytest.Iat in ms

// signingConfigV is verifier 1 of the authoritytest fixtures.
func signingConfigV(t *testing.T) Config {
	cfg := testConfigV()
	cfg.ReplicaID = authoritytest.VerifierNodeID(1)
	cfg.Ed25519Seed = authoritytest.VerifierKey(1).Seed()
	cfg.NetworkID = authoritytest.NetworkID
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.GenesisSnapshotID = authoritytest.GenesisSnapshotID
	cfg.StateDir = t.TempDir()
	return cfg
}

func newSigningVerifier(t *testing.T, cfg Config, client *dataplane.Client, nowMs *int64) *Role {
	t.Helper()
	role, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{},
		Now: func() time.Time { return time.UnixMilli(*nowMs) }})
	if err != nil {
		t.Fatal(err)
	}
	return role
}

func TestRegister_VerifierSignsWithItsEvidenceKey(t *testing.T) {
	fake := &membershipFakeV{}
	now := fixtureMillisV
	role := newSigningVerifier(t, signingConfigV(t), startMembershipFakeV(t, fake), &now)
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.snapshot()
	if len(regs) != 1 || len(marks) != 1 {
		t.Fatalf("%d registrations, %d activations", len(regs), len(marks))
	}
	reg, mark := regs[0], marks[0]
	// ed25519 is deterministic: the requests reproduce the pinned vectors.
	if reg.GetRegistrationSeq() != authoritytest.RegistrationSeq || mark.GetRegistrationSeq() != authoritytest.RegistrationSeq ||
		reg.GetEd25519Signature() != authoritytest.VerifierRegistrationSignature || mark.GetEd25519Signature() != authoritytest.VerifierMarkActiveSignature {
		t.Fatalf("registration %v / activation %v differ from the authoritytest vectors", reg, mark)
	}
	pub := authoritytest.VerifierKey(1).Public().(ed25519.PublicKey)
	if err := authority.VerifyVerifierMessage(pub, authority.VerifierMessageRegistration, authoritytest.Context(),
		wire.RegistrationFromPB(reg), reg.GetEd25519Signature()); err != nil {
		t.Fatal(err)
	}
	if reg.GetSignerJws() != "" || !slices.Contains(reg.GetFeatures(), arbiter.SignedClaimsFeature) {
		t.Fatalf("registration = %v", reg)
	}
}

// TestRegister_VerifierSeqStaysAboveAfterTheStateFileIsLost is the amended
// rule (CONTRACT §3a) for the verifier's registration.json.
func TestRegister_VerifierSeqStaysAboveAfterTheStateFileIsLost(t *testing.T) {
	fake := &membershipFakeV{}
	client := startMembershipFakeV(t, fake)
	cfg := signingConfigV(t)
	now := fixtureMillisV
	if err := newSigningVerifier(t, cfg, client, &now).Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(cfg.StateDir, "registration.json")); err != nil {
		t.Fatal(err)
	}
	now = fixtureMillisV + 5
	if err := newSigningVerifier(t, cfg, client, &now).Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.snapshot()
	if len(regs) != 2 || regs[1].GetRegistrationSeq() != uint64(fixtureMillisV+5) || marks[1].GetRegistrationSeq() != regs[1].GetRegistrationSeq() {
		t.Fatalf("seqs = %v / %v: the second registration must carry a larger seq", regs, marks)
	}
}

func TestRegister_VerifierSeqNeverGoesBack(t *testing.T) {
	for _, stateDir := range []bool{true, false} {
		fake := &membershipFakeV{}
		cfg := signingConfigV(t)
		if !stateDir {
			cfg.StateDir = "" // in memory, still clock-floored
		}
		now := fixtureMillisV
		role := newSigningVerifier(t, cfg, startMembershipFakeV(t, fake), &now)
		if err := role.Register(context.Background()); err != nil {
			t.Fatal(err)
		}
		now = fixtureMillisV - 60_000
		if err := role.Register(context.Background()); err != nil {
			t.Fatal(err)
		}
		if regs, _ := fake.snapshot(); len(regs) != 2 || regs[1].GetRegistrationSeq() != uint64(fixtureMillisV)+1 {
			t.Fatalf("state dir %v: seqs = %v, want persisted+1", stateDir, regs)
		}
	}
}

func TestVerifierPurgeReportIsSigned(t *testing.T) {
	now := fixtureMillisV
	role := newSigningVerifier(t, signingConfigV(t), startMembershipFakeV(t, &membershipFakeV{}), &now)
	report, err := role.signTablePurged(authoritytest.VerifierNodeID(1), 5)
	if err != nil || report != (wire.RecordTablePurged{NodeID: authoritytest.VerifierNodeID(1), IncarnationSeq: 5, Ed25519Signature: authoritytest.VerifierTablePurgedSignature}) {
		t.Fatalf("report = %+v, %v: want the authoritytest vector", report, err)
	}
}

func TestNew_VerifierDerivesTheGenesisSnapshotID(t *testing.T) {
	cfg := signingConfigV(t)
	cfg.GenesisSnapshotID = ""
	now := fixtureMillisV
	role := newSigningVerifier(t, cfg, startMembershipFakeV(t, &membershipFakeV{}), &now)
	want, err := dataplane.GenesisSnapshotID(cfg.NetworkID, cfg.SchemaSnapshotID, cfg.ExecutorProfileID, cfg.Tables)
	if err != nil || role.genesisID != want {
		t.Fatalf("genesis id = %q, want %q (%v)", role.genesisID, want, err)
	}
}
