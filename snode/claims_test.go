package snode

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// claimsFakeS records every request an SNode sends to the arbiter.
// failRegistrations makes the first RegisterNode calls answer Unavailable
// after recording them (an uncertain commit).
type claimsFakeS struct {
	pb.UnimplementedMembershipServer
	pb.UnimplementedSourceClaimsServer
	pb.UnimplementedPromotionGatewayServer

	mu                sync.Mutex
	failRegistrations int
	regs              []*pb.NodeRegistration
	marks             []*pb.NodeRef
	claims            []*pb.RCRecord
	acks              []*pb.PromotionAck
	cleanups          []*pb.CleanupAck
}

func (f *claimsFakeS) RegisterNode(_ context.Context, m *pb.NodeRegistration) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.regs = append(f.regs, m)
	if f.failRegistrations > 0 {
		f.failRegistrations--
		return nil, status.Error(codes.Unavailable, "leader lost before the reply")
	}
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) MarkActive(_ context.Context, m *pb.NodeRef) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, m)
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) RegisterResultClaim(_ context.Context, m *pb.RCRecord) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.claims = append(f.claims, m)
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) AckPromotion(_ context.Context, m *pb.PromotionAck) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.acks = append(f.acks, m)
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) AckCleanup(_ context.Context, m *pb.CleanupAck) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cleanups = append(f.cleanups, m)
	return &pb.Ack{}, nil
}

func (f *claimsFakeS) registrations() ([]*pb.NodeRegistration, []*pb.NodeRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.regs), slices.Clone(f.marks)
}

func (f *claimsFakeS) sent() ([]*pb.RCRecord, []*pb.PromotionAck, []*pb.CleanupAck) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.claims), slices.Clone(f.acks), slices.Clone(f.cleanups)
}

func startClaimsFakeS(t *testing.T, f *claimsFakeS) *dataplane.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterMembershipServer(srv, f)
	pb.RegisterSourceClaimsServer(srv, f)
	pb.RegisterPromotionGatewayServer(srv, f)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// fixtureMillis is authoritytest.Iat in milliseconds.
const fixtureMillis = int64(1_760_054_400_000)

func clockS(ms *int64) func() time.Time { return func() time.Time { return time.UnixMilli(*ms) } }

// signingConfigS is testConfigS as indexer 1's SNode in the authoritytest
// context.
func signingConfigS(t *testing.T) Config {
	cfg := testConfigS(t)
	cfg.NodeID = authoritytest.SNodeNodeID1
	cfg.NetworkID = authoritytest.NetworkID
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.IndexerID = 1
	cfg.GenesisSnapshotID = authoritytest.GenesisSnapshotID
	cfg.AuthorityAddresses = []string{authoritytest.AuthorityAddr}
	return cfg
}

// setSnapshot installs s, with owners and si_indexers, as the next version.
func (v *fakeRegistryS) setSnapshot(s wire.TableRegistrySnapshot) {
	v.mu.Lock()
	defer v.mu.Unlock()
	s.Version = v.snap.Version + 1
	v.snap, v.enabled = s, true
	close(v.changed)
	v.changed = make(chan struct{})
}

func TestRegister_SignsRegistrationAndMarkActiveWithTheIndexerKey(t *testing.T) {
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if len(regs) != 1 || len(marks) != 1 {
		t.Fatalf("requests: %d registrations, %d activations", len(regs), len(marks))
	}
	reg, mark := regs[0], marks[0]
	// Same body, seq (the clock floor), context and iat as the pinned vectors.
	if reg.GetRegistrationSeq() != authoritytest.RegistrationSeq || mark.GetRegistrationSeq() != authoritytest.RegistrationSeq ||
		reg.GetSignerJws() != authoritytest.SNodeRegistrationJWS || mark.GetSignerJws() != authoritytest.SNodeMarkActiveJWS {
		t.Fatalf("registration %v / activation %v differ from the authoritytest vectors", reg, mark)
	}
	// The FSM verifies the registration it decodes from the Raft command.
	if err := authority.VerifySNodeMessage(authority.SNodeMessageRegistration, authoritytest.Context(), wire.RegistrationFromPB(reg),
		reg.GetSignerJws(), authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(reg.GetFeatures(), arbiter.SignedClaimsFeature) || reg.GetEd25519Signature() != "" {
		t.Fatalf("features = %v: a signing SNode advertises signed_claims_v1", reg.GetFeatures())
	}
	reopened, err := openStateStore(role.cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if next, err := reopened.NextRegistrationSeq(0); err != nil || next != authoritytest.RegistrationSeq+1 {
		t.Fatalf("the registration_seq was not durable: next = %d, %v", next, err)
	}
}

// TestRegister_SeqStaysAboveAnEarlierRegistrationAfterTheStateFileIsLost is
// the amended rule (CONTRACT §3a): next = max(persisted+1, now in ms), so a
// node whose state.json was lost still registers above its last seq.
func TestRegister_SeqStaysAboveAnEarlierRegistrationAfterTheStateFileIsLost(t *testing.T) {
	fake := &claimsFakeS{}
	client := startClaimsFakeS(t, fake)
	cfg := signingConfigS(t)
	signer := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
	now := fixtureMillis
	first, err := New(cfg, Deps{Client: client, ClaimSigner: signer, Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(cfg.StateDir, "state.json")); err != nil {
		t.Fatal(err)
	}
	now = fixtureMillis + 5 // the restart took five milliseconds
	second, err := New(cfg, Deps{Client: client, ClaimSigner: signer, Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if len(regs) != 2 || regs[0].GetRegistrationSeq() != uint64(fixtureMillis) || regs[1].GetRegistrationSeq() != uint64(fixtureMillis+5) ||
		marks[1].GetRegistrationSeq() != regs[1].GetRegistrationSeq() {
		t.Fatalf("seqs = %v / %v: the second registration must carry a larger seq", regs, marks)
	}
}

func TestRegister_SeqNeverGoesBackWithTheClock(t *testing.T) {
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	now = fixtureMillis - 60_000 // the clock stepped back a minute
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, _ := fake.registrations()
	if len(regs) != 2 || regs[1].GetRegistrationSeq() != uint64(fixtureMillis)+1 {
		t.Fatalf("seqs = %v: want persisted+1 when the clock is behind", regs)
	}
}

// TestRegister_RetriesReuseOneSeqAndSignature documents why the FSM must
// treat an exact duplicate registration as idempotent (see Contract
// conflicts): WithLeaderRetry resends the identical request after an
// uncertain commit.
func TestRegister_RetriesReuseOneSeqAndSignature(t *testing.T) {
	fake := &claimsFakeS{failRegistrations: 1}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if len(regs) != 2 || regs[0].GetRegistrationSeq() != regs[1].GetRegistrationSeq() || regs[0].GetSignerJws() != regs[1].GetSignerJws() ||
		len(marks) != 1 || marks[0].GetRegistrationSeq() != regs[1].GetRegistrationSeq() {
		t.Fatalf("registrations %v, activations %v: one Register call uses one seq", regs, marks)
	}
}

func TestRegister_WithoutAClaimSignerSendsTheLegacyRequests(t *testing.T) {
	fake := &claimsFakeS{}
	role, err := New(testConfigS(t), Deps{Client: startClaimsFakeS(t, fake)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, marks := fake.registrations()
	if regs[0].GetRegistrationSeq() != 0 || regs[0].GetSignerJws() != "" || marks[0].GetRegistrationSeq() != 0 || marks[0].GetSignerJws() != "" ||
		slices.Contains(regs[0].GetFeatures(), arbiter.SignedClaimsFeature) {
		t.Fatalf("legacy SNode sent %v / %v", regs[0], marks[0])
	}
	if _, err := os.Stat(filepath.Join(role.cfg.StateDir, "state.json")); !os.IsNotExist(err) {
		t.Fatalf("a legacy registration must not write state: %v", err)
	}
}

func TestRegisterPreparedClaim_SignsTheResultClaim(t *testing.T) {
	fake := &claimsFakeS{}
	cfg := signingConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{intakeSchema()}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, err := New(cfg, Deps{Client: startClaimsFakeS(t, fake), ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	env := intakeEnvelope([]byte("native"))
	env.NetworkID = cfg.NetworkID
	env.SchemaHash = payloadexec.TableSchemaHash(cfg.NetworkID, intakeSchema())
	rc := arbiter.RCRecord{StatementID: env.StatementID, SourceNode: cfg.NodeID, CandidateParts: []arbiter.CandidatePart{}, SourceClaimRoot: "0xroot"}
	rec := intakeRecord{StatementID: env.StatementID.Flat(), Lifecycle: LifecyclePreparing, Envelope: env,
		PayloadEncoding: env.PayloadFormat, Revision: testRevision, ExpectedRowCount: 1}
	if err := role.journal.save(rec); err != nil {
		t.Fatal(err)
	}
	rec.Lifecycle, rec.RC = LifecycleUnsafeWritten, &rc
	rec.Result = &PreparedLocalResult{StatementID: rec.StatementID, SourceNode: cfg.NodeID, Lifecycle: LifecycleUnsafeWritten}
	if err := role.journal.save(rec); err != nil {
		t.Fatal(err)
	}
	if out, err := role.RegisterPreparedClaim(context.Background(), rec.StatementID); err != nil || out.Category != ClaimAccepted {
		t.Fatalf("register: %+v, %v", out, err)
	}
	claims, _, _ := fake.sent()
	// The FSM verifies the claim it decodes: [] candidate parts decode as nil.
	if len(claims) != 1 || authority.VerifySNodeMessage(authority.SNodeMessageResultClaim, authoritytest.Context(),
		wire.RCFromPB(claims[0]), claims[0].GetSourceJws(), authoritytest.IndexerAddr1) != nil {
		t.Fatalf("claims = %v: want one RC signed by indexer 1's key", claims)
	}
}

func TestPromotionAndCleanupAcksAreSigned(t *testing.T) {
	fake := &claimsFakeS{}
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake), ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	authoritySigner := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	// A base the SNode does not hold: the base-CAS refusal acknowledges
	// Applied:false without touching ClickHouse. It must be signed too, since
	// the FSM consumes a promotion on it (spec §6.6).
	promote := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "all", PromotionSeq: 7, BaseSafeSnapshotID: "snap", BasePartitionRoot: "0xnot-the-local-base"}
	jws, err := authoritySigner.SignPromotion(promote)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handlePromote(context.Background(), wire.PromoteToPB(promote), jws); err != nil {
		t.Fatal(err)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 7}
	cleanupJWS, err := authoritySigner.SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), cleanupJWS); err != nil {
		t.Fatal(err)
	}
	_, acks, cleanups := fake.sent()
	if len(acks) != 1 || acks[0].GetApplied() || len(cleanups) != 1 {
		t.Fatalf("acks %v, cleanups %v", acks, cleanups)
	}
	ctx := authoritytest.Context()
	if err := authority.VerifySNodeMessage(authority.SNodeMessagePromotionAck, ctx, wire.PromotionAckFromPB(acks[0]), acks[0].GetSourceJws(), authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
	if err := authority.VerifySNodeMessage(authority.SNodeMessageCleanupAck, ctx, wire.CleanupAckFromPB(cleanups[0]), cleanups[0].GetSourceJws(), authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
}

func TestTablePurgedReportIsSignedWithTheIndexerKey(t *testing.T) {
	now := fixtureMillis
	r := &Role{cfg: signingConfigS(t), genesisID: authoritytest.GenesisSnapshotID,
		d: Deps{ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)}}
	report, err := r.signTablePurged(authoritytest.SNodeNodeID1, 5)
	if err != nil || report != (wire.RecordTablePurged{NodeID: authoritytest.SNodeNodeID1, IncarnationSeq: 5, SignerJWS: authoritytest.SNodeTablePurgedJWS}) {
		t.Fatalf("report = %+v, %v: want the authoritytest vector", report, err)
	}
	if _, ok := r.purgeArbiter().(dataplane.PurgeReporter); !ok {
		t.Fatal("a signing SNode must report purges through the signing reporter")
	}
	r.d.ClaimSigner = nil
	if _, ok := r.purgeArbiter().(*dataplane.Client); !ok {
		t.Fatal("a legacy SNode keeps reporting through the plain client")
	}
}

func TestNew_SigningSNodeGenesisSnapshotID(t *testing.T) {
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	signer := authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)
	derived := signingConfigS(t)
	derived.GenesisSnapshotID = ""
	role, err := New(derived, Deps{Client: client, ClaimSigner: signer})
	if err != nil {
		t.Fatal(err)
	}
	want, err := dataplane.GenesisSnapshotID(derived.NetworkID, derived.SchemaSnapshotID, derived.ExecutorProfileID, derived.Tables)
	if err != nil || role.genesisID != want {
		t.Fatalf("genesis id = %q, want the arbiter's derivation %q (%v)", role.genesisID, want, err)
	}
	// An SNode owning no genesis table (a non-founding indexer) may run with
	// an empty genesis set only as a signing registry follower, and must be
	// told the genesis snapshot id.
	empty := signingConfigS(t)
	empty.Tables, empty.SchemaRoot = nil, payloadexec.SchemaRoot(empty.NetworkID, nil)
	empty.SchemaSource = ddl.SchemaSourceNetworkState
	follower := Deps{Client: client, Conn: nopConnS{}, Registry: newFakeRegistryS(), ClaimSigner: signer}
	if _, err := New(empty, follower); err != nil {
		t.Fatalf("a signing registry follower may own no genesis table: %v", err)
	}
	for name, d := range map[string]Deps{
		"no claim signer": {Client: client, Conn: nopConnS{}, Registry: newFakeRegistryS()},
		"no registry":     {Client: client, ClaimSigner: signer},
	} {
		if _, err := New(empty, d); err == nil || !strings.Contains(err.Error(), "at least one table schema") {
			t.Fatalf("%s: err = %v", name, err)
		}
	}
	noID := empty
	noID.GenesisSnapshotID = ""
	if _, err := New(noID, follower); err == nil || !strings.Contains(err.Error(), "genesis snapshot id") {
		t.Fatalf("missing genesis snapshot id: err = %v", err)
	}
}

// ownedSnapshotS: db.t (genesis) and db.theirs belong to indexer 0, db.mine
// to indexer 1, after the signed-claims activation.
func ownedSnapshotS(t *testing.T, cfg Config) wire.TableRegistrySnapshot {
	zero, one := uint64(0), uint64(1)
	genesis := genesisIncarnationS(1, cfg.Tables[0])
	genesis.OwnerIndexerID = &zero
	mine := chainIncarnationS(t, 2, chainSchemaS("db.mine"), wire.TableStatusActive)
	mine.OwnerIndexerID = &one
	theirs := chainIncarnationS(t, 3, chainSchemaS("db.theirs"), wire.TableStatusActive)
	theirs.OwnerIndexerID = &zero
	return wire.TableRegistrySnapshot{Seeded: true, SeededIndexers: []uint64{0},
		SIIndexers:   []arbiter.SIIndexerEntry{authoritytest.SIIndexerEntry0(), authoritytest.SIIndexerEntry1(200)},
		Incarnations: []wire.TableIncarnation{genesis, mine, theirs}}
}

func TestOwnerScopedReads(t *testing.T) {
	cfg := signingConfigS(t)
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	view := newFakeRegistryS()
	view.setSnapshot(ownedSnapshotS(t, cfg))
	r := &Role{cfg: cfg, state: st, journal: journal, d: Deps{Registry: view, ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)}}

	hash := payloadexec.TableSchemaHash("testnet", chainSchemaS("db.mine"))
	_, want, err := replay.AssembleStateRoot(cfg.SchemaSnapshotID, payloadexec.SchemaRootFromHashes(map[string]string{"db.mine": hash}),
		cfg.ExecutorProfileID, []replay.TableManifest{{TableID: "db.mine", SchemaHash: hash}})
	if err != nil {
		t.Fatal(err)
	}
	owned, err := r.sourceClaimRoot()
	if err != nil || owned != want {
		t.Fatalf("source claim root = %s, want %s over db.mine only (%v)", owned, want, err)
	}
	theirsHash := payloadexec.TableSchemaHash("testnet", chainSchemaS("db.theirs"))
	for name, err := range map[string]error{
		"fresh intake":         r.requireAdmissible("db.theirs", theirsHash),
		"genesis of indexer 0": r.requireAdmissible("db.t", payloadexec.TableSchemaHash("testnet", cfg.Tables[0])),
	} {
		if !errors.Is(err, ErrTableNotOwned) || !errors.Is(err, ErrSchemaUnknown) {
			t.Fatalf("%s: err = %v, want ErrTableNotOwned (an ErrSchemaUnknown)", name, err)
		}
	}
	if _, err := r.PromotedUnsafeParts("db.theirs"); !errors.Is(err, ErrTableNotOwned) {
		t.Fatalf("promoted-unsafe read of another owner's table: %v", err)
	}
	if got, err := r.PromotedUnsafeParts("db.mine"); err != nil || got != nil {
		t.Fatalf("own table: %v, %v", got, err)
	}
	rec := testRecord("0xabc:7:n")
	rec.Envelope.TargetTableID = "db.theirs"
	if err := journal.save(rec); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.LookupPreparedStatement(context.Background(), rec.StatementID); !errors.Is(err, ErrTableNotOwned) {
		t.Fatalf("prepared lookup of another owner's table: %v", err)
	}
	// A legacy SNode (no claim signer) still serves every table.
	r.d.ClaimSigner = nil
	if _, err := r.PromotedUnsafeParts("db.theirs"); err != nil {
		t.Fatalf("legacy SNode: %v", err)
	}
	if legacy, err := r.sourceClaimRoot(); err != nil || legacy == owned {
		t.Fatalf("legacy root %s must cover every Active table (%v)", legacy, err)
	}
}

func TestPromotionOfAnotherOwnersTableIsDroppedWithoutAck(t *testing.T) {
	fake := &claimsFakeS{}
	cfg := signingConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	view := newFakeRegistryS()
	view.setSnapshot(ownedSnapshotS(t, cfg))
	role, err := New(cfg, Deps{Client: startClaimsFakeS(t, fake), Conn: nopConnS{}, Registry: view,
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	authoritySigner := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	promote := arbiter.PromoteSafePartition{TableID: "db.theirs", PartitionID: "all", PromotionSeq: 9, BasePartitionRoot: "0xbase"}
	jws, err := authoritySigner.SignPromotion(promote)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handlePromote(context.Background(), wire.PromoteToPB(promote), jws); err != nil {
		t.Fatal(err)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.theirs", PartitionID: "all", PromotionSeq: 9}
	cleanupJWS, err := authoritySigner.SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), cleanupJWS); err != nil {
		t.Fatal(err)
	}
	if _, acks, cleanups := fake.sent(); len(acks) != 0 || len(cleanups) != 0 {
		t.Fatalf("acknowledged another owner's work: %v / %v", acks, cleanups)
	}
}
