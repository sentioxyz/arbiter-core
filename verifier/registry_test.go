package verifier

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// fakeRegistryView is a settable dataplane.RegistryView.
type fakeRegistryView struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	changed chan struct{}
	ready   chan struct{}
}

func newFakeRegistryView() *fakeRegistryView {
	v := &fakeRegistryView{changed: make(chan struct{}), ready: make(chan struct{})}
	close(v.ready)
	return v
}

func (v *fakeRegistryView) View() (wire.TableRegistrySnapshot, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snap, v.enabled
}

func (v *fakeRegistryView) Changed() <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.changed
}

func (v *fakeRegistryView) Ready() <-chan struct{} { return v.ready }

func (v *fakeRegistryView) set(incs ...wire.TableIncarnation) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snap = wire.TableRegistrySnapshot{Version: v.snap.Version + 1, Seeded: true, Incarnations: incs}
	v.enabled = true
	close(v.changed)
	v.changed = make(chan struct{})
}

func chainIncarnationV(t *testing.T, seq uint64, networkID string, schema payloadexec.TableSchema, status wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: status,
		SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash(networkID, schema), SchemaJSON: string(js)}
}

func genesisIncarnationV(seq uint64, networkID string, schema payloadexec.TableSchema) wire.TableIncarnation {
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginGenesis,
		Status: wire.TableStatusActive, SchemaHash: payloadexec.TableSchemaHash(networkID, schema)}
}

func addTransitionJob(blockSeq uint64, tableID string) *pb.ReplayJob {
	return wire.ReplayJobToPB(replay.ReplayJob{BlockSeq: blockSeq, TableSetTransition: &replay.ReplayTableSetTransition{
		Adds: []replay.ReplayTableSchema{{TableID: tableID, SchemaJSON: "{}"}}, NewSchemaRoot: "0xroot"}})
}

// newRegistryRoleV builds a verifier on real ClickHouse that follows view.
func newRegistryRoleV(t *testing.T, view *fakeRegistryView, core *fakeReplayCore) (*Role, *verifierFakeServer, Config) {
	t.Helper()
	conn := requireCH(t)
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	server := newVerifierFakeServer()
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startVerifierFakeServer(t, server)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	cfg := testConfigV()
	cfg.ReplicaID = "verifier-" + suffix
	g := scanTableSchema()
	g.TableID = "db.g_" + suffix
	cfg.Tables = []payloadexec.TableSchema{g}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase = "hg_unsafe_"+suffix, "hg_safe_"+suffix, "hg_promote_"+suffix
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	cfg.AddTransitionReadyWait = 200 * time.Millisecond
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	view.set(genesisIncarnationV(1, cfg.NetworkID, g))
	role, err := New(cfg, Deps{Client: client, Replay: core, Scanner: &fakeScanner{}, Conn: conn, Registry: view})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatalf("register: %v", err)
	}
	return role, server, cfg
}

func TestReplayJob_AddTransitionIsAttestedOnlyOnceTheTableIsReady(t *testing.T) {
	view, core := newFakeRegistryView(), &fakeReplayCore{}
	role, server, cfg := newRegistryRoleV(t, view, core)
	n := scanTableSchema()
	n.TableID = "db.n_" + cfg.ReplicaID[len("verifier-"):]
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, n, wire.TableStatusPending))

	// No reconcile pass runs: the gate waits its bound, then refuses.
	err := role.handleReplayJob(context.Background(), addTransitionJob(7, n.TableID))
	if !errors.Is(err, ErrAddedTableNotReady) {
		t.Fatalf("err = %v, want ErrAddedTableNotReady", err)
	}
	if core.jobCount() != 0 {
		t.Fatal("the replay core must not run before the added table is ready")
	}
	if _, _, atts, _ := server.snapshot(); len(atts) != 0 {
		t.Fatalf("attestations = %d, want 0", len(atts))
	}

	if err := role.tables.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
		t.Fatal(err)
	}
	if err := role.handleReplayJob(context.Background(), addTransitionJob(7, n.TableID)); err != nil {
		t.Fatalf("after the table is ready: %v", err)
	}
	if _, _, atts, _ := server.snapshot(); len(atts) != 1 {
		t.Fatalf("attestations = %d, want 1", len(atts))
	}
}

func TestRun_AddTransitionGateLetsTheReconcileLoopCreateTheTable(t *testing.T) {
	view, core := newFakeRegistryView(), &fakeReplayCore{}
	role, server, cfg := newRegistryRoleV(t, view, core)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	n := scanTableSchema()
	n.TableID = "db.n_" + cfg.ReplicaID[len("verifier-"):]
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, n, wire.TableStatusPending))
	// The arbiter re-sends an unattested job every dispatch.retry_interval;
	// the test re-sends it every 500ms until the verifier attests.
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		server.push(&pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: addTransitionJob(8, n.TableID)}})
		wait := time.Now().Add(500 * time.Millisecond)
		for time.Now().Before(wait) {
			if _, _, atts, _ := server.snapshot(); len(atts) > 0 {
				if !role.TableReady(n.TableID) {
					t.Fatal("attested before the added table was ready")
				}
				return
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	t.Fatal("the transition was never attested")
}

func TestScanner_ResolvesChainTablesThroughTheRegistry(t *testing.T) {
	cfg := testConfigV()
	chain := payloadexec.TableSchema{TableID: "db.c", Columns: []lthash.Column{{Name: "w", Type: "String"}}}
	view := newFakeRegistryView()
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, chain, wire.TableStatusActive))
	s := NewRegistryScanner(cfg, nil, view)
	got, err := s.schemaFor("db.c")
	if err != nil || got.TableID != "db.c" || len(got.Columns) != 1 || got.Columns[0].Name != "w" {
		t.Fatalf("schemaFor(db.c) = %+v, %v", got, err)
	}
	if got, err := s.schemaFor("db.t"); err != nil || got.Columns[0].Name != "v" {
		t.Fatalf("schemaFor(db.t) = %+v, %v", got, err)
	}
	if _, err := NewScanner(cfg, nil).schemaFor("db.c"); err == nil {
		t.Fatal("a scanner without the registry must not know chain tables")
	}
	// A same-name chain recreation of a retired genesis table is scanned
	// with its registry schema, not the stale genesis one.
	recreated := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "x", Type: "Int64"}}}
	purged := genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0])
	purged.Status = wire.TableStatusPurged
	view.set(purged, chainIncarnationV(t, 2, cfg.NetworkID, recreated, wire.TableStatusActive))
	if got, err := s.schemaFor("db.t"); err != nil || got.Columns[0].Name != "x" {
		t.Fatalf("schemaFor(recreated db.t) = %+v, %v", got, err)
	}
}

// R-F3: the scanner resolves registry schemas by the SNode's rule. A
// schema_json that does not hash to the incarnation's schema_hash is refused.
func TestScanner_RefusesARegistrySchemaThatDoesNotHashToItsSchemaHash(t *testing.T) {
	cfg := testConfigV()
	chain := payloadexec.TableSchema{TableID: "db.c", Columns: []lthash.Column{{Name: "w", Type: "String"}}}
	forged := chainIncarnationV(t, 2, cfg.NetworkID, chain, wire.TableStatusActive)
	other := payloadexec.TableSchema{TableID: "db.c", Columns: []lthash.Column{{Name: "w", Type: "UInt64"}}}
	forged.SchemaHash = payloadexec.TableSchemaHash(cfg.NetworkID, other)
	view := newFakeRegistryView()
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), forged)
	_, err := NewRegistryScanner(cfg, nil, view).schemaFor("db.c")
	if err == nil || !strings.Contains(err.Error(), "hashes to") {
		t.Fatalf("schemaFor(forged db.c) = %v, want a schema_hash mismatch", err)
	}
	// A schema_json hashed under another network is refused the same way.
	crossNet := chainIncarnationV(t, 2, "othernet", chain, wire.TableStatusActive)
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), crossNet)
	if _, err := NewRegistryScanner(cfg, nil, view).schemaFor("db.c"); err == nil {
		t.Fatal("a schema hashed under another network id must be refused")
	}
}

// R-F3: while the registry is enabled only a genesis-origin live incarnation
// may use the configured tables; any other key is resolved by the registry or
// refused, never by the configured schema of the same name.
func TestScanner_EnabledRegistryNeverFallsBackToConfiguredTables(t *testing.T) {
	cfg := testConfigV() // configures db.t
	view := newFakeRegistryView()
	s := NewRegistryScanner(cfg, nil, view)

	// Registry not (yet) enabled: the configured tables apply.
	if got, err := s.schemaFor("db.t"); err != nil || got.Columns[0].Name != "v" {
		t.Fatalf("disabled registry: schemaFor(db.t) = %+v, %v", got, err)
	}

	recreated := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "x", Type: "Int64"}}}
	purged := genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0])
	purged.Status = wire.TableStatusPurged
	cases := []struct {
		name string
		incs []wire.TableIncarnation
	}{
		{"key absent from the registry", nil},
		{"chain incarnation with an undecodable schema_json", func() []wire.TableIncarnation {
			bad := chainIncarnationV(t, 2, cfg.NetworkID, recreated, wire.TableStatusActive)
			bad.SchemaJSON = "{not json"
			return []wire.TableIncarnation{purged, bad}
		}()},
		{"chain incarnation whose schema_json does not hash", func() []wire.TableIncarnation {
			bad := chainIncarnationV(t, 2, cfg.NetworkID, recreated, wire.TableStatusActive)
			bad.SchemaHash = payloadexec.TableSchemaHash(cfg.NetworkID, cfg.Tables[0])
			return []wire.TableIncarnation{purged, bad}
		}()},
		{"pending chain recreation", []wire.TableIncarnation{purged, chainIncarnationV(t, 2, cfg.NetworkID, recreated, wire.TableStatusPending)}},
		{"only the purged genesis incarnation", []wire.TableIncarnation{purged}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			view.set(tc.incs...)
			if got, err := s.schemaFor("db.t"); err == nil {
				t.Fatalf("schemaFor(db.t) = %+v; an enabled registry must not fall back to the configured schema", got)
			}
		})
	}
	// Retiring and Purging chain incarnations still resolve (a block admitted
	// before the retirement is still scanned).
	for _, status := range []wire.TableIncarnationStatus{wire.TableStatusRetiring, wire.TableStatusPurging} {
		view.set(purged, chainIncarnationV(t, 2, cfg.NetworkID, recreated, status))
		if got, err := s.schemaFor("db.t"); err != nil || got.Columns[0].Name != "x" {
			t.Fatalf("%s: schemaFor(db.t) = %+v, %v", status, got, err)
		}
	}
}

func TestHandleSnapshotQueryJob_RefusesTablesOutsideTheGenesisSet(t *testing.T) {
	core := &fakeSnapshotQueryCore{}
	references := &fakeSnapshotQueryReferenceProvider{references: []string{"ref"}}
	role, server := newRoleHarnessVWithSnapshotQuery(t, core, references)
	job := signedSnapshotQueryJob(t)
	job.Statement.Envelope.Input.ReadSet.Tables = []replay.SnapshotReadTable{{TableID: "db.dynamic"}}
	err := role.handleSnapshotQueryJob(context.Background(), wire.SnapshotQueryJobToPB(job))
	if err == nil || !strings.Contains(err.Error(), "outside the genesis table set") {
		t.Fatalf("err = %v", err)
	}
	if jobs, _ := core.snapshot(); len(jobs) != 0 || len(references.snapshot()) != 0 || len(server.queryAttestationsSnapshot()) != 0 {
		t.Fatal("a refused read must use no dependency and submit nothing")
	}
}

func TestNew_FollowingTheRegistryNeedsManagedTables(t *testing.T) {
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	cfg := testConfigV() // SchemaSourceUnmanaged
	_, err = New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}, Registry: newFakeRegistryView()})
	if err == nil || !strings.Contains(err.Error(), "requires managed protocol tables") {
		t.Fatalf("unmanaged verifier following the registry: %v", err)
	}
	if _, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}}); err != nil {
		t.Fatalf("a registry-less unmanaged role is unchanged: %v", err)
	}
}

func TestConfig_AddTransitionReadyWaitDefaultsAndRejectsNegative(t *testing.T) {
	cfg := testConfigV()
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.AddTransitionReadyWait != DefaultAddTransitionReadyWait {
		t.Fatalf("AddTransitionReadyWait = %v, want %v", cfg.AddTransitionReadyWait, DefaultAddTransitionReadyWait)
	}
	cfg = testConfigV()
	cfg.AddTransitionReadyWait = -time.Second
	if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "add transition ready wait") {
		t.Fatalf("negative wait: %v", err)
	}
}

type nopConnV struct{ clickhouse.Conn }

type nopArbiterV struct{}

func (nopArbiterV) SubmitTablePurged(context.Context, string, uint64) error { return nil }
func (nopArbiterV) PurgeNodeSet(context.Context) ([]string, error)          { return nil, nil }

// R-wake: a registry version accepted while a pass runs (the startup pass or
// a periodic one) must wake the next pass at once, not after the interval.
func TestReconcileLoop_RegistryChangeDuringAPassWakesTheNextPass(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const interval = time.Hour
		logger := slog.New(slog.NewTextHandler(io.Discard, nil))
		view := newFakeRegistryView()
		tables, err := tableset.New(tableset.Config{
			Pinned:   ddl.Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "v1"},
			Interval: interval,
		}, tableset.Deps{Conn: nopConnV{}, Registry: view, Arbiter: nopArbiterV{}, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		var mu sync.Mutex
		var passes []time.Duration
		r := &Role{cfg: Config{ProtocolTablesReconcile: interval}, d: Deps{Registry: view, Logger: logger}, tables: tables}
		r.ensureFn = func(context.Context, ddl.Mode) error {
			mu.Lock()
			passes = append(passes, time.Since(start))
			n := len(passes)
			mu.Unlock()
			if n <= 2 {
				view.set() // accepted while this pass runs
			}
			return nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		registryChanged, err := r.startupEnsure(ctx)
		if err != nil {
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- r.reconcileProtocolTablesFrom(ctx, registryChanged) }()
		synctest.Wait()
		mu.Lock()
		got := append([]time.Duration(nil), passes...)
		mu.Unlock()
		// startup pass (changes the registry), a woken pass (changes it
		// again), a woken pass; then the loop sleeps on the interval timer.
		if len(got) != 3 {
			t.Fatalf("passes = %v, want 3 before the loop sleeps", got)
		}
		for i, at := range got {
			if at != 0 {
				t.Fatalf("pass %d ran after %v: a registry change during the previous pass waited for the timer", i+1, at)
			}
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("loop exit = %v, want context canceled", err)
		}
	})
}

func newGateRoleV(t *testing.T, registry dataplane.RegistryView, managed bool, logs io.Writer) (*Role, *fakeReplayCore) {
	t.Helper()
	logger := slog.New(slog.NewTextHandler(logs, nil))
	core := &fakeReplayCore{}
	r := &Role{cfg: Config{AddTransitionReadyWait: time.Hour}, d: Deps{Replay: core, Registry: registry, Logger: logger}}
	if managed {
		tables, err := tableset.New(tableset.Config{
			Pinned: ddl.Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "v1"},
		}, tableset.Deps{Conn: nopConnV{}, Registry: registry, Arbiter: nopArbiterV{}, Logger: logger})
		if err != nil {
			t.Fatal(err)
		}
		r.tables = tables
	}
	return r, core
}

// Without an enabled registry no chain table is ever reconciled, so the gate
// refuses at once instead of blocking the subscription for its whole bound
// on every redelivery.
func TestReplayJob_AddTransitionIsRefusedAtOnceWithoutAnEnabledRegistry(t *testing.T) {
	for _, tc := range []struct {
		name     string
		registry func() dataplane.RegistryView
		managed  bool
	}{
		{"managed tables, no registry", func() dataplane.RegistryView { return nil }, true},
		{"managed tables, registry disabled", func() dataplane.RegistryView { return newFakeRegistryView() }, true},
		{"unmanaged tables", func() dataplane.RegistryView { return nil }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				r, core := newGateRoleV(t, tc.registry(), tc.managed, io.Discard)
				start := time.Now()
				err := r.handleReplayJob(t.Context(), addTransitionJob(7, "db.n"))
				if !errors.Is(err, ErrAddedTableNotReady) {
					t.Fatalf("err = %v, want ErrAddedTableNotReady", err)
				}
				if waited := time.Since(start); waited != 0 {
					t.Fatalf("the gate waited %v before refusing; it must refuse at once", waited)
				}
				if core.jobCount() != 0 {
					t.Fatal("the replay core must not run")
				}
			})
		})
	}
}

// A gate wait that ends because the context ended returns the context's error
// and does not log the "has not created" refusal.
func TestReplayJob_CanceledGateWaitIsNotLoggedAsARefusal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		view := newFakeRegistryView()
		view.set(chainIncarnationV(t, 2, "testnet", payloadexec.TableSchema{TableID: "db.n", Columns: []lthash.Column{{Name: "w", Type: "String"}}}, wire.TableStatusPending))
		var logs strings.Builder
		var mu sync.Mutex
		r, core := newGateRoleV(t, view, true, lockedWriter{&mu, &logs})
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- r.handleReplayJob(ctx, addTransitionJob(7, "db.n")) }()
		synctest.Wait() // the gate is waiting for the table
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context canceled", err)
		}
		if core.jobCount() != 0 {
			t.Fatal("the replay core must not run")
		}
		mu.Lock()
		defer mu.Unlock()
		if strings.Contains(logs.String(), "has not created") {
			t.Fatalf("a canceled wait was logged as a refusal:\n%s", logs.String())
		}
	})
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}
