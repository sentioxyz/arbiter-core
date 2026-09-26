package snode

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/fspayload"
	"github.com/sentioxyz/arbiter-core/wire"
)

// fakeRegistryS is a settable dataplane.RegistryView.
type fakeRegistryS struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	changed chan struct{}
	ready   chan struct{}
}

func newFakeRegistryS() *fakeRegistryS {
	v := &fakeRegistryS{changed: make(chan struct{}), ready: make(chan struct{})}
	close(v.ready)
	return v
}

func (v *fakeRegistryS) View() (wire.TableRegistrySnapshot, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snap, v.enabled
}

func (v *fakeRegistryS) Changed() <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.changed
}

func (v *fakeRegistryS) Ready() <-chan struct{} { return v.ready }

func (v *fakeRegistryS) set(incs ...wire.TableIncarnation) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snap = wire.TableRegistrySnapshot{Version: v.snap.Version + 1, Seeded: true, Incarnations: incs}
	v.enabled = true
	close(v.changed)
	v.changed = make(chan struct{})
}

func chainIncarnationS(t *testing.T, seq uint64, schema payloadexec.TableSchema, status wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	js, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: status,
		SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash("testnet", schema), SchemaJSON: string(js)}
}

func genesisIncarnationS(seq uint64, schema payloadexec.TableSchema) wire.TableIncarnation {
	db, table, _ := strings.Cut(schema.TableID, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginGenesis,
		Status: wire.TableStatusActive, SchemaHash: payloadexec.TableSchemaHash("testnet", schema)}
}

func chainSchemaS(tableID string) payloadexec.TableSchema {
	s := intakeSchema()
	s.TableID = tableID
	return s
}

func TestSchemaFor_FollowsTheLiveIncarnation(t *testing.T) {
	cfg := testConfigS(t)
	view := newFakeRegistryS()
	r := &Role{cfg: cfg, d: Deps{Registry: view}}
	if got, err := r.schemaFor("db.t"); err != nil || got.TableID != "db.t" {
		t.Fatalf("disabled registry: %+v, %v", got, err)
	}
	c := chainSchemaS("db.c")
	for _, tc := range []struct {
		status wire.TableIncarnationStatus
		ok     bool
	}{
		{wire.TableStatusPending, false}, {wire.TableStatusActive, true}, {wire.TableStatusRetiring, true},
		{wire.TableStatusPurging, true}, {wire.TableStatusPurged, false}, {wire.TableStatusRefused, false},
	} {
		view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, tc.status))
		got, err := r.schemaFor("db.c")
		if (err == nil) != tc.ok {
			t.Fatalf("%s: err = %v, want ok=%v", tc.status, err, tc.ok)
		}
		if tc.ok && (got.PartitionBy != "p" || len(got.Columns) != 2) {
			t.Fatalf("%s: schema = %+v", tc.status, got)
		}
	}
	if got, err := r.schemaFor("db.t"); err != nil || len(got.Columns) != 1 {
		t.Fatalf("genesis-origin schema comes from config: %+v, %v", got, err)
	}
	if _, err := r.schemaFor("db.none"); err == nil {
		t.Fatal("a key outside the registry must be unknown")
	}
	bad := chainIncarnationS(t, 2, c, wire.TableStatusActive)
	bad.SchemaHash = "0xother"
	view.set(genesisIncarnationS(1, cfg.Tables[0]), bad)
	if _, err := r.schemaFor("db.c"); err == nil || !strings.Contains(err.Error(), "hashes to") {
		t.Fatalf("a schema_json that does not hash to the registry hash must be refused: %v", err)
	}
}

func TestRequireAdmissible_GatesFreshIntake(t *testing.T) {
	cfg := testConfigS(t)
	view := newFakeRegistryS()
	r := &Role{cfg: cfg, d: Deps{Registry: view}}
	c := chainSchemaS("db.c")
	hash := payloadexec.TableSchemaHash("testnet", c)
	if err := r.requireAdmissible("db.c", hash); err != nil {
		t.Fatalf("disabled registry admits as before: %v", err)
	}
	for _, tc := range []struct {
		status wire.TableIncarnationStatus
		want   error
	}{
		{wire.TableStatusPending, ErrTableNotReady},
		{wire.TableStatusActive, ErrTableNotReady}, // not Ready: no reconciler pass
		{wire.TableStatusRetiring, ErrSchemaUnknown},
		{wire.TableStatusPurged, ErrSchemaUnknown},
	} {
		view.set(chainIncarnationS(t, 1, c, tc.status))
		if err := r.requireAdmissible("db.c", hash); !errors.Is(err, tc.want) {
			t.Fatalf("%s: err = %v, want %v", tc.status, err, tc.want)
		}
	}
	view.set(chainIncarnationS(t, 1, c, wire.TableStatusActive))
	if err := r.requireAdmissible("db.c", "0xstale"); !errors.Is(err, ErrSchemaHashMismatch) {
		t.Fatalf("err = %v, want ErrSchemaHashMismatch", err)
	}
	if err := r.requireAdmissible("db.none", hash); !errors.Is(err, ErrSchemaUnknown) {
		t.Fatalf("err = %v, want ErrSchemaUnknown", err)
	}
}

func TestSourceClaimRoot_CoversActiveAndRetiringRegistryTables(t *testing.T) {
	cfg := testConfigS(t)
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	view := newFakeRegistryS()
	r := &Role{cfg: cfg, state: st, d: Deps{Registry: view}}
	static, err := r.sourceClaimRoot()
	if err != nil {
		t.Fatal(err)
	}
	view.set(genesisIncarnationS(1, cfg.Tables[0]))
	if got, err := r.sourceClaimRoot(); err != nil || got != static {
		t.Fatalf("a registry holding only the genesis table must give the static root: %s vs %s (%v)", got, static, err)
	}
	a, b, p := chainSchemaS("db.a"), chainSchemaS("db.b"), chainSchemaS("db.p")
	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, a, wire.TableStatusActive),
		chainIncarnationS(t, 3, b, wire.TableStatusRetiring), chainIncarnationS(t, 4, p, wire.TableStatusPending))
	hashes := map[string]string{
		"db.t": payloadexec.TableSchemaHash("testnet", cfg.Tables[0]),
		"db.a": payloadexec.TableSchemaHash("testnet", a),
		"db.b": payloadexec.TableSchemaHash("testnet", b),
	}
	var tables []replay.TableManifest
	for _, id := range []string{"db.a", "db.b", "db.t"} {
		tables = append(tables, replay.TableManifest{TableID: id, SchemaHash: hashes[id]})
	}
	_, want, err := replay.AssembleStateRoot(cfg.SchemaSnapshotID, payloadexec.SchemaRootFromHashes(hashes), cfg.ExecutorProfileID, tables)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := r.sourceClaimRoot(); err != nil || got != want {
		t.Fatalf("root = %s, want %s over Active and Retiring only (%v)", got, want, err)
	}
}

func TestTableQuiescent_WaitsForPromotionCleanupAndIntake(t *testing.T) {
	cfg := testConfigS(t)
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	journal, err := openIntakeJournal(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	r := &Role{cfg: cfg, state: st, journal: journal}
	quiet := func() bool {
		t.Helper()
		ok, err := r.tableQuiescent("db.t")
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	if !quiet() {
		t.Fatal("an untouched table is quiescent")
	}
	k := partitionKey{Table: "db.t", Partition: "p_x"}
	one := "0x" + strings.Repeat("01", len(lthash.New().Bytes()))
	if err := st.AddUnpromoted(k, one); err != nil {
		t.Fatal(err)
	}
	if quiet() {
		t.Fatal("unpromoted rows keep the table busy")
	}
	if err := st.DrainUnpromoted(k, []string{one}); err != nil {
		t.Fatal(err)
	}
	if !quiet() {
		t.Fatal("a drained partition is quiescent")
	}
	st.mu.Lock()
	st.s.PromotedUnsafeParts[key("db.t", "p_x")] = []string{"p_x_1_1_0"}
	st.mu.Unlock()
	if quiet() {
		t.Fatal("a promoted part awaiting cleanup keeps the table busy")
	}
	if err := st.RecordCleanup(k, []string{"p_x_1_1_0"}); err != nil {
		t.Fatal(err)
	}
	other := testRecord("0xabc:6:n")
	other.Envelope.TargetTableID = "db.other"
	if err := journal.save(other); err != nil {
		t.Fatal(err)
	}
	if !quiet() {
		t.Fatal("an unfinished intake of another table does not block this one")
	}
	rec := testRecord("0xabc:7:n")
	rec.Envelope.TargetTableID = "db.t"
	if err := journal.save(rec); err != nil {
		t.Fatal(err)
	}
	if quiet() {
		t.Fatal("an unfinished intake keeps the table busy")
	}
}

func TestPrepareLocalStatement_RegistryGatesFreshIntakeOnChainTables(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	cfg := testConfigS(t)
	setUniqueDatabases(t, &cfg)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	suffix := strings.TrimPrefix(cfg.UnsafeDatabase, "hg_unsafe_")
	cfg.NodeID = "snode-" + suffix
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	claims := &sourceClaimsFake{}
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startSourceClaimsFake(t, claims)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	payloads, err := fspayload.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	view := newFakeRegistryS()
	c := chainSchemaS("db.c_" + suffix)
	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, wire.TableStatusPending))
	role, err := New(cfg, Deps{Client: client, Conn: conn, Payloads: payloads, Registry: view})
	if err != nil {
		t.Fatal(err)
	}

	payload := nativePayload(t, pv{"p0", 1})
	env := intakeEnvelope(payload)
	env.TargetTableID = c.TableID
	env.SQL = "INSERT INTO " + c.TableID + " FORMAT Native"
	env.SQLHash = replay.DigestString(env.SQL)
	env.SchemaHash = payloadexec.TableSchemaHash("testnet", c)
	req := PrepareRequest{Envelope: env, PayloadEncoding: stagedNativeEncoding, Revision: testRevision}
	if _, err := role.PrepareLocalStatement(ctx, req, payload); !errors.Is(err, ErrTableNotReady) {
		t.Fatalf("pending table: err = %v, want ErrTableNotReady", err)
	}
	if _, ok, _ := role.journal.load(env.StatementID.Flat()); ok {
		t.Fatal("a not-ready refusal must write no journal record")
	}

	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, wire.TableStatusActive))
	if err := role.ensureProtocolTables(ctx); err != nil {
		t.Fatalf("startup reconcile: %v", err)
	}
	if !role.TableReady(c.TableID) {
		t.Fatal("the Active chain table must be Ready after the reconcile")
	}
	first, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare on a ready chain table: %v", err)
	}

	// Retired: a fresh statement is refused, the recorded one still converges.
	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, wire.TableStatusRetiring))
	again, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil || again.StatementID != first.StatementID {
		t.Fatalf("recorded statement after retirement: %+v, %v", again, err)
	}
	fresh := req
	fresh.Envelope.StatementID.ClientSeq = 2
	if _, err := role.PrepareLocalStatement(ctx, fresh, payload); !errors.Is(err, ErrSchemaUnknown) {
		t.Fatalf("fresh statement on a retiring table: err = %v, want ErrSchemaUnknown", err)
	}
}

func TestNew_FollowingTheRegistryNeedsManagedTablesAndAConnection(t *testing.T) {
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	cfg := testConfigS(t) // SchemaSourceUnmanaged: the host owns DDL
	if _, err := New(cfg, Deps{Client: client, Registry: newFakeRegistryS()}); err == nil || !strings.Contains(err.Error(), "managed protocol tables") {
		t.Fatalf("unmanaged DDL with a registry: err = %v", err)
	}
	cfg = testConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	if _, err := New(cfg, Deps{Client: client, Registry: newFakeRegistryS()}); err == nil || !strings.Contains(err.Error(), "clickhouse connection") {
		t.Fatalf("managed DDL without a connection and with a registry: err = %v", err)
	}
	cfg = testConfigS(t)
	if _, err := New(cfg, Deps{Client: client}); err != nil {
		t.Fatalf("a registry-less unmanaged role is unchanged: %v", err)
	}
}
