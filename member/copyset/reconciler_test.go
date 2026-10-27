package copyset

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

const network = "testnet"

// The fixture is member-b's view of a three-indexer network: host indexer 1,
// peers 0 and 2. Indexer 0 is the founding indexer, so its genesis-origin
// incarnation (recorded before the signed-claims activation) names no owner.
var (
	peers   = []uint64{0, 2}
	genesis = []payloadexec.TableSchema{{TableID: "db0.g", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}}
)

func chainSchema(key string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: key, PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}
}

func chainInc(seq uint64, key string, owner uint64, status wire.TableIncarnationStatus) wire.TableIncarnation {
	schema := chainSchema(key)
	raw, err := json.Marshal(schema)
	if err != nil {
		panic(err)
	}
	db, table, _ := strings.Cut(key, ".")
	return wire.TableIncarnation{
		Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: status,
		SchemaVersion: 1, SchemaHash: payloadexec.TableSchemaHash(network, schema), SchemaJSON: string(raw), OwnerIndexerID: &owner,
	}
}

func genesisInc(seq uint64, key string, status wire.TableIncarnationStatus) wire.TableIncarnation {
	db, table, _ := strings.Cut(key, ".")
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginGenesis, Status: status}
}

func snapshot(incs ...wire.TableIncarnation) wire.TableRegistrySnapshot {
	return wire.TableRegistrySnapshot{
		Version: 1, Seeded: true, Params: arbiter.TableRegistryParams{SIIndexerID: 0},
		SIIndexers: []arbiter.SIIndexerEntry{
			{IndexerID: 0, SNodeNodeID: "snode-1"}, {IndexerID: 1, SNodeNodeID: "snode-2"}, {IndexerID: 2, SNodeNodeID: "snode-3"},
		},
		Incarnations: incs,
	}
}

// fakeRegistry is a dataplane.RegistryView whose snapshot the test sets.
type fakeRegistry struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	changed chan struct{}
	ready   chan struct{}
}

var _ dataplane.RegistryView = (*fakeRegistry)(nil)

func newFakeRegistry(snap wire.TableRegistrySnapshot, enabled bool) *fakeRegistry {
	f := &fakeRegistry{snap: snap, enabled: enabled, changed: make(chan struct{}), ready: make(chan struct{})}
	close(f.ready)
	return f
}

func (f *fakeRegistry) View() (wire.TableRegistrySnapshot, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap, f.enabled
}

func (f *fakeRegistry) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

func (f *fakeRegistry) Ready() <-chan struct{} { return f.ready }

func (f *fakeRegistry) set(snap wire.TableRegistrySnapshot, enabled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.snap, f.enabled = snap, enabled
	close(f.changed)
	f.changed = make(chan struct{})
}

// fakeStore is the member's ClickHouse as the reconciler sees it: a table
// listing, copies it creates and verifies, and drops in hg_replica. It records
// every ensure and drop.
type fakeStore struct {
	mu        sync.Mutex
	replicaDB string
	tables    []ddl.LocalTable
	ensured   []string
	dropped   []string
	ensureErr map[string]error
}

func newFakeStore(local ...ddl.LocalTable) *fakeStore {
	return &fakeStore{replicaDB: ddl.ReplicaDatabase, tables: local, ensureErr: map[string]error{}}
}

func (s *fakeStore) list(context.Context) ([]ddl.LocalTable, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.tables), nil
}

func (s *fakeStore) ensure(_ context.Context, schema payloadexec.TableSchema, incarnationSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := ddl.CopyTableName(schema.TableID, incarnationSeq)
	s.ensured = append(s.ensured, name)
	if err := s.ensureErr[name]; err != nil {
		return err
	}
	if !slices.ContainsFunc(s.tables, func(t ddl.LocalTable) bool { return t.Database == s.replicaDB && t.Table == name }) {
		s.tables = append(s.tables, ddl.LocalTable{Database: s.replicaDB, Table: name, Comment: ddl.IncarnationComment(incarnationSeq)})
	}
	return nil
}

func (s *fakeStore) drop(_ context.Context, table string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dropped = append(s.dropped, table)
	s.tables = slices.DeleteFunc(s.tables, func(t ddl.LocalTable) bool { return t.Database == s.replicaDB && t.Table == table })
	return nil
}

func (s *fakeStore) calls() (ensured, dropped []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.ensured), slices.Clone(s.dropped)
}

func (s *fakeStore) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ensured, s.dropped = nil, nil
}

// droppedCopies records every Dropped hook call.
type droppedCopies struct {
	mu   sync.Mutex
	seen []Copy
}

func (d *droppedCopies) hook(_ context.Context, c Copy) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seen = append(d.seen, c)
	return nil
}

func (d *droppedCopies) all() []Copy {
	d.mu.Lock()
	defer d.mu.Unlock()
	return slices.Clone(d.seen)
}

func (d *droppedCopies) tables() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	var out []string
	for _, c := range d.seen {
		out = append(out, c.Table)
	}
	return out
}

func testConfig() Config {
	return Config{
		Pinned:    ddl.DefaultMemberPinned("member-b", 0, "member"),
		Protocol:  ddl.Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "snode-2"},
		NetworkID: network, Genesis: genesis, Owners: peers, HostIndexerID: 1, Interval: time.Hour,
	}
}

func newTestReconciler(t *testing.T, reg dataplane.RegistryView, store *fakeStore, dropped *droppedCopies) *Reconciler {
	t.Helper()
	d := Deps{Registry: reg}
	if dropped != nil {
		d.Dropped = dropped.hook
	}
	r, err := newReconciler(testConfig(), d, store)
	if err != nil {
		t.Fatalf("newReconciler: %v", err)
	}
	return r
}

func copyTable(name, comment string) ddl.LocalTable {
	return ddl.LocalTable{Database: ddl.ReplicaDatabase, Table: name, Comment: comment}
}

// unusedConn is a non-nil clickhouse.Conn for the configuration refusals,
// which New returns before any call.
type unusedConn struct{ clickhouse.Conn }

func readyCopyTables(r *Reconciler) []string {
	var out []string
	for _, c := range r.Copies() {
		out = append(out, c.Table)
	}
	return out
}

// TestCopysetCreatesACopyForEveryPhysicalIncarnationOfAPeerOwner: every
// Pending or Active incarnation owned by a peer gets one verified copy,
// genesis-origin ones included (their schema is the configured one, their
// owner the founding indexer); nothing else does (plan S3-C2 K31).
func TestCopysetCreatesACopyForEveryPhysicalIncarnationOfAPeerOwner(t *testing.T) {
	reg := newFakeRegistry(snapshot(
		genesisInc(1, "db0.g", wire.TableStatusActive),
		chainInc(2, "db0.c", 0, wire.TableStatusActive),
		chainInc(3, "db2.p", 2, wire.TableStatusPending),
		chainInc(4, "db1.h", 1, wire.TableStatusActive),
		chainInc(5, "db5.x", 5, wire.TableStatusActive),
		chainInc(6, "db2.l", 2, wire.TableStatusLegacy),
	), true)
	store := newFakeStore()
	r := newTestReconciler(t, reg, store, nil)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	ensured, dropped := store.calls()
	if want := []string{"db0__c__i2", "db0__g__i1", "db2__p__i3"}; !slices.Equal(ensured, want) || len(dropped) != 0 {
		t.Fatalf("ensured %v dropped %v, want ensured %v and nothing dropped", ensured, dropped, want)
	}
	got := r.Copies()
	want := []Copy{
		{Owner: 0, TableKey: "db0.c", IncarnationSeq: 2, Table: "db0__c__i2", Status: wire.TableStatusActive, Schema: chainSchema("db0.c"), Generation: 1, Ready: true},
		{Owner: 0, TableKey: "db0.g", IncarnationSeq: 1, Table: "db0__g__i1", Status: wire.TableStatusActive, Schema: genesis[0], Generation: 2, Ready: true},
		{Owner: 2, TableKey: "db2.p", IncarnationSeq: 3, Table: "db2__p__i3", Status: wire.TableStatusPending, Schema: chainSchema("db2.p"), Generation: 3, Ready: true},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Copies() = %+v\nwant %+v", got, want)
	}
	if c, ok := r.Copy("db2.p"); !ok || !reflect.DeepEqual(c, want[2]) {
		t.Fatalf("Copy(db2.p) = %+v, %v", c, ok)
	}
	for _, key := range []string{"db1.h", "db5.x", "db2.l"} {
		if c, ok := r.Copy(key); ok {
			t.Fatalf("Copy(%s) = %+v, want none", key, c)
		}
	}
	if s := r.Stats(); s.Copies != 3 || s.Dropped != 0 || s.Unknown != 0 || len(s.Failures) != 0 {
		t.Fatalf("Stats = %+v", s)
	}

	// A copy that fails to verify is not ready, is counted, and is retried by
	// the next pass; the others stay ready.
	store.reset()
	store.ensureErr["db2__p__i3"] = errors.New("ddl: protocol table drift: hg_replica.db2__p__i3 comment")
	err := r.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "db2__p__i3") {
		t.Fatalf("Reconcile with a drifted copy = %v, want its error", err)
	}
	if got := readyCopyTables(r); !slices.Equal(got, []string{"db0__c__i2", "db0__g__i1"}) {
		t.Fatalf("ready copies = %v, want the two that verify", got)
	}
	if c, ok := r.Copy("db2.p"); !ok || c.Ready {
		t.Fatalf("Copy(db2.p) = %+v, %v; want present and not ready", c, ok)
	}
	if s := r.Stats(); s.Copies != 2 || s.Failures["db2.p"] != 1 {
		t.Fatalf("Stats = %+v, want 2 copies and one db2.p failure", s)
	}
	delete(store.ensureErr, "db2__p__i3")
	if err := r.Reconcile(context.Background()); err != nil || len(r.Copies()) != 3 {
		t.Fatalf("Reconcile after the fix = %v, copies %v", err, readyCopyTables(r))
	}

	// A chain schema_json that does not hash to its schema_hash, or a
	// genesis-origin key with no configured schema, is a failure of that copy
	// alone.
	broken := chainInc(2, "db0.c", 0, wire.TableStatusActive)
	broken.SchemaHash = "0xother"
	reg.set(snapshot(genesisInc(1, "db0.z", wire.TableStatusActive), broken), true)
	err = r.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "genesis table db0.z is not configured") || !strings.Contains(err.Error(), "hashes to") {
		t.Fatalf("Reconcile with unresolvable schemas = %v", err)
	}
	if got := r.Copies(); len(got) != 0 {
		t.Fatalf("Copies() = %+v, want none ready", got)
	}
}

// TestCopysetGivesARecreatedCopyANewGeneration: a copy whose table stays
// keeps its generation from pass to pass; a copy whose table vanished is
// created again empty under a new generation and Changed fires, so the
// installer's audit coverage and the role's copy key no longer match it
// (Codex review of PR #54, reconciler.go:299).
func TestCopysetGivesARecreatedCopyANewGeneration(t *testing.T) {
	reg := newFakeRegistry(snapshot(
		chainInc(2, "db0.c", 0, wire.TableStatusActive),
		chainInc(3, "db2.p", 2, wire.TableStatusActive),
	), true)
	store := newFakeStore()
	r := newTestReconciler(t, reg, store, nil)
	ctx := context.Background()
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	gens := func() map[string]uint64 {
		out := map[string]uint64{}
		for _, c := range r.Copies() {
			out[c.TableKey] = c.Generation
		}
		return out
	}
	first := gens()
	if first["db0.c"] == 0 || first["db2.p"] == 0 || first["db0.c"] == first["db2.p"] {
		t.Fatalf("generations after the first pass = %v, want two distinct non-zero ones", first)
	}
	changed := r.Changed()
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if got := gens(); !reflect.DeepEqual(got, first) {
		t.Fatalf("generations after a pass that found every table = %v, want %v", got, first)
	}
	select {
	case <-changed:
		t.Fatal("Changed fired on a pass that changed no copy")
	default:
	}

	// The table of db2.p vanishes behind the reconciler's back.
	if err := store.drop(ctx, "db2__p__i3"); err != nil {
		t.Fatal(err)
	}
	store.reset()
	if err := r.Reconcile(ctx); err != nil {
		t.Fatal(err)
	}
	if ensured, _ := store.calls(); !slices.Contains(ensured, "db2__p__i3") {
		t.Fatalf("ensured %v, want db2__p__i3 created again", ensured)
	}
	got := gens()
	if got["db0.c"] != first["db0.c"] || got["db2.p"] == first["db2.p"] || got["db2.p"] == 0 {
		t.Fatalf("generations after db2.p was created again = %v, want db0.c kept at %d and db2.p new (was %d)", got, first["db0.c"], first["db2.p"])
	}
	select {
	case <-changed:
	default:
		t.Fatal("Changed did not fire when db2.p was created again")
	}
}

// TestCopysetIgnoresTheHostsOwnTables: a member never copies its host's
// incarnations (they are the host SNode's hg_safe), and never touches the
// host's protocol tables.
func TestCopysetIgnoresTheHostsOwnTables(t *testing.T) {
	var incs []wire.TableIncarnation
	for i, status := range []wire.TableIncarnationStatus{wire.TableStatusPending, wire.TableStatusActive, wire.TableStatusRetiring, wire.TableStatusPurging} {
		incs = append(incs, chainInc(uint64(i+1), "db1.h"+string(status), 1, status))
	}
	reg := newFakeRegistry(snapshot(incs...), true)
	store := newFakeStore(
		ddl.LocalTable{Database: "hg_safe", Table: "db1__hactive", Comment: ddl.IncarnationComment(2)},
		ddl.LocalTable{Database: "hg_unsafe", Table: "db1__hactive", Comment: ddl.IncarnationComment(2)},
		ddl.LocalTable{Database: "hg_promote", Table: "db1__hactive", Comment: ddl.IncarnationComment(2)},
	)
	r := newTestReconciler(t, reg, store, nil)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	ensured, dropped := store.calls()
	if len(ensured) != 0 || len(dropped) != 0 || len(r.Copies()) != 0 {
		t.Fatalf("host-owned incarnations: ensured %v dropped %v copies %+v; want nothing", ensured, dropped, r.Copies())
	}
	for _, inc := range incs {
		if c, ok := r.Copy(inc.Key()); ok {
			t.Fatalf("Copy(%s) = %+v, want none", inc.Key(), c)
		}
	}
	if s := r.Stats(); s.Copies != 0 || s.Dropped != 0 || s.Unknown != 0 {
		t.Fatalf("Stats = %+v", s)
	}
}

// TestCopysetDropsTheCopyFromRetiringOn: a member stops keeping a copy once
// its incarnation is Retiring (spec §8.8); Run reacts to the registry's next
// version, tells the installer (Dropped) and signals the changed copy set.
func TestCopysetDropsTheCopyFromRetiringOn(t *testing.T) {
	active := snapshot(chainInc(1, "db0.c", 0, wire.TableStatusActive), chainInc(2, "db2.p", 2, wire.TableStatusActive))
	reg := newFakeRegistry(active, true)
	store := newFakeStore()
	var dropped droppedCopies
	r := newTestReconciler(t, reg, store, &dropped)
	changed := r.Changed()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("Run = %v, want context.Canceled", err)
		}
	}()
	waitChanged(t, changed)
	if got := readyCopyTables(r); !slices.Equal(got, []string{"db0__c__i1", "db2__p__i2"}) {
		t.Fatalf("ready copies = %v", got)
	}

	changed = r.Changed()
	reg.set(snapshot(chainInc(1, "db0.c", 0, wire.TableStatusRetiring), chainInc(2, "db2.p", 2, wire.TableStatusPurging)), true)
	waitChanged(t, changed)
	if got := r.Copies(); len(got) != 0 {
		t.Fatalf("Copies() after Retiring and Purging = %+v, want none", got)
	}
	_, drops := store.calls()
	if want := []string{"db0__c__i1", "db2__p__i2"}; !slices.Equal(drops, want) || !slices.Equal(dropped.tables(), want) {
		t.Fatalf("dropped %v, Dropped hook %v; want %v", drops, dropped.tables(), want)
	}
	if seen := dropped.all(); seen[0].Status != wire.TableStatusRetiring || seen[0].TableKey != "db0.c" || seen[0].IncarnationSeq != 1 || seen[0].Owner != 0 ||
		seen[1].Status != wire.TableStatusPurging || seen[1].Owner != 2 {
		t.Fatalf("Dropped hook saw %+v", seen)
	}
	if s := r.Stats(); s.Copies != 0 || s.Dropped != 2 || s.Unknown != 0 {
		t.Fatalf("Stats = %+v, want 2 dropped from Retiring on", s)
	}
}

func waitChanged(t *testing.T, changed <-chan struct{}) {
	t.Helper()
	select {
	case <-changed:
	case <-time.After(10 * time.Second):
		t.Fatal("the copy set did not change within 10s")
	}
}

// TestCopysetDropsCopiesOfPurgedRefusedAndUnknownIncarnations: every
// hg_replica table that is not the copy of a peer owner's Pending or Active
// incarnation, under its own name and comment, is dropped and counted
// Unknown (plan S3-C2 contract §21.6).
func TestCopysetDropsCopiesOfPurgedRefusedAndUnknownIncarnations(t *testing.T) {
	reg := newFakeRegistry(snapshot(
		chainInc(1, "db0.c", 0, wire.TableStatusPurged),
		chainInc(2, "db0.r", 0, wire.TableStatusRefused),
		chainInc(3, "db0.l", 0, wire.TableStatusLegacy),
		chainInc(4, "db5.x", 5, wire.TableStatusActive),
		chainInc(5, "db0.c", 0, wire.TableStatusActive),
	), true)
	store := newFakeStore(
		copyTable("db0__c__i1", ddl.IncarnationComment(1)),   // Purged
		copyTable("db0__r__i2", ddl.IncarnationComment(2)),   // Refused
		copyTable("db0__l__i3", ddl.IncarnationComment(3)),   // Legacy
		copyTable("db5__x__i4", ddl.IncarnationComment(4)),   // an owner outside the scope
		copyTable("junk", ""),                                // no incarnation comment
		copyTable("db0__c__i99", ddl.IncarnationComment(99)), // an incarnation the registry does not know
		copyTable("db0__c__i5", "hg_incarnation=05"),         // not a canonical comment
		copyTable("db0__other", ddl.IncarnationComment(5)),   // a comment of another table's incarnation
	)
	var dropped droppedCopies
	r := newTestReconciler(t, reg, store, &dropped)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	ensured, drops := store.calls()
	wantDrops := []string{"db0__c__i1", "db0__c__i5", "db0__c__i99", "db0__l__i3", "db0__other", "db0__r__i2", "db5__x__i4", "junk"}
	if !slices.Equal(drops, wantDrops) || !slices.Equal(dropped.tables(), wantDrops) {
		t.Fatalf("dropped %v, Dropped hook %v; want %v", drops, dropped.tables(), wantDrops)
	}
	if !slices.Equal(ensured, []string{"db0__c__i5"}) {
		t.Fatalf("ensured %v, want the live copy db0__c__i5 re-created after its foreign-comment table was dropped", ensured)
	}
	if s := r.Stats(); s.Unknown != 8 || s.Dropped != 0 || s.Copies != 1 {
		t.Fatalf("Stats = %+v, want 8 unknown drops and the one live copy", s)
	}
}

// TestCopysetRetriesAFailedDroppedHookBeforeRecreatingTheCopy: when the
// installer could not forget a dropped table, no copy of that name is made
// ready until a later pass's retry of the hook succeeds, so the installer's
// journal never describes an empty re-created copy.
func TestCopysetRetriesAFailedDroppedHookBeforeRecreatingTheCopy(t *testing.T) {
	reg := newFakeRegistry(snapshot(chainInc(1, "db0.c", 0, wire.TableStatusActive)), true)
	store := newFakeStore(copyTable("db0__c__i1", "hg_incarnation=01"))
	failing := true
	var calls []string
	d := Deps{Registry: reg, Dropped: func(_ context.Context, c Copy) error {
		calls = append(calls, c.Table)
		if failing {
			return errors.New("journal is busy")
		}
		return nil
	}}
	r, err := newReconciler(testConfig(), d, store)
	if err != nil {
		t.Fatal(err)
	}
	err = r.Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "journal is busy") || !strings.Contains(err.Error(), "waits until the installer forgets") {
		t.Fatalf("Reconcile with a failing hook = %v", err)
	}
	if ensured, dropped := store.calls(); len(ensured) != 0 || !slices.Equal(dropped, []string{"db0__c__i1"}) {
		t.Fatalf("ensured %v dropped %v, want the drop and no re-create", ensured, dropped)
	}
	if len(r.Copies()) != 0 {
		t.Fatalf("Copies() = %+v, want none while the hook is owed", r.Copies())
	}
	failing = false
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile after the hook recovered: %v", err)
	}
	if !slices.Equal(calls, []string{"db0__c__i1", "db0__c__i1"}) || !slices.Equal(readyCopyTables(r), []string{"db0__c__i1"}) {
		t.Fatalf("hook calls %v, ready %v; want one retry and the copy ready", calls, readyCopyTables(r))
	}
}

// TestCopysetKeepsNoCopyWhileTheRegistryIsDisabled: without an enabled
// registry nothing is a copy: none is created or reported ready, and nothing
// is dropped, because only the registry can tell a copy from an unknown
// table. Before the follower's first answer a pass refuses to run.
func TestCopysetKeepsNoCopyWhileTheRegistryIsDisabled(t *testing.T) {
	snap := snapshot(chainInc(1, "db0.c", 0, wire.TableStatusActive))
	unready := &fakeRegistry{snap: snap, enabled: true, changed: make(chan struct{}), ready: make(chan struct{})}
	store := newFakeStore(copyTable("db0__c__i1", ddl.IncarnationComment(1)), copyTable("junk", ""))
	r := newTestReconciler(t, unready, store, nil)
	if err := r.Reconcile(context.Background()); err == nil || err.Error() != "copyset: the table registry has not answered yet" {
		t.Fatalf("Reconcile before the registry answered = %v", err)
	}
	reg := newFakeRegistry(snap, false)
	r = newTestReconciler(t, reg, store, nil)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile with a disabled registry: %v", err)
	}
	ensured, dropped := store.calls()
	if len(ensured) != 0 || len(dropped) != 0 || len(r.Copies()) != 0 {
		t.Fatalf("disabled registry: ensured %v dropped %v copies %+v; want nothing", ensured, dropped, r.Copies())
	}
	reg.set(snap, true)
	if err := r.Reconcile(context.Background()); err != nil || !slices.Equal(readyCopyTables(r), []string{"db0__c__i1"}) {
		t.Fatalf("enabled registry: %v, copies %v", err, readyCopyTables(r))
	}
	changed := r.Changed()
	reg.set(snap, false)
	store.reset()
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitChanged(t, changed)
	if _, dropped := store.calls(); len(r.Copies()) != 0 || len(dropped) != 0 {
		t.Fatalf("disabled again: copies %+v dropped %v; want no copy and no drop", r.Copies(), dropped)
	}
	if _, ok := r.Copy("db0.c"); ok {
		t.Fatal("Copy(db0.c) is still reported while the registry is disabled")
	}
}

// TestCopysetNeverTouchesProtocolOrStageDatabases: whatever the listing
// holds, the reconciler judges and drops only hg_replica tables; hg_safe,
// hg_unsafe, hg_promote, hg_rstage and hg_member_unsafe are never its own
// (spec §8.3).
func TestCopysetNeverTouchesProtocolOrStageDatabases(t *testing.T) {
	reg := newFakeRegistry(snapshot(chainInc(1, "db0.c", 0, wire.TableStatusRetiring), chainInc(2, "db0.d", 0, wire.TableStatusActive)), true)
	var local []ddl.LocalTable
	for _, db := range []string{"hg_safe", "hg_unsafe", "hg_promote", ddl.StageDatabase, ddl.MemberUnsafeDatabase} {
		local = append(local,
			ddl.LocalTable{Database: db, Table: "db0__c__i1", Comment: ddl.IncarnationComment(1)},
			ddl.LocalTable{Database: db, Table: "junk"},
			ddl.LocalTable{Database: db, Table: "db0__d", Comment: ddl.IncarnationComment(2)},
		)
	}
	store := newFakeStore(local...)
	var dropped droppedCopies
	r := newTestReconciler(t, reg, store, &dropped)
	if err := r.Reconcile(context.Background()); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	ensured, drops := store.calls()
	if len(drops) != 0 || len(dropped.tables()) != 0 {
		t.Fatalf("dropped %v (hook %v) from other databases' listings, want nothing", drops, dropped.tables())
	}
	if !slices.Equal(ensured, []string{"db0__d__i2"}) {
		t.Fatalf("ensured %v, want only the copy in hg_replica", ensured)
	}
	if s := r.Stats(); s.Dropped != 0 || s.Unknown != 0 {
		t.Fatalf("Stats = %+v", s)
	}
}

// TestCopysetRefusesMemberDatabasesThatIntersectTheSNodes: the startup
// refusal of spec §8.3 runs in New.
func TestCopysetRefusesMemberDatabasesThatIntersectTheSNodes(t *testing.T) {
	cfg := testConfig()
	cfg.Pinned.ReplicaDB = "hg_safe"
	_, err := New(cfg, Deps{Conn: unusedConn{}, Registry: newFakeRegistry(snapshot(), true)})
	if !errors.Is(err, ddl.ErrMemberDatabasesIntersect) ||
		err.Error() != "ddl: member databases [hg_safe] intersect the protocol databases [hg_unsafe hg_safe hg_promote]" {
		t.Fatalf("New with hg_replica renamed to hg_safe = %v", err)
	}
}

// TestCopysetRefusesTheHostAsAnOwner: the static group scope is every peer
// but the host (spec §16 P2), named once each in ascending order.
func TestCopysetRefusesTheHostAsAnOwner(t *testing.T) {
	for _, tc := range []struct {
		owners []uint64
		want   string
	}{
		{[]uint64{0, 1, 2}, "copyset: owner 1 is the host's own indexer"},
		{[]uint64{2, 0}, "copyset: Owners must be ascending without duplicates, got [2 0]"},
		{[]uint64{0, 0}, "copyset: Owners must be ascending without duplicates, got [0 0]"},
		{nil, "copyset: Owners is empty: a member copies at least one peer owner"},
	} {
		cfg := testConfig()
		cfg.Owners = tc.owners
		if _, err := New(cfg, Deps{Conn: unusedConn{}, Registry: newFakeRegistry(snapshot(), true)}); err == nil || err.Error() != tc.want {
			t.Fatalf("New with owners %v = %v, want %q", tc.owners, err, tc.want)
		}
	}
}

// TestCopysetNewRefusesAnIncompleteConfig covers New's other refusals.
func TestCopysetNewRefusesAnIncompleteConfig(t *testing.T) {
	reg := newFakeRegistry(snapshot(), true)
	for _, tc := range []struct {
		name string
		edit func(*Config, *Deps)
		want string
	}{
		{"no connection", func(_ *Config, d *Deps) { d.Conn = nil }, "copyset: clickhouse connection is required"},
		{"no registry", func(_ *Config, d *Deps) { d.Registry = nil }, "copyset: a table registry view is required"},
		{"no network id", func(c *Config, _ *Deps) { c.NetworkID = "" }, "copyset: NetworkID is required"},
		{"colliding genesis names", func(c *Config, _ *Deps) {
			c.Genesis = []payloadexec.TableSchema{{TableID: "a.b__c"}, {TableID: "a__b.c"}}
		}, "ddl: physical table name collision"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, d := testConfig(), Deps{Conn: unusedConn{}, Registry: reg}
			tc.edit(&cfg, &d)
			if _, err := New(cfg, d); err == nil || !strings.HasPrefix(err.Error(), tc.want) {
				t.Fatalf("New = %v, want %q", err, tc.want)
			}
		})
	}
	r, err := New(testConfig(), Deps{Conn: unusedConn{}, Registry: reg})
	if err != nil || r == nil {
		t.Fatalf("New of a complete config = %v, %v", r, err)
	}
}
