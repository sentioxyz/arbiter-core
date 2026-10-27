package install

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/copyset"
	"github.com/sentioxyz/arbiter-core/member/manifestfeed"
	"github.com/sentioxyz/arbiter-core/member/sources"
	"github.com/sentioxyz/arbiter-core/wire"
)

const testNetwork = "s3c2-install-test"

// requireCH opens CH_ADDR (default 127.0.0.1:9000) through chtest once
// ARBITER_CH_INTEGRATION=1 is set, as the other ClickHouse-gated packages do.
func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or 127.0.0.1:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	return chtest.Open(t, addr)
}

// recordingConn records every statement the installer executes and fails the
// test on a replacing partition statement, an OPTIMIZE, a TRUNCATE or a
// SYSTEM statement, none of which the installer may issue.
type recordingConn struct {
	clickhouse.Conn
	t     *testing.T
	mu    sync.Mutex
	execs []string
}

func (c *recordingConn) Exec(ctx context.Context, query string, args ...any) error {
	c.mu.Lock()
	c.execs = append(c.execs, query)
	c.mu.Unlock()
	upper := strings.ToUpper(strings.TrimSpace(query))
	if strings.Contains(upper, "REPLACE PARTITION") || strings.HasPrefix(upper, "OPTIMIZE") || strings.HasPrefix(upper, "TRUNCATE") || strings.HasPrefix(upper, "SYSTEM") {
		c.t.Errorf("the installer issued a statement it never may: %s", query)
	}
	return c.Conn.Exec(ctx, query, args...)
}

// alters returns, in order, the ALTER TABLE statements since the last reset.
func (c *recordingConn) alters() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, q := range c.execs {
		if strings.HasPrefix(q, "ALTER TABLE") {
			out = append(out, q)
		}
	}
	return out
}

func (c *recordingConn) reset() {
	c.mu.Lock()
	c.execs = nil
	c.mu.Unlock()
}

// fakeCopies is a fixed copy set.
type fakeCopies struct {
	mu   sync.Mutex
	list []copyset.Copy
}

func (f *fakeCopies) Copies() []copyset.Copy {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.list)
}

func (f *fakeCopies) Copy(tableKey string) (copyset.Copy, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.list {
		if c.TableKey == tableKey {
			return c, true
		}
	}
	return copyset.Copy{}, false
}

// row is one row of a test table: its partition (column day) and value.
type row struct {
	day string
	v   uint64
}

// fixture is one member ClickHouse with an owner database standing in for
// the owners' hg_safe, a copy set, sources per owner, a clock and a state
// directory that survives a member restart (newInstaller again).
type fixture struct {
	t        *testing.T
	raw      clickhouse.Conn
	conn     *recordingConn
	pinned   ddl.MemberPinned
	ownerDB  string
	stateDir string
	copies   *fakeCopies

	mu      sync.Mutex
	now     time.Time
	sources map[uint64][]sources.Source
}

func newFixture(t *testing.T, raw clickhouse.Conn) *fixture {
	t.Helper()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	prefix := "t32_" + hex.EncodeToString(b[:])
	f := &fixture{
		t: t, raw: raw, conn: &recordingConn{Conn: raw, t: t},
		pinned:   ddl.MemberPinned{MemberUnsafeDB: prefix + "_munsafe", ReplicaDB: prefix + "_replica", StageDB: prefix + "_rstage", NodeID: "member-a"},
		ownerDB:  prefix + "_owner",
		stateDir: t.TempDir(),
		copies:   &fakeCopies{},
		now:      time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC),
		sources:  map[uint64][]sources.Source{},
	}
	ctx := context.Background()
	if err := ddl.EnsureMemberDatabases(ctx, raw, f.pinned); err != nil {
		t.Fatal(err)
	}
	f.exec("CREATE DATABASE IF NOT EXISTS " + quoteIdent(f.ownerDB))
	t.Cleanup(func() {
		for _, db := range []string{f.pinned.ReplicaDB, f.pinned.StageDB, f.ownerDB} {
			_ = raw.Exec(context.Background(), "DROP DATABASE IF EXISTS "+quoteIdent(db)+" SYNC")
		}
	})
	return f
}

func (f *fixture) exec(query string) {
	f.t.Helper()
	if err := f.raw.Exec(context.Background(), query); err != nil {
		f.t.Fatalf("%s: %v", query, err)
	}
}

func (f *fixture) clock() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fixture) advance(d time.Duration) {
	f.mu.Lock()
	f.now = f.now.Add(d)
	f.mu.Unlock()
}

func (f *fixture) setSources(owner uint64, list ...sources.Source) {
	f.mu.Lock()
	f.sources[owner] = list
	f.mu.Unlock()
}

func (f *fixture) sourcesOf(owner uint64) []sources.Source {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.sources[owner]
}

func testSchema(key string, partitioned bool) payloadexec.TableSchema {
	s := payloadexec.TableSchema{TableID: key, Columns: []lthash.Column{{Name: "day", Type: "String"}, {Name: "v", Type: "UInt64"}}}
	if partitioned {
		s.PartitionBy = "day"
	}
	return s
}

// table adds owner's table key: its hg_safe stand-in in ownerDB and its ready
// copy of incarnation seq in ReplicaDB.
func (f *fixture) table(owner uint64, key string, seq uint64, partitioned bool) copyset.Copy {
	f.t.Helper()
	schema := testSchema(key, partitioned)
	_, safe, _, err := ddl.Intents(ddl.Pinned{UnsafeDB: "unused_unsafe", SafeDB: f.ownerDB, PromoteDB: "unused_promote", NodeID: "owner"}, schema)
	if err != nil {
		f.t.Fatal(err)
	}
	f.exec(safe.SQL())
	if err := ddl.EnsureCopy(context.Background(), f.raw, f.pinned, schema, seq); err != nil {
		f.t.Fatal(err)
	}
	c := copyset.Copy{Owner: owner, TableKey: key, IncarnationSeq: seq, Table: ddl.CopyTableName(key, seq), Status: wire.TableStatusActive, Schema: schema, Ready: true}
	f.copies.mu.Lock()
	f.copies.list = append(f.copies.list, c)
	f.copies.mu.Unlock()
	return c
}

func rowID(v uint64) string { return fmt.Sprintf("r%031d", v) }

// insertInto writes rows into db.table in one INSERT, so they form one part
// (or one per partition).
func (f *fixture) insertInto(db, table string, rows ...row) {
	f.t.Helper()
	values := make([]string, 0, len(rows))
	for _, r := range rows {
		values = append(values, fmt.Sprintf("(toFixedString(%s, 32), %s, %d)", quoteLiteral(rowID(r.v)), quoteLiteral(r.day), r.v))
	}
	f.exec(fmt.Sprintf("INSERT INTO %s (%s, day, v) VALUES %s", qualified(db, table), quoteIdent(ddl.RowIDColumn), strings.Join(values, ", ")))
}

// promote is the owner writing rows into its hg_safe stand-in.
func (f *fixture) promote(c copyset.Copy, rows ...row) {
	f.insertInto(f.ownerDB, ddl.CHTableName(c.TableKey), rows...)
}

// tableManifest describes what the owner's table holds now.
func (f *fixture) tableManifest(c copyset.Copy) replay.TableManifest {
	f.t.Helper()
	ctx := context.Background()
	table := ddl.CHTableName(c.TableKey)
	parts, err := chparts.ActiveParts(ctx, f.raw, f.ownerDB, table)
	if err != nil {
		f.t.Fatal(err)
	}
	scans, err := chexec.ScanAllParts(ctx, f.raw, f.ownerDB+"."+table, c.Schema)
	if err != nil {
		f.t.Fatal(err)
	}
	hashOf := map[string]chexec.PartScanResult{}
	for _, s := range scans {
		hashOf[s.PartName] = s
	}
	tm := replay.TableManifest{TableID: c.TableKey, SchemaHash: payloadexec.TableSchemaHash(testNetwork, c.Schema)}
	byPartition := map[string][]string{}
	for _, p := range parts {
		id := chparts.LogicalPartitionID(c.Schema, p)
		s := hashOf[p.Name]
		tm.ActiveParts = append(tm.ActiveParts, replay.PartManifestEntry{
			TableID: c.TableKey, PartitionID: id, PartName: p.Name, PartPhysHash: p.PhysHash, PartRowLtHash: s.RowLtHash, RowCount: s.RowCount, Bytes: p.Bytes,
		})
		byPartition[id] = append(byPartition[id], s.RowLtHash)
	}
	for _, id := range slices.Sorted(maps.Keys(byPartition)) {
		root, err := payloadexec.SumPartRowLtHashes(byPartition[id])
		if err != nil {
			f.t.Fatal(err)
		}
		tm.PartitionRoots = append(tm.PartitionRoots, replay.PartitionCommitment{TableID: c.TableKey, PartitionID: id, Root: root})
	}
	return tm
}

// frontier is the manifest entry of owner after seq blocks and promotions.
func frontier(owner, seq uint64) replay.TrackFrontier {
	return replay.TrackFrontier{IndexerID: owner, SafeBlockSeq: seq, AppliedPromotionSeq: seq}
}

// update is the feed's update for a manifest listing the tables of copies as
// their owners hold them now, with an entry of seq for every owner of them.
func (f *fixture) update(snapshotID string, seq uint64, copies ...copyset.Copy) manifestfeed.Update {
	f.t.Helper()
	u := manifestfeed.Update{SnapshotID: snapshotID, SafeBlockSeq: seq, Entries: map[uint64]replay.TrackFrontier{}}
	u.Manifest.SnapshotID = snapshotID
	for _, c := range copies {
		u.Manifest.Tables = append(u.Manifest.Tables, f.tableManifest(c))
		u.Entries[c.Owner] = frontier(c.Owner, seq)
	}
	return u
}

func (f *fixture) newInstaller(change func(*Config, *Deps)) *Installer {
	f.t.Helper()
	nonce, err := NewBootNonce()
	if err != nil {
		f.t.Fatal(err)
	}
	cfg := Config{Pinned: f.pinned, NetworkID: testNetwork, StateDir: f.stateDir, BootNonce: nonce, DiskBudgetBytes: 1 << 40}
	d := Deps{Conn: f.conn, Copies: f.copies, Sources: f.sourcesOf, Now: f.clock}
	if change != nil {
		change(&cfg, &d)
	}
	inst, err := New(cfg, d)
	if err != nil {
		f.t.Fatal(err)
	}
	return inst
}

// start is the role's startup order for the installer: drop the stale
// scratch, then audit.
func (f *fixture) start(inst *Installer) {
	f.t.Helper()
	ctx := context.Background()
	if err := inst.DropStaleScratch(ctx); err != nil {
		f.t.Fatal(err)
	}
	if err := inst.Audit(ctx, TriggerStartup); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) mustApply(inst *Installer, u manifestfeed.Update) {
	f.t.Helper()
	if err := inst.Apply(context.Background(), u); err != nil {
		f.t.Fatalf("Apply(%s): %v", u.SnapshotID, err)
	}
}

// copyHashes is the sorted multiset of row hashes the copy holds.
func (f *fixture) copyHashes(c copyset.Copy) []string {
	f.t.Helper()
	scans, err := chexec.ScanAllParts(context.Background(), f.raw, f.pinned.ReplicaDB+"."+c.Table, c.Schema)
	if err != nil {
		f.t.Fatal(err)
	}
	out := make([]string, 0, len(scans))
	for _, s := range scans {
		out = append(out, s.RowLtHash)
	}
	slices.Sort(out)
	return out
}

func manifestHashes(u manifestfeed.Update, key string) []string {
	var out []string
	for _, tm := range u.Manifest.Tables {
		if tm.TableID == key {
			for _, p := range tm.ActiveParts {
				out = append(out, p.PartRowLtHash)
			}
		}
	}
	slices.Sort(out)
	return out
}

func (f *fixture) requireCopyEqualsManifest(c copyset.Copy, u manifestfeed.Update) {
	f.t.Helper()
	if got, want := f.copyHashes(c), manifestHashes(u, c.TableKey); !slices.Equal(got, want) {
		f.t.Fatalf("copy %s holds %d parts, want the %d parts manifest %s lists (or different ones)", c.Table, len(got), len(want), u.SnapshotID)
	}
}

func (f *fixture) copyParts(c copyset.Copy) []chparts.Part {
	f.t.Helper()
	parts, err := nonEmptyParts(context.Background(), f.raw, f.pinned.ReplicaDB, c.Table)
	if err != nil {
		f.t.Fatal(err)
	}
	return parts
}

func (f *fixture) stageTables() []string {
	f.t.Helper()
	rows, err := f.raw.Query(context.Background(), "SELECT name FROM system.tables WHERE database = ? ORDER BY name", f.pinned.StageDB)
	if err != nil {
		f.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			f.t.Fatal(err)
		}
		out = append(out, name)
	}
	return out
}

// ownerPart is the owner's part that holds row r of table c.
func (f *fixture) ownerPart(c copyset.Copy, r row) string {
	f.t.Helper()
	var name string
	q := fmt.Sprintf("SELECT _part FROM %s WHERE v = ? LIMIT 1", qualified(f.ownerDB, ddl.CHTableName(c.TableKey)))
	if err := f.raw.QueryRow(context.Background(), q, r.v).Scan(&name); err != nil {
		f.t.Fatal(err)
	}
	return name
}

// pullTo copies the owner's part with need's hash_of_all_files into scratch
// times times, each in its own INSERT; zero copies nothing.
func (f *fixture) pullTo(ctx context.Context, need sources.Need, scratch sources.Scratch, times int) sources.Result {
	var name string
	q := "SELECT name FROM system.parts WHERE database = ? AND table = ? AND active AND hash_of_all_files = ?"
	if err := f.raw.QueryRow(ctx, q, f.ownerDB, need.Table, need.PartPhysHash).Scan(&name); err != nil {
		return sources.Result{Outcome: sources.OutcomeMissed}
	}
	for range times {
		insert := fmt.Sprintf("INSERT INTO %s SELECT * FROM %s WHERE _part = %s", qualified(scratch.Database, scratch.Table), qualified(f.ownerDB, need.Table), quoteLiteral(name))
		settings := clickhouse.Settings{"min_insert_block_size_rows": max(need.RowCount, 1), "min_insert_block_size_bytes": 0, "max_threads": 1, "max_insert_threads": 1}
		if err := f.raw.Exec(clickhouse.Context(ctx, clickhouse.WithSettings(settings)), insert); err != nil {
			return sources.Result{Outcome: sources.OutcomeRefused, Err: err}
		}
	}
	return sources.Result{Outcome: sources.OutcomeFetched}
}

// splitTo copies the owner's part row by row, one INSERT per row.
func (f *fixture) splitTo(ctx context.Context, need sources.Need, scratch sources.Scratch) sources.Result {
	q := fmt.Sprintf("SELECT toString(%s), day, v FROM %s WHERE _part IN (SELECT name FROM system.parts WHERE database = ? AND table = ? AND active AND hash_of_all_files = ?)",
		quoteIdent(ddl.RowIDColumn), qualified(f.ownerDB, need.Table))
	rows, err := f.raw.Query(ctx, q, f.ownerDB, need.Table, need.PartPhysHash)
	if err != nil {
		return sources.Result{Outcome: sources.OutcomeRefused, Err: err}
	}
	var all []row
	for rows.Next() {
		var id string
		var r row
		if err := rows.Scan(&id, &r.day, &r.v); err != nil {
			rows.Close()
			return sources.Result{Outcome: sources.OutcomeRefused, Err: err}
		}
		all = append(all, r)
	}
	rows.Close()
	for _, r := range all {
		f.insertInto(scratch.Database, scratch.Table, r)
	}
	return sources.Result{Outcome: sources.OutcomeFetched}
}

// fetchFunc is one scripted Fetch.
type fetchFunc func(ctx context.Context, need sources.Need, scratch sources.Scratch) sources.Result

// scripted is a source whose n-th Fetch runs steps[n] (the last step once
// they run out) and that records the parts it was asked for.
type scripted struct {
	name  string
	mu    sync.Mutex
	steps []fetchFunc
	asked []string
}

func (s *scripted) Name() string { return s.name }

func (s *scripted) Fetch(ctx context.Context, need sources.Need, _ payloadexec.TableSchema, scratch sources.Scratch) sources.Result {
	s.mu.Lock()
	n := len(s.asked)
	s.asked = append(s.asked, need.PartName)
	step := s.steps[min(n, len(s.steps)-1)]
	s.mu.Unlock()
	res := step(ctx, need, scratch)
	res.Source = s.name
	return res
}

func (s *scripted) calls() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.asked)
}

func (f *fixture) pull(times int) fetchFunc {
	return func(ctx context.Context, need sources.Need, scratch sources.Scratch) sources.Result {
		return f.pullTo(ctx, need, scratch, times)
	}
}

func (f *fixture) split() fetchFunc {
	return func(ctx context.Context, need sources.Need, scratch sources.Scratch) sources.Result {
		return f.splitTo(ctx, need, scratch)
	}
}

// foreign writes a row the manifest does not list into the scratch.
func (f *fixture) foreign(r row) fetchFunc {
	return func(ctx context.Context, need sources.Need, scratch sources.Scratch) sources.Result {
		f.insertInto(scratch.Database, scratch.Table, r)
		return sources.Result{Outcome: sources.OutcomeFetched}
	}
}

// restoreWithExtra is a restore whose backup is ahead of publication: the
// needed part and one more part.
func (f *fixture) restoreWithExtra(extra row) fetchFunc {
	return func(ctx context.Context, need sources.Need, scratch sources.Scratch) sources.Result {
		res := f.pullTo(ctx, need, scratch, 1)
		f.insertInto(scratch.Database, scratch.Table, extra)
		return res
	}
}

func outcome(o sources.Outcome, err error) fetchFunc {
	return func(context.Context, sources.Need, sources.Scratch) sources.Result {
		return sources.Result{Outcome: o, Err: err}
	}
}

var errMemoryLimit = errors.New("code: 241, message: (total) memory limit exceeded")

func (f *fixture) source(name string, steps ...fetchFunc) *scripted {
	return &scripted{name: name, steps: steps}
}

func requireRefusal(t *testing.T, err error, kind error, text string) {
	t.Helper()
	if !errors.Is(err, kind) {
		t.Fatalf("err = %v, want one wrapping %v", err, kind)
	}
	if !strings.Contains(err.Error(), text) {
		t.Fatalf("err = %q, want it to contain %q", err, text)
	}
}
