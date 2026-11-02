package consensus

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The fixture member: authoritytest's member-b (Plan S3-E1 Task 59) on host
// indexer 1, a peer of owner indexer 0, whose database dex has one chain
// table, dex.swaps.
const (
	testNetworkID = "testnet"
	testGenesisID = "genesis-snap"
	testNodeID    = authoritytest.MemberNodeIDB
	testOwner     = uint64(0)
	testBootNonce = "0a1b2c3d"
)

var swapsSchema = payloadexec.TableSchema{TableID: "dex.swaps", PartitionBy: "p",
	Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}

func testConfig(t *testing.T) Config {
	t.Helper()
	return Config{NodeID: testNodeID, NetworkID: testNetworkID, GenesisSnapshotID: testGenesisID, StateDir: t.TempDir(), StoragePolicy: "default",
		Ed25519Seed: authoritytest.MemberKeyB().Seed(), KeeperShardID: 0}
}

// chainIncarnation is dex.swaps (or another table of dex) as the registry
// records a chain-origin incarnation of it.
func chainIncarnation(t *testing.T, seq uint64, schema payloadexec.TableSchema, status wire.TableIncarnationStatus, scoped bool) wire.TableIncarnation {
	t.Helper()
	b, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	db, table, _ := strings.Cut(schema.TableID, ".")
	owner := testOwner
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: status,
		SchemaJSON: string(b), SchemaHash: payloadexec.TableSchemaHash(testNetworkID, schema), OwnerIndexerID: &owner,
		ScopedKeeperPath: scoped}
}

// groupSnapshot is an enabled registry snapshot with the purge switch as
// given, the incarnations, and one group G(0, dex) whose members are given.
func groupSnapshot(purge bool, members []string, leaving []wire.LeavingMember, incs ...wire.TableIncarnation) wire.TableRegistrySnapshot {
	snap := wire.TableRegistrySnapshot{Version: 1, Incarnations: incs,
		Stage3: &arbiter.Stage3Params{GroupSize: 3, MinSafeCopies: 1, Purge: purge, Placement: true}}
	if members != nil || leaving != nil {
		g := wire.DatabaseGroup{OwnerIndexerID: testOwner, DatabaseID: "dex", Generation: 1, Leaving: leaving}
		for _, m := range members {
			g.Members = append(g.Members, wire.GroupMember{NodeID: m, JoinedGeneration: 1})
		}
		snap.Groups = []wire.DatabaseGroup{g}
	}
	return snap
}

// viewFake is a registry view whose snapshot a test sets.
type viewFake struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	changed chan struct{}
}

func newViewFake(snap wire.TableRegistrySnapshot) *viewFake {
	return &viewFake{snap: snap, enabled: true, changed: make(chan struct{})}
}

func (v *viewFake) View() (wire.TableRegistrySnapshot, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snap, v.enabled
}

func (v *viewFake) Changed() <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.changed
}

func (v *viewFake) Ready() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (v *viewFake) set(snap wire.TableRegistrySnapshot) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snap = snap
	close(v.changed)
	v.changed = make(chan struct{})
}

// arbiterFake is the reconciler's tableset.Arbiter: PurgeNodeSet answers
// nodes; a purge report is a test failure (the reconciler never reports).
type arbiterFake struct {
	mu      sync.Mutex
	nodes   []string
	err     error
	calls   int
	reports int
}

func (a *arbiterFake) PurgeNodeSet(context.Context) ([]string, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.calls++
	return slices.Clone(a.nodes), a.err
}

func (a *arbiterFake) SubmitTablePurged(context.Context, string, uint64) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reports++
	return errors.New("test: the reconciler must never report a purge")
}

// chTable is one table of the fake server.
type chTable struct {
	database, name, comment, engine, engineFull, sortingKey, partitionKey string
	zkPath, replica                                                       string
	columns                                                               []lthash.Column
}

// chFake is a clickhouse.Conn that keeps the tables of hg_member_unsafe:
// it answers the reconciler's DDL and system.tables listing, the queries of
// ddl.VerifyProtocolTable and the byte-side scan query, and records every
// statement. createErr makes the CREATE of one table name fail. Any other
// method panics (nil embedded Conn).
type chFake struct {
	clickhouse.Conn
	mu        sync.Mutex
	tables    map[string]*chTable // by database.name
	statement []string
	createErr map[string]error
	scanRows  map[string][][]any // by qualified table, rows of the scan query
	scanQuery []string
}

func newCHFake() *chFake {
	return &chFake{tables: map[string]*chTable{}, createErr: map[string]error{}, scanRows: map[string][][]any{}}
}

var (
	createTablePattern = regexp.MustCompile("(?s)^CREATE TABLE IF NOT EXISTS `([^`]+)`\\.`([^`]+)` \\(.*\\) ENGINE = ReplicatedMergeTree\\('([^']*)', '([^']*)'\\).*COMMENT '([^']*)'$")
	dropTablePattern   = regexp.MustCompile("^DROP TABLE IF EXISTS `([^`]+)`\\.`([^`]+)` SYNC$")
)

func (c *chFake) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statement = append(c.statement, query)
	switch {
	case query == "CREATE DATABASE IF NOT EXISTS `hg_member_unsafe`":
		return nil
	case createTablePattern.MatchString(query):
		m := createTablePattern.FindStringSubmatch(query)
		if err := c.createErr[m[2]]; err != nil {
			return err
		}
		if _, ok := c.tables[m[1]+"."+m[2]]; ok {
			return nil // IF NOT EXISTS
		}
		c.tables[m[1]+"."+m[2]] = &chTable{database: m[1], name: m[2], comment: m[5], engine: ddl.EngineReplicatedMergeTree,
			engineFull: fmt.Sprintf("ReplicatedMergeTree('%s', '%s') ORDER BY _hg_row_id SETTINGS index_granularity = 8192", m[3], m[4]),
			zkPath:     m[3], replica: m[4]}
		return nil
	case dropTablePattern.MatchString(query):
		m := dropTablePattern.FindStringSubmatch(query)
		delete(c.tables, m[1]+"."+m[2])
		return nil
	}
	return fmt.Errorf("chFake: unexpected statement %q", query)
}

func (c *chFake) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statement = append(c.statement, query)
	switch {
	case strings.HasPrefix(query, "SELECT name, comment, engine_full FROM system.tables WHERE database = ?"):
		var rows [][]any
		for _, t := range c.sortedLocked(args[0].(string)) {
			rows = append(rows, []any{t.name, t.comment, t.engineFull})
		}
		return &rowsFake{rows: rows}, nil
	case strings.HasPrefix(query, "SELECT name, type FROM system.columns"):
		t, ok := c.tables[args[0].(string)+"."+args[1].(string)]
		var rows [][]any
		if ok {
			for _, col := range t.columns {
				rows = append(rows, []any{col.Name, col.Type})
			}
		}
		return &rowsFake{rows: rows}, nil
	case strings.HasPrefix(query, "SELECT _part, _hg_row_id"):
		c.scanQuery = append(c.scanQuery, query)
		for qualified, rows := range c.scanRows {
			if strings.Contains(query, " FROM "+qualified+" WHERE _part IN (") {
				return &rowsFake{rows: rows}, nil
			}
		}
		return &rowsFake{}, nil
	}
	return nil, fmt.Errorf("chFake: unexpected query %q", query)
}

func (c *chFake) QueryRow(_ context.Context, query string, args ...any) driver.Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.statement = append(c.statement, query)
	switch {
	case strings.Contains(query, "SELECT engine, engine_full, sorting_key, partition_key, comment"):
		t, ok := c.tables[args[0].(string)+"."+args[1].(string)]
		if !ok {
			return rowFake{err: sql.ErrNoRows}
		}
		return rowFake{values: []any{t.engine, t.engineFull, t.sortingKey, t.partitionKey, t.comment}}
	case strings.HasPrefix(query, "SELECT zookeeper_path, replica_name FROM system.replicas"):
		t, ok := c.tables[args[0].(string)+"."+args[1].(string)]
		if !ok {
			return rowFake{err: sql.ErrNoRows}
		}
		return rowFake{values: []any{t.zkPath, t.replica}}
	}
	return rowFake{err: fmt.Errorf("chFake: unexpected query %q", query)}
}

// addIntent adds a table exactly as ClickHouse would show the one intent
// creates.
func (c *chFake) addIntent(intent ddl.TableIntent) {
	c.mu.Lock()
	defer c.mu.Unlock()
	settings := make([]string, 0, len(intent.Settings)+1)
	for _, s := range intent.Settings {
		settings = append(settings, s.Name+" = "+s.Value)
	}
	settings = append(settings, "index_granularity = 8192")
	head := fmt.Sprintf("ReplicatedMergeTree('%s', '%s')", intent.ZooKeeperPath, intent.ReplicaName)
	if intent.PartitionKey != "" {
		head += " PARTITION BY " + intent.PartitionKey
	}
	c.tables[intent.Database+"."+intent.Table] = &chTable{database: intent.Database, name: intent.Table, comment: intent.Comment,
		engine: intent.Engine, engineFull: head + " ORDER BY (" + strings.Join(intent.SortingKey, ", ") + ") SETTINGS " + strings.Join(settings, ", "),
		sortingKey: strings.Join(intent.SortingKey, ", "), partitionKey: intent.PartitionKey,
		zkPath: intent.ZooKeeperPath, replica: intent.ReplicaName, columns: slices.Clone(intent.Columns)}
}

// addRaw adds a table with the given comment and engine_full only.
func (c *chFake) addRaw(database, name, comment, engineFull string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tables[database+"."+name] = &chTable{database: database, name: name, comment: comment, engine: ddl.EngineReplicatedMergeTree, engineFull: engineFull}
}

func (c *chFake) has(database, name string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, ok := c.tables[database+"."+name]
	return ok
}

func (c *chFake) statements() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.statement)
}

// writes lists the recorded Exec statements (DDL), not the reads.
func (c *chFake) writes() []string {
	var out []string
	for _, s := range c.statements() {
		if !strings.HasPrefix(s, "SELECT") && !strings.HasPrefix(strings.TrimSpace(s), "SELECT") {
			out = append(out, s)
		}
	}
	return out
}

func (c *chFake) sortedLocked(database string) []*chTable {
	var out []*chTable
	for _, t := range c.tables {
		if t.database == database {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].name < out[j].name })
	return out
}

// rowsFake is driver.Rows over fixed rows: Scan assigns each value to the
// destination it points to. Any other method panics (nil embedded Rows).
type rowsFake struct {
	driver.Rows
	rows [][]any
	i    int
}

func (r *rowsFake) Next() bool {
	if r.i >= len(r.rows) {
		return false
	}
	r.i++
	return true
}

func (r *rowsFake) Scan(dest ...any) error { return assign(r.rows[r.i-1], dest) }
func (r *rowsFake) Err() error             { return nil }
func (r *rowsFake) Close() error           { return nil }

// rowFake is driver.Row over one fixed row or an error.
type rowFake struct {
	values []any
	err    error
}

func (r rowFake) Err() error { return r.err }

func (r rowFake) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	return assign(r.values, dest)
}

func (r rowFake) ScanStruct(any) error { return errors.New("rowFake: ScanStruct is not supported") }

func assign(values, dest []any) error {
	if len(values) != len(dest) {
		return fmt.Errorf("scan %d values into %d destinations", len(values), len(dest))
	}
	for i, v := range values {
		d := reflect.ValueOf(dest[i])
		if d.Kind() != reflect.Pointer || !reflect.ValueOf(v).Type().AssignableTo(d.Elem().Type()) {
			return fmt.Errorf("scan %T into %T", v, dest[i])
		}
		d.Elem().Set(reflect.ValueOf(v))
	}
	return nil
}

// gatewayFake serves the arbiter RPCs the member calls: Membership
// (RegisterNode, MarkActive), the verifier dispatch stream and evidence
// submissions, and SafeState.GetBlockVerification from blocks.
type gatewayFake struct {
	pb.UnimplementedVerifierGatewayServer
	pb.UnimplementedMembershipServer
	pb.UnimplementedSafeStateServer

	mu             sync.Mutex
	regs           []*pb.NodeRegistration
	marks          []*pb.NodeRef
	hellos         []string
	attestations   []*pb.ReplayAttestation
	scans          []*pb.ByteSideScanMsg
	blocks         map[uint64]*pb.BlockVerification
	reads          []uint64
	dispatch       chan *pb.VerifierDispatch
	afterBlockRead func()
}

func newGatewayFake() *gatewayFake {
	return &gatewayFake{blocks: map[uint64]*pb.BlockVerification{}, dispatch: make(chan *pb.VerifierDispatch, 8)}
}

func (f *gatewayFake) RegisterNode(_ context.Context, m *pb.NodeRegistration) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.regs = append(f.regs, m)
	return &pb.Ack{}, nil
}

func (f *gatewayFake) MarkActive(_ context.Context, m *pb.NodeRef) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, m)
	return &pb.Ack{}, nil
}

func (f *gatewayFake) SubscribeVerifierDispatch(hello *pb.VerifierHello, stream grpc.ServerStreamingServer[pb.VerifierDispatch]) error {
	f.mu.Lock()
	f.hellos = append(f.hellos, hello.GetReplicaId())
	f.mu.Unlock()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case d := <-f.dispatch:
			if err := stream.Send(d); err != nil {
				return err
			}
		}
	}
}

func (f *gatewayFake) SubmitAttestation(_ context.Context, m *pb.ReplayAttestation) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attestations = append(f.attestations, m)
	return &pb.Ack{}, nil
}

func (f *gatewayFake) SubmitByteSideScan(_ context.Context, m *pb.ByteSideScanMsg) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scans = append(f.scans, m)
	return &pb.Ack{}, nil
}

func (f *gatewayFake) GetBlockVerification(_ context.Context, ref *pb.L3BlockRef) (*pb.BlockVerification, error) {
	f.mu.Lock()
	f.reads = append(f.reads, ref.GetL3BlockSeq())
	bv, ok := f.blocks[ref.GetL3BlockSeq()]
	hook := f.afterBlockRead
	f.mu.Unlock()
	if !ok {
		return nil, status.Errorf(codes.NotFound, "no sealed block %d", ref.GetL3BlockSeq())
	}
	if hook != nil {
		hook()
	}
	return bv, nil
}

func (f *gatewayFake) snapshot() (regs []*pb.NodeRegistration, marks []*pb.NodeRef, hellos []string, atts []*pb.ReplayAttestation, scans []*pb.ByteSideScanMsg) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.regs), slices.Clone(f.marks), slices.Clone(f.hellos), slices.Clone(f.attestations), slices.Clone(f.scans)
}

// blockView is a sealed block's verification view as GetBlockVerification
// answers it.
func blockView(seq uint64, verifierSet []string, verdict bool) *pb.BlockVerification {
	return &pb.BlockVerification{Header: &pb.L3BlockHeader{L3BlockSeq: seq}, VerifierSet: verifierSet, VerdictEvaluated: verdict}
}

// startGatewayFake serves f on a loopback port and returns a client for it.
func startGatewayFake(t *testing.T, f *gatewayFake) *dataplane.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterVerifierGatewayServer(srv, f)
	pb.RegisterMembershipServer(srv, f)
	pb.RegisterSafeStateServer(srv, f)
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ln)
		close(done)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// replayFake answers every job with att and records it; before, when set,
// runs first inside Verify.
type replayFake struct {
	mu     sync.Mutex
	jobs   []replay.ReplayJob
	att    replay.ReplayAttestation
	before func(job replay.ReplayJob)
}

func (r *replayFake) Verify(_ context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) {
	if r.before != nil {
		r.before(job)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs = append(r.jobs, job)
	return r.att, nil
}

func (r *replayFake) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.jobs)
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func textLogger(w *syncBuffer) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }

// swapsJob is block seq's replay job with one INSERT into dex.swaps.
func swapsJob(seq uint64) replay.ReplayJob {
	return replay.ReplayJob{BlockSeq: seq, PrevSafeSnapshotID: "snap-1", PrevStateRoot: "0xprev", SchemaSnapshotID: "schema",
		ExecutorProfileID: "housegate-replay-mvp-v0", SourceClaimRoot: "0xsource",
		Statements: []replay.Statement{{StatementID: "st-1", StatementSeq: 1, TargetTableID: "dex.swaps"}}}
}

func testMemberPinned() ddl.MemberPinned {
	p := ddl.DefaultMemberPinned(testNodeID, 0, "")
	p.StoragePolicy = "default"
	return p
}
