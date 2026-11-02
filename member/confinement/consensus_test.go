package confinement

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/consensus"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The stage-3 member consensus mode on the fenced server (Plan S3-E1 Task
// 62): the verify role's replay scratch as si_member, and the
// hg_member_unsafe reconciler as si_member_ddl.

const consensusNetwork = "confnet"

// statementRecorder passes every call to its connection and records the
// statement text of each Exec, Query, QueryRow and PrepareBatch.
type statementRecorder struct {
	clickhouse.Conn
	mu    sync.Mutex
	stmts []string
}

func (r *statementRecorder) record(q string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.stmts = append(r.stmts, q)
}

func (r *statementRecorder) Exec(ctx context.Context, q string, args ...any) error {
	r.record(q)
	return r.Conn.Exec(ctx, q, args...)
}

func (r *statementRecorder) Query(ctx context.Context, q string, args ...any) (driver.Rows, error) {
	r.record(q)
	return r.Conn.Query(ctx, q, args...)
}

func (r *statementRecorder) QueryRow(ctx context.Context, q string, args ...any) driver.Row {
	r.record(q)
	return r.Conn.QueryRow(ctx, q, args...)
}

func (r *statementRecorder) PrepareBatch(ctx context.Context, q string, opts ...driver.PrepareBatchOption) (driver.Batch, error) {
	r.record(q)
	return r.Conn.PrepareBatch(ctx, q, opts...)
}

func (r *statementRecorder) statements() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.stmts)
}

func TestMemberReplayScratchLivesInRstageUnderTheMemberPrefix(t *testing.T) {
	s := setup(t)
	nonce, err := install.NewBootNonce()
	if err != nil {
		t.Fatal(err)
	}
	prefix := consensus.ReplayScratchPrefix(memberNodeID, nonce)
	if !strings.HasPrefix(prefix, install.NodePrefix(memberNodeID)) || prefix != install.ScratchPrefix(memberNodeID, nonce)+"v" {
		t.Fatalf("replay prefix %q is not under the member's scratch prefix", prefix)
	}
	rec := &statementRecorder{Conn: s.member}
	m, err := consensus.NewReplayMaterializer(consensusNetwork, rec, memberNodeID, nonce, "default")
	if err != nil {
		t.Fatal(err)
	}
	schema := testSchema(s.name("replay"))
	st := replay.PreparedStatement{Statement: replay.Statement{StatementID: "st-1", TargetTableID: schema.TableID,
		PayloadFormat: payloadexec.PayloadFormatCSVWithNames}, Payload: []byte("p,v\na,1\nb,2\n")}
	for range 2 {
		rows, err := m.Materialize(context.Background(), schema, st)
		if err != nil {
			t.Fatalf("Materialize as %s: %v", memberUser, err) // a 497 lands here
		}
		if len(rows) != 2 {
			t.Fatalf("Materialize read back %d rows, want 2", len(rows))
		}
	}
	scratch := regexp.MustCompile("`([^`]+)`\\.`([^`]+)`")
	seen := map[string][]string{}
	for _, q := range rec.statements() {
		refs := scratch.FindAllStringSubmatch(q, -1)
		if len(refs) != 1 {
			t.Fatalf("statement %q names %d tables, want one scratch", q, len(refs))
		}
		db, table := refs[0][1], refs[0][2]
		if db != ddl.StageDatabase || !regexp.MustCompile("^"+regexp.QuoteMeta(prefix)+"replay_[0-9]+$").MatchString(table) {
			t.Fatalf("statement %q names %s.%s, want %s.%sreplay_<n>", q, db, table, ddl.StageDatabase, prefix)
		}
		verb := strings.Fields(q)[0]
		if verb == "CREATE" && !strings.Contains(q, "SETTINGS storage_policy = 'default'") {
			t.Fatalf("replay scratch omitted its explicitly configured policy: %q", q)
		}
		if verb == "CREATE" && !strings.Contains(q, ") ENGINE = MergeTree ORDER BY tuple()") {
			t.Fatalf("scratch %s is not created with an explicit MergeTree engine: %q", table, q)
		}
		seen[table] = append(seen[table], verb)
	}
	want := []string{"CREATE", "INSERT", "SELECT", "DROP"}
	if len(seen) != 2 || !slices.Equal(seen[prefix+"replay_1"], want) || !slices.Equal(seen[prefix+"replay_2"], want) {
		t.Fatalf("scratch statements %v, want %v for replay_1 and replay_2", seen, want)
	}
	if n := s.count(s.admin, "SELECT count() FROM system.tables WHERE database = ? AND startsWith(name, ?)", ddl.StageDatabase, prefix); n != 0 {
		t.Fatalf("%d replay scratch tables are left in %s", n, ddl.StageDatabase)
	}
}

// consensusView is an enabled registry view whose snapshot the test sets.
type consensusView struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	changed chan struct{}
}

func newConsensusView(snap wire.TableRegistrySnapshot) *consensusView {
	return &consensusView{snap: snap, changed: make(chan struct{})}
}

func (v *consensusView) View() (wire.TableRegistrySnapshot, bool) {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.snap, true
}

func (v *consensusView) Changed() <-chan struct{} {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.changed
}

func (v *consensusView) Ready() <-chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}

func (v *consensusView) set(snap wire.TableRegistrySnapshot) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.snap = snap
	close(v.changed)
	v.changed = make(chan struct{})
}

// consensusArbiter lists the owner's SNode and the member in the purge node
// set; a purge report fails the test (the reconciler never reports).
type consensusArbiter struct{ t *testing.T }

func (consensusArbiter) PurgeNodeSet(context.Context) ([]string, error) {
	return []string{memberNodeID, protocol.NodeID}, nil
}

func (a consensusArbiter) SubmitTablePurged(context.Context, string, uint64) error {
	a.t.Error("the member reconciler reported a purge")
	return errors.New("unexpected purge report")
}

// memberIncarnation is a chain incarnation of schema owned by indexer 0 on
// the incarnation-scoped Keeper path.
func memberIncarnation(t *testing.T, seq uint64, schema payloadexec.TableSchema, status wire.TableIncarnationStatus) wire.TableIncarnation {
	t.Helper()
	b, err := json.Marshal(schema)
	if err != nil {
		t.Fatal(err)
	}
	db, table, _ := strings.Cut(schema.TableID, ".")
	owner := uint64(0)
	return wire.TableIncarnation{Seq: seq, DatabaseID: db, TableID: table, Origin: wire.TableOriginChain, Status: status,
		SchemaJSON: string(b), SchemaHash: payloadexec.TableSchemaHash(consensusNetwork, schema), OwnerIndexerID: &owner, ScopedKeeperPath: true}
}

// memberGroupSnapshot places the member in G(0, <database>) with the purge
// switch on.
func memberGroupSnapshot(database string, incs ...wire.TableIncarnation) wire.TableRegistrySnapshot {
	return wire.TableRegistrySnapshot{Version: uint64(len(incs)) + 1, Incarnations: incs,
		Stage3: &arbiter.Stage3Params{GroupSize: 3, MinSafeCopies: 1, Purge: true, Placement: true},
		Groups: []wire.DatabaseGroup{{OwnerIndexerID: 0, DatabaseID: database, Generation: 1,
			Members: []wire.GroupMember{{NodeID: memberNodeID, JoinedGeneration: 1}}}}}
}

// activeParts lists a table's active parts as "name hash_of_all_files".
func (s *suite) activeParts(db, table string) []string {
	s.t.Helper()
	rows, err := s.admin.Query(context.Background(), "SELECT name, hash_of_all_files FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY name", db, table)
	if err != nil {
		s.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name, hash string
		if err := rows.Scan(&name, &hash); err != nil {
			s.t.Fatal(err)
		}
		out = append(out, name+" "+hash)
	}
	if err := rows.Err(); err != nil {
		s.t.Fatal(err)
	}
	return out
}

// keeperReplicas lists the replicas registered under a Keeper path.
func (s *suite) keeperReplicas(path string) []string {
	s.t.Helper()
	rows, err := s.admin.Query(context.Background(), "SELECT name FROM system.zookeeper WHERE path = ?", path+"/replicas")
	if err != nil {
		s.t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			s.t.Fatal(err)
		}
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// runReconciler runs a reconciler as si_member_ddl until the test ends.
func runReconciler(t *testing.T, ddlConn clickhouse.Conn, view *consensusView, stateDir string) *consensus.UnsafeReconciler {
	t.Helper()
	r, err := consensus.NewUnsafeReconciler(consensus.ReconcilerConfig{NodeID: memberNodeID, StateDir: stateDir, KeeperShardID: 0,
		CreateReplicas: true, Interval: 200 * time.Millisecond, NetworkID: consensusNetwork},
		consensus.ReconcilerDeps{DDLConn: ddlConn, Registry: view, Arbiter: consensusArbiter{t: t}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Errorf("reconciler Run = %v", err)
		}
	})
	return r
}

func TestMemberUnsafeReplicaAsSIMemberDDLReplicatesTheOwnersParts(t *testing.T) {
	s := setup(t)
	ddlConn := s.ddlUser()
	database := "confdb_" + s.sfx
	schema := payloadexec.TableSchema{TableID: database + ".swaps", PartitionBy: "p", Columns: testSchema("x").Columns}
	table := ddl.CHTableName(schema.TableID)
	const seq = 7
	path := ddl.ZooKeeperPathFor(0, schema.TableID, ddl.KeeperScope{IncarnationSeq: seq, Scoped: true})
	if path != "/sentio/0/unsafe_inc/"+table+"/7" {
		t.Fatalf("incarnation path %s", path)
	}

	// The owner's hg_unsafe replica of incarnation 7, made by the admin as the
	// owner's SNode makes it, with two parts.
	owner, _, _, err := ddl.Intents(protocol, schema)
	if err != nil {
		t.Fatal(err)
	}
	owner.ZooKeeperPath, owner.Comment = path, ddl.IncarnationComment(seq)
	s.exec(s.admin, "CREATE DATABASE IF NOT EXISTS hg_unsafe")
	s.exec(s.admin, owner.SQL())
	t.Cleanup(func() {
		_ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualified(ddl.MemberUnsafeDatabase, table)+" SYNC")
		_ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualified("hg_unsafe", table)+" SYNC")
	})
	for i, p := range []string{"a", "b"} {
		s.exec(s.admin, fmt.Sprintf("INSERT INTO %s (_hg_row_id, p, v) VALUES ('%s', '%s', %d)", qualified("hg_unsafe", table), rowID(i), p, i+1))
	}
	ownerParts := s.activeParts("hg_unsafe", table)
	if len(ownerParts) != 2 {
		t.Fatalf("owner parts %v, want 2", ownerParts)
	}

	// The reconciler, as si_member_ddl, creates member-conf's replica at the
	// incarnation's path, and it fetches the owner's parts byte for byte.
	inc := memberIncarnation(t, seq, schema, wire.TableStatusActive)
	view := newConsensusView(memberGroupSnapshot(database, inc))
	r := runReconciler(t, ddlConn, view, t.TempDir())
	waitFor(t, "the member replica", func() bool { return r.Stats().Replicas == 1 })
	s.exec(s.member, "SYSTEM SYNC REPLICA "+qualified(ddl.MemberUnsafeDatabase, table))
	if got := s.activeParts(ddl.MemberUnsafeDatabase, table); !slices.Equal(got, ownerParts) {
		t.Fatalf("member parts %v, want the owner's %v (same names and hash_of_all_files)", got, ownerParts)
	}
	if got := s.keeperReplicas(path); !slices.Equal(got, []string{memberNodeID, protocol.NodeID}) {
		t.Fatalf("replicas at %s: %v", path, got)
	}
	// The verify role's readiness check passes as si_member.
	intent, err := ddl.MemberUnsafeIntent(pinned, schema, seq, path)
	if err != nil {
		t.Fatal(err)
	}
	if err := ddl.VerifyProtocolTable(context.Background(), s.member, intent); err != nil {
		t.Fatalf("VerifyProtocolTable as %s: %v", memberUser, err)
	}

	// Purging and quiet: the replica is dropped with DROP TABLE … SYNC; the
	// owner's table, its parts and its Keeper replica stay.
	inc.Status = wire.TableStatusPurging
	view.set(memberGroupSnapshot(database, inc))
	waitFor(t, "the member replica to be dropped", func() bool {
		return r.Stats().Replicas == 0 && !s.tableExists(ddl.MemberUnsafeDatabase, table)
	})
	if got := s.keeperReplicas(path); !slices.Equal(got, []string{protocol.NodeID}) {
		t.Fatalf("replicas at %s after the drop: %v, want only the owner's", path, got)
	}
	if got := s.activeParts("hg_unsafe", table); !slices.Equal(got, ownerParts) {
		t.Fatalf("owner parts after the member drop %v, want %v", got, ownerParts)
	}
}

func TestMemberUnsafeReconcilerStopsOnAStaleReplicaAndDropsNothing(t *testing.T) {
	s := setup(t)
	ddlConn := s.ddlUser()
	database := "confdb_" + s.sfx
	schema := payloadexec.TableSchema{TableID: database + ".pools", PartitionBy: "p", Columns: testSchema("x").Columns}
	table := ddl.CHTableName(schema.TableID)
	const seq = 8
	path := ddl.ZooKeeperPathFor(0, schema.TableID, ddl.KeeperScope{IncarnationSeq: seq, Scoped: true})
	// A replica named member-conf already exists at the path: the admin's
	// table stands in for one a member left when it lost its state.
	stale := "stale_" + s.sfx
	s.exec(s.admin, "CREATE DATABASE IF NOT EXISTS hg_unsafe")
	s.exec(s.admin, fmt.Sprintf("CREATE TABLE %s (_hg_row_id FixedString(32), p String, v UInt64) ENGINE = ReplicatedMergeTree(%s, %s) PARTITION BY p ORDER BY (p, _hg_row_id)",
		qualified("hg_unsafe", stale), literal(path), literal(memberNodeID)))
	t.Cleanup(func() {
		_ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualified("hg_unsafe", stale)+" SYNC")
	})
	view := newConsensusView(memberGroupSnapshot(database, memberIncarnation(t, seq, schema, wire.TableStatusActive)))
	r := runReconciler(t, ddlConn, view, t.TempDir())
	waitFor(t, "the REPLICA_ALREADY_EXISTS stop", func() bool { return r.Stats().ReplicaAlreadyExists == 1 })
	time.Sleep(time.Second) // several more passes
	if st := r.Stats(); st.ReplicaAlreadyExists != 1 || st.Replicas != 0 || st.Failures[schema.TableID] != 1 {
		t.Fatalf("stats %+v: want one stop and no replica", st)
	}
	if s.tableExists(ddl.MemberUnsafeDatabase, table) || !s.tableExists("hg_unsafe", stale) {
		t.Fatal("the stop created a member table or dropped the stale replica's table")
	}
	if got := s.keeperReplicas(path); !slices.Equal(got, []string{memberNodeID}) {
		t.Fatalf("replicas at %s: %v, want the stale member-conf left in place", path, got)
	}
}
