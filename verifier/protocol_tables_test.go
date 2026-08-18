package verifier

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

func TestNew_ProtocolTablesModeRequiresConn(t *testing.T) {
	cfg := testConfigV()
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	defer client.Close()
	if _, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}}); err == nil {
		t.Fatal("ensure mode without a ClickHouse connection must fail at construction")
	}
}

func TestRegister_EnsuresProtocolTablesOnVerifier(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but no Keeper: %v", err)
		}
		t.Skipf("no Keeper configured: %v", err)
	}
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	server := newVerifierFakeServer()
	addr := startVerifierFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	cfg := testConfigV()
	cfg.ReplicaID = "verifier-" + suffix
	sch := scanTableSchema()
	sch.TableID = "db.t_" + suffix
	cfg.Tables = []payloadexec.TableSchema{sch}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.UnsafeDatabase = "hg_unsafe_" + suffix
	cfg.SafeDatabase = "hg_safe_" + suffix
	cfg.PromoteDatabase = "hg_promote_" + suffix
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	t.Cleanup(func() {
		for _, database := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" SYNC")
		}
	})
	role, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}, Conn: conn})
	if err != nil {
		t.Fatalf("new role: %v", err)
	}
	if err := role.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	var replica string
	if err := conn.QueryRow(ctx, "SELECT replica_name FROM system.replicas WHERE database = ? AND table = ?", cfg.UnsafeDatabase, ddl.CHTableName(sch.TableID)).Scan(&replica); err != nil || replica != cfg.ReplicaID {
		t.Fatalf("replica_name = %q err=%v want %q", replica, err, cfg.ReplicaID)
	}
	if regs, _, _, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("registration after ensure: %+v", regs)
	}
}

func TestConfigRejectsNegativeProtocolTablesReconcile(t *testing.T) {
	cfg := testConfigV()
	cfg.ProtocolTablesReconcile = -time.Second
	if err := cfg.validate(); err == nil {
		t.Fatal("negative protocol table reconcile interval must fail validation")
	}
}

func newProtocolTableRunHarnessV(t *testing.T, conn clickhouse.Conn) (*Role, *verifierFakeServer, payloadexec.TableSchema) {
	t.Helper()
	var keeperRows uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&keeperRows); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but no Keeper: %v", err)
		}
		t.Skipf("no Keeper configured: %v", err)
	}
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	server := newVerifierFakeServer()
	addr := startVerifierFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	cfg := testConfigV()
	cfg.ReplicaID = "verifier-run-" + suffix
	schema := scanTableSchema()
	schema.TableID = "db.run_" + suffix
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.UnsafeDatabase = "hg_unsafe_run_" + suffix
	cfg.SafeDatabase = "hg_safe_run_" + suffix
	cfg.PromoteDatabase = "hg_promote_run_" + suffix
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	cfg.ProtocolTablesReconcile = 20 * time.Millisecond
	t.Cleanup(func() {
		for _, database := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" SYNC")
		}
	})
	role, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}, Conn: conn})
	if err != nil {
		t.Fatalf("new verifier: %v", err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatalf("register: %v", err)
	}
	return role, server, schema
}

func waitVerifierSubscriptions(t *testing.T, server *verifierFakeServer, wantStarts, wantActive int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		starts, active := server.subscriptionSnapshot()
		if starts >= wantStarts && active == wantActive {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	starts, active := server.subscriptionSnapshot()
	t.Fatalf("subscriptions starts=%d active=%d, want starts>=%d active=%d", starts, active, wantStarts, wantActive)
}

type countingProtocolConnV struct {
	clickhouse.Conn
	reads atomic.Int64
}

func (c *countingProtocolConnV) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.reads.Add(1)
	return c.Conn.Query(ctx, query, args...)
}

func (c *countingProtocolConnV) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.reads.Add(1)
	return c.Conn.QueryRow(ctx, query, args...)
}

func waitForProtocolReadsV(t *testing.T, conn *countingProtocolConnV) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for conn.reads.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if conn.reads.Load() == 0 {
		t.Fatal("periodic reconcile did not read protocol table metadata")
	}
}

func TestRun_ReconcileDriftFailsClosedAndCancelsSubscription(t *testing.T) {
	conn := requireCH(t)
	role, server, schema := newProtocolTableRunHarnessV(t, conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()
	waitVerifierSubscriptions(t, server, 1, 1)

	qualified := role.cfg.SafeDatabase + "." + ddl.CHTableName(schema.TableID)
	if err := conn.Exec(context.Background(), "ALTER TABLE "+qualified+" MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1"); err != nil {
		t.Fatalf("tamper safe setting: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ddl.ErrProtocolTableDrift) {
			t.Fatalf("Run err = %v, want ErrProtocolTableDrift", err)
		}
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("Run did not fail closed after protocol table drift")
	}
	waitVerifierSubscriptions(t, server, 1, 0)
}

func TestRun_CancellationDoesNotLeakVerifierSubscriptionAcrossRuns(t *testing.T) {
	conn := requireCH(t)
	role, server, _ := newProtocolTableRunHarnessV(t, conn)
	for run := 1; run <= 2; run++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- role.Run(ctx) }()
		waitVerifierSubscriptions(t, server, run, 1)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("run %d exit = %v, want context canceled", run, err)
		}
		waitVerifierSubscriptions(t, server, run, 0)
	}
}

func TestRun_SubscriptionFailureStopsReconcileBeforeReturn(t *testing.T) {
	conn := requireCH(t)
	role, server, _ := newProtocolTableRunHarnessV(t, conn)
	counting := &countingProtocolConnV{Conn: conn}
	role.d.Conn = counting
	release := make(chan struct{})
	server.failSubscriptionWhen(release, status.Error(codes.InvalidArgument, "subscription rejected"))

	parent := context.Background()
	done := make(chan error, 1)
	go func() { done <- role.Run(parent) }()
	waitVerifierSubscriptions(t, server, 1, 1)
	waitForProtocolReadsV(t, counting)
	close(release)
	select {
	case err := <-done:
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Run err = %v, want InvalidArgument", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after non-retryable subscription failure")
	}
	if parent.Err() != nil {
		t.Fatalf("parent context unexpectedly ended: %v", parent.Err())
	}
	waitVerifierSubscriptions(t, server, 1, 0)
	readsAtReturn := counting.reads.Load()
	time.Sleep(4 * role.cfg.ProtocolTablesReconcile)
	if got := counting.reads.Load(); got != readsAtReturn {
		t.Fatalf("reconcile worker still read metadata after Run returned: got %d reads, want %d", got, readsAtReturn)
	}
}
