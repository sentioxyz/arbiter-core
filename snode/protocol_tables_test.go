package snode

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

func requireKeeperS(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but no Keeper: %v", err)
		}
		t.Skipf("no Keeper configured: %v", err)
	}
}

func TestRegister_EnsuresProtocolTablesThenFailsClosedOnDrift(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	server := &snodeFakeServer{}
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)

	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	schema := intakeSchema()
	schema.TableID = "db.t_" + suffix
	cfg := testConfigS(t)
	cfg.NodeID = "snode-" + suffix
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	setUniqueDatabases(t, &cfg)
	t.Cleanup(func() {
		for _, database := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" SYNC")
		}
	})
	role, err := New(cfg, Deps{Client: client, Conn: conn})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}

	if err := role.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	table := CHTableName(schema.TableID)
	var engine string
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", role.cfg.UnsafeDatabase, table).Scan(&engine); err != nil || engine != "ReplicatedMergeTree" {
		t.Fatalf("hg_unsafe engine = %q err=%v", engine, err)
	}
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", role.cfg.SafeDatabase, table).Scan(&engine); err != nil || engine != "MergeTree" {
		t.Fatalf("hg_safe engine = %q err=%v", engine, err)
	}
	if regs, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("registration must still happen after ensure: %+v", regs)
	}

	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1", role.cfg.SafeDatabase, table)); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	err = role.Register(ctx)
	if !errors.Is(err, ddl.ErrProtocolTableDrift) {
		t.Fatalf("drift must fail closed before re-registration, got %v", err)
	}
	if regs, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("drifted role must not register again: %+v", regs)
	}
}

func TestRegister_ProtocolTablesModeRequiresConn(t *testing.T) {
	cfg := testConfigS(t)
	cfg.ProtocolTables = ddl.ModeVerifyOnly
	server := &snodeFakeServer{}
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	role, err := New(cfg, Deps{Client: client})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}
	if err := role.Register(context.Background()); err == nil {
		t.Fatal("ensure mode without a ClickHouse connection must fail")
	}
}

func TestConfigRejectsNegativeProtocolTablesReconcile(t *testing.T) {
	cfg := testConfigS(t)
	cfg.ProtocolTablesReconcile = -time.Second
	if err := cfg.validate(); err == nil {
		t.Fatal("negative protocol table reconcile interval must fail validation")
	}
}

func TestConfigRejectsNegativeHardPartsPerPartition(t *testing.T) {
	cfg := testConfigS(t)
	cfg.HardPartsPerPartition = -1
	if err := cfg.validate(); err == nil {
		t.Fatal("negative hard parts per partition must fail validation")
	}
}

func newProtocolTableRunHarnessS(t *testing.T, conn clickhouse.Conn) (*Role, *snodeFakeServer, payloadexec.TableSchema) {
	t.Helper()
	requireKeeperS(t, conn)
	server := &snodeFakeServer{}
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	schema := intakeSchema()
	schema.TableID = "db.run_" + suffix
	cfg := testConfigS(t)
	cfg.NodeID = "snode-run-" + suffix
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	cfg.ProtocolTablesReconcile = 20 * time.Millisecond
	setUniqueDatabases(t, &cfg)
	t.Cleanup(func() {
		for _, database := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" SYNC")
		}
	})
	role, err := New(cfg, Deps{Client: client, Conn: conn})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatalf("register: %v", err)
	}
	return role, server, schema
}

func waitSNodeSubscriptions(t *testing.T, server *snodeFakeServer, wantStarts, wantActive int) {
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

type countingProtocolConnS struct {
	clickhouse.Conn
	reads atomic.Int64
}

func (c *countingProtocolConnS) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	c.reads.Add(1)
	return c.Conn.Query(ctx, query, args...)
}

func (c *countingProtocolConnS) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	c.reads.Add(1)
	return c.Conn.QueryRow(ctx, query, args...)
}

func waitForProtocolReadsS(t *testing.T, conn *countingProtocolConnS) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for conn.reads.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if conn.reads.Load() == 0 {
		t.Fatal("periodic reconcile did not read protocol table metadata")
	}
}

func TestRun_ReconcileIsVerifyOnlyAndDroppedTableFailsClosed(t *testing.T) {
	conn := requireCH(t)
	role, server, schema := newProtocolTableRunHarnessS(t, conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()
	waitSNodeSubscriptions(t, server, 1, 1)

	qualified := role.cfg.UnsafeDatabase + "." + CHTableName(schema.TableID)
	if err := conn.Exec(context.Background(), "DROP TABLE "+qualified+" SYNC"); err != nil {
		t.Fatalf("drop protocol table: %v", err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, ddl.ErrProtocolTableMissing) {
			t.Fatalf("Run err = %v, want ErrProtocolTableMissing", err)
		}
	case <-time.After(2 * time.Second):
		cancel()
		<-done
		t.Fatal("Run did not fail closed after a protocol table was dropped")
	}
	waitSNodeSubscriptions(t, server, 1, 0)
	var count uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.tables WHERE database = ? AND name = ?", role.cfg.UnsafeDatabase, CHTableName(schema.TableID)).Scan(&count); err != nil {
		t.Fatalf("count dropped table: %v", err)
	}
	if count != 0 {
		t.Fatalf("verify-only reconcile recreated the dropped table, count=%d", count)
	}
}

func TestRun_CancellationDoesNotLeakReconcileOrSubscriptionAcrossRuns(t *testing.T) {
	conn := requireCH(t)
	role, server, _ := newProtocolTableRunHarnessS(t, conn)
	for run := 1; run <= 2; run++ {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- role.Run(ctx) }()
		waitSNodeSubscriptions(t, server, run, 1)
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("run %d exit = %v, want context canceled", run, err)
		}
		waitSNodeSubscriptions(t, server, run, 0)
	}
}

func TestRun_SubscriptionFailureStopsReconcileBeforeReturn(t *testing.T) {
	conn := requireCH(t)
	role, server, _ := newProtocolTableRunHarnessS(t, conn)
	counting := &countingProtocolConnS{Conn: conn}
	role.d.Conn = counting
	release := make(chan struct{})
	server.failSubscriptionWhen(release, status.Error(codes.InvalidArgument, "subscription rejected"))

	parent := context.Background()
	done := make(chan error, 1)
	go func() { done <- role.Run(parent) }()
	waitSNodeSubscriptions(t, server, 1, 1)
	waitForProtocolReadsS(t, counting)
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
	waitSNodeSubscriptions(t, server, 1, 0)
	readsAtReturn := counting.reads.Load()
	time.Sleep(4 * role.cfg.ProtocolTablesReconcile)
	if got := counting.reads.Load(); got != readsAtReturn {
		t.Fatalf("reconcile worker still read metadata after Run returned: got %d reads, want %d", got, readsAtReturn)
	}
}
