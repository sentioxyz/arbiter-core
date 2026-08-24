package snode

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/lthash"
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
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
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
	cfg.SchemaSource = ddl.SchemaSourceClickHouse
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

func TestConfigRejectsNegativeProtocolTablesMaxFailures(t *testing.T) {
	cfg := testConfigS(t)
	cfg.ProtocolTablesMaxFailures = -1
	if err := cfg.validate(); err == nil {
		t.Fatal("negative protocol table reconcile max failures must fail validation")
	}
}

func TestConfigRequiresSchemaSource(t *testing.T) {
	cfg := testConfigS(t)
	cfg.SchemaSource = ""
	if err := cfg.validate(); err == nil {
		t.Fatal("an unset schema_source must be rejected; the old zero value silently disabled the lifecycle")
	}
}

func TestConfigDerivesProtocolTableMode(t *testing.T) {
	cfg := testConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceClickHouse
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got, err := cfg.ProtocolTablesMode(); err != nil || got != ddl.ModeVerifyOnly {
		t.Fatalf("clickhouse schema source derived %v, %v; want verify, nil", got, err)
	}
	cfg = testConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	if err := cfg.validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if got, err := cfg.ProtocolTablesMode(); err != nil || got != ddl.ModeCreateAndVerify {
		t.Fatalf("network_state schema source derived %v, %v; want create, nil", got, err)
	}
}

func TestConfigRejectsNegativeHardPartsPerPartition(t *testing.T) {
	cfg := testConfigS(t)
	cfg.HardPartsPerPartition = -1
	if err := cfg.validate(); err == nil {
		t.Fatal("negative hard parts per partition must fail validation")
	}
}

func TestConfigRejectsColumnTypeOutsideWhitelist(t *testing.T) {
	cfg := testConfigS(t)
	schema := intakeSchema()
	schema.Columns = append(schema.Columns, lthash.Column{Name: "bad", Type: "Nullable(String)"})
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	err := cfg.validate()
	if !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
		t.Fatalf("validate = %v, want ErrUnsupportedColumnType", err)
	}
	if !strings.Contains(err.Error(), `tables[0]: table db.t column "bad"`) {
		t.Fatalf("validate lacks shared table/column context: %v", err)
	}
}

func TestConfigRejectsPartitionFreezeViolation(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate     func(*payloadexec.TableSchema)
		wantDetail string
	}{
		"expression": {
			mutate:     func(s *payloadexec.TableSchema) { s.PartitionBy = "toYYYYMM(d)" },
			wantDetail: `got expression "toYYYYMM(d)"`,
		},
		"non_string": {
			mutate: func(s *payloadexec.TableSchema) {
				s.Columns[0].Type = "UInt64"
				s.PartitionBy = s.Columns[0].Name
			},
			wantDetail: `column "p" has type UInt64`,
		},
		"undeclared": {
			mutate:     func(s *payloadexec.TableSchema) { s.PartitionBy = "nope" },
			wantDetail: `"nope" names no declared column`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			cfg := testConfigS(t)
			schema := intakeSchema()
			tc.mutate(&schema)
			cfg.Tables = []payloadexec.TableSchema{schema}
			cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
			err := cfg.validate()
			if !errors.Is(err, ddl.ErrPartitionFreeze) {
				t.Fatalf("validate = %v, want ErrPartitionFreeze", err)
			}
			for _, want := range []string{"tables[0] (db.t): " + ddl.ErrPartitionFreeze.Error(), tc.wantDetail} {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("validate lacks shared context %q: %v", want, err)
				}
			}
		})
	}
}

func TestConfigValidationOrdersPartitionBeforeColumnTypes(t *testing.T) {
	cfg := testConfigS(t)
	schema := intakeSchema()
	schema.PartitionBy = "toYYYYMM(d)"
	schema.Columns = append(schema.Columns, lthash.Column{Name: "bad", Type: "Nullable(String)"})
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	err := cfg.validate()
	if !errors.Is(err, ddl.ErrPartitionFreeze) || !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
		t.Fatalf("validate = %v, want both partition and column-type errors", err)
	}
	partitionAt := strings.Index(err.Error(), "tables[0] (db.t): "+ddl.ErrPartitionFreeze.Error())
	typeAt := strings.Index(err.Error(), `tables[0]: table db.t column "bad"`)
	if partitionAt < 0 || typeAt < 0 || partitionAt >= typeAt {
		t.Fatalf("validation error order = %q, want partition freeze before column types", err)
	}
}

func TestReconcile_RetriesTransientErrorsAndDiesOnDrift(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transientErr := errors.New("connection reset by peer")
		driftErr := fmt.Errorf("wrapped drift: %w", ddl.ErrProtocolTableDrift)
		attempts := 0
		role := &Role{
			cfg: Config{
				ProtocolTablesReconcile:   time.Minute,
				ProtocolTablesMaxFailures: 3,
			},
			d: Deps{Logger: slog.Default()},
			ensureFn: func(context.Context, ddl.Mode) error {
				attempts++
				if attempts == 1 {
					return transientErr
				}
				return driftErr
			},
		}

		err := role.reconcileProtocolTables(t.Context())
		if !errors.Is(err, ddl.ErrProtocolTableDrift) {
			t.Fatalf("reconcile = %v, want ErrProtocolTableDrift", err)
		}
		if attempts != 2 {
			t.Fatalf("attempts = %d, want transient retry followed by drift", attempts)
		}
	})
}

func TestReconcile_GivesUpAfterMaxConsecutiveTransientFailures(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		transientErr := errors.New("connection refused")
		attempts := 0
		role := &Role{
			cfg: Config{
				ProtocolTablesReconcile:   time.Minute,
				ProtocolTablesMaxFailures: 2,
			},
			d: Deps{Logger: slog.Default()},
			ensureFn: func(_ context.Context, mode ddl.Mode) error {
				if mode != ddl.ModeVerifyOnly {
					t.Fatalf("reconcile mode = %s, want verify", mode)
				}
				attempts++
				return transientErr
			},
		}

		err := role.reconcileProtocolTables(t.Context())
		if !errors.Is(err, transientErr) || !strings.Contains(err.Error(), "failed 2 consecutive times") {
			t.Fatalf("reconcile = %v, want wrapped two-failure exhaustion", err)
		}
		if attempts != 2 {
			t.Fatalf("attempts = %d, want exactly ProtocolTablesMaxFailures", attempts)
		}
	})
}

func TestReconcile_DefaultMaxFailuresAndSuccessReset(t *testing.T) {
	t.Run("zero uses default", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			transientErr := errors.New("EOF")
			attempts := 0
			role := &Role{
				cfg: Config{ProtocolTablesReconcile: time.Minute},
				d:   Deps{Logger: slog.Default()},
				ensureFn: func(context.Context, ddl.Mode) error {
					attempts++
					return transientErr
				},
			}

			err := role.reconcileProtocolTables(t.Context())
			if !errors.Is(err, transientErr) {
				t.Fatalf("reconcile = %v, want wrapped transient error", err)
			}
			if attempts != ddl.DefaultReconcileMaxFailures {
				t.Fatalf("attempts = %d, want default %d", attempts, ddl.DefaultReconcileMaxFailures)
			}
		})
	})

	t.Run("success resets consecutive failures", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			transientErr := errors.New("connection reset")
			attempts := 0
			role := &Role{
				cfg: Config{
					ProtocolTablesReconcile:   time.Minute,
					ProtocolTablesMaxFailures: 2,
				},
				d: Deps{Logger: slog.Default()},
				ensureFn: func(context.Context, ddl.Mode) error {
					attempts++
					if attempts == 2 {
						return nil
					}
					return transientErr
				},
			}

			err := role.reconcileProtocolTables(t.Context())
			if !errors.Is(err, transientErr) {
				t.Fatalf("reconcile = %v, want wrapped transient error", err)
			}
			if attempts != 4 {
				t.Fatalf("attempts = %d, want failure, success, then two consecutive failures", attempts)
			}
		})
	})
}

func TestReconcile_CancellationTakesPrecedenceOverEnsureError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		role := &Role{
			cfg: Config{ProtocolTablesReconcile: time.Minute},
			d:   Deps{Logger: slog.Default()},
			ensureFn: func(context.Context, ddl.Mode) error {
				cancel()
				return fmt.Errorf("late drift: %w", ddl.ErrProtocolTableDrift)
			},
		}

		err := role.reconcileProtocolTables(ctx)
		if !errors.Is(err, context.Canceled) || errors.Is(err, ddl.ErrProtocolTableDrift) {
			t.Fatalf("reconcile = %v, want context cancellation precedence", err)
		}
	})
}

func TestRunWithReady_EnsuresBeforeConvergeAndWorkers(t *testing.T) {
	server := &snodeFakeServer{}
	role := newPrepareTestRole(t, server)
	rec := testRecord("0xabc:8:startup-ensure-order")
	rec.Envelope.TargetTableID = "unknown.table"
	if err := role.journal.save(rec); err != nil {
		t.Fatalf("seed intake record: %v", err)
	}

	startupErr := errors.New("startup protocol-table ensure failed")
	role.cfg.protocolTables = ddl.ModeCreateAndVerify
	var gotModes []ddl.Mode
	role.ensureFn = func(_ context.Context, mode ddl.Mode) error {
		gotModes = append(gotModes, mode)
		return startupErr
	}
	readyCalls := 0
	err := role.RunWithReady(t.Context(), func() { readyCalls++ })
	if !errors.Is(err, startupErr) || strings.Contains(err.Error(), "converge schema") {
		t.Fatalf("RunWithReady = %v, want startup ensure before convergence", err)
	}
	if len(gotModes) != 1 || gotModes[0] != ddl.ModeCreateAndVerify {
		t.Fatalf("startup ensure modes = %v, want [create]", gotModes)
	}
	if readyCalls != 0 {
		t.Fatalf("ready calls = %d, want 0 after startup ensure failure", readyCalls)
	}
	if starts, active := server.subscriptionSnapshot(); starts != 0 || active != 0 {
		t.Fatalf("subscription crossed startup ensure failure: starts=%d active=%d", starts, active)
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
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
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

type blockingProtocolEnsureS struct {
	initial     func(context.Context, ddl.Mode) error
	cancelErr   error
	mu          sync.Mutex
	calls       int
	modes       []ddl.Mode
	releaseOnce sync.Once
	entered     chan struct{}
	cancelSeen  chan struct{}
	allowExit   chan struct{}
	exited      chan struct{}
}

func newBlockingProtocolEnsureS(initial func(context.Context, ddl.Mode) error, cancelErr error) *blockingProtocolEnsureS {
	return &blockingProtocolEnsureS{
		initial:    initial,
		cancelErr:  cancelErr,
		entered:    make(chan struct{}),
		cancelSeen: make(chan struct{}),
		allowExit:  make(chan struct{}),
		exited:     make(chan struct{}),
	}
}

func (p *blockingProtocolEnsureS) ensure(ctx context.Context, mode ddl.Mode) error {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.modes = append(p.modes, mode)
	p.mu.Unlock()
	if call == 1 {
		return p.initial(ctx, mode)
	}
	if call != 2 {
		return fmt.Errorf("unexpected protocol-table ensure call %d", call)
	}
	close(p.entered)
	<-ctx.Done()
	close(p.cancelSeen)
	<-p.allowExit
	close(p.exited)
	return p.cancelErr
}

func (p *blockingProtocolEnsureS) release() {
	p.releaseOnce.Do(func() { close(p.allowExit) })
}

func (p *blockingProtocolEnsureS) modesSnapshot() []ddl.Mode {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]ddl.Mode(nil), p.modes...)
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

func TestRun_SubscriptionFailureJoinsReconcileBeforeReturn(t *testing.T) {
	conn := requireCH(t)
	role, server, _ := newProtocolTableRunHarnessS(t, conn)
	probe := newBlockingProtocolEnsureS(role.ensureFn, errors.New("reconcile cancellation artifact"))
	t.Cleanup(probe.release)
	role.ensureFn = probe.ensure
	subscriptionRelease := make(chan struct{})
	server.failSubscriptionWhen(subscriptionRelease, status.Error(codes.InvalidArgument, "subscription rejected"))

	parent := context.Background()
	done := make(chan error, 1)
	go func() { done <- role.Run(parent) }()
	waitSNodeSubscriptions(t, server, 1, 1)
	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("periodic reconcile did not enter the blocking metadata probe")
	}
	close(subscriptionRelease)
	select {
	case <-probe.cancelSeen:
	case <-time.After(2 * time.Second):
		t.Fatal("subscription failure did not cancel the in-flight reconcile probe")
	}
	select {
	case err := <-done:
		t.Fatalf("Run returned before the reconcile probe exited: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	probe.release()
	select {
	case <-probe.exited:
	case <-time.After(2 * time.Second):
		t.Fatal("reconcile probe did not exit after release")
	}
	if got := probe.modesSnapshot(); len(got) != 2 || got[0] != ddl.ModeCreateAndVerify || got[1] != ddl.ModeVerifyOnly {
		t.Fatalf("ensure modes = %v, want [create verify]", got)
	}
	select {
	case err := <-done:
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("Run err = %v, want InvalidArgument", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after the reconcile probe exited")
	}
	if parent.Err() != nil {
		t.Fatalf("parent context unexpectedly ended: %v", parent.Err())
	}
	waitSNodeSubscriptions(t, server, 1, 0)
}
