package snode

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ownerAuditConnS is a ClickHouse for the owner audit's background runs without
// a server: uptime() from a start time the test moves, an empty system.parts
// (failing once after failNextListing), and an Exec that succeeds. Any other
// method is unimplemented (the embedded nil Conn panics).
type ownerAuditConnS struct {
	clickhouse.Conn
	mu       sync.Mutex
	started  time.Time
	partsErr error
	listings int
}

func (c *ownerAuditConnS) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	c.mu.Lock()
	defer c.mu.Unlock()
	if query != "SELECT toUInt64(uptime())" {
		return uptimeRowS{err: fmt.Errorf("unexpected query %q", query)}
	}
	return uptimeRowS{uptime: uint64(time.Since(c.started) / time.Second)}
}

func (c *ownerAuditConnS) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !strings.Contains(query, "FROM system.parts") {
		return nil, fmt.Errorf("unexpected query %q", query)
	}
	c.listings++
	if err := c.partsErr; err != nil {
		c.partsErr = nil
		return nil, err
	}
	return emptyRowsS{}, nil
}

func (c *ownerAuditConnS) Exec(context.Context, string, ...any) error { return nil }

func (c *ownerAuditConnS) restart() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = time.Now()
}

func (c *ownerAuditConnS) failNextListing(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.partsErr = err
}

type uptimeRowS struct {
	uptime uint64
	err    error
}

func (r uptimeRowS) Err() error { return r.err }

func (r uptimeRowS) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	p, ok := dest[0].(*uint64)
	if !ok || len(dest) != 1 {
		return fmt.Errorf("scan into %v, want one *uint64", dest)
	}
	*p = r.uptime
	return nil
}

func (r uptimeRowS) ScanStruct(any) error {
	return errors.New("uptimeRowS: ScanStruct is not supported")
}

// emptyRowsS is a result without rows.
type emptyRowsS struct{}

func (emptyRowsS) Next() bool                       { return false }
func (emptyRowsS) Scan(...any) error                { return errors.New("emptyRowsS: no row") }
func (emptyRowsS) ScanStruct(any) error             { return errors.New("emptyRowsS: no row") }
func (emptyRowsS) ColumnTypes() []driver.ColumnType { return nil }
func (emptyRowsS) Totals(...any) error              { return nil }
func (emptyRowsS) Columns() []string                { return nil }
func (emptyRowsS) Close() error                     { return nil }
func (emptyRowsS) Err() error                       { return nil }
func (emptyRowsS) HasData() bool                    { return false }

func newOwnerAuditWorkerRoleS(t *testing.T, mode OwnerAuditMode) (*Role, *ownerAuditConnS) {
	t.Helper()
	conn := &ownerAuditConnS{started: time.Now().Add(-time.Hour)}
	cfg := testConfigS(t)
	cfg.OwnerAudit = mode
	cfg.OwnerAuditProbeInterval = MinOwnerAuditProbeInterval
	role, err := New(cfg, Deps{Client: startClaimsFakeS(t, &claimsFakeS{}), Conn: conn})
	if err != nil {
		t.Fatal(err)
	}
	return role, conn
}

// TestOwnerAuditWorkerReauditsEveryTableAfterAClickHouseRestart: in repair mode
// a table is held from New until the startup audit completed; the watcher then
// sees ClickHouse's start time move, holds every table again and the worker
// re-audits them (housegate spec 2026-10-14 §14). The arbiter here serves no
// GetPartitionStates, which is counted and leaves the local inventory (K3).
func TestOwnerAuditWorkerReauditsEveryTableAfterAClickHouseRestart(t *testing.T) {
	role, conn := newOwnerAuditWorkerRoleS(t, OwnerAuditRepair)
	if role.OwnerAuditClean("db.t") {
		t.Fatal("a repair-mode table is clean before its startup audit")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := role.startOwnerAudit(ctx)
	defer stop()
	stats := role.OwnerAuditStats()
	if stats.Runs[OwnerAuditTriggerStartup] != 1 || stats.ClickHouseRestarts != 0 || len(stats.Unclean) != 0 || stats.LastRun.IsZero() || stats.FSMUnavailable != 1 {
		t.Fatalf("stats after the startup audit = %+v", stats)
	}
	if !role.OwnerAuditClean("db.t") {
		t.Fatal("the table is held after a clean startup audit")
	}
	conn.restart()
	waitForS(t, 10*time.Second, "the re-audit after the ClickHouse restart", func() bool {
		s := role.OwnerAuditStats()
		return s.ClickHouseRestarts == 1 && s.Runs[OwnerAuditTriggerClickHouseRestart] == 1
	})
	if !role.OwnerAuditClean("db.t") {
		t.Fatal("the table is held after a clean re-audit")
	}
	time.Sleep(2 * MinOwnerAuditProbeInterval)
	if s := role.OwnerAuditStats(); s.ClickHouseRestarts != 1 || s.Runs[OwnerAuditTriggerClickHouseRestart] != 1 {
		t.Fatalf("a start time that stays put was reported again: %+v", s)
	}
}

// TestOwnerAuditWorkerRetriesATableWhoseAuditFailed: a ClickHouse error during
// the startup audit holds the table (repair mode) and never fails the role; the
// worker retries the pass on its next probe interval.
func TestOwnerAuditWorkerRetriesATableWhoseAuditFailed(t *testing.T) {
	role, conn := newOwnerAuditWorkerRoleS(t, OwnerAuditRepair)
	conn.failNextListing(errors.New("read tcp 127.0.0.1:9000: connection reset by peer"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := role.startOwnerAudit(ctx)
	defer stop()
	if role.OwnerAuditClean("db.t") {
		t.Fatal("a table whose startup audit failed is not held in repair mode")
	}
	// A pass releases its tables before it counts its run, so wait for the
	// count: once the retry is counted, its table is released.
	waitForS(t, 10*time.Second, "the retried startup audit", func() bool {
		return role.OwnerAuditStats().Runs[OwnerAuditTriggerStartup] >= 2
	})
	if s := role.OwnerAuditStats(); s.Runs[OwnerAuditTriggerStartup] != 2 || !role.OwnerAuditClean("db.t") {
		t.Fatalf("startup runs = %d, clean %v; want the failed run and its clean retry", s.Runs[OwnerAuditTriggerStartup], role.OwnerAuditClean("db.t"))
	}
}

// TestOwnerAuditPassAsksAFailingArbiterOnce: once the arbiter's partition
// states fail in a full pass, the pass audits its other tables on their own
// inventories without asking again, so an unreachable arbiter costs a pass
// (and the startup that waits for it) one OwnerAuditFSMTimeout rather than one
// per table (clarification K3). Each table still counts FSMUnavailable.
func TestOwnerAuditPassAsksAFailingArbiterOnce(t *testing.T) {
	cfg := testConfigS(t)
	cfg.Tables = append(cfg.Tables, payloadexec.TableSchema{TableID: "db.u", Columns: cfg.Tables[0].Columns})
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.OwnerAudit = OwnerAuditRepair
	// InvalidArgument is not retried, so each read reaches the fake once.
	fsm := &fsmPartitionsFakeS{err: status.Error(codes.InvalidArgument, "injected arbiter refusal")}
	role, _ := newOwnerAuditHarnessS(t, &ownerAuditConnS{started: time.Now().Add(-time.Hour)}, cfg, fsm)

	role.runOwnerAuditPass(context.Background())

	s := role.OwnerAuditStats()
	if fsm.readCount() != 1 || s.FSMUnavailable != 2 || len(s.Unclean) != 0 || !role.OwnerAuditClean("db.t") || !role.OwnerAuditClean("db.u") {
		t.Fatalf("arbiter reads %d, stats %+v; want one read, two unavailable counts and both tables clean", fsm.readCount(), s)
	}
}

// TestOwnerAuditWorkerReleasesARepairedPartition: in repair mode the worker
// re-audits, every probe interval, each partition with a finding, so one that
// is clean again (an operator removed the foreign part; here the fake's
// hg_safe is empty) stops holding its table without a promotion or a restart
// (housegate spec 2026-10-14 §14: Pending only while unclean). The re-audit
// is not a run.
func TestOwnerAuditWorkerReleasesARepairedPartition(t *testing.T) {
	role, _ := newOwnerAuditWorkerRoleS(t, OwnerAuditRepair)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := role.startOwnerAudit(ctx)
	defer stop()
	role.audit.set(OwnerAuditFinding{TableID: "db.t", PartitionID: "all", Reason: OwnerAuditUnacknowledgedPart,
		Parts: []string{"all_9_9_0"}, Trigger: OwnerAuditTriggerStartup})

	waitForS(t, 10*time.Second, "the re-audit of the repaired partition", func() bool { return role.OwnerAuditClean("db.t") })

	if s := role.OwnerAuditStats(); len(s.Unclean) != 0 || s.Runs[OwnerAuditTriggerStartup] != 1 || s.Runs[OwnerAuditTriggerClickHouseRestart] != 0 {
		t.Fatalf("stats after the re-audit = %+v, want no finding and only the startup run", s)
	}
}

// TestOwnerAuditReportModeNeverHoldsATable: in report mode no state of the
// audit, before it ran, while it fails, or with a finding, holds a table.
func TestOwnerAuditReportModeNeverHoldsATable(t *testing.T) {
	role, conn := newOwnerAuditWorkerRoleS(t, OwnerAuditReport)
	conn.failNextListing(errors.New("read tcp 127.0.0.1:9000: connection reset by peer"))
	if !role.OwnerAuditClean("db.t") {
		t.Fatal("report mode held a table before its startup audit")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop := role.startOwnerAudit(ctx)
	defer stop()
	if !role.OwnerAuditClean("db.t") {
		t.Fatal("report mode held a table whose audit failed")
	}
	role.audit.set(OwnerAuditFinding{TableID: "db.t", PartitionID: "all", Reason: OwnerAuditUnacknowledgedPart})
	if !role.OwnerAuditClean("db.t") || role.OwnerAuditStats().Mode != OwnerAuditReport {
		t.Fatal("report mode held a table with a finding")
	}
}
