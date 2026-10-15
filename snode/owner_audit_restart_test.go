package snode

import (
	"context"
	"os"
	"slices"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
)

// requireRestartableCHS returns a connection to CH_RESTART_ADDR: a ClickHouse
// whose container restarts itself (docker --restart always, fixed host port)
// and that no other test target shares.
func requireRestartableCHS(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_RESTART") != "1" {
		t.Skip("set ARBITER_CH_RESTART=1 and CH_RESTART_ADDR (a ClickHouse whose container restarts itself) to run")
	}
	addr := os.Getenv("CH_RESTART_ADDR")
	if addr == "" {
		t.Fatal("ARBITER_CH_RESTART=1 requires CH_RESTART_ADDR")
	}
	return chtest.Open(t, addr)
}

// waitUptimeS waits until the server has been up for at least d, so that a
// restart moves its start time by more than the watcher's skew.
func waitUptimeS(t *testing.T, ctx context.Context, conn clickhouse.Conn, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d + time.Minute)
	for {
		started, err := chrestart.StartedAt(ctx, conn, time.Now())
		if err == nil && time.Since(started) >= d {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("server not up for %v after a minute (last error %v)", d, err)
		}
		time.Sleep(time.Second)
	}
}

// killClickHouseS aborts the server with SYSTEM KILL, the abrupt restart that
// M3 pins as resurrecting the parts a REPLACE PARTITION retired, and waits until
// it answers again with a later start time.
func killClickHouseS(t *testing.T, ctx context.Context, conn clickhouse.Conn) {
	t.Helper()
	before, err := chrestart.StartedAt(ctx, conn, time.Now())
	if err != nil {
		t.Fatalf("start time before the kill: %v", err)
	}
	// The server dies while it answers, so the statement's own error is expected.
	_ = conn.Exec(ctx, "SYSTEM KILL")
	deadline := time.Now().Add(2 * time.Minute)
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		after, err := chrestart.StartedAt(probeCtx, conn, time.Now())
		cancel()
		if err == nil && after.After(before.Add(chrestart.DefaultRestartSkew)) {
			return
		}
	}
	t.Fatal("ClickHouse did not come back with a later start time within 2m: does its container run with --restart always?")
}

// waitWritableS waits until the hg_unsafe replica accepts writes again: after a
// restart a ReplicatedMergeTree is read-only until its Keeper session is back.
func waitWritableS(t *testing.T, ctx context.Context, conn clickhouse.Conn, db, table string) {
	t.Helper()
	deadline := time.Now().Add(time.Minute)
	for {
		var readonly uint8
		err := conn.QueryRow(ctx, "SELECT is_readonly FROM system.replicas WHERE database = ? AND table = ?", db, table).Scan(&readonly)
		if err == nil && readonly == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s.%s is still read-only a minute after the restart (last error %v)", db, table, err)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// TestOwnerAuditRepairsResurrectedPartsAcrossAClickHouseRestart is the
// restart-resurrection hazard end to end (housegate spec 2026-10-14 §2, §14,
// §17 "the owner audit across a ClickHouse-only restart"): the second promotion's
// REPLACE PARTITION retires the first promotion's part, an abrupt restart inside
// old_parts_lifetime reactivates it beside its equal-hash replacement, the
// watcher sees the start time move and the restart pass audits the table. Repair
// mode drops the twin, the rows are back and the third promotion goes through;
// report mode drops nothing and reports the twin.
func TestOwnerAuditRepairsResurrectedPartsAcrossAClickHouseRestart(t *testing.T) {
	conn := requireRestartableCHS(t)
	for _, mode := range []OwnerAuditMode{OwnerAuditRepair, OwnerAuditReport} {
		t.Run(string(mode), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			f := newOwnerAuditFixtureS(t, conn, mode, nil)
			f.promoteAndClean(ctx, 1)
			f.promoteAndClean(ctx, 2)
			ack := f.lastAck()
			if got := f.safeRows(ctx); got != 4 {
				t.Fatalf("safe rows after two promotions = %d, want 4", got)
			}
			stop := f.role.startOwnerAudit(ctx)
			defer stop()
			if s := f.role.OwnerAuditStats(); s.Runs[OwnerAuditTriggerStartup] != 1 || len(s.Unclean) != 0 {
				t.Fatalf("startup audit = %+v, want one clean run", s)
			}

			waitUptimeS(t, ctx, conn, 2*chrestart.DefaultRestartSkew)
			killClickHouseS(t, ctx, conn)
			waitForS(t, 2*time.Minute, "the owner audit after the ClickHouse restart", func() bool {
				s := f.role.OwnerAuditStats()
				return s.ClickHouseRestarts >= 1 && s.Runs[OwnerAuditTriggerClickHouseRestart] >= 1 && !f.role.audit.due()
			})

			stats := f.role.OwnerAuditStats()
			switch mode {
			case OwnerAuditRepair:
				if stats.DroppedTwins != 1 || len(stats.Unclean) != 0 {
					t.Fatalf("stats = %+v, want the one resurrected twin dropped and nothing unclean (no resurrection at all? see M3)", stats)
				}
				if got := f.safeRows(ctx); got != 4 {
					t.Fatalf("safe rows after the repair = %d, want 4", got)
				}
				assertCompleteSafeInventory(t, ctx, f.role, f.schema, auditPartitionS, ack.SafePartitionParts)
				if !f.role.OwnerAuditClean(f.schema.TableID) {
					t.Fatal("the repaired table is held")
				}
				waitWritableS(t, ctx, conn, f.role.cfg.UnsafeDatabase, f.table())
				third := f.submit(ctx, 3)
				if err := f.promote(ctx, 3, third); err != nil {
					t.Fatalf("third promotion after the repair: %v", err)
				}
				if got := f.safeRows(ctx); got != 6 {
					t.Fatalf("safe rows after the third promotion = %d, want 6", got)
				}
			case OwnerAuditReport:
				if stats.DroppedTwins != 0 || len(stats.Unclean) != 1 {
					t.Fatalf("stats = %+v, want one finding and no drop (no resurrection at all? see M3)", stats)
				}
				u := stats.Unclean[0]
				if u.Reason != OwnerAuditUnacknowledgedPart || u.Trigger != OwnerAuditTriggerClickHouseRestart || len(u.Parts) != 1 {
					t.Fatalf("finding = %+v, want one resurrected part found by the restart pass", u)
				}
				if got := f.safeRows(ctx); got != 6 {
					t.Fatalf("safe rows = %d, want 6: the resurrected part's rows are served twice and report mode leaves them", got)
				}
				var twin partInfo
				for _, p := range f.safeParts(ctx) {
					if p.Name == u.Parts[0] {
						twin = p
					}
				}
				if twin.Name == "" || !slices.ContainsFunc(ack.SafePartitionParts, func(m arbiter.SafePartMapping) bool {
					return m.PartPhysHash == twin.PhysHash && m.SafePartName != twin.Name
				}) {
					t.Fatalf("the resurrected part %+v is not the equal-hash twin of an acknowledged part %+v", twin, ack.SafePartitionParts)
				}
				if !f.role.OwnerAuditClean(f.schema.TableID) {
					t.Fatal("report mode held the table back")
				}
			}
		})
	}
}
