package install

import (
	"context"
	"os"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
)

// requireRestartableCH returns a connection to CH_RESTART_ADDR: a ClickHouse
// whose container restarts itself (docker --restart always, fixed host port)
// and that no other test target shares (scripts/ci/clickhouse-mechanics.sh's
// node B).
func requireRestartableCH(t *testing.T) clickhouse.Conn {
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

// waitUptime waits until the server has been up for at least d, so that a
// restart moves its start time by more than chrestart's skew.
func waitUptime(t *testing.T, ctx context.Context, conn clickhouse.Conn, d time.Duration) {
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

// killClickHouse aborts the server with SYSTEM KILL, the abrupt restart that
// mechanics M3 pins as reactivating the parts a partition replacement retired,
// and waits until it answers again with a later start time.
func killClickHouse(t *testing.T, ctx context.Context, conn clickhouse.Conn) {
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

// TestInstallDivergedRepairSurvivesAReloadInsideTheOutdatedWindow: a Diverged
// partition repaired with DROP PARTITION and ATTACH PARTITION ... FROM stays
// repaired across an abrupt ClickHouse restart well inside old_parts_lifetime
// (480 s): the dropped foreign part does not come back and no twin appears,
// so the restart audit is clean and the next pass has nothing to do (spec §2,
// §8.5 step 2, §17 "Diverged repair with a reload inside the outdated
// window"; mechanics M4).
func TestInstallDivergedRepairSurvivesAReloadInsideTheOutdatedWindow(t *testing.T) {
	conn := requireRestartableCH(t)
	ctx := context.Background()
	f := newFixture(t, conn)
	c := f.table(0, "dex.swaps", 3, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	f.promote(c, row{"", 2})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	f.insertInto(f.pinned.ReplicaDB, c.Table, row{"", 77})

	inst = f.newInstaller(nil)
	f.start(inst)
	repairedAt := time.Now()
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	if s := inst.Stats(); s.DivergedPartitions[0] != 1 || !s.AuditClean {
		t.Fatalf("stats after the repair = %+v, want one Diverged partition, repaired", s)
	}
	repaired := f.copyParts(c)

	waitUptime(t, ctx, conn, 2*chrestart.DefaultRestartSkew)
	killClickHouse(t, ctx, conn)
	if since := time.Since(repairedAt); since >= 480*time.Second {
		t.Fatalf("the restart came %s after the repair, outside the outdated-part window", since)
	}
	f.requireCopyEqualsManifest(c, s1)
	if parts := f.copyParts(c); len(parts) != len(repaired) {
		t.Fatalf("copy parts after the restart = %+v, want the %d repaired ones", parts, len(repaired))
	}
	inst = f.newInstaller(nil)
	f.conn.reset()
	f.start(inst)
	if err := inst.Audit(ctx, TriggerClickHouseRestart); err != nil {
		t.Fatal(err)
	}
	if s := inst.Stats(); !s.AuditClean || s.AuditDroppedTwins != 0 {
		t.Fatalf("stats after the restart = %+v, want a clean audit with no twin", s)
	}
	f.mustApply(inst, s1)
	if alters := f.conn.alters(); len(alters) != 0 {
		t.Fatalf("ALTER statements after the restart = %q, want none", alters)
	}
	t.Logf("install: the Diverged repair survived a restart %s after it", time.Since(repairedAt).Round(time.Second))
}
