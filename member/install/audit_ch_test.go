package install

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// TestStartupAuditDropsEqualHashTwinsWithDropPart: a second part with the
// row hash and hash_of_all_files of a journaled part is a twin; the startup
// audit keeps the journaled name and drops the twin with DROP PART (spec
// §8.7).
func TestStartupAuditDropsEqualHashTwinsWithDropPart(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	journaled := f.copyParts(c)[0]

	// ATTACH PARTITION ... FROM the copy itself adds a byte-identical part.
	f.exec("ALTER TABLE " + qualified(f.pinned.ReplicaDB, c.Table) + " ATTACH PARTITION tuple() FROM " + qualified(f.pinned.ReplicaDB, c.Table))
	parts := f.copyParts(c)
	if len(parts) != 2 || parts[0].PhysHash != parts[1].PhysHash {
		t.Fatalf("copy parts = %+v, want two equal-hash twins", parts)
	}
	twin := parts[1].Name
	if twin == journaled.Name {
		twin = parts[0].Name
	}

	inst = f.newInstaller(nil)
	f.conn.reset()
	f.start(inst)
	want := "ALTER TABLE `" + f.pinned.ReplicaDB + "`.`" + c.Table + "` DROP PART '" + twin + "'"
	if alters := f.conn.alters(); !slices.Equal(alters, []string{want}) {
		t.Fatalf("ALTER statements = %q, want %q", alters, want)
	}
	if parts := f.copyParts(c); len(parts) != 1 || parts[0].Name != journaled.Name {
		t.Fatalf("copy parts after the audit = %+v, want the journaled %s", parts, journaled.Name)
	}
	s := inst.Stats()
	if s.AuditDroppedTwins != 1 || s.AuditRuns[TriggerStartup] != 1 || !s.AuditClean {
		t.Fatalf("stats = %+v, want one dropped twin, one startup run, clean", s)
	}
	f.requireCopyEqualsManifest(c, s1)
}

// TestStartupAuditMarksAnyOtherMismatchDiverged: a journaled part missing from
// the copy marks its partition Diverged; the audit is unclean until the next
// pass rebuilds the partition with DROP PARTITION and ATTACH PARTITION ...
// FROM.
func TestStartupAuditMarksAnyOtherMismatchDiverged(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	f.promote(c, row{"", 2})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	lost := f.copyParts(c)[0].Name
	f.exec("ALTER TABLE " + qualified(f.pinned.ReplicaDB, c.Table) + " DROP PART '" + lost + "'")

	inst = f.newInstaller(nil)
	f.start(inst)
	s := inst.Stats()
	if s.AuditClean || s.DivergedPartitions[0] != 1 {
		t.Fatalf("stats = %+v, want an unclean audit with one Diverged partition", s)
	}
	j, _ := inst.Entry(0)
	if !slices.Equal(j.Tables["dex.swaps"].Diverged, []string{"all"}) {
		t.Fatalf("journal Diverged = %v, want [all]", j.Tables["dex.swaps"].Diverged)
	}
	if st := inst.Tables(0); !st[0].Diverged {
		t.Fatalf("Tables(0) = %+v, want dex.swaps Diverged", st)
	}
	f.conn.reset()
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	alters := f.conn.alters()
	if len(alters) != 3 || !strings.HasSuffix(alters[0], " DROP PARTITION tuple()") || !strings.Contains(alters[1], " ATTACH PARTITION tuple() FROM ") || !strings.Contains(alters[2], " ATTACH PARTITION tuple() FROM ") {
		t.Fatalf("ALTER statements = %q, want DROP PARTITION then two ATTACH PARTITION ... FROM", alters)
	}
	if !inst.Stats().AuditClean {
		t.Fatal("the audit is not clean after the repair")
	}
}

// TestAStartWithACurrentJournalAuditsTheCopiesItFindsLater (Plan S3-C2 review
// fix 245-1): the member restarts with its journal at the watermark, and its
// copyset makes the copies ready only after the startup audit ran: one copy was
// lost and its first pass created it again empty, the other lost a journaled
// part. The audit covered neither, so the audit is unclean, both copies report
// Diverged and NeedsPass holds until Apply of the very manifest the journal is
// at audits them and rebuilds what they lost.
func TestAStartWithACurrentJournalAuditsTheCopiesItFindsLater(t *testing.T) {
	f := newFixture(t, requireCH(t))
	lost := f.table(0, "dex.swaps", 3, false)
	altered := f.table(0, "dex.pools", 4, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(lost, row{"", 1})
	f.promote(altered, row{"", 2})
	f.promote(altered, row{"", 3})
	s1 := f.update("s1", 10, lost, altered)
	f.mustApply(inst, s1)

	// While the member is down: one copy is gone (the copyset's first pass
	// creates it again, empty), the other loses a journaled part.
	f.exec("DROP TABLE " + qualified(f.pinned.ReplicaDB, lost.Table) + " SYNC")
	if err := ddl.EnsureCopy(context.Background(), f.raw, f.pinned, lost.Schema, lost.IncarnationSeq); err != nil {
		t.Fatal(err)
	}
	f.exec("ALTER TABLE " + qualified(f.pinned.ReplicaDB, altered.Table) + " DROP PART '" + f.copyParts(altered)[0].Name + "'")

	// The new process audits while its copyset reports no ready copy yet.
	ready := f.copies.Copies()
	f.copies.mu.Lock()
	f.copies.list = nil
	f.copies.mu.Unlock()
	inst = f.newInstaller(nil)
	f.start(inst)
	if !inst.Stats().AuditClean {
		t.Fatalf("stats = %+v, want a clean audit while no copy is ready", inst.Stats())
	}
	f.copies.mu.Lock()
	f.copies.list = ready
	f.copies.mu.Unlock()
	if inst.Stats().AuditClean || !inst.NeedsPass() {
		t.Fatalf("AuditClean %v, NeedsPass %v with ready copies the audit never covered; want false and true", inst.Stats().AuditClean, inst.NeedsPass())
	}
	for _, st := range inst.Tables(0) {
		if !st.Diverged || st.SnapshotID != "s1" {
			t.Fatalf("Tables(0) = %+v, want every copy Diverged at s1 until it is audited", inst.Tables(0))
		}
	}
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(lost, s1)
	f.requireCopyEqualsManifest(altered, s1)
	if s := inst.Stats(); !s.AuditClean || s.AuditRuns[TriggerStartup] != 1 || inst.NeedsPass() {
		t.Fatalf("stats = %+v, NeedsPass %v; want clean after the pass, one startup run and nothing left", s, inst.NeedsPass())
	}
}

// TestAnAuditThatFindsDamageAsksForAPass (Plan S3-C2 review fix 245-2): with the
// journal at the manifest and the copy set unchanged, a self-audit and then a
// restart audit that find a journaled part gone persist Diverged; NeedsPass
// holds until Apply of the same manifest rebuilds the partition, so the role
// repairs the copy while the watermark stands still.
func TestAnAuditThatFindsDamageAsksForAPass(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	f.promote(c, row{"", 2})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	if inst.NeedsPass() {
		t.Fatal("NeedsPass = true with every copy at the manifest")
	}
	ctx := context.Background()
	for _, trigger := range []string{TriggerSelfAudit, TriggerClickHouseRestart} {
		f.exec("ALTER TABLE " + qualified(f.pinned.ReplicaDB, c.Table) + " DROP PART '" + f.copyParts(c)[0].Name + "'")
		if err := inst.Audit(ctx, trigger); err != nil {
			t.Fatalf("Audit(%s): %v", trigger, err)
		}
		if !inst.NeedsPass() || inst.Stats().AuditClean {
			t.Fatalf("after the %s audit: NeedsPass %v, AuditClean %v; want true and false", trigger, inst.NeedsPass(), inst.Stats().AuditClean)
		}
		f.mustApply(inst, s1)
		f.requireCopyEqualsManifest(c, s1)
		if inst.NeedsPass() || !inst.Stats().AuditClean {
			t.Fatalf("after repairing the %s audit's finding: NeedsPass %v, AuditClean %v; want false and true", trigger, inst.NeedsPass(), inst.Stats().AuditClean)
		}
	}
	if j, _ := inst.Entry(0); j.SnapshotID != "s1" {
		t.Fatalf("Entry(0) = %s, want s1: the repairs needed no new manifest", j.SnapshotID)
	}
}

// uptimeConn answers chrestart's uptime probe from a start time the test
// moves; every other call reaches ClickHouse.
type uptimeConn struct {
	clickhouse.Conn
	started atomic.Int64 // unix nanoseconds
}

func (c *uptimeConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if query == "SELECT toUInt64(uptime())" {
		return uptimeRow{uptime: uint64(time.Since(time.Unix(0, c.started.Load())) / time.Second)}
	}
	return c.Conn.QueryRow(ctx, query, args...)
}

type uptimeRow struct{ uptime uint64 }

func (r uptimeRow) Err() error { return nil }

func (r uptimeRow) Scan(dest ...any) error {
	*dest[0].(*uint64) = r.uptime
	return nil
}

func (r uptimeRow) ScanStruct(any) error { return nil }

// TestAuditRerunsWhenClickHouseUptimeRegresses: when the server's start time
// moves later (its uptime regressed), the watcher reruns the audit as
// clickhouse_restart; it finds what the restart changed, holds the audit
// unclean until the next pass repairs it, and Apply waits for a restart audit
// that failed.
func TestAuditRerunsWhenClickHouseUptimeRegresses(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	f.promote(c, row{"", 2})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)

	probe := &uptimeConn{Conn: f.raw}
	probe.started.Store(time.Now().Add(-time.Hour).UnixNano())
	var mu sync.Mutex
	var audits []error
	done := make(chan struct{}, 1)
	w := &chrestart.Watcher{Interval: 50 * time.Millisecond, OnRestart: func(ctx context.Context, previous, current time.Time) {
		err := inst.Audit(ctx, TriggerClickHouseRestart)
		mu.Lock()
		audits = append(audits, err)
		mu.Unlock()
		done <- struct{}{}
	}}
	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() { defer close(stopped); w.Run(ctx, probe) }()
	defer func() { cancel(); <-stopped }()

	time.Sleep(200 * time.Millisecond)
	f.exec("ALTER TABLE " + qualified(f.pinned.ReplicaDB, c.Table) + " DROP PART '" + f.copyParts(c)[0].Name + "'")
	probe.started.Store(time.Now().UnixNano())
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("no audit ran after the start time moved")
	}
	mu.Lock()
	if len(audits) != 1 || audits[0] != nil {
		t.Fatalf("restart audits = %v, want one that succeeded", audits)
	}
	mu.Unlock()
	s := inst.Stats()
	if s.AuditRuns[TriggerClickHouseRestart] != 1 || s.AuditClean || s.DivergedPartitions[0] != 1 {
		t.Fatalf("stats = %+v, want one clickhouse_restart run, unclean, one Diverged partition", s)
	}
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	if !inst.Stats().AuditClean {
		t.Fatal("the audit is not clean after the repair")
	}

	// A restart audit that cannot complete leaves installs waiting for it:
	// Apply runs it again first.
	cancelled, cancelNow := context.WithCancel(context.Background())
	cancelNow()
	if err := inst.Audit(cancelled, TriggerClickHouseRestart); err == nil {
		t.Fatal("an audit on a cancelled context succeeded")
	}
	if inst.Stats().AuditClean {
		t.Fatal("the audit is clean although the restart audit did not complete")
	}
	f.mustApply(inst, s1)
	if s := inst.Stats(); !s.AuditClean || s.AuditRuns[TriggerClickHouseRestart] != 3 {
		t.Fatalf("stats = %+v, want Apply to have rerun the restart audit (3 runs) and be clean", s)
	}
}

// TestStartupDropsOnlyThisNodesStageTables: DropStaleScratch drops every
// StageDB table of this node's prefix, of any boot and kind, except the
// foreign scratch the journal keeps; other nodes' tables stay.
func TestStartupDropsOnlyThisNodesStageTables(t *testing.T) {
	f := newFixture(t, requireCH(t))
	for _, name := range []string{
		"member-a_0123abcd_1", "member-a_0123abcd_1_r", "member-a_0123abcd_rreplay_1", "member-a_89abcdef_rscan_2",
		"member-a_0123abcd_7", "member-b_0123abcd_1", "member-ab_0123abcd_1", "other",
	} {
		f.exec("CREATE TABLE " + qualified(f.pinned.StageDB, name) + " (x UInt64) ENGINE = MergeTree ORDER BY x")
	}
	j := newJournal("member-a")
	j.owner(0).Tables["dex.swaps"] = &TableJournal{IncarnationSeq: 3, Copy: "dex__swaps__i3", Foreign: true, ForeignScratch: "member-a_0123abcd_7"}
	b, err := json.Marshal(j)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(f.stateDir, JournalFile), b, 0o600); err != nil {
		t.Fatal(err)
	}
	inst := f.newInstaller(nil)
	if err := inst.DropStaleScratch(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := []string{"member-a_0123abcd_7", "member-ab_0123abcd_1", "member-b_0123abcd_1", "other"}
	if got := f.stageTables(); !slices.Equal(got, want) {
		t.Fatalf("stage tables = %v, want %v", got, want)
	}
}

// TestSelfAuditRunsAtMostOncePerHalfInterval: a self-audit scans every copy
// in full and a second one inside half SelfAuditInterval is skipped.
func TestSelfAuditRunsAtMostOncePerHalfInterval(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	f.mustApply(inst, f.update("s1", 10, c))
	ctx := context.Background()
	for _, step := range []time.Duration{0, time.Hour, 2 * time.Hour} {
		f.advance(step)
		if err := inst.Audit(ctx, TriggerSelfAudit); err != nil {
			t.Fatal(err)
		}
	}
	if s := inst.Stats(); s.AuditRuns[TriggerSelfAudit] != 2 || !s.AuditClean {
		t.Fatalf("stats = %+v, want 2 self-audits (one skipped at +1h of 6h) and clean", s)
	}
	if err := inst.Audit(ctx, "hourly"); err == nil || !strings.Contains(err.Error(), `unknown audit trigger "hourly"`) {
		t.Fatalf("Audit(hourly) = %v", err)
	}
}
