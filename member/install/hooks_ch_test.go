package install

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// hookLog records the installer's BeforeInstall and AfterInstall calls into
// the fixture's statement log, so a test reads hooks and statements in the
// one order they happened in. AfterInstall also records the owner's journal
// snapshot at that moment.
type hookLog struct {
	f    *fixture
	inst *Installer

	mu         sync.Mutex
	fail       map[string]error // database -> BeforeInstall's error
	befores    map[string]int
	afters     map[string]int
	journalled map[string]string // database -> the owner's journal snapshot when AfterInstall ran
}

func newHookLog(f *fixture) *hookLog {
	return &hookLog{f: f, fail: map[string]error{}, befores: map[string]int{}, afters: map[string]int{}, journalled: map[string]string{}}
}

func (h *hookLog) mark(line string) {
	h.f.conn.mu.Lock()
	h.f.conn.execs = append(h.f.conn.execs, line)
	h.f.conn.mu.Unlock()
}

func (h *hookLog) before(_ context.Context, owner uint64, database string) error {
	h.mark(fmt.Sprintf("-- before %d %s", owner, database))
	h.mu.Lock()
	defer h.mu.Unlock()
	h.befores[database]++
	return h.fail[database]
}

func (h *hookLog) after(owner uint64, database string) {
	h.mark(fmt.Sprintf("-- after %d %s", owner, database))
	j, _ := h.inst.Entry(owner)
	h.mu.Lock()
	defer h.mu.Unlock()
	h.afters[database]++
	h.journalled[database] = j.SnapshotID
}

func (h *hookLog) reset() {
	h.f.conn.reset()
	h.mu.Lock()
	defer h.mu.Unlock()
	h.befores, h.afters, h.journalled = map[string]int{}, map[string]int{}, map[string]string{}
}

// requireHeldAround checks that the statements on copies of database's
// tables are exactly those between its BeforeInstall and its AfterInstall,
// which appear once each and in that order.
func (h *hookLog) requireHeldAround(owner uint64, database string) {
	h.f.t.Helper()
	h.f.conn.mu.Lock()
	log := slices.Clone(h.f.conn.execs)
	h.f.conn.mu.Unlock()
	before := slices.Index(log, fmt.Sprintf("-- before %d %s", owner, database))
	after := slices.Index(log, fmt.Sprintf("-- after %d %s", owner, database))
	if before < 0 || after < before || slices.Index(log[before+1:], log[before]) >= 0 || slices.Index(log[after+1:], log[after]) >= 0 {
		h.f.t.Fatalf("hooks of %s: before at %d, after at %d in %q; want each once, before first", database, before, after, log)
	}
	copies := "ALTER TABLE `" + h.f.pinned.ReplicaDB + "`.`" + database + "__"
	changed := 0
	for i, q := range log {
		if strings.HasPrefix(q, copies) {
			changed++
			if i < before || i > after {
				h.f.t.Fatalf("%q ran outside %s's BeforeInstall (%d) and AfterInstall (%d), at %d", q, database, before, after, i)
			}
		}
	}
	if changed == 0 {
		h.f.t.Fatalf("no statement changed a copy of %s between its hooks: %q", database, log)
	}
}

// TestInstallerWithoutAGateIsUnchanged: with Deps.BeforeInstall and
// Deps.AfterInstall nil the installer is P2's (Plan S3-C4 contract §30.6).
// The same two-owner scenario, run on two fresh fixtures, once without the
// seams and once with seams that never wait, issues the same statements in
// the same order (database names and boot nonces aside) and leaves every
// copy equal to its manifest.
func TestInstallerWithoutAGateIsUnchanged(t *testing.T) {
	raw := requireCH(t)
	bootNonce := regexp.MustCompile(`member-a_[0-9a-f]{8}_`)
	run := func(seams bool) ([]string, int) {
		f := newFixture(t, raw)
		swaps := f.table(0, "dex.swaps", 3, true)
		mints := f.table(2, "nft.mints", 4, false)
		f.setSources(0, f.source("owner_pull", f.pull(1)))
		f.setSources(2, f.source("owner_pull", f.pull(1)))
		calls := 0
		inst := f.newInstaller(func(_ *Config, d *Deps) {
			if seams {
				d.BeforeInstall = func(context.Context, uint64, string) error { calls++; return nil }
				d.AfterInstall = func(uint64, string) { calls++ }
			}
		})
		f.start(inst)
		f.conn.reset()
		f.promote(swaps, row{"a", 1}, row{"b", 2})
		f.promote(mints, row{"", 3})
		s1 := f.update("s1", 10, swaps, mints)
		f.mustApply(inst, s1)
		f.promote(swaps, row{"a", 4})
		s2 := f.update("s2", 11, swaps, mints)
		s2.Entries[2] = s1.Entries[2]
		f.advance(time.Minute)
		f.mustApply(inst, s2)
		f.requireCopyEqualsManifest(swaps, s2)
		f.requireCopyEqualsManifest(mints, s2)
		f.conn.mu.Lock()
		defer f.conn.mu.Unlock()
		out := make([]string, 0, len(f.conn.execs))
		for _, q := range f.conn.execs {
			for name, placeholder := range map[string]string{f.pinned.ReplicaDB: "<replica>", f.pinned.StageDB: "<stage>", f.ownerDB: "<owner>"} {
				q = strings.ReplaceAll(q, name, placeholder)
			}
			out = append(out, bootNonce.ReplaceAllString(q, "member-a_<boot>_"))
		}
		return out, calls
	}
	without, none := run(false)
	with, calls := run(true)
	if !slices.Equal(without, with) {
		t.Fatalf("statements without the seams:\n%s\nwith seams that never wait:\n%s", strings.Join(without, "\n"), strings.Join(with, "\n"))
	}
	if none != 0 || calls != 6 {
		t.Fatalf("seam calls %d without and %d with, want 0 and 6 (dex and nft at s1, dex at s2)", none, calls)
	}
	attaches := 0
	for _, q := range without {
		if strings.Contains(q, " ATTACH PARTITION ") {
			attaches++
		}
	}
	if attaches != 4 {
		t.Fatalf("%d ATTACH PARTITION statements, want 4 (dex.swaps' two partitions and nft.mints at s1, one partition at s2): %q", attaches, without)
	}
}

// TestInstallerHoldsEachChangedDatabaseUntilItsJournalWrite: the installer
// calls BeforeInstall once per database before the first statement that
// changes one of its copies and AfterInstall once after the owner's journal
// records the pass (Plan S3-C4 K44): an owner whose entry did not move holds
// nothing, a refused BeforeInstall changes nothing of its database and is not
// paired, and the startup audit's twin drop is held too.
func TestInstallerHoldsEachChangedDatabaseUntilItsJournalWrite(t *testing.T) {
	f := newFixture(t, requireCH(t))
	swaps := f.table(0, "dex.swaps", 3, true)
	pools := f.table(0, "dex.pools", 4, false)
	mints := f.table(2, "nft.mints", 5, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	f.setSources(2, f.source("owner_pull", f.pull(1)))
	hooks := newHookLog(f)
	newInstaller := func() *Installer {
		inst := f.newInstaller(func(_ *Config, d *Deps) { d.BeforeInstall, d.AfterInstall = hooks.before, hooks.after })
		hooks.inst = inst
		return inst
	}
	inst := newInstaller()
	f.start(inst)
	if len(hooks.befores) != 0 {
		t.Fatalf("a clean startup audit called BeforeInstall for %v", hooks.befores)
	}

	// s1: both owners change; dex has two copies and is held once.
	hooks.reset()
	f.promote(swaps, row{"a", 1}, row{"b", 2})
	f.promote(pools, row{"", 3})
	f.promote(mints, row{"", 4})
	s1 := f.update("s1", 10, swaps, pools, mints)
	f.mustApply(inst, s1)
	hooks.requireHeldAround(0, "dex")
	hooks.requireHeldAround(2, "nft")
	if hooks.journalled["dex"] != "s1" || hooks.journalled["nft"] != "s1" {
		t.Fatalf("the owners' journals when AfterInstall ran: %v, want both at s1", hooks.journalled)
	}
	log := slices.Clone(f.conn.execs)
	if slices.Index(log, "-- after 0 dex") > slices.Index(log, "-- before 2 nft") {
		t.Fatalf("owner 0's pass still held dex when owner 2's began: %q", log)
	}

	// s2: owner 2's entry is carried, so its journal moves without a hold.
	hooks.reset()
	f.promote(swaps, row{"a", 5})
	s2 := f.update("s2", 11, swaps, pools, mints)
	s2.Entries[2] = s1.Entries[2]
	f.advance(time.Minute)
	f.mustApply(inst, s2)
	hooks.requireHeldAround(0, "dex")
	if hooks.befores["nft"] != 0 || hooks.afters["nft"] != 0 {
		t.Fatalf("owner 2's carried entry held nft: %v before, %v after", hooks.befores, hooks.afters)
	}
	if j, ok := inst.Entry(2); !ok || j.SnapshotID != "s2" || j.Entry != s1.Entries[2] {
		t.Fatalf("Entry(2) = %+v %t, want s2 with the carried entry", j, ok)
	}

	// s3: BeforeInstall refuses dex; nothing of dex changes and nothing is
	// paired, and the next offer of s3 installs it.
	hooks.reset()
	errHeld := errors.New("the host still holds dex")
	hooks.fail["dex"] = errHeld
	f.promote(pools, row{"", 6})
	s3 := f.update("s3", 12, swaps, pools, mints)
	s3.Entries[2] = s1.Entries[2]
	f.advance(time.Minute)
	err := inst.Apply(context.Background(), s3)
	if !errors.Is(err, errHeld) || !strings.Contains(err.Error(), "install: before changing the copies of dex of owner 0") {
		t.Fatalf("Apply under a refused BeforeInstall = %v, want it to wrap the refusal", err)
	}
	for _, q := range f.conn.alters() {
		if strings.Contains(q, "`"+f.pinned.ReplicaDB+"`") {
			t.Fatalf("a copy changed under a refused BeforeInstall: %s", q)
		}
	}
	if hooks.befores["dex"] != 1 || hooks.afters["dex"] != 0 {
		t.Fatalf("hooks %v before, %v after, want one refused BeforeInstall and no AfterInstall", hooks.befores, hooks.afters)
	}
	if j, _ := inst.Entry(0); j.SnapshotID != "s2" {
		t.Fatalf("owner 0's journal is at %s, want s2", j.SnapshotID)
	}
	f.requireCopyEqualsManifest(pools, s2)
	hooks.reset()
	delete(hooks.fail, "dex")
	f.advance(time.Minute)
	f.mustApply(inst, s3)
	hooks.requireHeldAround(0, "dex")
	f.requireCopyEqualsManifest(pools, s3)

	// A restart's audit drops equal-hash twins inside one hold: ATTACH
	// PARTITION ... FROM the copy itself doubles both parts of dex.pools.
	f.exec("ALTER TABLE " + qualified(f.pinned.ReplicaDB, pools.Table) + " ATTACH PARTITION tuple() FROM " + qualified(f.pinned.ReplicaDB, pools.Table))
	inst = newInstaller()
	hooks.reset()
	f.start(inst)
	hooks.requireHeldAround(0, "dex")
	if s := inst.Stats(); s.AuditDroppedTwins != 2 || !s.AuditClean {
		t.Fatalf("stats after the audit = %+v, want two dropped twins and a clean audit", s)
	}
	f.requireCopyEqualsManifest(pools, s3)
}
