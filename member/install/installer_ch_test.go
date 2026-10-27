package install

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core/member/sources"
)

// TestInstallAppendsNeededPartsWithAttachPartitionFrom: a copy at manifest S1
// is brought to S2 by fetching only the part S2 adds into a scratch and
// attaching it with ATTACH PARTITION ... FROM; the part it held keeps its name
// and the journal records the new part by its name and hash_of_all_files in
// the copy (spec §8.5 steps 3-5).
func TestInstallAppendsNeededPartsWithAttachPartitionFrom(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	pull := f.source("owner_pull", f.pull(1))
	f.setSources(0, pull)
	inst := f.newInstaller(nil)
	f.start(inst)

	f.promote(c, row{"", 1}, row{"", 2})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	first := f.copyParts(c)

	f.promote(c, row{"", 3})
	s2 := f.update("s2", 11, c)
	f.conn.reset()
	f.mustApply(inst, s2)
	f.requireCopyEqualsManifest(c, s2)
	if pull.calls() != 2 {
		t.Fatalf("the owner pull ran %d times, want 2: S2 needs one part only", pull.calls())
	}
	alters := f.conn.alters()
	if len(alters) != 1 || !strings.HasPrefix(alters[0], "ALTER TABLE `"+f.pinned.ReplicaDB+"`.`"+c.Table+"` ATTACH PARTITION tuple() FROM `"+f.pinned.StageDB+"`.`member-a_") {
		t.Fatalf("ALTER statements = %q, want one ATTACH PARTITION tuple() FROM a scratch", alters)
	}
	after := f.copyParts(c)
	if len(after) != 2 || after[0].Name != first[0].Name || after[0].PhysHash != first[0].PhysHash {
		t.Fatalf("copy parts after S2 = %+v, want %s kept and one new part", after, first[0].Name)
	}
	j, ok := inst.Entry(0)
	if !ok || j.SnapshotID != "s2" || j.Entry != frontier(0, 11) {
		t.Fatalf("Entry(0) = %+v %v, want s2 at frontier 11", j, ok)
	}
	tj := j.Tables["dex.swaps"]
	if tj == nil || tj.Copy != c.Table || tj.IncarnationSeq != 3 || len(tj.Parts) != 2 {
		t.Fatalf("journal of dex.swaps = %+v", tj)
	}
	for k, p := range tj.Parts {
		if p.Name != after[k].Name || p.HashOfAllFiles != after[k].PhysHash || p.PartitionID != "all" || p.Rows != after[k].Rows {
			t.Fatalf("journal part %d = %+v, copy part %+v", k, p, after[k])
		}
	}
	if s := inst.Stats(); s.Outcomes[OutcomeKept] != 2 || s.Installs["owner_pull"] != 2 || !s.AuditClean {
		t.Fatalf("stats = %+v, want 2 kept, 2 owner_pull installs, audit clean", s)
	}
	if left := f.stageTables(); len(left) != 0 {
		t.Fatalf("scratch tables left after the passes: %v", left)
	}
}

// TestInstallClosureFailureInstallsNothing: when the scratch holds other rows
// at the closure check than the classifier kept, Σ(L) + Σ(kept) != R and
// nothing reaches the copy (spec §8.5 step 6).
func TestInstallClosureFailureInstallsNothing(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	inst.beforeVerify = func(_ context.Context, scratches []sources.Scratch) {
		f.insertInto(scratches[0].Database, scratches[0].Table, row{"", 99})
	}
	f.promote(c, row{"", 1})
	s1 := f.update("s1", 10, c)
	f.conn.reset()
	err := inst.Apply(context.Background(), s1)
	requireRefusal(t, err, ErrClosure, "install: partition all of dex.swaps does not close: local 0x")
	if alters := f.conn.alters(); len(alters) != 0 {
		t.Fatalf("ALTER statements after a closure failure = %q, want none", alters)
	}
	if got := f.copyHashes(c); len(got) != 0 {
		t.Fatalf("copy holds %d parts after a closure failure, want none", len(got))
	}
	if _, ok := inst.Entry(0); ok {
		t.Fatal("the owner's entry advanced past a closure failure")
	}
	if left := f.stageTables(); len(left) != 0 {
		t.Fatalf("scratch tables left after the failed pass: %v", left)
	}
}

// TestInstallPartitionSetMismatchInstallsNothing: a manifest that lists a
// part in another partition than the one its rows belong to leaves the copy's
// partition set unequal to the manifest's, and nothing is attached.
func TestInstallPartitionSetMismatchInstallsNothing(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, true)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"2026-10-01", 1})
	s1 := f.update("s1", 10, c)
	s1.Manifest.Tables[0].ActiveParts[0].PartitionID = "p_2026-10-09"
	s1.Manifest.Tables[0].PartitionRoots[0].PartitionID = "p_2026-10-09"
	f.conn.reset()
	err := inst.Apply(context.Background(), s1)
	requireRefusal(t, err, ErrPartitionSet, "install: dex.swaps has partitions [p_2026-10-01], manifest s1 lists [p_2026-10-09]")
	if alters := f.conn.alters(); len(alters) != 0 {
		t.Fatalf("ALTER statements = %q, want none", alters)
	}
	if got := f.copyHashes(c); len(got) != 0 {
		t.Fatalf("copy holds %d parts, want none", len(got))
	}
}

// TestInstallDivergedPartitionIsRebuiltWithDropAndAttach: a part in the copy
// that the manifest does not list makes its partition Diverged; the partition
// is rebuilt from every manifest part in fresh scratch, dropped with DROP
// PARTITION and attached again with ATTACH PARTITION ... FROM, while the other
// partition keeps its parts (spec §8.5 step 2).
func TestInstallDivergedPartitionIsRebuiltWithDropAndAttach(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, true)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"2026-10-01", 1})
	f.promote(c, row{"2026-10-02", 2})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	before := f.copyParts(c)

	// A row reaches the copy behind the installer's back.
	f.insertInto(f.pinned.ReplicaDB, c.Table, row{"2026-10-02", 77})
	inst = f.newInstaller(nil)
	f.start(inst)
	if s := inst.Stats(); s.AuditClean {
		t.Fatal("the audit is clean although the copy holds a part the journal does not list")
	}
	f.conn.reset()
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	alters := f.conn.alters()
	if len(alters) != 2 ||
		alters[0] != "ALTER TABLE `"+f.pinned.ReplicaDB+"`.`"+c.Table+"` DROP PARTITION '2026-10-02'" ||
		!strings.HasPrefix(alters[1], "ALTER TABLE `"+f.pinned.ReplicaDB+"`.`"+c.Table+"` ATTACH PARTITION '2026-10-02' FROM `"+f.pinned.StageDB+"`.`member-a_") {
		t.Fatalf("ALTER statements = %q, want DROP PARTITION '2026-10-02' then one ATTACH PARTITION '2026-10-02' FROM a scratch", alters)
	}
	after := f.copyParts(c)
	if after[0].Name != before[0].Name {
		t.Fatalf("partition 2026-10-01 changed: %s became %s", before[0].Name, after[0].Name)
	}
	s := inst.Stats()
	if s.DivergedPartitions[0] != 1 || !s.AuditClean {
		t.Fatalf("stats = %+v, want one Diverged partition, repaired, audit clean", s)
	}
	if j, _ := inst.Entry(0); len(j.Tables["dex.swaps"].Diverged) != 0 {
		t.Fatalf("journal keeps Diverged %v after the repair", j.Tables["dex.swaps"].Diverged)
	}
}

// TestInstallForeignScratchQuarantinesTheTableAndKeepsTheScratch: a pull that
// delivers rows the manifest does not list quarantines the whole table: the
// scratch stays for forensics across a restart, nothing more is fetched for
// the table until ClearForeign, and the copy is rebuilt after it (K36).
func TestInstallForeignScratchQuarantinesTheTableAndKeepsTheScratch(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	bad := f.source("owner_pull", f.foreign(row{"", 66}))
	f.setSources(0, bad)
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	s1 := f.update("s1", 10, c)
	err := inst.Apply(context.Background(), s1)
	requireRefusal(t, err, ErrForeign, "holds a part manifest s1 does not list; dex.swaps is quarantined locally")
	j, _ := inst.Entry(0)
	tj := j.Tables["dex.swaps"]
	if tj == nil || !tj.Foreign || !strings.HasPrefix(tj.ForeignScratch, "member-a_") {
		t.Fatalf("journal of dex.swaps = %+v, want it Foreign with its scratch", tj)
	}
	if left := f.stageTables(); !slices.Equal(left, []string{tj.ForeignScratch}) {
		t.Fatalf("stage tables = %v, want only the kept scratch %s", left, tj.ForeignScratch)
	}
	if st := inst.Tables(0); len(st) != 1 || !st[0].Foreign {
		t.Fatalf("Tables(0) = %+v, want dex.swaps Foreign", st)
	}
	if s := inst.Stats(); s.Outcomes[OutcomeForeign] != 1 {
		t.Fatalf("foreign outcomes = %d, want 1", s.Outcomes[OutcomeForeign])
	}
	requireRefusal(t, inst.Apply(context.Background(), s1), ErrForeign, "clear it offline with sentio-node si-member clear-foreign --table dex.swaps")
	if bad.calls() != 1 {
		t.Fatalf("the source ran %d times, want 1: a quarantined table fetches nothing", bad.calls())
	}
	inst = f.newInstaller(nil)
	f.start(inst)
	if left := f.stageTables(); !slices.Equal(left, []string{tj.ForeignScratch}) {
		t.Fatalf("stage tables after a restart = %v, want the kept scratch %s", left, tj.ForeignScratch)
	}
	if err := ClearForeign(f.stateDir, "dex.swaps"); err != nil {
		t.Fatal(err)
	}
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	inst = f.newInstaller(nil)
	f.start(inst)
	if left := f.stageTables(); len(left) != 0 {
		t.Fatalf("stage tables after clear-foreign and a restart = %v, want none", left)
	}
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
}

// TestInstallRenameRaceRetriesOnceFromACleanScratch: a pull that finds no rows
// is retried once from a clean scratch; a second empty pull moves to the next
// source (spec §8.5 step 4).
func TestInstallRenameRaceRetriesOnceFromACleanScratch(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	empty := outcome(sources.OutcomeFetched, nil)
	first := f.source("owner_pull", empty, f.pull(1))
	f.setSources(0, first)
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	if first.calls() != 2 {
		t.Fatalf("owner pull ran %d times, want 2 (one retry)", first.calls())
	}
	if s := inst.Stats(); s.Outcomes[OutcomeRenameRace] != 1 || s.Outcomes[OutcomeKept] != 1 {
		t.Fatalf("outcomes = %v, want one rename_race and one kept", s.Outcomes)
	}

	f.promote(c, row{"", 2})
	s2 := f.update("s2", 11, c)
	again := f.source("owner_pull", empty)
	next := f.source("peer_unsafe_pull", f.pull(1))
	f.setSources(0, again, next)
	f.mustApply(inst, s2)
	f.requireCopyEqualsManifest(c, s2)
	if again.calls() != 2 || next.calls() != 1 {
		t.Fatalf("calls = %d and %d, want 2 (one retry) then 1", again.calls(), next.calls())
	}
	if s := inst.Stats(); s.Installs["peer_unsafe_pull"] != 1 {
		t.Fatalf("installs = %v, want one from peer_unsafe_pull", s.Installs)
	}
}

// TestInstallSplitFallsBackToTheNextSource: a pull that arrives as parts
// summing to the needed part is not retried; the next source delivers it.
func TestInstallSplitFallsBackToTheNextSource(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	splitting := f.source("owner_pull", f.split())
	next := f.source("peer_unsafe_pull", f.pull(1))
	f.setSources(0, splitting, next)
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1}, row{"", 2})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	if splitting.calls() != 1 || next.calls() != 1 {
		t.Fatalf("calls = %d and %d, want 1 and 1", splitting.calls(), next.calls())
	}
	if s := inst.Stats(); s.Outcomes[OutcomeSplit] != 1 || s.Installs["peer_unsafe_pull"] != 1 {
		t.Fatalf("stats = %+v, want one split and one peer_unsafe_pull install", s)
	}
	if parts := f.copyParts(c); len(parts) != 1 || parts[0].Rows != 2 {
		t.Fatalf("copy parts = %+v, want one part of 2 rows", parts)
	}
}

// TestInstallDoubledRetryIsDroppedAndRetriedOnce: a pull that wrote the part's
// rows twice is dropped and retried once from a clean scratch.
func TestInstallDoubledRetryIsDroppedAndRetriedOnce(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	src := f.source("owner_pull", f.pull(2), f.pull(1))
	f.setSources(0, src)
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(c, row{"", 1})
	s1 := f.update("s1", 10, c)
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	if src.calls() != 2 {
		t.Fatalf("owner pull ran %d times, want 2", src.calls())
	}
	if s := inst.Stats(); s.Outcomes[OutcomeDoubled] != 1 || s.Outcomes[OutcomeKept] != 1 {
		t.Fatalf("outcomes = %v, want one doubled and one kept", s.Outcomes)
	}
}

// TestInstallRefusedPullFallsBackToRestoreAndDefers: a pull refused by the
// memory limit is not retried and the restore delivers the part, dropping
// the backup's extra part inside the scratch; a part no source delivers is
// deferred from 1m doubling, and no source runs before the deadline (K35).
func TestInstallRefusedPullFallsBackToRestoreAndDefers(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	refused := f.source("owner_pull", outcome(sources.OutcomeRefused, errMemoryLimit))
	restore := f.source("restore", f.restoreWithExtra(row{"", 55}))
	f.setSources(0, refused, restore)
	inst := f.newInstaller(nil)
	f.start(inst)
	// The owner is journaled first (its table empty at s0), so s1 is no
	// bootstrap and the sources run in the P2 order; the bootstrap's restore
	// probe is TestInstallBootstrapProbesTheRestoreOncePerCopy's.
	f.mustApply(inst, f.update("s0", 9, c))
	f.promote(c, row{"", 1})
	s1 := f.update("s1", 10, c)
	f.conn.reset()
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	if refused.calls() != 1 || restore.calls() != 1 {
		t.Fatalf("calls = %d and %d, want 1 and 1", refused.calls(), restore.calls())
	}
	dropped := 0
	for _, q := range f.conn.alters() {
		if strings.HasPrefix(q, "ALTER TABLE `"+f.pinned.StageDB+"`.`member-a_") && strings.Contains(q, " DROP PART '") {
			dropped++
		}
	}
	if dropped != 1 {
		t.Fatalf("scratch DROP PART statements = %d, want 1 for the backup's extra part (%q)", dropped, f.conn.alters())
	}
	if s := inst.Stats(); s.Outcomes[OutcomeRefused] != 1 || s.Installs["restore"] != 1 {
		t.Fatalf("stats = %+v, want one refused and one restore install", s)
	}

	f.promote(c, row{"", 2})
	s2 := f.update("s2", 11, c)
	missed := f.source("restore", outcome(sources.OutcomeMissed, nil))
	f.setSources(0, refused, missed)
	t0 := f.clock()
	err := inst.Apply(context.Background(), s2)
	requireRefusal(t, err, ErrNoSourceSucceeded, "of dex.swaps ([owner_pull: refused (code: 241, message: (total) memory limit exceeded) restore: missed]); deferred until "+t0.Add(time.Minute).Format(time.RFC3339))
	requireRefusal(t, inst.Apply(context.Background(), s2), ErrNoSourceSucceeded, "deferred until "+t0.Add(time.Minute).Format(time.RFC3339))
	if refused.calls() != 2 || missed.calls() != 1 {
		t.Fatalf("calls = %d and %d, want 2 and 1: no source runs before the deadline", refused.calls(), missed.calls())
	}
	f.advance(61 * time.Second)
	requireRefusal(t, inst.Apply(context.Background(), s2), ErrNoSourceSucceeded, "deferred until "+t0.Add(61*time.Second+2*time.Minute).Format(time.RFC3339))
	if refused.calls() != 3 {
		t.Fatalf("owner pull ran %d times, want 3 after the deadline", refused.calls())
	}
	if j, _ := inst.Entry(0); j.SnapshotID != "s1" {
		t.Fatalf("Entry(0) = %s, want s1 while the part is deferred", j.SnapshotID)
	}
}

// TestInstallBootstrapProbesTheRestoreOncePerCopy (housegate spec 2026-10-14
// §8.6, the P1 backups are the bootstrap source; Plan S3-C2 review fix 245-4):
// while an owner has no journaled entry, the first part each of its copies
// fetches tries the restore before the owner pull, once per copy and process;
// every other part, and every part of a journaled owner, keeps the P2 order. A
// probe that misses costs that attempt, and a pass that fails after it does not
// probe again when it runs anew.
func TestInstallBootstrapProbesTheRestoreOncePerCopy(t *testing.T) {
	f := newFixture(t, requireCH(t))
	pools := f.table(0, "dex.pools", 4, false)
	swaps := f.table(0, "dex.swaps", 3, false)
	pull := f.source("owner_pull", f.pull(1))
	restore := f.source("restore", f.pull(1))
	f.setSources(0, pull, restore)
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(pools, row{"", 1})
	f.promote(swaps, row{"", 2})
	f.promote(swaps, row{"", 3})
	s1 := f.update("s1", 10, pools, swaps)
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(pools, s1)
	f.requireCopyEqualsManifest(swaps, s1)
	if restore.calls() != 2 || pull.calls() != 1 {
		t.Fatalf("calls: restore %d, owner pull %d; want 2 and 1 (each copy's first part from the restore, the rest by owner pull)", restore.calls(), pull.calls())
	}
	if s := inst.Stats(); s.Installs["restore"] != 2 || s.Installs["owner_pull"] != 1 {
		t.Fatalf("installs = %v, want 2 from the restore and 1 by owner pull", s.Installs)
	}

	// The owner is journaled now: its next part keeps the P2 order.
	f.promote(swaps, row{"", 4})
	s2 := f.update("s2", 11, pools, swaps)
	f.mustApply(inst, s2)
	f.requireCopyEqualsManifest(swaps, s2)
	if restore.calls() != 2 || pull.calls() != 2 {
		t.Fatalf("calls after s2: restore %d, owner pull %d; want 2 and 2", restore.calls(), pull.calls())
	}

	// Owner 2's backups hold nothing and its owner pull misses once: the
	// probe costs one restore attempt, the pass fails and defers the part,
	// and its next run takes both parts by owner pull without probing again.
	mints := f.table(2, "nft.mints", 5, false)
	pull2 := f.source("owner_pull", outcome(sources.OutcomeMissed, nil), f.pull(1))
	restore2 := f.source("restore", outcome(sources.OutcomeMissed, nil))
	f.setSources(2, pull2, restore2)
	f.promote(mints, row{"", 5})
	f.promote(mints, row{"", 6})
	s3 := f.update("s3", 12, pools, swaps, mints)
	requireRefusal(t, inst.Apply(context.Background(), s3), ErrNoSourceSucceeded, "of nft.mints ([restore: missed owner_pull: missed])")
	f.advance(61 * time.Second)
	f.mustApply(inst, s3)
	f.requireCopyEqualsManifest(mints, s3)
	if restore2.calls() != 1 || pull2.calls() != 3 {
		t.Fatalf("owner 2 calls: restore %d, owner pull %d; want 1 and 3", restore2.calls(), pull2.calls())
	}
	if s := inst.Stats(); s.Installs["restore"] != 2 || s.Installs["owner_pull"] != 4 {
		t.Fatalf("installs = %v, want 2 from the restore and 4 by owner pull", s.Installs)
	}
}

// TestInstallJournalsTheEntryPerOwner: each owner's entry advances on its
// own, and an owner advances only when every copy of it closes, in one
// journal write with the copies' parts (spec §8.5 step 7).
func TestInstallJournalsTheEntryPerOwner(t *testing.T) {
	f := newFixture(t, requireCH(t))
	swaps := f.table(0, "dex.swaps", 3, false)
	mints := f.table(2, "nft.mints", 4, false)
	owners := f.table(2, "nft.owners", 5, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	f.setSources(2, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(swaps, row{"", 1})
	f.promote(mints, row{"", 2})
	f.promote(owners, row{"", 3})
	u := f.update("s1", 10, swaps, mints, owners)
	u.Entries[2] = frontier(2, 7)

	f.setSources(2, f.source("owner_pull", func(ctx context.Context, need sources.Need, scratch sources.Scratch) sources.Result {
		if need.TableKey == "nft.owners" {
			return sources.Result{Outcome: sources.OutcomeMissed}
		}
		return f.pullTo(ctx, need, scratch, 1)
	}))
	requireRefusal(t, inst.Apply(context.Background(), u), ErrNoSourceSucceeded, "of nft.owners")
	if j, ok := inst.Entry(0); !ok || j.SnapshotID != "s1" || j.Entry != frontier(0, 10) {
		t.Fatalf("Entry(0) = %+v %v, want s1 at 10", j, ok)
	}
	if _, ok := inst.Entry(2); ok {
		t.Fatal("owner 2 has an entry although nft.owners did not close")
	}
	if got := f.copyHashes(mints); len(got) != 0 {
		t.Fatalf("nft.mints holds %d parts, want none: an owner's copies move together", len(got))
	}

	b, err := os.ReadFile(filepath.Join(f.stateDir, JournalFile))
	if err != nil {
		t.Fatal(err)
	}
	var raw map[string]any
	if err := json.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	owner0 := raw["owners"].(map[string]any)["0"].(map[string]any)
	if raw["version"] != float64(1) || raw["node_id"] != "member-a" || owner0["snapshot_id"] != "s1" {
		t.Fatalf("journal file = %s", b)
	}
	entry := owner0["entry"].(map[string]any)
	part := owner0["tables"].(map[string]any)["dex.swaps"].(map[string]any)["parts"].([]any)[0].(map[string]any)
	for _, k := range []string{"indexer_id", "safe_block_seq", "applied_promotion_seq"} {
		if _, ok := entry[k]; !ok {
			t.Fatalf("journal entry lacks %s: %s", k, b)
		}
	}
	for _, k := range []string{"name", "partition_id", "part_row_lthash", "hash_of_all_files", "rows"} {
		if _, ok := part[k]; !ok {
			t.Fatalf("journal part lacks %s: %s", k, b)
		}
	}

	f.setSources(2, f.source("owner_pull", f.pull(1)))
	f.advance(time.Minute)
	f.mustApply(inst, u)
	if j, ok := inst.Entry(2); !ok || j.SnapshotID != "s1" || j.Entry != frontier(2, 7) || len(j.Tables) != 2 {
		t.Fatalf("Entry(2) = %+v %v, want s1 at 7 with both tables", j, ok)
	}
	f.requireCopyEqualsManifest(mints, u)
	f.requireCopyEqualsManifest(owners, u)
	reopened := f.newInstaller(nil)
	if j, _ := reopened.Entry(2); j.SnapshotID != "s1" {
		t.Fatalf("the reopened journal has owner 2 at %q", j.SnapshotID)
	}
}

// TestApplyNeverTakesAnOwnerBackAfterAPartialPass (Plan S3-C2 review fix 245-5):
// owners 0 and 2 are at s1, and a self-audit finds a journaled part of owner 2's
// copy gone. s2 advances owner 0's entry with a new part; its pass commits and
// journals owner 0 while owner 2's repair finds no source, so Apply fails. A
// copy-watch tick that re-applied s1, the last update that had succeeded, would
// rebuild owner 0's partition back to s1 and take its journal back; Apply skips
// an owner whose journaled entry is ahead of the update, so owner 0 keeps s2's
// entry and parts and no statement touches its copy. The tick the role makes
// now re-applies s2, the newest update the feed offered: owner 0 is current
// and owner 2 is repaired once its source answers.
func TestApplyNeverTakesAnOwnerBackAfterAPartialPass(t *testing.T) {
	ctx := context.Background()
	f := newFixture(t, requireCH(t))
	swaps := f.table(0, "dex.swaps", 3, false)
	mints := f.table(2, "nft.mints", 4, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	f.setSources(2, f.source("owner_pull", f.pull(1)))
	inst := f.newInstaller(nil)
	f.start(inst)
	f.promote(swaps, row{"", 1})
	f.promote(mints, row{"", 2})
	f.promote(mints, row{"", 3})
	s1 := f.update("s1", 10, swaps, mints)
	f.mustApply(inst, s1)

	f.exec("ALTER TABLE " + qualified(f.pinned.ReplicaDB, mints.Table) + " DROP PART '" + f.copyParts(mints)[0].Name + "'")
	if err := inst.Audit(ctx, TriggerSelfAudit); err != nil {
		t.Fatalf("Audit(self_audit): %v", err)
	}
	f.promote(swaps, row{"", 4})
	s2 := f.update("s2", 12, swaps, mints)
	s2.Entries[2] = frontier(2, 10)
	f.setSources(2, f.source("owner_pull", outcome(sources.OutcomeUnreachable, nil)))
	requireRefusal(t, inst.Apply(ctx, s2), ErrNoSourceSucceeded, "of nft.mints")
	if j, _ := inst.Entry(0); j.SnapshotID != "s2" || j.Entry != frontier(0, 12) {
		t.Fatalf("Entry(0) = %s at %+v, want s2 at 12: owner 0's pass commits on its own", j.SnapshotID, j.Entry)
	}
	f.requireCopyEqualsManifest(swaps, s2)
	kept := partNames(f.copyParts(swaps))
	if !inst.NeedsPass() {
		t.Fatal("NeedsPass = false with owner 2's partition Diverged")
	}

	// The copy watch's tick before the fix: s1 again, owner 2 still unreachable.
	f.advance(time.Hour)
	f.conn.reset()
	requireRefusal(t, inst.Apply(ctx, s1), ErrNoSourceSucceeded, "of nft.mints")
	for _, q := range f.conn.alters() {
		if strings.Contains(q, "`"+swaps.Table+"`") {
			t.Fatalf("re-applying s1 changed owner 0's copy: %s", q)
		}
	}
	if j, _ := inst.Entry(0); j.SnapshotID != "s2" || j.Entry != frontier(0, 12) {
		t.Fatalf("Entry(0) = %s at %+v after s1 was offered again, want s2 at 12: a journal entry never goes back", j.SnapshotID, j.Entry)
	}
	if got := partNames(f.copyParts(swaps)); !slices.Equal(got, kept) {
		t.Fatalf("owner 0's copy holds %v after s1 was offered again, want s2's %v", got, kept)
	}
	f.requireCopyEqualsManifest(swaps, s2)

	// The copy watch's tick now: s2 again, owner 2's source answers.
	f.setSources(2, f.source("owner_pull", f.pull(1)))
	f.advance(time.Hour)
	f.conn.reset()
	f.mustApply(inst, s2)
	for _, q := range f.conn.alters() {
		if strings.Contains(q, "`"+swaps.Table+"`") {
			t.Fatalf("re-applying s2 changed owner 0's copy, which is at s2: %s", q)
		}
	}
	if got := partNames(f.copyParts(swaps)); !slices.Equal(got, kept) {
		t.Fatalf("owner 0's copy holds %v after s2 was offered again, want %v", got, kept)
	}
	f.requireCopyEqualsManifest(mints, s2)
	if j, _ := inst.Entry(2); j.SnapshotID != "s2" || j.Entry != frontier(2, 10) {
		t.Fatalf("Entry(2) = %s at %+v, want s2 at 10 after the repair", j.SnapshotID, j.Entry)
	}
	if inst.NeedsPass() || !inst.Stats().AuditClean {
		t.Fatalf("NeedsPass %v, AuditClean %v after the repair; want false and true", inst.NeedsPass(), inst.Stats().AuditClean)
	}
}

// TestInstallRecoversAPartAttachedBeforeTheJournalWrite: a member stopped
// between ATTACH and the journal write leaves an unjournaled part; after the
// restart the startup audit holds the copy unclean and the next pass adopts
// the part, because the target manifest still needs its row hash, without
// fetching it again (K36).
func TestInstallRecoversAPartAttachedBeforeTheJournalWrite(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	src := f.source("owner_pull", f.pull(1))
	f.setSources(0, src)
	crash := errors.New("stopped after the attach")
	inst := f.newInstaller(func(_ *Config, d *Deps) {
		d.AfterAttach = func(string, string) error { return crash }
	})
	f.start(inst)
	f.promote(c, row{"", 1})
	s1 := f.update("s1", 10, c)
	if err := inst.Apply(context.Background(), s1); !errors.Is(err, crash) {
		t.Fatalf("Apply = %v, want the injected stop", err)
	}
	if got := f.copyHashes(c); len(got) != 1 {
		t.Fatalf("copy holds %d parts after the stop, want the attached one", len(got))
	}
	if j, ok := inst.Entry(0); ok && len(j.Tables["dex.swaps"].Parts) != 0 {
		t.Fatalf("the journal lists %v although the member stopped before writing it", j.Tables["dex.swaps"].Parts)
	}

	inst = f.newInstaller(nil)
	f.start(inst)
	if inst.Stats().AuditClean || !inst.Tables(0)[0].Diverged {
		t.Fatal("the startup audit is clean although the copy holds an unjournaled part")
	}
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	if src.calls() != 1 {
		t.Fatalf("the owner pull ran %d times, want 1: the attached part is adopted, not fetched again", src.calls())
	}
	j, _ := inst.Entry(0)
	parts := f.copyParts(c)
	if len(j.Tables["dex.swaps"].Parts) != 1 || j.Tables["dex.swaps"].Parts[0].Name != parts[0].Name {
		t.Fatalf("journal parts = %+v, copy parts = %+v", j.Tables["dex.swaps"].Parts, parts)
	}
	if !inst.Stats().AuditClean {
		t.Fatal("the copy is not clean after the adoption")
	}
}

// TestInstallsPauseAboveTheDiskBudget: at or above the configured
// stop_installs_ratio of the disk budget, summed over every part of ReplicaDB
// and StageDB, Apply attaches nothing while audits continue (K35). The ratio is
// devnet2's 0.5 (decision D28), with the use exactly at the threshold; the same
// budget at the default 0.8 is not reached, and the pass installs.
func TestInstallsPauseAboveTheDiskBudget(t *testing.T) {
	f := newFixture(t, requireCH(t))
	c := f.table(0, "dex.swaps", 3, false)
	f.setSources(0, f.source("owner_pull", f.pull(1)))
	f.exec("CREATE TABLE " + qualified(f.pinned.StageDB, "member-b_0123abcd_1") + " (x UInt64) ENGINE = MergeTree ORDER BY x")
	f.exec("INSERT INTO " + qualified(f.pinned.StageDB, "member-b_0123abcd_1") + " VALUES (1)")
	var used uint64
	query := "SELECT sum(bytes_on_disk) FROM system.parts WHERE database IN (" + quoteLiteral(f.pinned.ReplicaDB) + ", " + quoteLiteral(f.pinned.StageDB) + ")"
	if err := f.raw.QueryRow(context.Background(), query).Scan(&used); err != nil || used == 0 {
		t.Fatalf("member disk use = %d (%v), want the other node's scratch part", used, err)
	}
	budget := 2 * used
	inst := f.newInstaller(func(cfg *Config, _ *Deps) { cfg.DiskBudgetBytes, cfg.StopInstallsRatio = budget, 0.5 })
	f.start(inst)
	f.promote(c, row{"", 1})
	s1 := f.update("s1", 10, c)
	f.conn.reset()
	err := inst.Apply(context.Background(), s1)
	requireRefusal(t, err, ErrInstallsPaused, fmt.Sprintf("install: member disk budget exceeded (%d of %d bytes); installs paused", used, budget))
	if alters := f.conn.alters(); len(alters) != 0 {
		t.Fatalf("ALTER statements while paused = %q", alters)
	}
	s := inst.Stats()
	if !s.InstallsPaused || s.DiskUsedBytes != used || s.DiskBudgetBytes != budget {
		t.Fatalf("stats = %+v, want installs paused at %d of %d bytes", s, used, budget)
	}
	if err := inst.Audit(context.Background(), TriggerStartup); err != nil {
		t.Fatalf("an audit while installs are paused: %v", err)
	}
	inst = f.newInstaller(func(cfg *Config, _ *Deps) { cfg.DiskBudgetBytes = budget })
	f.start(inst)
	f.mustApply(inst, s1)
	f.requireCopyEqualsManifest(c, s1)
	if s := inst.Stats(); s.InstallsPaused || s.DiskUsedBytes != used {
		t.Fatalf("stats = %+v, want installs running at %d of %d bytes under the default ratio", s, used, budget)
	}
}
