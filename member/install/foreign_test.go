package install

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"syscall"
	"testing"
)

// TestClearForeignRefusesWhileTheStateDirIsLocked: clear-foreign is offline,
// so it refuses while a running si-member holds <state_dir>/LOCK; once the
// lock is free it lifts the quarantine and marks every partition the table
// lists Diverged, so the next start rebuilds the copy and drops the scratch.
func TestClearForeignRefusesWhileTheStateDirIsLocked(t *testing.T) {
	dir := t.TempDir()
	j := newJournal("member-a")
	j.owner(0).Tables["dex.swaps"] = &TableJournal{
		IncarnationSeq: 3, Copy: "dex__swaps__i3", Foreign: true, ForeignScratch: "member-a_0123abcd_7",
		Parts: []JournalPart{{Name: "all_1_1_0", PartitionID: "all"}, {Name: "p_x_2_2_0", PartitionID: "p_x"}},
	}
	j.owner(0).Tables["dex.pools"] = &TableJournal{IncarnationSeq: 4, Copy: "dex__pools__i4"}
	if err := writeJournal(filepath.Join(dir, JournalFile), j); err != nil {
		t.Fatal(err)
	}
	lock, err := os.OpenFile(filepath.Join(dir, LockFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	err = ClearForeign(dir, "dex.swaps")
	if !errors.Is(err, ErrStateDirLocked) || err.Error() != "install: state dir "+dir+" is locked by a running si-member" {
		t.Fatalf("ClearForeign under the lock = %v", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	_ = lock.Close()

	if err := ClearForeign(dir, "dex.pools"); err == nil || err.Error() != "install: dex.pools is not quarantined as foreign in "+filepath.Join(dir, JournalFile) {
		t.Fatalf("ClearForeign of a table that is not foreign = %v", err)
	}
	if err := ClearForeign(dir, "dex.swaps"); err != nil {
		t.Fatal(err)
	}
	got, err := readJournal(filepath.Join(dir, JournalFile))
	if err != nil {
		t.Fatal(err)
	}
	tj := got.Owners["0"].Tables["dex.swaps"]
	if tj.Foreign || tj.ForeignScratch != "" || !slices.Equal(tj.Diverged, []string{"all", "p_x"}) {
		t.Fatalf("journal after clear-foreign = %+v, want no quarantine and both partitions Diverged", tj)
	}
	if err := ClearForeign(t.TempDir(), "dex.swaps"); err == nil {
		t.Fatal("ClearForeign without a journal succeeded")
	}
}
