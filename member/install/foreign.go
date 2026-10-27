package install

import (
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"syscall"
)

// LockFile is the si-member's state-directory lock (Plan S3-C2 contract
// §23.1, member.LockFile): the running role holds an exclusive flock on it,
// so ClearForeign, an offline command, refuses while the role runs.
const LockFile = "LOCK"

// ErrStateDirLocked reports that a running si-member holds the state
// directory's lock.
var ErrStateDirLocked = errors.New("install: state dir is locked")

// ClearForeign releases the local quarantine of one table (Plan S3-C2 K36),
// offline, for `sentio-node si-member clear-foreign --table <db.table>`. It
// takes the state directory's lock itself, so it refuses while an si-member
// runs on stateDir. The table's journal loses its foreign mark and its
// foreign_scratch, and every partition it lists is marked Diverged, so the
// next start drops the kept scratch (DropStaleScratch) and rebuilds the copy
// partition by partition through the gate (DROP PARTITION, then ATTACH
// PARTITION ... FROM fresh scratch), trusting none of its parts. tableKey is
// the logical <database>.<table>.
func ClearForeign(stateDir, tableKey string) error {
	unlock, err := lockStateDir(stateDir)
	if err != nil {
		return err
	}
	defer unlock()
	path := filepath.Join(stateDir, JournalFile)
	j, err := readJournal(path)
	if err != nil {
		return err
	}
	if j == nil {
		return fmt.Errorf("install: no install journal in %s", stateDir)
	}
	cleared := 0
	for _, owner := range slices.Sorted(maps.Keys(j.Owners)) {
		tj := j.Owners[owner].Tables[tableKey]
		if tj == nil || !tj.Foreign {
			continue
		}
		for _, p := range tj.partitions() {
			tj.markDiverged(p)
		}
		tj.Foreign, tj.ForeignScratch = false, ""
		cleared++
	}
	if cleared == 0 {
		return fmt.Errorf("install: %s is not quarantined as foreign in %s", tableKey, path)
	}
	return writeJournal(path, j)
}

// lockStateDir takes the exclusive, non-blocking flock that a running
// si-member holds on <stateDir>/LOCK.
func lockStateDir(stateDir string) (func(), error) {
	f, err := os.OpenFile(filepath.Join(stateDir, LockFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("install: open the lock of state dir %s: %w", stateDir, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, refuse(ErrStateDirLocked, "install: state dir %s is locked by a running si-member", stateDir)
		}
		return nil, fmt.Errorf("install: lock state dir %s: %w", stateDir, err)
	}
	return func() {
		_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		_ = f.Close()
	}, nil
}

// foreignScratches lists the scratch tables the journal keeps for forensics.
func (i *Installer) foreignScratches() map[string]bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := map[string]bool{}
	for _, oj := range i.journal.Owners {
		for _, tj := range oj.Tables {
			if tj.Foreign && tj.ForeignScratch != "" {
				out[tj.ForeignScratch] = true
			}
		}
	}
	return out
}

// foreignTables lists, sorted, the table keys of owner the journal holds
// quarantined, whatever incarnation they belonged to.
func (i *Installer) foreignTables(owner uint64) []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	oj := i.journal.Owners[ownerKey(owner)]
	if oj == nil {
		return nil
	}
	var out []string
	for key, tj := range oj.Tables {
		if tj.Foreign {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}
