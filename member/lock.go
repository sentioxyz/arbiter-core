package member

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// lockStateDir creates dir (mode 0700) if needed and takes an exclusive,
// non-blocking flock on dir/LOCK. The lock lasts until the returned file is
// closed or the process exits, so a crashed member never leaves it behind.
// install.ClearForeign takes the same lock: a second si-member and an
// offline clear-foreign both fail while a member runs on dir.
func lockStateDir(dir string) (*os.File, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("member: create state dir %s: %w", dir, err)
	}
	f, err := os.OpenFile(filepath.Join(dir, LockFile), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("member: open %s: %w", filepath.Join(dir, LockFile), err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("member: state dir %s is locked by a running si-member", dir)
		}
		return nil, fmt.Errorf("member: lock state dir %s: %w", dir, err)
	}
	return f, nil
}
