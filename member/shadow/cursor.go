package shadow

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// CursorFile is the shadow's durable cursor in the state directory:
// {"version":1,"next_block_seq":<n>}.
const CursorFile = "shadow-cursor.json"

const cursorVersion = 1

type cursorState struct {
	Version      int    `json:"version"`
	NextBlockSeq uint64 `json:"next_block_seq"`
}

// loadCursor returns the persisted next block, or 0 when there is no cursor
// file yet.
func loadCursor(stateDir string) (uint64, error) {
	path := filepath.Join(stateDir, CursorFile)
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("shadow: read cursor %s: %w", path, err)
	}
	var c cursorState
	if err := json.Unmarshal(b, &c); err != nil {
		return 0, fmt.Errorf("shadow: cursor %s: %w", path, err)
	}
	if c.Version != cursorVersion {
		return 0, fmt.Errorf("shadow: cursor %s has version %d, want %d", path, c.Version, cursorVersion)
	}
	if c.NextBlockSeq == 0 {
		return 0, fmt.Errorf("shadow: cursor %s has next_block_seq 0", path)
	}
	return c.NextBlockSeq, nil
}

// storeCursor writes the cursor atomically: a temp file in the same
// directory, fsync, rename, directory fsync (the snode state-file pattern).
func storeCursor(stateDir string, next uint64) error {
	path := filepath.Join(stateDir, CursorFile)
	b, err := json.Marshal(cursorState{Version: cursorVersion, NextBlockSeq: next})
	if err != nil {
		return fmt.Errorf("shadow: encode cursor: %w", err)
	}
	tmp, err := os.CreateTemp(stateDir, ".shadow-cursor-*")
	if err != nil {
		return fmt.Errorf("shadow: write cursor %s: %w", path, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("shadow: write cursor %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("shadow: write cursor %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("shadow: write cursor %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("shadow: write cursor %s: %w", path, err)
	}
	dir, err := os.Open(stateDir)
	if err != nil {
		return fmt.Errorf("shadow: write cursor %s: %w", path, err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("shadow: write cursor %s: %w", path, err)
	}
	return nil
}
