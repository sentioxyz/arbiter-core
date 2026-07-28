package snode

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/sentioxyz/arbiter-core"
)

type IntakeLifecycle string

const (
	LifecyclePreparing     IntakeLifecycle = "Preparing"
	LifecycleUnsafeWritten IntakeLifecycle = "UnsafeWritten"
	LifecycleRCBound       IntakeLifecycle = "RCBound"
	LifecycleAbortPending  IntakeLifecycle = "AbortPending"
	LifecycleCleaned       IntakeLifecycle = "Cleaned"
)

type intakeAbort struct {
	PartNames []string `json:"part_names"`
	Reason    string   `json:"reason"`
}

type intakeRecord struct {
	StatementID       string                    `json:"statement_id"`
	Lifecycle         IntakeLifecycle           `json:"lifecycle"`
	Envelope          arbiter.StatementEnvelope `json:"envelope"`
	PayloadEncoding   string                    `json:"payload_encoding"`
	Revision          int                       `json:"revision"`
	TouchedPartitions []string                  `json:"touched_partitions,omitempty"`
	PreWriteInventory map[string][]string       `json:"pre_write_inventory,omitempty"`
	ExpectedRowCount  uint64                    `json:"expected_row_count"`
	Result            *PreparedLocalResult      `json:"result,omitempty"`
	RC                *arbiter.RCRecord         `json:"rc,omitempty"`
	Abort             *intakeAbort              `json:"abort,omitempty"`
	ConvergeFailed    string                    `json:"converge_failed,omitempty"`
}

type intakeJournal struct {
	dir string
}

func openIntakeJournal(stateDir string) (*intakeJournal, error) {
	dir := filepath.Join(stateDir, "intake")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("create intake journal: %w", err)
	}
	return &intakeJournal{dir: dir}, nil
}

func (j *intakeJournal) load(flatID string) (intakeRecord, bool, error) {
	path := j.recordPath(flatID)
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return intakeRecord{}, false, nil
	}
	if err != nil {
		return intakeRecord{}, false, fmt.Errorf("read intake record: %w", err)
	}
	rec, err := decodeIntakeRecord(path, data)
	if err != nil {
		return intakeRecord{}, false, err
	}
	return rec, true, nil
}

func (j *intakeJournal) save(rec intakeRecord) error {
	if err := validateIntakeRecord(rec); err != nil {
		return err
	}
	if previous, ok, err := j.load(rec.StatementID); err != nil {
		return fmt.Errorf("load previous intake record: %w", err)
	} else if ok && !validIntakeTransition(previous.Lifecycle, rec.Lifecycle) {
		return fmt.Errorf("invalid intake lifecycle transition %s -> %s", previous.Lifecycle, rec.Lifecycle)
	} else if !ok && rec.Lifecycle != LifecyclePreparing {
		return fmt.Errorf("new intake record must start at %s, got %s", LifecyclePreparing, rec.Lifecycle)
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal intake record: %w", err)
	}
	path := j.recordPath(rec.StatementID)
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return fmt.Errorf("open intake record temp file: %w", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
		_ = os.Remove(tmpPath)
	}()
	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("write intake record: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync intake record: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close intake record: %w", err)
	}
	closed = true
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("replace intake record: %w", err)
	}
	dir, err := os.Open(j.dir)
	if err != nil {
		return fmt.Errorf("open intake journal directory: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync intake journal directory: %w", err)
	}
	return nil
}

func (j *intakeJournal) list() ([]intakeRecord, error) {
	entries, err := os.ReadDir(j.dir)
	if err != nil {
		return nil, fmt.Errorf("list intake journal: %w", err)
	}
	records := make([]intakeRecord, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(j.dir, entry.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read intake record %s: %w", entry.Name(), err)
		}
		rec, err := decodeIntakeRecord(path, data)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func (j *intakeJournal) recordPath(flatID string) string {
	name := hex.EncodeToString([]byte(flatID)) + ".json"
	return filepath.Join(j.dir, name)
}

func decodeIntakeRecord(path string, data []byte) (intakeRecord, error) {
	var rec intakeRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return intakeRecord{}, fmt.Errorf("decode intake record %s: %w", filepath.Base(path), err)
	}
	if err := validateIntakeRecord(rec); err != nil {
		return intakeRecord{}, fmt.Errorf("validate intake record %s: %w", filepath.Base(path), err)
	}
	return rec, nil
}

func validateIntakeRecord(rec intakeRecord) error {
	if rec.StatementID == "" {
		return fmt.Errorf("intake record statement_id is required")
	}
	switch rec.Lifecycle {
	case LifecyclePreparing:
	case LifecycleUnsafeWritten, LifecycleRCBound:
		if rec.Result == nil || rec.RC == nil {
			return fmt.Errorf("intake record in %s requires result and rc", rec.Lifecycle)
		}
	case LifecycleAbortPending, LifecycleCleaned:
		if rec.Abort == nil {
			return fmt.Errorf("intake record in %s requires abort details", rec.Lifecycle)
		}
	default:
		return fmt.Errorf("unknown intake lifecycle %q", rec.Lifecycle)
	}
	return nil
}

func validIntakeTransition(from, to IntakeLifecycle) bool {
	if from == to {
		return true
	}
	switch from {
	case LifecyclePreparing:
		return to == LifecycleUnsafeWritten || to == LifecycleAbortPending
	case LifecycleUnsafeWritten:
		return to == LifecycleRCBound || to == LifecycleAbortPending
	case LifecycleAbortPending:
		return to == LifecycleCleaned
	case LifecycleCleaned:
		return to == LifecyclePreparing
	default:
		return false
	}
}
