package verifier

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// registrationStateFile is the verifier's only on-disk state.
const registrationStateFile = "registration.json"

type registrationState struct {
	RegistrationSeq uint64 `json:"registration_seq"`
}

// nextRegistrationSeq reserves the registration_seq of the next Register
// call: max(last + 1, now in Unix ms), where last is the larger of this
// process's and the persisted value. With a StateDir it is durable before it
// is returned (CONTRACT §3a); a lost file only drops the persisted floor.
func (r *Role) nextRegistrationSeq() (uint64, error) {
	r.seqMu.Lock()
	defer r.seqMu.Unlock()
	last := r.lastSeq
	if r.cfg.StateDir != "" {
		persisted, err := readRegistrationSeq(r.cfg.StateDir)
		if err != nil {
			return 0, err
		}
		last = max(last, persisted)
	}
	next := max(last+1, r.nowMillis())
	if r.cfg.StateDir != "" {
		if err := writeRegistrationSeq(r.cfg.StateDir, next); err != nil {
			return 0, err
		}
	}
	r.lastSeq = next
	return next, nil
}

func readRegistrationSeq(dir string) (uint64, error) {
	b, err := os.ReadFile(filepath.Join(dir, registrationStateFile))
	if os.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return 0, fmt.Errorf("verifier registration state: %w", err)
	}
	var st registrationState
	if err := json.Unmarshal(b, &st); err != nil {
		return 0, fmt.Errorf("verifier registration state corrupt: %w", err)
	}
	return st.RegistrationSeq, nil
}

// writeRegistrationSeq persists seq like the SNode's state.json
// (snode/state.go persistStateLocked): temp file, fsync, rename, directory fsync.
func writeRegistrationSeq(dir string, seq uint64) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("verifier state dir: %w", err)
	}
	b, err := json.Marshal(registrationState{RegistrationSeq: seq})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".registration-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, registrationStateFile)); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
