package snode

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"housegate/housegate/pkg/lthash"

	"github.com/sentioxyz/arbiter-core"
)

type partitionKey struct {
	Table     string
	Partition string
}

type localState struct {
	Watermarks      map[string]uint64               `json:"watermarks"`
	LastAcks        map[string]arbiter.PromotionAck `json:"last_acks"`
	BaseRoots       map[string]string               `json:"base_roots"`
	BaseSnapshotIDs map[string]string               `json:"base_snapshot_ids"`
	UnpromotedSums  map[string]string               `json:"unpromoted_sums"`
	IntakeParts     map[string]string               `json:"intake_parts,omitempty"`
}

type stateStore struct {
	path string
	mu   sync.Mutex
	s    localState
}

func openStateStore(dir string) (*stateStore, error) {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("snode state dir: %w", err)
	}
	st := &stateStore{path: filepath.Join(dir, "state.json"), s: newLocalState()}
	b, err := os.ReadFile(st.path)
	if os.IsNotExist(err) {
		return st, nil
	}
	if err != nil {
		return nil, fmt.Errorf("snode state: %w", err)
	}
	if err := json.Unmarshal(b, &st.s); err != nil {
		return nil, fmt.Errorf("snode state corrupt: %w", err)
	}
	st.s.ensureMaps()
	return st, nil
}

func newLocalState() localState {
	s := localState{}
	s.ensureMaps()
	return s
}

func (s *localState) ensureMaps() {
	if s.Watermarks == nil {
		s.Watermarks = map[string]uint64{}
	}
	if s.LastAcks == nil {
		s.LastAcks = map[string]arbiter.PromotionAck{}
	}
	if s.BaseRoots == nil {
		s.BaseRoots = map[string]string{}
	}
	if s.BaseSnapshotIDs == nil {
		s.BaseSnapshotIDs = map[string]string{}
	}
	if s.UnpromotedSums == nil {
		s.UnpromotedSums = map[string]string{}
	}
	if s.IntakeParts == nil {
		s.IntakeParts = map[string]string{}
	}
}

func (st *stateStore) Watermark(k partitionKey) uint64 {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.s.Watermarks[key(k.Table, k.Partition)]
}

func (st *stateStore) LastAck(k partitionKey) (arbiter.PromotionAck, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	ack, ok := st.s.LastAcks[key(k.Table, k.Partition)]
	return ack, ok
}

func (st *stateStore) BaseRoot(k partitionKey) (string, string) {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	return st.s.BaseRoots[ks], st.s.BaseSnapshotIDs[ks]
}

func (st *stateStore) UnpromotedSum(k partitionKey) string {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.s.UnpromotedSums[key(k.Table, k.Partition)]
}

func (st *stateStore) RecordAck(k partitionKey, seq uint64, ack arbiter.PromotionAck, newBaseRoot, newBaseSnapshotID string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	st.s.Watermarks[ks] = seq
	st.s.LastAcks[ks] = ack
	st.s.BaseRoots[ks] = newBaseRoot
	st.s.BaseSnapshotIDs[ks] = newBaseSnapshotID
	return st.persistLocked()
}

func (st *stateStore) RecordAppliedPromotion(k partitionKey, seq uint64, ack arbiter.PromotionAck, newBaseRoot, newBaseSnapshotID string, partLtHashHexes []string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	acc, err := parseAccumulatorHex(st.s.UnpromotedSums[ks])
	if err != nil {
		return err
	}
	for _, h := range partLtHashHexes {
		part, err := parseAccumulatorHex(h)
		if err != nil {
			return err
		}
		acc.SubHash(part)
	}
	st.s.UnpromotedSums[ks] = accumulatorHex(acc)
	st.s.Watermarks[ks] = seq
	st.s.LastAcks[ks] = ack
	st.s.BaseRoots[ks] = newBaseRoot
	st.s.BaseSnapshotIDs[ks] = newBaseSnapshotID
	return st.persistLocked()
}

func (st *stateStore) AddUnpromoted(k partitionKey, partLtHashHex string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	acc, err := parseAccumulatorHex(st.s.UnpromotedSums[ks])
	if err != nil {
		return err
	}
	part, err := parseAccumulatorHex(partLtHashHex)
	if err != nil {
		return err
	}
	acc.AddHash(part)
	st.s.UnpromotedSums[ks] = accumulatorHex(acc)
	return st.persistLocked()
}

func (st *stateStore) AddUnpromotedPart(k partitionKey, partName, partLtHashHex string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	partKey := intakePartKey(k, partName)
	if previous, ok := st.s.IntakeParts[partKey]; ok {
		if previous == partLtHashHex {
			return nil
		}
		return fmt.Errorf("intake part %s already recorded with a different hash", partName)
	}
	ks := key(k.Table, k.Partition)
	acc, err := parseAccumulatorHex(st.s.UnpromotedSums[ks])
	if err != nil {
		return err
	}
	part, err := parseAccumulatorHex(partLtHashHex)
	if err != nil {
		return err
	}
	acc.AddHash(part)
	st.s.UnpromotedSums[ks] = accumulatorHex(acc)
	st.s.IntakeParts[partKey] = partLtHashHex
	return st.persistLocked()
}

func (st *stateStore) RemoveUnpromotedPart(k partitionKey, partName, partLtHashHex string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	partKey := intakePartKey(k, partName)
	previous, ok := st.s.IntakeParts[partKey]
	if ok && previous == "" {
		return nil
	}
	if ok && previous != partLtHashHex {
		return fmt.Errorf("intake part %s hash mismatch during removal", partName)
	}
	ks := key(k.Table, k.Partition)
	acc, err := parseAccumulatorHex(st.s.UnpromotedSums[ks])
	if err != nil {
		return err
	}
	part, err := parseAccumulatorHex(partLtHashHex)
	if err != nil {
		return err
	}
	acc.SubHash(part)
	st.s.UnpromotedSums[ks] = accumulatorHex(acc)
	st.s.IntakeParts[partKey] = ""
	return st.persistLocked()
}

func (st *stateStore) DrainUnpromoted(k partitionKey, partLtHashHexes []string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	acc, err := parseAccumulatorHex(st.s.UnpromotedSums[ks])
	if err != nil {
		return err
	}
	for _, h := range partLtHashHexes {
		part, err := parseAccumulatorHex(h)
		if err != nil {
			return err
		}
		acc.SubHash(part)
	}
	st.s.UnpromotedSums[ks] = accumulatorHex(acc)
	return st.persistLocked()
}

func (st *stateStore) persistLocked() error {
	b, err := json.Marshal(st.s)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(st.path), ".state-*")
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
	if err := os.Rename(tmp.Name(), st.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(st.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

func key(table, partition string) string {
	return table + "\x00" + partition
}

func intakePartKey(k partitionKey, partName string) string {
	return key(k.Table, k.Partition) + "\x00" + partName
}

func parseAccumulatorHex(s string) (*lthash.Hash, error) {
	if s == "" {
		return lthash.New(), nil
	}
	raw := strings.TrimPrefix(s, "0x")
	b, err := hex.DecodeString(raw)
	if err != nil {
		return nil, fmt.Errorf("lthash accumulator hex: %w", err)
	}
	h, err := lthash.FromBytes(b)
	if err != nil {
		return nil, err
	}
	return h, nil
}

func accumulatorHex(h *lthash.Hash) string {
	return "0x" + hex.EncodeToString(h.Bytes())
}
