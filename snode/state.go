package snode

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/housegate/housegate/pkg/lthash"

	"github.com/sentioxyz/arbiter-core"
)

type partitionKey struct {
	Table     string
	Partition string
}

// promotionSafePart identifies the safe-side part inventory immediately
// before a promotion publishes. Name plus physical hash distinguishes an
// unchanged base from a content-neutral REPLACE that happens to reuse a name.
type promotionSafePart struct {
	Name           string `json:"name"`
	PartitionID    string `json:"partition_id"`
	PartitionValue string `json:"partition_value"`
	PhysHash       string `json:"phys_hash"`
}

// promotionIntent is the write-ahead boundary for safe publication. It is
// durable before REPLACE PARTITION and remains until the applied ACK state is
// durable. While one exists, unsafe_latest reads fail closed: the process
// cannot know whether REPLACE became visible before a crash until it
// reconciles ClickHouse on the command retry.
type promotionIntent struct {
	PromotionSeq        uint64              `json:"promotion_seq"`
	BasePartitionRoot   string              `json:"base_partition_root"`
	PostPartitionRoot   string              `json:"post_partition_root"`
	BaseSafeSnapshotID  string              `json:"base_safe_snapshot_id"`
	CandidatePartHashes []string            `json:"candidate_part_hashes"`
	UnsafePartNames     []string            `json:"unsafe_part_names"`
	SafePartsBefore     []promotionSafePart `json:"safe_parts_before"`
}

type localState struct {
	Watermarks      map[string]uint64               `json:"watermarks"`
	LastAcks        map[string]arbiter.PromotionAck `json:"last_acks"`
	BaseRoots       map[string]string               `json:"base_roots"`
	BaseSnapshotIDs map[string]string               `json:"base_snapshot_ids"`
	UnpromotedSums  map[string]string               `json:"unpromoted_sums"`
	IntakeParts     map[string]string               `json:"intake_parts,omitempty"`
	// PromotedUnsafeParts records, per partition key, the hg_unsafe part
	// names an applied promotion copied into hg_safe and whose cleanup we
	// have not journaled yet. HouseGate's unsafe_latest read surface
	// excludes exactly these (`_part NOT IN (...)`) so promoted rows are
	// not counted twice between REPLACE PARTITION and cleanup (Spec G D2).
	PromotedUnsafeParts map[string][]string `json:"promoted_unsafe_parts,omitempty"`
	// PromotionIntents are persisted before safe publication. Their presence
	// makes the read-state port fail closed until retry reconciliation either
	// publishes or recognizes and finalizes the already-published partition.
	PromotionIntents map[string]promotionIntent `json:"promotion_intents,omitempty"`
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
	if s.PromotedUnsafeParts == nil {
		s.PromotedUnsafeParts = map[string][]string{}
	}
	if s.PromotionIntents == nil {
		s.PromotionIntents = map[string]promotionIntent{}
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

// BeginPromotion durably records the publication intent. Repeating the exact
// intent is idempotent; a different command cannot replace an unresolved one.
func (st *stateStore) BeginPromotion(k partitionKey, intent promotionIntent) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	if existing, ok := st.s.PromotionIntents[ks]; ok {
		if promotionIntentsEqual(existing, intent) {
			return nil
		}
		return fmt.Errorf("promotion intent already exists for %s/%s at seq %d", k.Table, k.Partition, existing.PromotionSeq)
	}
	next := cloneLocalState(st.s)
	next.PromotionIntents[ks] = clonePromotionIntent(intent)
	return st.persistStateLocked(next)
}

func (st *stateStore) PendingPromotion(k partitionKey) (promotionIntent, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	intent, ok := st.s.PromotionIntents[key(k.Table, k.Partition)]
	return clonePromotionIntent(intent), ok
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

func (st *stateStore) RecordAppliedPromotion(k partitionKey, seq uint64, ack arbiter.PromotionAck, newBaseRoot, newBaseSnapshotID string, partLtHashHexes, unsafePartNames []string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	next := cloneLocalState(st.s)
	ks := key(k.Table, k.Partition)
	acc, err := parseAccumulatorHex(next.UnpromotedSums[ks])
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
	next.UnpromotedSums[ks] = accumulatorHex(acc)
	next.Watermarks[ks] = seq
	next.LastAcks[ks] = ack
	next.BaseRoots[ks] = newBaseRoot
	next.BaseSnapshotIDs[ks] = newBaseSnapshotID
	if len(unsafePartNames) > 0 {
		existing := next.PromotedUnsafeParts[ks]
		seen := make(map[string]bool, len(existing)+len(unsafePartNames))
		for _, p := range existing {
			seen[p] = true
		}
		for _, p := range unsafePartNames {
			if p == "" || seen[p] {
				continue
			}
			seen[p] = true
			existing = append(existing, p)
		}
		next.PromotedUnsafeParts[ks] = existing
	}
	delete(next.PromotionIntents, ks)
	return st.persistStateLocked(next)
}

// RecordCleanup forgets promoted unsafe parts once their cleanup ran. It is
// journaled before the cleanup ack is sent: the parts are already dropped
// from ClickHouse, so even if the ack send fails the exclusion list must not
// keep naming parts that no longer exist. The important direction is never to
// drop a name before the part is gone.
func (st *stateStore) RecordCleanup(k partitionKey, partNames []string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	existing := st.s.PromotedUnsafeParts[ks]
	if len(existing) == 0 {
		return nil
	}
	drop := make(map[string]bool, len(partNames))
	for _, p := range partNames {
		drop[p] = true
	}
	kept := existing[:0]
	for _, p := range existing {
		if !drop[p] {
			kept = append(kept, p)
		}
	}
	if len(kept) == 0 {
		delete(st.s.PromotedUnsafeParts, ks)
	} else {
		st.s.PromotedUnsafeParts[ks] = kept
	}
	return st.persistLocked()
}

// PromotedUnsafeParts returns the sorted union, over every partition of
// table, of promoted-not-yet-cleaned unsafe part names; nil when none. Any
// unresolved write-ahead intent fails closed because publication may already
// be visible even though its final exclusion journal is not durable yet.
func (st *stateStore) PromotedUnsafeParts(table string) ([]string, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	prefix := table + "\x00"
	var pending []string
	for ks := range st.s.PromotionIntents {
		if strings.HasPrefix(ks, prefix) {
			pending = append(pending, strings.TrimPrefix(ks, prefix))
		}
	}
	if len(pending) > 0 {
		sort.Strings(pending)
		return nil, fmt.Errorf("storage-integrity promotion for %s partition(s) %s is unresolved; unsafe_latest reads are unavailable until retry reconciliation", table, strings.Join(pending, ","))
	}
	var out []string
	for ks, parts := range st.s.PromotedUnsafeParts {
		if !strings.HasPrefix(ks, prefix) {
			continue
		}
		out = append(out, parts...)
	}
	if len(out) == 0 {
		return nil, nil
	}
	sort.Strings(out)
	return out, nil
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
	return st.persistStateLocked(st.s)
}

// persistStateLocked publishes next to memory only after the file and its
// containing directory are durable. Critical promotion transitions build a
// cloned next state so a failed write cannot advance an in-memory watermark
// or discard an unresolved intent.
func (st *stateStore) persistStateLocked(next localState) error {
	b, err := json.Marshal(next)
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
	if err := dir.Sync(); err != nil {
		return err
	}
	st.s = next
	return nil
}

func cloneLocalState(s localState) localState {
	next := localState{
		Watermarks:          maps.Clone(s.Watermarks),
		LastAcks:            maps.Clone(s.LastAcks),
		BaseRoots:           maps.Clone(s.BaseRoots),
		BaseSnapshotIDs:     maps.Clone(s.BaseSnapshotIDs),
		UnpromotedSums:      maps.Clone(s.UnpromotedSums),
		IntakeParts:         maps.Clone(s.IntakeParts),
		PromotedUnsafeParts: make(map[string][]string, len(s.PromotedUnsafeParts)),
		PromotionIntents:    make(map[string]promotionIntent, len(s.PromotionIntents)),
	}
	for k, parts := range s.PromotedUnsafeParts {
		next.PromotedUnsafeParts[k] = slices.Clone(parts)
	}
	for k, intent := range s.PromotionIntents {
		next.PromotionIntents[k] = clonePromotionIntent(intent)
	}
	next.ensureMaps()
	return next
}

func clonePromotionIntent(intent promotionIntent) promotionIntent {
	intent.CandidatePartHashes = slices.Clone(intent.CandidatePartHashes)
	intent.UnsafePartNames = slices.Clone(intent.UnsafePartNames)
	intent.SafePartsBefore = slices.Clone(intent.SafePartsBefore)
	return intent
}

func promotionIntentsEqual(a, b promotionIntent) bool {
	return a.PromotionSeq == b.PromotionSeq &&
		a.BasePartitionRoot == b.BasePartitionRoot &&
		a.PostPartitionRoot == b.PostPartitionRoot &&
		a.BaseSafeSnapshotID == b.BaseSafeSnapshotID &&
		slices.Equal(a.CandidatePartHashes, b.CandidatePartHashes) &&
		slices.Equal(a.UnsafePartNames, b.UnsafePartNames) &&
		slices.Equal(a.SafePartsBefore, b.SafePartsBefore)
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
