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

// ackedSafeInventory is the complete hg_safe inventory of one partition that
// this SNode last acknowledged as applied: PromotionAck.SafePartitionParts of
// promotion PromotionSeq. The owner audit compares hg_safe with it (housegate
// spec 2026-10-14 §14). It lives apart from LastAcks, which a not-applied
// acknowledgement (a base CAS refusal) overwrites.
type ackedSafeInventory struct {
	PromotionSeq uint64                    `json:"promotion_seq"`
	Parts        []arbiter.SafePartMapping `json:"parts"`
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
	// RegistrationSeq is the last registration_seq this SNode reserved
	// (housegate spec 2026-10-10 §6.5); see NextRegistrationSeq.
	RegistrationSeq uint64 `json:"registration_seq,omitempty"`
	// OwedCleanupAcks are the cleanup acknowledgements this SNode owes the
	// arbiter, keyed by cleanupAckKey (one cleanup, one entry): the cleanup's
	// DROPs ran and were journaled, but AckCleanup has not yet returned
	// success (RecordExecutedCleanup, ClearOwedCleanupAck). While one is owed,
	// the purge gate and ForgetTable hold its table. A v0.14.0 state file has
	// none and loads unchanged; a v0.14.0 binary ignores the field.
	OwedCleanupAcks map[string]arbiter.CleanupAck `json:"owed_cleanup_acks,omitempty"`
	// AckedSafeInventories is, per partition key, the inventory of the last
	// applied promotion this SNode acknowledged: the owner audit's local
	// reference. RecordAppliedPromotion and a RecordRefreshedAck that
	// completes a legacy acknowledgement write it, RecordAck keeps it, and
	// ForgetTable forgets it. A v0.14.1 state file has none and loads
	// unchanged (ackedSafeInventory derives it from an applied LastAcks
	// entry); a v0.14.1 binary ignores the field.
	AckedSafeInventories map[string]ackedSafeInventory `json:"acked_safe_inventories,omitempty"`
}

type stateStore struct {
	path string
	mu   sync.Mutex
	s    localState
	// beforeWrite, when set, sees every state persistStateLocked is about to
	// write, under mu, and fails the write with its error. Only tests set it,
	// to count and inspect the writes of one operation or to fail one.
	beforeWrite func(next localState) error
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
	if s.OwedCleanupAcks == nil {
		s.OwedCleanupAcks = map[string]arbiter.CleanupAck{}
	}
	if s.AckedSafeInventories == nil {
		s.AckedSafeInventories = map[string]ackedSafeInventory{}
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
// intent is idempotent. The same command may refresh its pre-publication safe
// inventory after reconciliation proves the content root is still the base;
// a different command cannot replace an unresolved one.
func (st *stateStore) BeginPromotion(k partitionKey, intent promotionIntent) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	if existing, ok := st.s.PromotionIntents[ks]; ok {
		if promotionIntentsEqual(existing, intent) {
			return nil
		}
		if promotionIntentCommandEqual(existing, intent) {
			next := cloneLocalState(st.s)
			next.PromotionIntents[ks] = clonePromotionIntent(intent)
			return st.persistStateLocked(next)
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

// AckedSafeInventory returns k's acknowledged inventory (ackedSafeInventory).
func (st *stateStore) AckedSafeInventory(k partitionKey) (ackedSafeInventory, bool) {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.s.ackedSafeInventory(key(k.Table, k.Partition))
}

// ackedInventoryPartitions returns, sorted, the partitions of table that have
// an acknowledged inventory, recorded or derived.
func (st *stateStore) ackedInventoryPartitions(table string) []string {
	st.mu.Lock()
	defer st.mu.Unlock()
	seen := map[string]bool{}
	for _, keys := range [][]string{slices.Collect(maps.Keys(st.s.AckedSafeInventories)), slices.Collect(maps.Keys(st.s.LastAcks))} {
		for _, ks := range keys {
			if pk, ok := splitKey(ks); ok && pk.Table == table {
				if _, has := st.s.ackedSafeInventory(ks); has {
					seen[pk.Partition] = true
				}
			}
		}
	}
	return slices.Sorted(maps.Keys(seen))
}

// ackedSafeInventory returns ks's recorded inventory, else the one an applied
// LastAcks entry with a complete SafePartitionParts carries (a state written
// before acked_safe_inventories).
func (s *localState) ackedSafeInventory(ks string) (ackedSafeInventory, bool) {
	if inv, ok := s.AckedSafeInventories[ks]; ok {
		return cloneAckedSafeInventory(inv), true
	}
	if ack, ok := s.LastAcks[ks]; ok && ack.Applied && len(ack.SafePartitionParts) > 0 {
		return ackedSafeInventory{PromotionSeq: ack.PromotionSeq, Parts: slices.Clone(ack.SafePartitionParts)}, true
	}
	return ackedSafeInventory{}, false
}

// recordAckedSafeInventory makes an applied acknowledgement's complete
// inventory ks's acknowledged inventory. An applied acknowledgement without
// one (a pre-v0.7.0 promotion) leaves no stale inventory behind.
func (s *localState) recordAckedSafeInventory(ks string, ack arbiter.PromotionAck) {
	if !ack.Applied {
		return
	}
	if len(ack.SafePartitionParts) == 0 {
		delete(s.AckedSafeInventories, ks)
		return
	}
	s.AckedSafeInventories[ks] = ackedSafeInventory{PromotionSeq: ack.PromotionSeq, Parts: slices.Clone(ack.SafePartitionParts)}
}

func cloneAckedSafeInventory(inv ackedSafeInventory) ackedSafeInventory {
	inv.Parts = slices.Clone(inv.Parts)
	return inv
}

func (st *stateStore) RecordAck(k partitionKey, seq uint64, ack arbiter.PromotionAck, newBaseRoot, newBaseSnapshotID string) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	next := cloneLocalState(st.s)
	// A not-applied acknowledgement replaces LastAcks[ks]: write down the
	// applied inventory a state from before acked_safe_inventories carries
	// only there, so the owner audit keeps its reference.
	if inv, ok := st.s.ackedSafeInventory(ks); ok {
		next.AckedSafeInventories[ks] = inv
	}
	next.Watermarks[ks] = seq
	next.LastAcks[ks] = ack
	next.BaseRoots[ks] = newBaseRoot
	next.BaseSnapshotIDs[ks] = newBaseSnapshotID
	return st.persistStateLocked(next)
}

// RecordRefreshedAck upgrades only the physical inventory of the latest ACK.
// Unlike recording an applied promotion, it must not advance a watermark or
// subtract candidate hashes/modify cleanup bookkeeping a second time.
func (st *stateStore) RecordRefreshedAck(k partitionKey, ack arbiter.PromotionAck) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ks := key(k.Table, k.Partition)
	previous, ok := st.s.LastAcks[ks]
	if !ok || previous.PromotionSeq != ack.PromotionSeq || st.s.Watermarks[ks] != ack.PromotionSeq ||
		previous.NodeID != ack.NodeID || previous.TableID != ack.TableID || previous.PartitionID != ack.PartitionID ||
		previous.PostPartitionCommitment != ack.PostPartitionCommitment || previous.Applied != ack.Applied {
		return fmt.Errorf("cannot refresh a different promotion ACK")
	}
	next := cloneLocalState(st.s)
	next.LastAcks[ks] = ack
	// The refresh completes a legacy acknowledgement's inventory. One the
	// applied promotion recorded for this seq stays: it describes hg_safe as
	// REPLACE left it.
	if inv, ok := st.s.ackedSafeInventory(ks); !ok || inv.PromotionSeq < ack.PromotionSeq {
		next.recordAckedSafeInventory(ks, ack)
	}
	return st.persistStateLocked(next)
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
	next.recordAckedSafeInventory(ks, ack)
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

// forgetPromotedParts removes partNames from k's promoted unsafe parts, the
// unsafe_latest exclusion of parts a promotion copied into hg_safe, and
// reports whether any was there. Only a cleanup whose DROPs succeeded may call
// it (RecordExecutedCleanup): a name must never leave the exclusion before its
// part is gone.
func (s *localState) forgetPromotedParts(k partitionKey, partNames []string) bool {
	ks := key(k.Table, k.Partition)
	existing := s.PromotedUnsafeParts[ks]
	kept := slices.DeleteFunc(slices.Clone(existing), func(p string) bool { return slices.Contains(partNames, p) })
	if len(kept) == len(existing) {
		return false
	}
	if len(kept) == 0 {
		delete(s.PromotedUnsafeParts, ks)
	} else {
		s.PromotedUnsafeParts[ks] = kept
	}
	return true
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

// CheckRejectedParts is the read-only half of a rejected-part cleanup of
// partition k. Before the cleanup drops anything it refuses a command whose
// promotionSeq is not 0: a rejected-part cleanup carries none (plan S2-A
// clarification C4), and a part a promotion applied has already left
// UnpromotedSums through RecordAppliedPromotion, which keeps its intake record,
// so the accounting of RecordExecutedCleanup would subtract its hash a second
// time. It also refuses a part that does not belong to k (the DROP targets the
// command's table, so such a part would be dropped from one place and accounted
// for under another), a part without a name, and a part that intake recorded
// under k with a row LtHash other than the one the command names, because the
// command then does not describe that part; the last fails closed like
// RemoveUnpromotedPart's hash mismatch. A part that was never recorded, or
// whose record already holds the removed marker, passes.
func (st *stateStore) CheckRejectedParts(k partitionKey, promotionSeq uint64, parts []arbiter.PartRef) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	return st.s.checkRejectedParts(k, promotionSeq, parts)
}

func (s *localState) checkRejectedParts(k partitionKey, promotionSeq uint64, parts []arbiter.PartRef) error {
	if promotionSeq != 0 {
		return fmt.Errorf("cleanup carries promotion_seq %d; it must be 0", promotionSeq)
	}
	for _, p := range parts {
		if p.TableID != k.Table || p.PartitionID != k.Partition {
			return fmt.Errorf("cleanup part %s names %s/%s, not the cleanup's %s/%s",
				p.PartName, p.TableID, p.PartitionID, k.Table, k.Partition)
		}
		if p.PartName == "" {
			return fmt.Errorf("cleanup part for %s/%s has empty part_name", k.Table, k.Partition)
		}
		if recorded, ok := s.IntakeParts[intakePartKey(k, p.PartName)]; ok && recorded != "" && recorded != p.PartRowLtHash {
			return fmt.Errorf("intake part %s of %s/%s is recorded with a different row lthash than the cleanup names",
				p.PartName, k.Table, k.Partition)
		}
	}
	return nil
}

// removeRejectedParts takes the parts a rejected-part cleanup of partition k
// dropped out of the unpromoted accounting: each part intake recorded with
// exactly the named row LtHash leaves k's UnpromotedSums entry and gets the
// removed marker RemoveUnpromotedPart uses. A rejected statement is never
// promoted, so nothing else would subtract those rows, and the purge gate
// (quiescent) and ForgetTable refuse a non-zero sum. A part that was never
// recorded, or whose record already holds the marker, is skipped: a
// redelivered cleanup changes nothing, and an unrecorded part cannot drive a
// sum negative. It reports whether anything changed.
func (s *localState) removeRejectedParts(k partitionKey, parts []arbiter.PartRef) (bool, error) {
	ks := key(k.Table, k.Partition)
	changed := false
	for _, p := range parts {
		partKey := intakePartKey(k, p.PartName)
		if recorded, ok := s.IntakeParts[partKey]; !ok || recorded == "" {
			continue
		}
		acc, err := parseAccumulatorHex(s.UnpromotedSums[ks])
		if err != nil {
			return false, err
		}
		part, err := parseAccumulatorHex(p.PartRowLtHash)
		if err != nil {
			return false, err
		}
		acc.SubHash(part)
		s.UnpromotedSums[ks] = accumulatorHex(acc)
		s.IntakeParts[partKey] = ""
		changed = true
	}
	return changed, nil
}

// cleanupAckKey keys the acknowledgement of one cleanup: its table,
// partition, promotion_seq and rejected_block_seq.
func cleanupAckKey(ack arbiter.CleanupAck) string {
	return fmt.Sprintf("%s\x00%d\x00%d", key(ack.TableID, ack.PartitionID), ack.PromotionSeq, ack.RejectedBlockSeq)
}

// RecordExecutedCleanup journals cmd, a cleanup whose DROPs all succeeded, in
// one state write: for a rejected-part cleanup the accounting of
// removeRejectedParts, after re-running CheckRejectedParts under the lock (so
// it never runs for a command that carries a promotion_seq, and never uses
// another key than the DROPs did); then forgetPromotedParts, for the parts a
// promotion copied into hg_safe; and ack, the acknowledgement this SNode now
// owes the arbiter for cmd. The purge gate (quiescent) and ForgetTable count
// an owed acknowledgement as work that still references its table, so no
// state, a crash included, opens the gate between the DROPs and the arbiter
// accepting ack: before this write the cleanup's promoted-part exclusion or
// unpromoted rows hold it, after it the owed entry does, until
// ClearOwedCleanupAck. A redelivered cmd writes nothing while its entry is
// owed, and records it again once it was cleared; the arbiter takes the
// repeated acknowledgement as a no-op. ack must be cmd's acknowledgement, so
// that one cleanup keys one entry; another is refused.
func (st *stateStore) RecordExecutedCleanup(cmd arbiter.UnsafeCleanup, ack arbiter.CleanupAck) error {
	if ack.TableID != cmd.TableID || ack.PartitionID != cmd.PartitionID ||
		ack.PromotionSeq != cmd.PromotionSeq || ack.RejectedBlockSeq != cmd.RejectedBlockSeq {
		return fmt.Errorf("acknowledgement of %s/%s (promotion_seq %d, rejected_block_seq %d) is not the acknowledgement of the cleanup of %s/%s (promotion_seq %d, rejected_block_seq %d)",
			ack.TableID, ack.PartitionID, ack.PromotionSeq, ack.RejectedBlockSeq,
			cmd.TableID, cmd.PartitionID, cmd.PromotionSeq, cmd.RejectedBlockSeq)
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	k := partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}
	next := cloneLocalState(st.s)
	changed := false
	if cmd.RejectedBlockSeq != 0 {
		if err := st.s.checkRejectedParts(k, cmd.PromotionSeq, cmd.Parts); err != nil {
			return err
		}
		removed, err := next.removeRejectedParts(k, cmd.Parts)
		if err != nil {
			return err
		}
		changed = removed
	}
	names := make([]string, 0, len(cmd.Parts))
	for _, p := range cmd.Parts {
		names = append(names, p.PartName)
	}
	if next.forgetPromotedParts(k, names) {
		changed = true
	}
	ak := cleanupAckKey(ack)
	if owed, ok := next.OwedCleanupAcks[ak]; !ok || owed != ack {
		next.OwedCleanupAcks[ak] = ack
		changed = true
	}
	if !changed {
		return nil
	}
	return st.persistStateLocked(next)
}

// ClearOwedCleanupAck clears ack's owed entry, in a state write of its own,
// once the arbiter accepted ack (AckCleanup returned success). When the write
// fails the entry stays, on disk and in memory, and ack is sent again later;
// the arbiter takes an acknowledgement it already has as a no-op. An entry
// that is not owed (another sender cleared it first) writes nothing.
func (st *stateStore) ClearOwedCleanupAck(ack arbiter.CleanupAck) error {
	st.mu.Lock()
	defer st.mu.Unlock()
	ak := cleanupAckKey(ack)
	if owed, ok := st.s.OwedCleanupAcks[ak]; !ok || owed != ack {
		return nil
	}
	next := cloneLocalState(st.s)
	delete(next.OwedCleanupAcks, ak)
	return st.persistStateLocked(next)
}

// OwedCleanupAcks returns the cleanup acknowledgements this SNode owes the
// arbiter, in key order; nil when none.
func (st *stateStore) OwedCleanupAcks() []arbiter.CleanupAck {
	st.mu.Lock()
	defer st.mu.Unlock()
	var out []arbiter.CleanupAck
	for _, ak := range slices.Sorted(maps.Keys(st.s.OwedCleanupAcks)) {
		out = append(out, st.s.OwedCleanupAcks[ak])
	}
	return out
}

// owedCleanupAckOf returns the first owed acknowledgement, in key order, of a
// cleanup of table.
func (s *localState) owedCleanupAckOf(table string) (arbiter.CleanupAck, bool) {
	for _, ak := range slices.Sorted(maps.Keys(s.OwedCleanupAcks)) {
		if ack := s.OwedCleanupAcks[ak]; ack.TableID == table {
			return ack, true
		}
	}
	return arbiter.CleanupAck{}, false
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
	if st.beforeWrite != nil {
		if err := st.beforeWrite(next); err != nil {
			return err
		}
	}
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

// NextRegistrationSeq reserves the registration_seq of the next Register
// call: max(last reserved + 1, nowMillis). It is durable (temp file, fsync,
// rename, directory fsync) before it is returned, so a crash after sending
// never reuses it, and the clock floor keeps it above every earlier
// registration even when state.json was lost (CONTRACT §3a).
func (st *stateStore) NextRegistrationSeq(nowMillis uint64) (uint64, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	next := max(st.s.RegistrationSeq+1, nowMillis)
	n := cloneLocalState(st.s)
	n.RegistrationSeq = next
	if err := st.persistStateLocked(n); err != nil {
		return 0, fmt.Errorf("persist registration_seq: %w", err)
	}
	return next, nil
}

func cloneLocalState(s localState) localState {
	next := localState{
		Watermarks:           maps.Clone(s.Watermarks),
		LastAcks:             maps.Clone(s.LastAcks),
		BaseRoots:            maps.Clone(s.BaseRoots),
		BaseSnapshotIDs:      maps.Clone(s.BaseSnapshotIDs),
		UnpromotedSums:       maps.Clone(s.UnpromotedSums),
		IntakeParts:          maps.Clone(s.IntakeParts),
		PromotedUnsafeParts:  make(map[string][]string, len(s.PromotedUnsafeParts)),
		PromotionIntents:     make(map[string]promotionIntent, len(s.PromotionIntents)),
		RegistrationSeq:      s.RegistrationSeq,
		OwedCleanupAcks:      maps.Clone(s.OwedCleanupAcks),
		AckedSafeInventories: make(map[string]ackedSafeInventory, len(s.AckedSafeInventories)),
	}
	for k, parts := range s.PromotedUnsafeParts {
		next.PromotedUnsafeParts[k] = slices.Clone(parts)
	}
	for k, intent := range s.PromotionIntents {
		next.PromotionIntents[k] = clonePromotionIntent(intent)
	}
	for k, inv := range s.AckedSafeInventories {
		next.AckedSafeInventories[k] = cloneAckedSafeInventory(inv)
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
	return promotionIntentCommandEqual(a, b) && slices.Equal(a.SafePartsBefore, b.SafePartsBefore)
}

func promotionIntentCommandEqual(a, b promotionIntent) bool {
	return a.PromotionSeq == b.PromotionSeq &&
		a.BasePartitionRoot == b.BasePartitionRoot &&
		a.PostPartitionRoot == b.PostPartitionRoot &&
		a.BaseSafeSnapshotID == b.BaseSafeSnapshotID &&
		slices.Equal(a.CandidatePartHashes, b.CandidatePartHashes) &&
		slices.Equal(a.UnsafePartNames, b.UnsafePartNames)
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
