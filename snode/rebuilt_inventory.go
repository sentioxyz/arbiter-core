package snode

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/sentioxyz/arbiter-core"
)

// The journal writer of a same-identity owner rebuild (housegate spec
// 2026-10-14 §16 P4; plan S3-D contract §34, clarification K52). The rebuild
// (package snode/rebuild) loses only the source ClickHouse: the host keeps
// its volume, so this SNode's journal survives, and after hg_safe is restored
// to the arbiter's SafeParts under new part names the journal records those
// names at the arbiter's PublishSeq, marked rebuilt, as each partition's
// acknowledged inventory. Nothing else of the journal changes: watermarks,
// last acknowledgements, base roots and unpromoted sums stay, so the next
// promotion's base CAS is exact. The caller guarantees that no SNode runs on
// the state directory (the rebuild runs as an init container of the host pod).

// RebuiltSafeInventory is one rebuilt hg_safe partition: its parts under the
// names the restore gave them, at the arbiter's PublishSeq of the partition.
type RebuiltSafeInventory struct {
	TableID, PartitionID string
	PublishSeq           uint64
	Parts                []arbiter.SafePartMapping
}

// ErrRebuildJournalBehind refuses a rebuild whose journal has not seen the
// arbiter's last applied promotion of a partition (its watermark is below the
// PublishSeq): the host's volume was lost too, or the journal is another one.
var ErrRebuildJournalBehind = errors.New("snode: the journal is behind the arbiter's acknowledged inventory; a rebuild needs the journal the lost ClickHouse served")

// ErrRebuildIntentPending refuses a rebuild while a promotion intent is
// journaled: the promotion is unresolved, and a settle resolves it first.
var ErrRebuildIntentPending = errors.New("snode: a promotion intent is pending; settle the track before a rebuild")

// CheckRebuildJournal is RecordRebuiltSafeInventories' read-only precheck,
// which a rebuild runs before it writes anything (its dry run included): it
// refuses as that function does, reads state.json without creating the
// directory (a missing journal is an empty one), and reports, per inventory in
// order, whether the journal already holds a rebuilt inventory of that
// partition at that PublishSeq.
func CheckRebuildJournal(stateDir string, inventories []RebuiltSafeInventory) ([]bool, error) {
	s, err := readLocalState(stateDir)
	if err != nil {
		return nil, err
	}
	if err := s.checkRebuild(inventories); err != nil {
		return nil, err
	}
	out := make([]bool, len(inventories))
	for i, inv := range inventories {
		got, ok := s.AckedSafeInventories[key(inv.TableID, inv.PartitionID)]
		out[i] = ok && got.Rebuilt && got.PromotionSeq == inv.PublishSeq
	}
	return out, nil
}

// RecordRebuiltSafeInventories writes, in one state write, every inventory as
// its partition's acknowledged inventory, marked rebuilt. It refuses, writing
// nothing, a journal behind the arbiter (ErrRebuildJournalBehind) and a
// journal with a pending promotion intent (ErrRebuildIntentPending).
func RecordRebuiltSafeInventories(stateDir string, inventories []RebuiltSafeInventory) error {
	if _, err := CheckRebuildJournal(stateDir, inventories); err != nil {
		return err
	}
	st, err := openStateStore(stateDir)
	if err != nil {
		return err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if err := st.s.checkRebuild(inventories); err != nil {
		return err
	}
	next := cloneLocalState(st.s)
	for _, inv := range inventories {
		next.AckedSafeInventories[key(inv.TableID, inv.PartitionID)] = ackedSafeInventory{
			PromotionSeq: inv.PublishSeq, Parts: slices.Clone(inv.Parts), Rebuilt: true,
		}
	}
	return st.persistStateLocked(next)
}

func (s *localState) checkRebuild(inventories []RebuiltSafeInventory) error {
	if len(s.PromotionIntents) > 0 {
		var pending []string
		for _, ks := range slices.Sorted(maps.Keys(s.PromotionIntents)) {
			if k, ok := splitKey(ks); ok {
				pending = append(pending, fmt.Sprintf("%s/%s at promotion %d", k.Table, k.Partition, s.PromotionIntents[ks].PromotionSeq))
			}
		}
		return fmt.Errorf("%w: %s", ErrRebuildIntentPending, strings.Join(pending, ", "))
	}
	for _, inv := range inventories {
		if w := s.Watermarks[key(inv.TableID, inv.PartitionID)]; w < inv.PublishSeq {
			return fmt.Errorf("%w: %s/%s is at promotion %d, the arbiter acknowledged promotion %d",
				ErrRebuildJournalBehind, inv.TableID, inv.PartitionID, w, inv.PublishSeq)
		}
	}
	return nil
}

// readLocalState reads dir's state.json without creating anything; a missing
// file is an empty state.
func readLocalState(dir string) (localState, error) {
	b, err := os.ReadFile(filepath.Join(dir, "state.json"))
	if os.IsNotExist(err) {
		return newLocalState(), nil
	}
	if err != nil {
		return localState{}, fmt.Errorf("snode state: %w", err)
	}
	s := newLocalState()
	if err := json.Unmarshal(b, &s); err != nil {
		return localState{}, fmt.Errorf("snode state corrupt: %w", err)
	}
	s.ensureMaps()
	return s, nil
}
