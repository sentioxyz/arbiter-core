package consensus

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"

	"github.com/sentioxyz/arbiter-core/dataplane"
)

// quiescenceVersion is the only quiescence.json version.
const quiescenceVersion = 1

// QuiescentTable is one table, at one incarnation, that a dispatched block's
// replay job names.
type QuiescentTable struct {
	TableID        string `json:"table_id"`
	IncarnationSeq uint64 `json:"incarnation_seq"`
}

// quiescenceBlock is one unresolved block of the ledger.
type quiescenceBlock struct {
	BlockSeq uint64           `json:"block_seq"`
	Tables   []QuiescentTable `json:"tables"`
}

// quiescenceFile is quiescence.json: the unresolved blocks, ascending.
type quiescenceFile struct {
	Version int               `json:"version"`
	Blocks  []quiescenceBlock `json:"blocks"`
}

// Quiescence is the main process's quiescence ledger (spec §8.8): every
// block whose replay job reached this member, with the tables it names,
// until the block no longer needs the member's evidence. The reconciler
// drops an hg_member_unsafe replica only while no unresolved block names its
// table at that incarnation, so a block the member may still be asked to
// byte-scan never loses the replica it scans. Safe for concurrent use.
type Quiescence struct {
	dir          string
	mu           sync.Mutex
	blocks       []quiescenceBlock
	observations map[uint64]*quiescenceObservation
}

// A non-zero-size token identifies each successful observation, including
// identical redelivery. It is private process state, not a persisted field.
type quiescenceObservation struct{ marker byte }

// OpenQuiescence opens the ledger in stateDir (QuiescenceFile); a missing
// file is an empty ledger.
func OpenQuiescence(stateDir string) (*Quiescence, error) {
	if stateDir == "" {
		return nil, errors.New("member consensus: the quiescence ledger needs a state dir")
	}
	l, err := ReadQuiescence(stateDir)
	if err != nil {
		return nil, err
	}
	return &Quiescence{dir: stateDir, blocks: l.blocks, observations: map[uint64]*quiescenceObservation{}}, nil
}

// Observe records that block blockSeq names tables. It is durable in
// quiescence.json (temp file, fsync, rename, directory fsync) before it
// returns, so the record exists before the job's replay and scan run. A
// block observed again (a redelivery) gains the tables it did not name yet;
// an Observe that adds nothing writes nothing.
func (q *Quiescence) Observe(blockSeq uint64, tables []QuiescentTable) error {
	if blockSeq == 0 {
		return errors.New("member consensus: quiescence needs a block seq")
	}
	if len(tables) == 0 {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	next := slices.Clone(q.blocks)
	i, found := slices.BinarySearchFunc(next, blockSeq, func(b quiescenceBlock, seq uint64) int { return cmp.Compare(b.BlockSeq, seq) })
	if !found {
		next = slices.Insert(next, i, quiescenceBlock{BlockSeq: blockSeq})
	}
	merged := mergeTables(next[i].Tables, tables)
	if found && slices.Equal(merged, next[i].Tables) {
		q.observations[blockSeq] = &quiescenceObservation{}
		return nil
	}
	next[i].Tables = merged
	if err := writeQuiescence(q.dir, next); err != nil {
		return err
	}
	q.blocks = next
	q.observations[blockSeq] = &quiescenceObservation{}
	return nil
}

// Resolve asks the arbiter about every unresolved block and drops each one
// that no longer needs this member's evidence: its verdict is evaluated
// (latched), or nodeID is no longer in its verifier set (re-selected away).
// It keeps a block it could not read and returns those errors joined, after
// persisting what it resolved.
func (q *Quiescence) Resolve(ctx context.Context, client *dataplane.Client, nodeID string) error {
	q.mu.Lock()
	pending := make([]uint64, 0, len(q.blocks))
	observed := make(map[uint64]*quiescenceObservation, len(q.blocks))
	for _, b := range q.blocks {
		pending = append(pending, b.BlockSeq)
		observed[b.BlockSeq] = q.observations[b.BlockSeq]
	}
	q.mu.Unlock()
	resolved := map[uint64]bool{}
	var errs []error
	for _, seq := range pending {
		if err := ctx.Err(); err != nil {
			return err
		}
		bv, err := client.GetBlockVerification(ctx, seq)
		if err != nil {
			errs = append(errs, fmt.Errorf("member consensus: resolve block %d: %w", seq, err))
			continue
		}
		if bv.VerdictEvaluated || !slices.Contains(bv.VerifierSet, nodeID) {
			resolved[seq] = true
		}
	}
	if len(resolved) > 0 {
		q.mu.Lock()
		next := slices.DeleteFunc(slices.Clone(q.blocks), func(b quiescenceBlock) bool {
			return resolved[b.BlockSeq] && observed[b.BlockSeq] == q.observations[b.BlockSeq]
		})
		err := writeQuiescence(q.dir, next)
		if err == nil {
			q.blocks = next
			for seq := range resolved {
				if observed[seq] == q.observations[seq] {
					delete(q.observations, seq)
				}
			}
		}
		q.mu.Unlock()
		if err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// QuiescenceLedger is a read-only copy of quiescence.json, as the reconciler
// process reads it.
type QuiescenceLedger struct {
	blocks []quiescenceBlock
}

// ReadQuiescence reads the ledger in stateDir; a missing file is an empty
// ledger.
func ReadQuiescence(stateDir string) (QuiescenceLedger, error) {
	b, err := os.ReadFile(filepath.Join(stateDir, QuiescenceFile))
	if errors.Is(err, os.ErrNotExist) {
		return QuiescenceLedger{}, nil
	}
	if err != nil {
		return QuiescenceLedger{}, fmt.Errorf("member consensus: read %s: %w", QuiescenceFile, err)
	}
	var f quiescenceFile
	if err := json.Unmarshal(b, &f); err != nil {
		return QuiescenceLedger{}, fmt.Errorf("member consensus: %s is corrupt: %w", QuiescenceFile, err)
	}
	if f.Version != quiescenceVersion {
		return QuiescenceLedger{}, fmt.Errorf("member consensus: %s has version %d, want %d", QuiescenceFile, f.Version, quiescenceVersion)
	}
	if f.Blocks == nil {
		return QuiescenceLedger{}, fmt.Errorf("member consensus: %s is corrupt: missing blocks", QuiescenceFile)
	}
	var prior uint64
	for _, block := range f.Blocks {
		if block.BlockSeq == 0 || block.BlockSeq <= prior || len(block.Tables) == 0 {
			return QuiescenceLedger{}, fmt.Errorf("member consensus: %s is corrupt: invalid block record", QuiescenceFile)
		}
		prior = block.BlockSeq
		for i, table := range block.Tables {
			if strings.TrimSpace(table.TableID) == "" || table.IncarnationSeq == 0 {
				return QuiescenceLedger{}, fmt.Errorf("member consensus: %s is corrupt: invalid table record", QuiescenceFile)
			}
			if i > 0 {
				prev := block.Tables[i-1]
				if cmp.Or(cmp.Compare(prev.TableID, table.TableID), cmp.Compare(prev.IncarnationSeq, table.IncarnationSeq)) >= 0 {
					return QuiescenceLedger{}, fmt.Errorf("member consensus: %s is corrupt: unordered table records", QuiescenceFile)
				}
			}
		}
	}
	return QuiescenceLedger{blocks: f.Blocks}, nil
}

// Quiet reports whether no unresolved block names tableID at incarnationSeq.
func (l QuiescenceLedger) Quiet(tableID string, incarnationSeq uint64) bool {
	want := QuiescentTable{TableID: tableID, IncarnationSeq: incarnationSeq}
	for _, b := range l.blocks {
		if slices.Contains(b.Tables, want) {
			return false
		}
	}
	return true
}

// mergeTables is the sorted, deduplicated union of a and b.
func mergeTables(a, b []QuiescentTable) []QuiescentTable {
	out := append(slices.Clone(a), b...)
	slices.SortFunc(out, func(x, y QuiescentTable) int {
		return cmp.Or(cmp.Compare(x.TableID, y.TableID), cmp.Compare(x.IncarnationSeq, y.IncarnationSeq))
	})
	return slices.Compact(out)
}

// writeQuiescence persists blocks like the SNode's state.json
// (snode/state.go persistStateLocked): temp file, fsync, rename, directory
// fsync.
func writeQuiescence(dir string, blocks []quiescenceBlock) error {
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("member consensus: state dir: %w", err)
	}
	if blocks == nil {
		blocks = []quiescenceBlock{}
	}
	b, err := json.Marshal(quiescenceFile{Version: quiescenceVersion, Blocks: blocks})
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".quiescence-*")
	if err != nil {
		return fmt.Errorf("member consensus: write %s: %w", QuiescenceFile, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("member consensus: write %s: %w", QuiescenceFile, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("member consensus: write %s: %w", QuiescenceFile, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("member consensus: write %s: %w", QuiescenceFile, err)
	}
	if err := os.Rename(tmp.Name(), filepath.Join(dir, QuiescenceFile)); err != nil {
		return fmt.Errorf("member consensus: write %s: %w", QuiescenceFile, err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("member consensus: write %s: %w", QuiescenceFile, err)
	}
	defer d.Close()
	return d.Sync()
}
