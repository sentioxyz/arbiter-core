package consensus

import (
	"cmp"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sync"
)

// replicaSignerAddress is a normalized replica_signer: lowercase 0x and 40
// hex characters, the form authority.Signer.Address returns.
var replicaSignerAddress = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

// CheckpointSeqStore is the member's checkpoint_seq journal, CheckpointSeqFile
// in the member state directory (housegate spec 2026-10-14 §9.1; plan S3-E1
// contract §42.3): per owner, the replica_signer address and the highest
// checkpoint_seq this member signed under it. Next writes a seq to disk (temp
// file, fsync, rename, directory fsync) before it returns it, so no seq is
// ever signed twice, across crashes included. It is safe for concurrent use.
type CheckpointSeqStore struct {
	mu   sync.Mutex
	path string
	seqs map[uint64]checkpointSeqEntry
}

// checkpointSeqEntry is one owner's line of the journal.
type checkpointSeqEntry struct {
	OwnerIndexerID uint64 `json:"owner_indexer_id"`
	ReplicaSigner  string `json:"replica_signer"`
	CheckpointSeq  uint64 `json:"checkpoint_seq"`
}

// checkpointSeqDocument is the journal file,
// {"owners":[{"owner_indexer_id":0,"replica_signer":"0x…","checkpoint_seq":7}]},
// sorted by owner.
type checkpointSeqDocument struct {
	Owners []checkpointSeqEntry `json:"owners"`
}

// OpenCheckpointSeqStore reads stateDir's journal. A missing file is an empty
// journal (a fresh member, or one that lost its state: Next then resumes from
// the published value). A journal that does not parse, or that names an owner
// twice, an unnormalized signer or a zero seq, is refused.
func OpenCheckpointSeqStore(stateDir string) (*CheckpointSeqStore, error) {
	if stateDir == "" {
		return nil, errors.New("member consensus: the checkpoint_seq journal needs a state directory")
	}
	s := &CheckpointSeqStore{path: filepath.Join(stateDir, CheckpointSeqFile), seqs: map[uint64]checkpointSeqEntry{}}
	b, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("member consensus: read %s: %w", s.path, err)
	}
	var doc checkpointSeqDocument
	if err := json.Unmarshal(b, &doc); err != nil {
		return nil, fmt.Errorf("member consensus: %s does not parse: %w", s.path, err)
	}
	for _, e := range doc.Owners {
		if _, dup := s.seqs[e.OwnerIndexerID]; dup || !replicaSignerAddress.MatchString(e.ReplicaSigner) || e.CheckpointSeq == 0 {
			return nil, fmt.Errorf("member consensus: %s: owner %d: want one entry per owner with a lowercase 0x replica_signer and a checkpoint_seq of at least 1", s.path, e.OwnerIndexerID)
		}
		s.seqs[e.OwnerIndexerID] = e
	}
	return s, nil
}

// Next returns the checkpoint_seq of owner's next checkpoint under
// replicaSigner: one above both the journal's value under that signer and
// published, the registry's value for this member and owner, which the caller
// passes only when the registry names the same signer (else 0). A journal
// entry under another signer belongs to a retired key and restarts from
// published, as Apply restarts the stored value when replica_signer rotates
// (spec §9.1). The new value is durable in the journal before Next returns;
// when the write fails the journal is unchanged and Next returns the error.
func (s *CheckpointSeqStore) Next(owner uint64, replicaSigner string, published uint64) (uint64, error) {
	if !replicaSignerAddress.MatchString(replicaSigner) {
		return 0, fmt.Errorf("member consensus: replica_signer %q is not a lowercase 0x-prefixed 20-byte address", replicaSigner)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	last := published
	if e, ok := s.seqs[owner]; ok && e.ReplicaSigner == replicaSigner {
		last = max(last, e.CheckpointSeq)
	}
	if last == math.MaxUint64 {
		return 0, fmt.Errorf("member consensus: owner %d: checkpoint_seq is exhausted", owner)
	}
	next := checkpointSeqEntry{OwnerIndexerID: owner, ReplicaSigner: replicaSigner, CheckpointSeq: last + 1}
	seqs := maps.Clone(s.seqs)
	seqs[owner] = next
	if err := writeCheckpointSeqs(s.path, seqs); err != nil {
		return 0, err
	}
	s.seqs = seqs
	return next.CheckpointSeq, nil
}

// writeCheckpointSeqs writes the journal atomically: a temp file in the same
// directory, fsync, rename over the journal, fsync of the directory.
func writeCheckpointSeqs(path string, seqs map[uint64]checkpointSeqEntry) error {
	doc := checkpointSeqDocument{Owners: slices.Collect(maps.Values(seqs))}
	slices.SortFunc(doc.Owners, func(a, b checkpointSeqEntry) int { return cmp.Compare(a.OwnerIndexerID, b.OwnerIndexerID) })
	b, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("member consensus: encode %s: %w", path, err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, CheckpointSeqFile+".tmp-*")
	if err != nil {
		return fmt.Errorf("member consensus: write %s: %w", path, err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("member consensus: write %s: %w", path, err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("member consensus: sync %s: %w", path, err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("member consensus: write %s: %w", path, err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("member consensus: replace %s: %w", path, err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("member consensus: sync %s: %w", dir, err)
	}
	defer func() { _ = d.Close() }()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("member consensus: sync %s: %w", dir, err)
	}
	return nil
}
