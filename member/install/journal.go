package install

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"

	"github.com/housegate/housegate/pkg/replay"
)

// JournalFile is the installer's journal in Config.StateDir. Nothing but this
// package writes it; Plan S3-E1 keeps its own files beside it.
const JournalFile = "install-journal.json"

// journalVersion is the only Journal.Version this package reads and writes.
const journalVersion = 1

// Journal records what every copy holds (housegate spec 2026-10-14 §8.5 step
// 7): per owner, the manifest and track frontier its copies were last brought
// to together, and per table the parts installed into its copy.
type Journal struct {
	Version int                      `json:"version"` // 1
	NodeID  string                   `json:"node_id"`
	Owners  map[string]*OwnerJournal `json:"owners"` // key: decimal indexer id
}

// OwnerJournal is one owner's part of the journal. SnapshotID and Entry move
// together with every table of the owner, in one write, once every ready copy
// of the owner closed at that manifest.
type OwnerJournal struct {
	SnapshotID string                   `json:"snapshot_id"`
	Entry      replay.TrackFrontier     `json:"entry"`
	Tables     map[string]*TableJournal `json:"tables"` // key: <database>.<table>
}

// TableJournal is one copy: its incarnation, its physical name in ReplicaDB
// and the parts it holds. Diverged lists the partitions awaiting a rebuild
// (spec §8.5 step 2); Foreign quarantines the whole table (Plan S3-C2 K36)
// and ForeignScratch names the hg_rstage table kept for forensics.
type TableJournal struct {
	IncarnationSeq uint64        `json:"incarnation_seq"`
	Copy           string        `json:"copy"`
	Parts          []JournalPart `json:"parts"`
	Diverged       []string      `json:"diverged,omitempty"` // partition ids awaiting repair
	Foreign        bool          `json:"foreign,omitempty"`
	ForeignScratch string        `json:"foreign_scratch,omitempty"`
}

// JournalPart is one installed part, by its name in the copy.
type JournalPart struct {
	Name           string `json:"name"`
	PartitionID    string `json:"partition_id"`
	PartRowLtHash  string `json:"part_row_lthash"`
	HashOfAllFiles string `json:"hash_of_all_files"`
	Rows           uint64 `json:"rows"`
}

func newJournal(nodeID string) *Journal {
	return &Journal{Version: journalVersion, NodeID: nodeID, Owners: map[string]*OwnerJournal{}}
}

func ownerKey(owner uint64) string { return strconv.FormatUint(owner, 10) }

// readJournal reads path; a missing file is (nil, nil).
func readJournal(path string) (*Journal, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("install: read journal %s: %w", path, err)
	}
	var j Journal
	if err := json.Unmarshal(b, &j); err != nil {
		return nil, fmt.Errorf("install: journal %s: %w", path, err)
	}
	if j.Version != journalVersion {
		return nil, fmt.Errorf("install: journal %s has version %d, want %d", path, j.Version, journalVersion)
	}
	if j.Owners == nil {
		j.Owners = map[string]*OwnerJournal{}
	}
	for key, oj := range j.Owners {
		if _, err := strconv.ParseUint(key, 10, 64); err != nil || oj == nil {
			return nil, fmt.Errorf("install: journal %s has a bad owner entry %q", path, key)
		}
		if oj.Tables == nil {
			oj.Tables = map[string]*TableJournal{}
		}
		for tableKey, tj := range oj.Tables {
			if tj == nil {
				return nil, fmt.Errorf("install: journal %s has an empty entry for table %s", path, tableKey)
			}
		}
	}
	return &j, nil
}

// writeJournal replaces path atomically: a temp file in the same directory,
// Sync, rename, directory Sync (the arbiter-core:snode/state.go pattern), so a
// crash leaves either the old or the new journal.
func writeJournal(path string, j *Journal) error {
	b, err := json.MarshalIndent(j, "", "  ")
	if err != nil {
		return fmt.Errorf("install: encode journal: %w", err)
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".install-journal-*")
	if err != nil {
		return fmt.Errorf("install: write journal: %w", err)
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("install: write journal: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("install: sync journal: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("install: write journal: %w", err)
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return fmt.Errorf("install: replace journal: %w", err)
	}
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("install: sync state dir: %w", err)
	}
	defer d.Close()
	if err := d.Sync(); err != nil {
		return fmt.Errorf("install: sync state dir: %w", err)
	}
	return nil
}

func (j *Journal) clone() *Journal {
	out := &Journal{Version: j.Version, NodeID: j.NodeID, Owners: make(map[string]*OwnerJournal, len(j.Owners))}
	for key, oj := range j.Owners {
		out.Owners[key] = oj.clone()
	}
	return out
}

func (oj *OwnerJournal) clone() *OwnerJournal {
	out := &OwnerJournal{SnapshotID: oj.SnapshotID, Entry: oj.Entry, Tables: make(map[string]*TableJournal, len(oj.Tables))}
	for key, tj := range oj.Tables {
		out.Tables[key] = tj.clone()
	}
	return out
}

func (tj *TableJournal) clone() *TableJournal {
	out := *tj
	out.Parts = slices.Clone(tj.Parts)
	out.Diverged = slices.Clone(tj.Diverged)
	return &out
}

// owner returns the owner's journal, creating it.
func (j *Journal) owner(owner uint64) *OwnerJournal {
	oj := j.Owners[ownerKey(owner)]
	if oj == nil {
		oj = &OwnerJournal{Tables: map[string]*TableJournal{}}
		j.Owners[ownerKey(owner)] = oj
	}
	return oj
}

// partitionHashes is L of spec §8.5 step 1: the row hashes the journal lists
// for one partition, repeats kept.
func (tj *TableJournal) partitionHashes(partitionID string) []string {
	var out []string
	for _, p := range tj.Parts {
		if p.PartitionID == partitionID {
			out = append(out, p.PartRowLtHash)
		}
	}
	return out
}

// partitions lists, sorted, the partitions the journal lists parts of.
func (tj *TableJournal) partitions() []string {
	var out []string
	for _, p := range tj.Parts {
		out = append(out, p.PartitionID)
	}
	slices.Sort(out)
	return slices.Compact(out)
}

func (tj *TableJournal) isDiverged(partitionID string) bool {
	return slices.Contains(tj.Diverged, partitionID)
}

// markDiverged records partitionID as awaiting a rebuild; it reports whether
// the partition was not marked yet.
func (tj *TableJournal) markDiverged(partitionID string) bool {
	if tj.isDiverged(partitionID) {
		return false
	}
	tj.Diverged = append(tj.Diverged, partitionID)
	slices.Sort(tj.Diverged)
	return true
}

// setPartition makes parts the journaled content of partitionID and clears
// its Diverged mark.
func (tj *TableJournal) setPartition(partitionID string, parts []JournalPart) {
	tj.Parts = slices.DeleteFunc(tj.Parts, func(p JournalPart) bool { return p.PartitionID == partitionID })
	tj.Parts = append(tj.Parts, parts...)
	sortJournalParts(tj.Parts)
	tj.Diverged = slices.DeleteFunc(tj.Diverged, func(p string) bool { return p == partitionID })
}

func (tj *TableJournal) addParts(parts ...JournalPart) {
	tj.Parts = append(tj.Parts, parts...)
	sortJournalParts(tj.Parts)
}

func sortJournalParts(parts []JournalPart) {
	slices.SortFunc(parts, func(a, b JournalPart) int {
		if c := strings.Compare(a.PartitionID, b.PartitionID); c != 0 {
			return c
		}
		return strings.Compare(a.Name, b.Name)
	})
}
