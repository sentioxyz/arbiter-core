package rebuild

import (
	"encoding/json"
	"slices"
)

// PartitionReport is the rebuild's account of one owned partition: the
// arbiter's PublishSeq, the parts of its SafeParts, how many of them were
// fetched from each source (a dry run: would be; both 0 for a partition that
// already held them), the parts no source holds (a dry run only; a real run
// refuses instead), and the base root they close over.
type PartitionReport struct {
	TableID, PartitionID                  string
	PublishSeq                            uint64
	Parts, FromOwnUnsafe, FromObjectStore int
	Missing                               []string
	Root                                  string
}

// Report is a rebuild's outcome: the fence, one entry per owned partition
// with safe parts, whether hg_safe and the journal were already rebuilt (then
// nothing was written) and whether it was a dry run.
type Report struct {
	Fence          []FenceResult
	Partitions     []PartitionReport
	AlreadyRebuilt bool
	DryRun         bool
}

type fenceLine struct {
	Phase   Phase  `json:"phase"`
	TableID string `json:"table_id"`
	Path    string `json:"path"`
	Present bool   `json:"present"`
	Dropped bool   `json:"dropped"`
}

type partitionLine struct {
	Phase           Phase    `json:"phase"`
	TableID         string   `json:"table_id"`
	PartitionID     string   `json:"partition_id"`
	PublishSeq      uint64   `json:"publish_seq"`
	Parts           int      `json:"parts"`
	FromOwnUnsafe   int      `json:"from_own_unsafe"`
	FromObjectStore int      `json:"from_object_store"`
	Missing         []string `json:"missing"`
	Root            string   `json:"root"`
}

type doneLine struct {
	Phase           Phase `json:"phase"`
	DryRun          bool  `json:"dry_run"`
	AlreadyRebuilt  bool  `json:"already_rebuilt"`
	Tables          int   `json:"tables"`
	Partitions      int   `json:"partitions"`
	Parts           int   `json:"parts"`
	FromOwnUnsafe   int   `json:"from_own_unsafe"`
	FromObjectStore int   `json:"from_object_store"`
	Missing         int   `json:"missing"`
}

// JSONLines renders r as one JSON object per line, each with a "phase": one
// "fence" line per fence result, one "plan" (dry run) or "restore" line per
// partition, and a final "done" line with the totals. The lines carry no key
// or password.
func (r Report) JSONLines() []string {
	var out []string
	add := func(v any) {
		b, err := json.Marshal(v)
		if err != nil {
			panic(err) // the line types marshal without error
		}
		out = append(out, string(b))
	}
	for _, f := range r.Fence {
		add(fenceLine{Phase: PhaseFence, TableID: f.TableID, Path: f.Path, Present: f.Present, Dropped: f.Dropped})
	}
	phase := PhaseRestore
	if r.DryRun {
		phase = PhasePlan
	}
	done := doneLine{Phase: PhaseDone, DryRun: r.DryRun, AlreadyRebuilt: r.AlreadyRebuilt}
	var tables []string
	for _, p := range r.Partitions {
		missing := p.Missing
		if missing == nil {
			missing = []string{}
		}
		add(partitionLine{Phase: phase, TableID: p.TableID, PartitionID: p.PartitionID, PublishSeq: p.PublishSeq, Parts: p.Parts,
			FromOwnUnsafe: p.FromOwnUnsafe, FromObjectStore: p.FromObjectStore, Missing: missing, Root: p.Root})
		if !slices.Contains(tables, p.TableID) {
			tables = append(tables, p.TableID)
		}
		done.Partitions++
		done.Parts += p.Parts
		done.FromOwnUnsafe += p.FromOwnUnsafe
		done.FromObjectStore += p.FromObjectStore
		done.Missing += len(p.Missing)
	}
	done.Tables = len(tables)
	add(done)
	return out
}
