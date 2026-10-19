// Package gate is the pure install gate of housegate spec 2026-10-14 §8.5:
// the per-partition plan against a published manifest (steps 1–3), the
// scratch classifier (step 4, classify.go) and the arithmetic closure and
// partition-set checks (step 6). It runs no ClickHouse statement and makes
// no network call; the callers that do (the object-store restore drill, the
// stage-3 member installer, the owner rebuild) feed it row LtHashes.
//
// Row hashes are LtHash accumulators in the executor's hex form, "0x" +
// lowercase hex (payloadexec.SumPartRowLtHashes); other spellings of the same
// accumulator are accepted and compared, and returned, in that form.
// Multisets are slices with repeats; their order never matters.
package gate

import (
	"fmt"
	"slices"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Plan is one partition's install plan (§8.5 steps 1–3). Diverged means the
// local copy holds a part the manifest does not list, or more copies of one
// than it lists: the partition is rebuilt from every manifest part, and Need
// is nil. Otherwise Need is M − L, sorted. Root is the partition root R.
type Plan struct {
	Diverged bool
	Need     []string
	Root     string
}

// PlanPartition plans partition p of table T at manifest S: manifest is M,
// the PartRowLtHash of every part S lists for (T, p); root is R; local is L,
// the row LtHashes of the local copy's parts. A manifest whose parts do not
// sum to its root is refused before L is looked at.
func PlanPartition(manifest []string, root string, local []string) (Plan, error) {
	m, err := canonicalList("manifest", manifest)
	if err != nil {
		return Plan{}, err
	}
	r, err := canonicalOne("partition root", root)
	if err != nil {
		return Plan{}, err
	}
	if got := sumOf(m); got != r {
		return Plan{}, fmt.Errorf("gate: manifest parts sum to %s, not the partition root %s", abbreviate(got), abbreviate(r))
	}
	l, err := canonicalList("local", local)
	if err != nil {
		return Plan{}, err
	}
	left := counts(m)
	for _, h := range l {
		if left[h] == 0 {
			return Plan{Diverged: true, Root: r}, nil
		}
		left[h]--
	}
	var need []string
	for _, h := range m {
		if left[h] > 0 {
			need = append(need, h)
			left[h]--
		}
	}
	slices.Sort(need)
	return Plan{Need: need, Root: r}, nil
}

// Closure is §8.5 step 6's arithmetic check: Σ(local) + Σ(kept) must equal
// the partition root. Repeats count.
func Closure(root string, local, kept []string) error {
	r, err := canonicalOne("partition root", root)
	if err != nil {
		return err
	}
	l, err := canonicalList("local", local)
	if err != nil {
		return err
	}
	k, err := canonicalList("kept", kept)
	if err != nil {
		return err
	}
	if got := sumOf(append(l, k...)); got != r {
		return fmt.Errorf("gate: closure: %d local and %d kept parts sum to %s, the partition root is %s", len(l), len(k), abbreviate(got), abbreviate(r))
	}
	return nil
}

// PartitionSet is §8.5 step 6's partition-set check: the set of logical
// partition ids of the local table must be the set the manifest lists for it.
// Both arguments may repeat an id (one per part, say); order is ignored.
func PartitionSet(manifest, local []string) error {
	m, l := set(manifest), set(local)
	if !slices.Equal(m, l) {
		return fmt.Errorf("gate: partition set %v is not the manifest's %v", l, m)
	}
	return nil
}

// canonicalList validates hashes as LtHash accumulators and returns each in
// the executor's form. The refusal names the list and the index.
func canonicalList(what string, hashes []string) ([]string, error) {
	if _, err := payloadexec.SumPartRowLtHashes(hashes); err != nil {
		return nil, fmt.Errorf("gate: %s: %w", what, err)
	}
	out := make([]string, 0, len(hashes))
	for _, h := range hashes {
		out = append(out, sumOf([]string{h}))
	}
	return out, nil
}

func canonicalOne(what, hash string) (string, error) {
	out, err := canonicalList(what, []string{hash})
	if err != nil {
		return "", err
	}
	return out[0], nil
}

// sumOf sums hashes that canonicalList accepted, so it cannot fail; were it
// to, the empty string equals no root or hash, and every check fails closed.
func sumOf(hashes []string) string {
	s, err := payloadexec.SumPartRowLtHashes(hashes)
	if err != nil {
		return ""
	}
	return s
}

func counts(hashes []string) map[string]int {
	out := make(map[string]int, len(hashes))
	for _, h := range hashes {
		out[h]++
	}
	return out
}

func set(ids []string) []string {
	out := slices.Clone(ids)
	slices.Sort(out)
	return slices.Compact(out)
}

// abbreviate shortens a 4098-character accumulator for a refusal, as the
// SNode's own messages do (snode abbreviateRoot).
func abbreviate(h string) string {
	if len(h) <= 26 {
		return h
	}
	return h[:18] + "…" + h[len(h)-8:]
}
