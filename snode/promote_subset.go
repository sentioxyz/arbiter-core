package snode

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

// shadowPart is one unsafe-origin part in the promote shadow, identified by its
// row LtHash. An empty RowLtHash marks a part that holds no rows.
type shadowPart struct {
	Name      string
	RowLtHash string
}

// attachCandidateSubset fills the shadow with base ⊕ candidates when the
// candidates are a strict subset of the active parts in the hg_unsafe
// partition (a later statement wrote to the same partition before this
// promotion). It uses ClickHouse statements only: the SNode reaches ClickHouse
// over TCP and need not share its filesystem.
//
//  1. ATTACH PARTITION ... FROM hg_unsafe copies every active unsafe part of the
//     partition into the shadow. ClickHouse hardlinks them on its own disk under
//     fresh block numbers, so the shadow parts that were not there before are
//     exactly the unsafe-origin ones.
//  2. Each unsafe-origin part is scanned with chexec.ScanParts, the row
//     derivation that produced the candidates' part_row_lthash at intake, and
//     the candidates are matched against those hashes as a multiset.
//  3. Every unsafe-origin part that no candidate consumed is dropped with
//     DROP PART, and the shadow inventory must then be exactly base ∪ matched.
//
// Any failure aborts before REPLACE: the caller's closure gate never runs and
// hg_safe is untouched, the shadow partition is dropped best-effort, and the
// subscription redelivers the command, whose prepareShadow starts from a clean
// shadow again. A write that lands in hg_unsafe after the ATTACH is not in the
// shadow; one that lands before it is attached, matches no candidate and is
// dropped. The caller's closure gate still re-derives the shadow root and
// rejects anything other than base ⊕ candidates.
func (r *Role) attachCandidateSubset(ctx context.Context, cmd arbiter.PromoteSafePartition, sch payloadexec.TableSchema, table, promote, unsafe, partitionSQL string) (err error) {
	// Registered first, so every failure in this function drops the shadow
	// partition, including the prepareShadow base copy. The drop is
	// best-effort: if it fails too, the leftover is harmless because nothing
	// reads the shadow outside a promotion and the redelivered command's
	// prepareShadow drops the partition before rebuilding it.
	defer func() {
		if err != nil {
			_ = r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partitionSQL)
		}
	}()
	base, err := r.shadowPartitionParts(ctx, table, sch, cmd.PartitionID)
	if err != nil {
		return err
	}
	if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", promote, partitionSQL, unsafe)); err != nil {
		return err
	}
	attached, err := r.unsafeOriginShadowParts(ctx, table, sch, cmd.PartitionID, base)
	if err != nil {
		return fmt.Errorf("subset promotion %d of %s/%s: %w", cmd.PromotionSeq, cmd.TableID, cmd.PartitionID, err)
	}
	keep, drop, err := matchShadowCandidates(cmd, attached)
	if err != nil {
		return fmt.Errorf("subset promotion %d of %s/%s: %w", cmd.PromotionSeq, cmd.TableID, cmd.PartitionID, err)
	}
	for _, name := range drop {
		if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PART '%s'", promote, escapeSQLString(name))); err != nil {
			return err
		}
	}
	final, err := r.shadowPartitionParts(ctx, table, sch, cmd.PartitionID)
	if err != nil {
		return err
	}
	want := append(slices.Clone(base), keep...)
	sort.Strings(want)
	if !slices.Equal(final, want) {
		return fmt.Errorf("subset promotion %d of %s/%s: shadow holds parts [%s] after dropping non-candidates, want base ∪ candidates [%s]",
			cmd.PromotionSeq, cmd.TableID, cmd.PartitionID, strings.Join(final, ", "), strings.Join(want, ", "))
	}
	return nil
}

// shadowPartitionParts returns the sorted names of the active shadow parts in
// one logical partition.
func (r *Role) shadowPartitionParts(ctx context.Context, table string, sch payloadexec.TableSchema, partitionID string) ([]string, error) {
	parts, err := activeParts(ctx, r.d.Conn, r.cfg.PromoteDatabase, table)
	if err != nil {
		return nil, err
	}
	var names []string
	for _, p := range partsInLogicalPartition(parts, sch, partitionID) {
		names = append(names, p.Name)
	}
	sort.Strings(names)
	return names, nil
}

// unsafeOriginShadowParts returns the shadow parts of the partition that were
// not present before the ATTACH from hg_unsafe, with their row LtHash. Every
// base part must still be present: the promotion lock serialises the shadow
// partition, so a vanished base part means the shadow is not what this attempt
// built.
func (r *Role) unsafeOriginShadowParts(ctx context.Context, table string, sch payloadexec.TableSchema, partitionID string, base []string) ([]shadowPart, error) {
	parts, err := activeParts(ctx, r.d.Conn, r.cfg.PromoteDatabase, table)
	if err != nil {
		return nil, err
	}
	parts = partsInLogicalPartition(parts, sch, partitionID)
	isBase := make(map[string]bool, len(base))
	for _, name := range base {
		isBase[name] = true
	}
	var out []shadowPart
	var scan []string
	for _, p := range parts {
		if isBase[p.Name] {
			delete(isBase, p.Name)
			continue
		}
		out = append(out, shadowPart{Name: p.Name})
		if p.Rows > 0 {
			scan = append(scan, p.Name)
		}
	}
	if len(isBase) != 0 {
		missing := make([]string, 0, len(isBase))
		for name := range isBase {
			missing = append(missing, name)
		}
		sort.Strings(missing)
		return nil, fmt.Errorf("shadow base parts [%s] vanished during the attach from hg_unsafe", strings.Join(missing, ", "))
	}
	if len(scan) == 0 {
		return out, nil
	}
	scans, err := chexec.ScanParts(ctx, r.d.Conn, r.cfg.PromoteDatabase+"."+table, sch, scan)
	if err != nil {
		return nil, fmt.Errorf("scan unsafe-origin shadow parts: %w", err)
	}
	hashes := make(map[string]string, len(scans))
	for _, s := range scans {
		if s.RowLtHash == "" {
			return nil, fmt.Errorf("shadow part %s scanned without a row LtHash", s.PartName)
		}
		hashes[s.PartName] = s.RowLtHash
	}
	for i := range out {
		if h, ok := hashes[out[i].Name]; ok {
			out[i].RowLtHash = h
			delete(hashes, out[i].Name)
		} else if slices.Contains(scan, out[i].Name) {
			return nil, fmt.Errorf("shadow scan omitted part %s", out[i].Name)
		}
	}
	if len(hashes) != 0 {
		return nil, fmt.Errorf("shadow scan returned %d unrequested parts", len(hashes))
	}
	return out, nil
}

// candidateMultiset counts the command's candidates by canonical row LtHash.
func candidateMultiset(cmd arbiter.PromoteSafePartition) (map[string]int, error) {
	want := make(map[string]int, len(cmd.CandidateParts))
	for _, cp := range cmd.CandidateParts {
		if cp.PartRowLtHash == "" {
			return nil, fmt.Errorf("candidate part %s has invalid part_row_lthash: empty", cp.PartName)
		}
		h, err := parseAccumulatorHex(cp.PartRowLtHash)
		if err != nil {
			return nil, fmt.Errorf("candidate part %s has invalid part_row_lthash: %w", cp.PartName, err)
		}
		want[accumulatorHex(h)]++
	}
	return want, nil
}

// matchShadowCandidates matches the candidates against the unsafe-origin
// shadow parts by row LtHash, as a multiset: each candidate consumes one shadow
// part with its content, so identical-content parts are kept once per
// candidate that claims them. It returns the shadow parts to keep and to drop,
// both in name order, or an error when any candidate is left unmatched. A part
// without rows is never a candidate and is always dropped.
func matchShadowCandidates(cmd arbiter.PromoteSafePartition, shadow []shadowPart) (keep, drop []string, err error) {
	want, err := candidateMultiset(cmd)
	if err != nil {
		return nil, nil, err
	}
	shadow = slices.Clone(shadow)
	sort.Slice(shadow, func(i, j int) bool { return shadow[i].Name < shadow[j].Name })
	for _, p := range shadow {
		if p.RowLtHash == "" {
			drop = append(drop, p.Name)
			continue
		}
		h, err := parseAccumulatorHex(p.RowLtHash)
		if err != nil {
			return nil, nil, fmt.Errorf("shadow part %s: %w", p.Name, err)
		}
		k := accumulatorHex(h)
		if want[k] > 0 {
			want[k]--
			keep = append(keep, p.Name)
			continue
		}
		drop = append(drop, p.Name)
	}
	var missing []string
	for _, cp := range cmd.CandidateParts {
		h, _ := parseAccumulatorHex(cp.PartRowLtHash)
		k := accumulatorHex(h)
		if want[k] > 0 {
			want[k]--
			missing = append(missing, fmt.Sprintf("%s(%s)", cp.PartName, abbreviateRoot(cp.PartRowLtHash)))
		}
	}
	if len(missing) != 0 {
		attached := make([]string, 0, len(shadow))
		for _, p := range shadow {
			attached = append(attached, fmt.Sprintf("%s(%s)", p.Name, abbreviateRoot(p.RowLtHash)))
		}
		return nil, nil, fmt.Errorf("candidate parts [%s] are not present in hg_unsafe: the attached unsafe partition holds [%s]",
			strings.Join(missing, ", "), strings.Join(attached, ", "))
	}
	return keep, drop, nil
}
