package snode

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

// ErrPromoteTableMissing means the protocol-owned hg_promote table does not
// exist. Spec L D5 moved it into EnsureProtocolTables, so its absence is a
// startup-detectable condition rather than a first-promotion surprise; the
// promotion path no longer creates it, because an ad-hoc CREATE would bypass
// the pinned DDL and its drift detection.
var ErrPromoteTableMissing = errors.New("snode: hg_promote table is missing; run the role with a create-capable schema source so EnsureProtocolTables can build it")

func (r *Role) buildAndReplace(ctx context.Context, cmd arbiter.PromoteSafePartition) (string, safePartMappings, error) {
	if r.d.Conn == nil {
		return "", safePartMappings{}, fmt.Errorf("snode: clickhouse connection is required")
	}
	sch, err := r.schemaFor(cmd.TableID)
	if err != nil {
		return "", safePartMappings{}, err
	}
	table := CHTableName(cmd.TableID)
	safe := r.cfg.SafeDatabase + "." + table
	promote := r.cfg.PromoteDatabase + "." + table
	partition, err := quotePartition(sch, cmd.PartitionID)
	if err != nil {
		return "", safePartMappings{}, err
	}
	if err := r.prepareShadow(ctx, cmd, sch, table, safe, promote, partition); err != nil {
		return "", safePartMappings{}, err
	}
	safeBefore, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return "", safePartMappings{}, err
	}
	if err := r.attachCandidateParts(ctx, cmd, sch, table, promote, partition); err != nil {
		return "", safePartMappings{}, err
	}
	// Shadow closure gate (§8.2 / spec §5b): the shadow must hold EXACTLY
	// base + candidates before it is published. Re-scan the shadow's target
	// partition and assert its physical content root equals base ⊕ candidates.
	// This is the safety boundary that makes the attach mechanism irrelevant:
	// if the whole-partition ATTACH fallback pulled in a stray unverified part
	// that a concurrent source write raced into the unsafe partition, the
	// shadow root diverges and we reject BEFORE the atomic REPLACE, so an
	// unverified part can never reach hg_safe. The failure propagates (no ack);
	// the orchestrator resends, and on retry the now-visible extra part makes
	// candidatesCoverUnsafePartition false, so the per-part hardlink path runs
	// and attaches only the candidates — the gate then passes (self-healing).
	post, err := lthashCombineHexAll(cmd.BasePartitionRoot, candidateHashes(cmd))
	if err != nil {
		return "", safePartMappings{}, err
	}
	shadowRoot, err := r.partitionContentRoot(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID)
	if err != nil {
		return "", safePartMappings{}, err
	}
	if shadowRoot != post {
		_ = r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partition)
		return "", safePartMappings{}, shadowClosureMismatch(shadowRoot, post, partsInLogicalPartition(safeBefore, sch, cmd.PartitionID), cmd)
	}
	safeBefore = partsInLogicalPartition(safeBefore, sch, cmd.PartitionID)
	intent := promotionIntentFor(cmd, post, safeBefore)
	if err := r.state.BeginPromotion(partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}, intent); err != nil {
		return "", safePartMappings{}, fmt.Errorf("journal promotion intent: %w", err)
	}
	if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION %s FROM %s", safe, partition, promote)); err != nil {
		return "", safePartMappings{}, err
	}
	if err := r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partition); err != nil {
		return "", safePartMappings{}, err
	}
	mappings, err := r.safeMappings(ctx, safe, table, sch, cmd, post)
	if err != nil {
		return "", safePartMappings{}, err
	}
	return post, mappings, nil
}

// reconcilePromotionIntent resolves the crash boundary around REPLACE. The
// current content root distinguishes "intent persisted, REPLACE not run" from
// "REPLACE visible, final ACK state not persisted" even when a content-neutral
// part rewrite changed the base inventory. In the latter case the persisted
// complete current partition derives mappings without publishing candidates twice.
func (r *Role) reconcilePromotionIntent(ctx context.Context, cmd arbiter.PromoteSafePartition, intent promotionIntent) (string, safePartMappings, bool, error) {
	post, err := lthashCombineHexAll(cmd.BasePartitionRoot, candidateHashes(cmd))
	if err != nil {
		return "", safePartMappings{}, false, err
	}
	if !promotionIntentMatchesCommand(intent, cmd, post) {
		return "", safePartMappings{}, false, fmt.Errorf("unresolved promotion intent seq %d does not match command seq %d", intent.PromotionSeq, cmd.PromotionSeq)
	}
	sch, err := r.schemaFor(cmd.TableID)
	if err != nil {
		return "", safePartMappings{}, false, err
	}
	table := CHTableName(cmd.TableID)
	safe := r.cfg.SafeDatabase + "." + table
	partition, err := quotePartition(sch, cmd.PartitionID)
	if err != nil {
		return "", safePartMappings{}, false, err
	}
	currentRoot, err := r.partitionContentRoot(ctx, r.cfg.SafeDatabase, table, sch, cmd.PartitionID)
	if err != nil {
		return "", safePartMappings{}, false, err
	}
	baseRoot, err := lthashCombineHexAll(cmd.BasePartitionRoot, nil)
	if err != nil {
		return "", safePartMappings{}, false, err
	}
	if currentRoot == baseRoot {
		return "", safePartMappings{}, false, nil
	}
	if currentRoot != post {
		return "", safePartMappings{}, false, fmt.Errorf("safe partition after unresolved promotion has root %s, expected base %s or post %s", currentRoot, baseRoot, post)
	}
	if err := r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partition); err != nil {
		return "", safePartMappings{}, false, err
	}
	mappings, err := r.safeMappings(ctx, safe, table, sch, cmd, post)
	if err != nil {
		return "", safePartMappings{}, false, err
	}
	return post, mappings, true, nil
}

func promotionIntentFor(cmd arbiter.PromoteSafePartition, post string, safeBefore []partInfo) promotionIntent {
	unsafeNames := make([]string, 0, len(cmd.CandidateParts))
	for _, cp := range cmd.CandidateParts {
		unsafeNames = append(unsafeNames, cp.PartName)
	}
	before := make([]promotionSafePart, 0, len(safeBefore))
	for _, p := range safeBefore {
		before = append(before, promotionSafePart{
			Name: p.Name, PartitionID: p.PartitionID,
			PartitionValue: p.PartitionValue, PhysHash: p.PhysHash,
		})
	}
	return promotionIntent{
		PromotionSeq:        cmd.PromotionSeq,
		BasePartitionRoot:   cmd.BasePartitionRoot,
		PostPartitionRoot:   post,
		BaseSafeSnapshotID:  cmd.BaseSafeSnapshotID,
		CandidatePartHashes: candidateHashes(cmd),
		UnsafePartNames:     unsafeNames,
		SafePartsBefore:     before,
	}
}

func promotionIntentMatchesCommand(intent promotionIntent, cmd arbiter.PromoteSafePartition, post string) bool {
	if intent.PromotionSeq != cmd.PromotionSeq ||
		intent.BasePartitionRoot != cmd.BasePartitionRoot ||
		intent.PostPartitionRoot != post ||
		intent.BaseSafeSnapshotID != cmd.BaseSafeSnapshotID ||
		!slices.Equal(intent.CandidatePartHashes, candidateHashes(cmd)) {
		return false
	}
	names := make([]string, 0, len(cmd.CandidateParts))
	for _, cp := range cmd.CandidateParts {
		names = append(names, cp.PartName)
	}
	return slices.Equal(intent.UnsafePartNames, names)
}

func partsInLogicalPartition(parts []partInfo, sch payloadexec.TableSchema, partitionID string) []partInfo {
	out := make([]partInfo, 0, len(parts))
	for _, p := range parts {
		if logicalPartitionID(sch, p) == partitionID {
			out = append(out, p)
		}
	}
	return out
}

func samePromotionSafeParts(before []promotionSafePart, current []partInfo) bool {
	if len(before) != len(current) {
		return false
	}
	for i := range before {
		if before[i].Name != current[i].Name || before[i].PhysHash != current[i].PhysHash {
			return false
		}
	}
	return true
}

// partitionContentRoot sums the row-LtHash of every active part in one logical
// partition of db.table, giving the partition's physical content commitment.
// It uses the same chexec.ScanParts row derivation as the executor, so the
// value is comparable to base ⊕ candidate hashes by construction.
func (r *Role) partitionContentRoot(ctx context.Context, db, table string, sch payloadexec.TableSchema, partitionID string) (string, error) {
	parts, err := activeParts(ctx, r.d.Conn, db, table)
	if err != nil {
		return "", err
	}
	var names []string
	for _, p := range parts {
		if logicalPartitionID(sch, p) == partitionID {
			names = append(names, p.Name)
		}
	}
	if len(names) == 0 {
		return accumulatorHex(lthash.New()), nil
	}
	scans, err := chexec.ScanParts(ctx, r.d.Conn, db+"."+table, sch, names)
	if err != nil {
		return "", err
	}
	acc := lthash.New()
	for _, s := range scans {
		h, err := parseAccumulatorHex(s.RowLtHash)
		if err != nil {
			return "", err
		}
		acc.AddHash(h)
	}
	return accumulatorHex(acc), nil
}

func (r *Role) prepareShadow(ctx context.Context, cmd arbiter.PromoteSafePartition, sch payloadexec.TableSchema, table, safe, promote, partition string) error {
	var exists uint64
	if err := r.d.Conn.QueryRow(ctx,
		"SELECT count() FROM system.tables WHERE database = ? AND name = ?", r.cfg.PromoteDatabase, table,
	).Scan(&exists); err != nil {
		return fmt.Errorf("snode: check %s: %w", promote, err)
	}
	if exists == 0 {
		return fmt.Errorf("%w: %s", ErrPromoteTableMissing, promote)
	}
	if err := r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partition); err != nil {
		return err
	}
	if cmd.BasePartitionRoot == "" {
		return nil
	}
	return r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", promote, partition, safe))
}

func (r *Role) attachCandidateParts(ctx context.Context, cmd arbiter.PromoteSafePartition, sch payloadexec.TableSchema, table, promote, partitionSQL string) error {
	if ok, err := r.candidatesCoverUnsafePartition(ctx, cmd, sch, table); err != nil {
		return err
	} else if ok {
		unsafe := r.cfg.UnsafeDatabase + "." + table
		return r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", promote, partitionSQL, unsafe))
	}
	for _, cp := range cmd.CandidateParts {
		if cp.PartName == "" {
			return fmt.Errorf("candidate part for %s/%s has empty part_name", cp.TableID, cp.PartitionID)
		}
		if err := r.attachCandidatePart(ctx, table, promote, cp.PartName); err != nil {
			return err
		}
	}
	return nil
}

func (r *Role) candidatesCoverUnsafePartition(ctx context.Context, cmd arbiter.PromoteSafePartition, sch payloadexec.TableSchema, table string) (bool, error) {
	parts, err := activeParts(ctx, r.d.Conn, r.cfg.UnsafeDatabase, table)
	if err != nil {
		return false, err
	}
	candidates := make(map[string]bool, len(cmd.CandidateParts))
	for _, cp := range cmd.CandidateParts {
		if cp.PartName == "" {
			return false, fmt.Errorf("candidate part for %s/%s has empty part_name", cp.TableID, cp.PartitionID)
		}
		candidates[cp.PartName] = true
	}
	var partitionParts int
	for _, p := range parts {
		if logicalPartitionID(sch, p) != cmd.PartitionID {
			continue
		}
		partitionParts++
		if !candidates[p.Name] {
			return false, nil
		}
	}
	return partitionParts == len(candidates), nil
}

// shadowClosureMismatch describes a failed shadow closure gate with what an
// operator needs to find the divergent side: the safe parts that entered the
// shadow and the claimed candidates. A safe part the base root does not account
// for (for example one a ClickHouse restart reactivated after an earlier
// REPLACE PARTITION) shows up here by name; compare the list with the base
// safe manifest's active parts. Roots are abbreviated: they are 2048-byte
// accumulators.
func shadowClosureMismatch(shadowRoot, post string, safeBefore []partInfo, cmd arbiter.PromoteSafePartition) error {
	safe := make([]string, 0, len(safeBefore))
	for _, p := range safeBefore {
		safe = append(safe, fmt.Sprintf("%s(rows=%d)", p.Name, p.Rows))
	}
	candidates := make([]string, 0, len(cmd.CandidateParts))
	for _, p := range cmd.CandidateParts {
		candidates = append(candidates, p.PartName)
	}
	return fmt.Errorf("shadow closure mismatch for %s/%s promotion %d: promote partition root %s != base+candidates %s (unverified or missing part in shadow); safe parts before promotion [%s], candidate parts [%s]",
		cmd.TableID, cmd.PartitionID, cmd.PromotionSeq, abbreviateRoot(shadowRoot), abbreviateRoot(post),
		strings.Join(safe, ", "), strings.Join(candidates, ", "))
}

func abbreviateRoot(root string) string {
	if len(root) <= 26 {
		return root
	}
	return root[:18] + "…" + root[len(root)-8:]
}
