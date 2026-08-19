package snode

import (
	"context"
	"fmt"
	"slices"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func (r *Role) buildAndReplace(ctx context.Context, cmd arbiter.PromoteSafePartition) (string, []arbiter.SafePartMapping, error) {
	if r.d.Conn == nil {
		return "", nil, fmt.Errorf("snode: clickhouse connection is required")
	}
	sch, err := r.schemaFor(cmd.TableID)
	if err != nil {
		return "", nil, err
	}
	table := CHTableName(cmd.TableID)
	safe := r.cfg.SafeDatabase + "." + table
	promote := r.cfg.PromoteDatabase + "." + table
	partition, err := quotePartition(sch, cmd.PartitionID)
	if err != nil {
		return "", nil, err
	}
	if err := r.prepareShadow(ctx, cmd, sch, table, safe, promote, partition); err != nil {
		return "", nil, err
	}
	safeBefore, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return "", nil, err
	}
	if err := r.attachCandidateParts(ctx, cmd, sch, table, promote, partition); err != nil {
		return "", nil, err
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
		return "", nil, err
	}
	shadowRoot, err := r.partitionContentRoot(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID)
	if err != nil {
		return "", nil, err
	}
	if shadowRoot != post {
		_ = r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partition)
		return "", nil, fmt.Errorf("shadow closure mismatch: promote partition root %s != base+candidates %s (unverified or missing part in shadow)", shadowRoot, post)
	}
	safeBefore = partsInLogicalPartition(safeBefore, sch, cmd.PartitionID)
	intent := promotionIntentFor(cmd, post, safeBefore)
	if err := r.state.BeginPromotion(partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}, intent); err != nil {
		return "", nil, fmt.Errorf("journal promotion intent: %w", err)
	}
	if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION %s FROM %s", safe, partition, promote)); err != nil {
		return "", nil, err
	}
	if err := r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partition); err != nil {
		return "", nil, err
	}
	mappings, err := r.safeMappings(ctx, safe, table, sch, safeBefore, cmd.CandidateParts)
	if err != nil {
		return "", nil, err
	}
	return post, mappings, nil
}

// reconcilePromotionIntent resolves the crash boundary around REPLACE. The
// pre-publication safe inventory distinguishes "intent persisted, REPLACE not
// run" from "REPLACE visible, final ACK state not persisted". In the latter
// case it derives the mappings from the already-published parts instead of
// publishing the candidates twice.
func (r *Role) reconcilePromotionIntent(ctx context.Context, cmd arbiter.PromoteSafePartition, intent promotionIntent) (string, []arbiter.SafePartMapping, bool, error) {
	post, err := lthashCombineHexAll(cmd.BasePartitionRoot, candidateHashes(cmd))
	if err != nil {
		return "", nil, false, err
	}
	if !promotionIntentMatchesCommand(intent, cmd, post) {
		return "", nil, false, fmt.Errorf("unresolved promotion intent seq %d does not match command seq %d", intent.PromotionSeq, cmd.PromotionSeq)
	}
	sch, err := r.schemaFor(cmd.TableID)
	if err != nil {
		return "", nil, false, err
	}
	table := CHTableName(cmd.TableID)
	safe := r.cfg.SafeDatabase + "." + table
	partition, err := quotePartition(sch, cmd.PartitionID)
	if err != nil {
		return "", nil, false, err
	}
	current, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return "", nil, false, err
	}
	current = partsInLogicalPartition(current, sch, cmd.PartitionID)
	currentRoot, err := r.partitionContentRoot(ctx, r.cfg.SafeDatabase, table, sch, cmd.PartitionID)
	if err != nil {
		return "", nil, false, err
	}
	baseRoot, err := lthashCombineHexAll(cmd.BasePartitionRoot, nil)
	if err != nil {
		return "", nil, false, err
	}
	if samePromotionSafeParts(intent.SafePartsBefore, current) {
		if currentRoot != baseRoot {
			return "", nil, false, fmt.Errorf("safe partition changed without a visible part-inventory change: root %s, expected base %s", currentRoot, baseRoot)
		}
		return "", nil, false, nil
	}
	if currentRoot != post {
		return "", nil, false, fmt.Errorf("safe partition after unresolved promotion has root %s, expected base %s or post %s", currentRoot, baseRoot, post)
	}
	if err := r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, table, sch, cmd.PartitionID, partition); err != nil {
		return "", nil, false, err
	}
	mappings, err := r.safeMappings(ctx, safe, table, sch, partInfosFromPromotionSnapshot(intent.SafePartsBefore), cmd.CandidateParts)
	if err != nil {
		return "", nil, false, err
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

func partInfosFromPromotionSnapshot(parts []promotionSafePart) []partInfo {
	out := make([]partInfo, 0, len(parts))
	for _, p := range parts {
		out = append(out, partInfo{
			Name: p.Name, PartitionID: p.PartitionID,
			PartitionValue: p.PartitionValue, PhysHash: p.PhysHash,
		})
	}
	return out
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
	if err := r.exec(ctx, fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS %s", promote, safe)); err != nil {
		return err
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
