package snode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The owner hg_safe audit (housegate spec 2026-10-14 §14; plan S3-A contract
// §3, clarifications K1-K4). A ClickHouse restart within old_parts_lifetime
// reactivates the parts a REPLACE PARTITION retired, under their old names and
// with the hash_of_all_files of their replacements (spec §2), so hg_safe
// serves rows twice and the next promotion of the partition fails its shadow
// closure gate. Before building each promotion's shadow, at startup and after
// every ClickHouse restart, the owner compares each partition's active parts
// with the newer of its own last acknowledged inventory and the arbiter's
// SafeParts.

// OwnerAuditMode selects what the audit does with an unclean partition.
type OwnerAuditMode string

const (
	// OwnerAuditReport, also the meaning of "", audits, logs at error level
	// and counts, but never drops a part and never holds a table back.
	OwnerAuditReport OwnerAuditMode = "report"
	// OwnerAuditRepair drops each resurrected equal-hash twin that carries no
	// acknowledged name (clarification K2) and holds an unclean table
	// not-ready: TableReady is false, so the host answers it Pending, and the
	// SNode builds no shadow for an unclean partition.
	OwnerAuditRepair OwnerAuditMode = "repair"
)

// OwnerAuditReason says why a partition is unclean.
type OwnerAuditReason string

const (
	// OwnerAuditUnacknowledgedPart: an active part whose name and
	// hash_of_all_files the reference does not carry; in repair mode it is
	// dropped only when it is the twin of an acknowledged part.
	OwnerAuditUnacknowledgedPart OwnerAuditReason = "unacknowledged_part"
	// OwnerAuditAcknowledgedPartMissing: a part of the reference is not active.
	OwnerAuditAcknowledgedPartMissing OwnerAuditReason = "acknowledged_part_missing"
	// OwnerAuditFSMAhead: the arbiter recorded an applied promotion of the
	// partition past this SNode's own promotion watermark.
	OwnerAuditFSMAhead OwnerAuditReason = "fsm_ahead"
	// OwnerAuditFSMDisagrees: this SNode and the arbiter acknowledged
	// different inventories for the same promotion.
	OwnerAuditFSMDisagrees OwnerAuditReason = "fsm_disagrees"
	// OwnerAuditNoAcknowledgedInventory: the partition has parts and there is
	// no reference at all (no local inventory, arbiter unreadable).
	OwnerAuditNoAcknowledgedInventory OwnerAuditReason = "no_acknowledged_inventory"
	// OwnerAuditUnresolvedIntent: a promotion intent of the partition is
	// journaled and not finalized; the audit skips it (clarification K4).
	OwnerAuditUnresolvedIntent OwnerAuditReason = "unresolved_intent"
)

// The audit's triggers, the keys of OwnerAuditStats.Runs.
const (
	OwnerAuditTriggerStartup           = "startup"
	OwnerAuditTriggerPromotion         = "promotion"
	OwnerAuditTriggerClickHouseRestart = "clickhouse_restart"
)

// ErrSafePartitionUnclean refuses, in repair mode, a promotion of a partition
// the audit found unclean, before its shadow is built; the subscription
// redelivers the command.
var ErrSafePartitionUnclean = errors.New("snode: hg_safe partition failed the owner audit")

// OwnerAuditFinding is the current unclean finding of one partition: why, the
// parts it names, the trigger of the audit that found it and when.
type OwnerAuditFinding struct {
	TableID     string
	PartitionID string
	Reason      OwnerAuditReason
	Parts       []string
	Trigger     string
	At          time.Time
}

// OwnerAuditStats is a point-in-time copy of the audit's counters for the host
// to export: the mode, the runs per trigger (every trigger present), the
// current findings sorted by table and partition, the twins dropped, the
// ClickHouse restarts detected, the arbiter reads that failed, and the end of
// the last run.
type OwnerAuditStats struct {
	Mode               OwnerAuditMode
	Runs               map[string]uint64
	Unclean            []OwnerAuditFinding
	DroppedTwins       uint64
	ClickHouseRestarts uint64
	FSMUnavailable     uint64
	LastRun            time.Time
}

// OwnerAuditStats returns a copy of the owner audit's counters and current
// findings for the host to export.
func (r *Role) OwnerAuditStats() OwnerAuditStats {
	return r.audit.stats(r.ownerAuditMode())
}

// OwnerAuditClean reports whether the owner audit lets tableID be served. It
// is false only in repair mode, while one of the table's hg_safe partitions
// has an unclean finding or while the table's first audit after startup or
// after a ClickHouse restart has not completed. TableReady folds it in.
func (r *Role) OwnerAuditClean(tableID string) bool {
	if r.ownerAuditMode() != OwnerAuditRepair {
		return true
	}
	return !r.audit.blocks(tableID)
}

func (r *Role) ownerAuditMode() OwnerAuditMode {
	if r.cfg.OwnerAudit == OwnerAuditRepair {
		return OwnerAuditRepair
	}
	return OwnerAuditReport
}

// ownerAuditProbeInterval is Config.OwnerAuditProbeInterval, or its default
// for a Role that New did not validate.
func (r *Role) ownerAuditProbeInterval() time.Duration {
	if r.cfg.OwnerAuditProbeInterval > 0 {
		return r.cfg.OwnerAuditProbeInterval
	}
	return DefaultOwnerAuditProbeInterval
}

// ownerAuditFSMTimeout is Config.OwnerAuditFSMTimeout, or its default.
func (r *Role) ownerAuditFSMTimeout() time.Duration {
	if r.cfg.OwnerAuditFSMTimeout > 0 {
		return r.cfg.OwnerAuditFSMTimeout
	}
	return DefaultOwnerAuditFSMTimeout
}

// auditBeforeShadow is the promotion trigger. buildAndReplace runs it under
// promotionLock(k), after handlePromote reconciled any promotion intent and
// passed the base CAS, and before prepareShadow copies hg_safe into the
// shadow. Report mode never refuses: a finding or an audit error is logged
// and counted and the promotion goes on as before. Repair mode refuses an
// unclean partition with ErrSafePartitionUnclean, so no shadow is built and
// the subscription redelivers the command.
func (r *Role) auditBeforeShadow(ctx context.Context, cmd arbiter.PromoteSafePartition, sch payloadexec.TableSchema, table string) error {
	if r.audit == nil {
		return nil
	}
	k := partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}
	fsm, available := r.ownerAuditFSM(ctx, cmd.TableID, cmd.PartitionID)
	finding, err := r.auditPartitionLocked(ctx, OwnerAuditTriggerPromotion, sch, table, k, fsm, available)
	r.audit.ran(OwnerAuditTriggerPromotion, r.now())
	repair := r.ownerAuditMode() == OwnerAuditRepair
	switch {
	case err != nil && repair:
		return fmt.Errorf("owner audit before promotion %d: %w", cmd.PromotionSeq, err)
	case err != nil:
		r.d.Logger.Error("owner audit before the shadow build failed; report mode lets the promotion continue",
			"promotion_seq", cmd.PromotionSeq, "table", k.Table, "partition", k.Partition, "err", err)
	case finding != nil && repair:
		return fmt.Errorf("%w: %s/%s before promotion %d: %s [%s]", ErrSafePartitionUnclean,
			k.Table, k.Partition, cmd.PromotionSeq, finding.Reason, strings.Join(finding.Parts, ", "))
	}
	return nil
}

// startOwnerAudit runs the startup audit, then watches ClickHouse's start
// time: a restart makes a full pass due, and the worker runs every due pass,
// retrying one whose tables did not all complete each probe interval. The
// start time is read before the startup pass and is the watcher's baseline,
// so a restart during that pass is caught. stop cancels both goroutines and
// waits for them. Without a ClickHouse connection there is nothing to audit.
func (r *Role) startOwnerAudit(ctx context.Context) (stop func()) {
	if r.audit == nil || r.d.Conn == nil {
		return func() {}
	}
	interval := r.ownerAuditProbeInterval()
	probeCtx, cancelProbe := context.WithTimeout(ctx, interval)
	baseline, err := chrestart.StartedAt(probeCtx, r.d.Conn, r.now())
	cancelProbe()
	if err != nil {
		r.d.Logger.Warn("owner audit: the ClickHouse start time is unreadable; the first successful probe becomes the baseline", "err", err)
		baseline = time.Time{}
	}
	r.runOwnerAuditPass(ctx)
	ctx, cancel := context.WithCancel(ctx)
	watcher := &chrestart.Watcher{Interval: interval, Skew: chrestart.DefaultRestartSkew, Now: r.now,
		OnRestart: r.ownerAuditRestarted, Baseline: baseline}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		watcher.Run(ctx, r.d.Conn)
	}()
	go func() {
		defer wg.Done()
		r.runOwnerAuditWorker(ctx, interval)
	}()
	return func() {
		cancel()
		wg.Wait()
	}
}

// runOwnerAuditWorker runs a full pass whenever one is due: at once after a
// restart (the wake), and every interval while a pass did not complete. In
// repair mode, when no pass is due, it re-audits every interval the partitions
// that hold their tables (reauditFindings).
func (r *Role) runOwnerAuditWorker(ctx context.Context, interval time.Duration) {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-r.audit.wake:
		case <-timer.C:
		}
		switch {
		case r.audit.due():
			r.runOwnerAuditPass(ctx)
		case r.ownerAuditMode() == OwnerAuditRepair:
			r.reauditFindings(ctx)
		}
		timer.Reset(interval)
	}
}

// reauditFindings audits again each partition with a finding, under the
// trigger that found it, so that in repair mode a partition an operator
// repaired, or whose reference the arbiter can supply again, stops holding its
// table without waiting for its next promotion or a restart. It takes no table
// set, so it holds no other table, and it is not a run: OwnerAuditStats.Runs
// counts the full passes and the promotion audits only. A finding of a table
// this SNode no longer serves is forgotten (nothing serves that table here).
func (r *Role) reauditFindings(ctx context.Context) {
	tables := r.audit.findingTables()
	if len(tables) == 0 {
		return
	}
	r.auditPassMu.Lock()
	defer r.auditPassMu.Unlock()
	served := r.ownerAuditTables()
	fsmDown := false
	for _, tableID := range tables {
		if ctx.Err() != nil {
			return
		}
		if !slices.Contains(served, tableID) {
			r.audit.forgetTable(tableID)
			continue
		}
		if err := r.reauditTable(ctx, tableID, &fsmDown); err != nil {
			r.d.Logger.Warn("owner audit: re-auditing an unclean table failed; the next probe interval retries it", "table", tableID, "err", err)
		}
	}
}

// reauditTable re-audits the partitions of tableID that have a finding, with
// one arbiter read (auditTable's *fsmDown rule).
func (r *Role) reauditTable(ctx context.Context, tableID string, fsmDown *bool) error {
	sch, err := r.schemaFor(tableID)
	if err != nil {
		return err
	}
	table := CHTableName(tableID)
	if err := r.loadSafeTable(ctx, table); err != nil {
		return err
	}
	var fsm map[string]wire.PartitionState
	available := false
	if *fsmDown {
		r.audit.countFSMUnavailable()
	} else if fsm, available = r.ownerAuditFSM(ctx, tableID, ""); !available {
		*fsmDown = true
	}
	for _, id := range r.audit.findingPartitions(tableID) {
		k := partitionKey{Table: tableID, Partition: id}
		f, ok := r.audit.finding(k)
		if !ok {
			continue
		}
		if err := r.auditPassPartition(ctx, f.Trigger, sch, table, k, fsm, available); err != nil {
			return err
		}
	}
	return nil
}

// ownerAuditRestarted is the watcher's OnRestart: it counts the restart and
// holds every table (in repair mode) until the restart pass audited it.
func (r *Role) ownerAuditRestarted(_ context.Context, previous, current time.Time) {
	r.audit.restarted()
	r.d.Logger.Warn("ClickHouse restarted; the owner audit re-audits every hg_safe partition",
		"previous_start", previous, "current_start", current, "mode", string(r.ownerAuditMode()))
}

// runOwnerAuditPass audits every hg_safe partition of every table this SNode
// serves (stateRootTables): the startup pass, the pass after a ClickHouse
// restart, and their retries. It never fails the role: a table whose audit
// meets a ClickHouse or schema error stays pending, held in repair mode,
// until a later pass completes it. Once one arbiter read of the pass fails,
// the pass audits its remaining tables on their own inventories without
// asking again, so an unreachable arbiter costs the pass (and the startup
// that waits for it) one OwnerAuditFSMTimeout, not one per table
// (clarification K3).
func (r *Role) runOwnerAuditPass(ctx context.Context) {
	if r.audit == nil || r.d.Conn == nil {
		return
	}
	r.auditPassMu.Lock()
	defer r.auditPassMu.Unlock()
	tables := r.ownerAuditTables()
	trigger := r.audit.begin(tables)
	fsmDown := false
	for _, tableID := range tables {
		if ctx.Err() != nil {
			return
		}
		if err := r.auditTable(ctx, trigger, tableID, &fsmDown); err != nil {
			r.d.Logger.Error("owner audit of a table failed; the next pass retries it", "trigger", trigger, "table", tableID, "err", err)
			continue
		}
		r.audit.tableDone(tableID)
	}
	r.audit.ran(trigger, r.now())
}

func (r *Role) ownerAuditTables() []string {
	_, hashes := r.stateRootTables()
	return sortedTableIDs(hashes)
}

// auditTable audits every partition of tableID that has active hg_safe parts,
// an acknowledged inventory, an arbiter state or a finding, with one arbiter
// read for the whole table. *fsmDown is the pass's memory of a failed arbiter
// read: while it is set the table is audited without one (and counted as
// FSMUnavailable), and a failed read sets it.
func (r *Role) auditTable(ctx context.Context, trigger, tableID string, fsmDown *bool) error {
	sch, err := r.schemaFor(tableID)
	if err != nil {
		return err
	}
	table := CHTableName(tableID)
	if err := r.loadSafeTable(ctx, table); err != nil {
		return err
	}
	var fsm map[string]wire.PartitionState
	available := false
	if *fsmDown {
		r.audit.countFSMUnavailable()
	} else if fsm, available = r.ownerAuditFSM(ctx, tableID, ""); !available {
		*fsmDown = true
	}
	parts, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return fmt.Errorf("list hg_safe parts of %s: %w", tableID, err)
	}
	partitions := map[string]bool{}
	for _, p := range parts {
		partitions[logicalPartitionID(sch, p)] = true
	}
	for _, id := range r.state.ackedInventoryPartitions(tableID) {
		partitions[id] = true
	}
	for id, state := range fsm {
		if state.PublishSeq > 0 || len(state.SafeParts) > 0 {
			partitions[id] = true
		}
	}
	for _, id := range r.audit.findingPartitions(tableID) {
		partitions[id] = true
	}
	for _, id := range slices.Sorted(maps.Keys(partitions)) {
		if err := r.auditPassPartition(ctx, trigger, sch, table, partitionKey{Table: tableID, Partition: id}, fsm, available); err != nil {
			return err
		}
	}
	return nil
}

// auditPassPartition audits one partition of a full pass under its promotion
// lock. A partition with an unresolved promotion intent is skipped and
// reported (clarification K4): the intent's redelivery reconciles it.
func (r *Role) auditPassPartition(ctx context.Context, trigger string, sch payloadexec.TableSchema, table string, k partitionKey, fsm map[string]wire.PartitionState, fsmAvailable bool) error {
	mu := r.promotionLock(k)
	mu.Lock()
	defer mu.Unlock()
	if intent, ok := r.state.PendingPromotion(k); ok {
		r.recordOwnerAudit(trigger, k, ownerAuditReference{}, ownerAuditPlan{Reason: OwnerAuditUnresolvedIntent, Detail: fmt.Sprintf(
			"promotion %d is journaled before REPLACE PARTITION and not finalized; its redelivery reconciles it, and one it cannot reconcile is a runbook repair",
			intent.PromotionSeq)})
		return nil
	}
	_, err := r.auditPartitionLocked(ctx, trigger, sch, table, k, fsm, fsmAvailable)
	return err
}

// auditPartitionLocked audits partition k of table (its physical name)
// against its reference and, in repair mode, drops resurrected twins. fsm is
// the arbiter's answer for k's table, used only when fsmAvailable. The caller
// holds promotionLock(k). It returns the recorded finding, nil when clean.
func (r *Role) auditPartitionLocked(ctx context.Context, trigger string, sch payloadexec.TableSchema, table string, k partitionKey, fsm map[string]wire.PartitionState, fsmAvailable bool) (*OwnerAuditFinding, error) {
	in := ownerAuditInputs{Watermark: r.state.Watermark(k), FSMAvailable: fsmAvailable}
	if inv, ok := r.state.AckedSafeInventory(k); ok {
		in.Local = &inv
	}
	if state, ok := fsm[k.Partition]; ok {
		in.FSM = &state
	}
	ref, haveRef, refusal, detail := ownerAuditReferenceFor(in)
	if err := r.loadSafeTable(ctx, table); err != nil {
		return nil, err
	}
	active, err := r.safePartitionParts(ctx, sch, table, k.Partition)
	if err != nil {
		return nil, fmt.Errorf("list hg_safe parts of %s/%s: %w", k.Table, k.Partition, err)
	}
	plan := ownerAuditPlan{Reason: refusal, Parts: activePartNames(active), Detail: detail}
	if refusal == "" {
		plan = planOwnerAudit(ref, haveRef, active)
	}
	if len(plan.Drop) > 0 && r.ownerAuditMode() == OwnerAuditRepair {
		if plan, err = r.dropResurrectedTwins(ctx, trigger, sch, table, k, ref, plan); err != nil {
			return nil, err
		}
	}
	return r.recordOwnerAudit(trigger, k, ref, plan), nil
}

// dropResurrectedTwins drops, in repair mode, the twins plan names with ALTER
// TABLE ... DROP PART (clarification K2: on 26.3 this leaves exactly the
// acknowledged parts active and no empty covering part, and the dropped
// parts stay dropped across a restart; M11 pins the deployed image), then
// audits the partition again against the same reference. Anything but a
// clean result keeps it unclean, and nothing is dropped twice.
func (r *Role) dropResurrectedTwins(ctx context.Context, trigger string, sch payloadexec.TableSchema, table string, k partitionKey, ref ownerAuditReference, plan ownerAuditPlan) (ownerAuditPlan, error) {
	for _, name := range plan.Drop {
		if err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s DROP PART '%s'", r.cfg.SafeDatabase, table, escapeSQLString(name))); err != nil {
			return ownerAuditPlan{}, fmt.Errorf("drop resurrected hg_safe twin %s of %s/%s: %w", name, k.Table, k.Partition, err)
		}
		r.audit.countDropped()
		r.d.Logger.Warn("owner audit dropped a resurrected hg_safe twin",
			"trigger", trigger, "table", k.Table, "partition", k.Partition, "part", name, "reference", ref.Source, "reference_seq", ref.Seq)
	}
	active, err := r.safePartitionParts(ctx, sch, table, k.Partition)
	if err != nil {
		return ownerAuditPlan{}, fmt.Errorf("list hg_safe parts of %s/%s after the repair: %w", k.Table, k.Partition, err)
	}
	after := planOwnerAudit(ref, true, active)
	if len(after.Drop) > 0 {
		after.Drop = nil
		after.Detail = "twins are still active after DROP PART"
	}
	return after, nil
}

// recordOwnerAudit records plan as k's current verdict: a clean plan clears
// the finding, an unclean one replaces it and, unless it repeats the finding
// it replaces (a re-audit, a redelivered promotion), is logged at error level.
func (r *Role) recordOwnerAudit(trigger string, k partitionKey, ref ownerAuditReference, plan ownerAuditPlan) *OwnerAuditFinding {
	if plan.clean() {
		r.audit.clear(k)
		return nil
	}
	f := OwnerAuditFinding{TableID: k.Table, PartitionID: k.Partition, Reason: plan.Reason, Parts: slices.Clone(plan.Parts), Trigger: trigger, At: r.now()}
	if !r.audit.set(f) {
		return &f
	}
	msg := "owner audit: hg_safe partition is unclean"
	r.d.Logger.Error(msg, "mode", string(r.ownerAuditMode()), "trigger", trigger, "table", k.Table, "partition", k.Partition,
		"reason", string(plan.Reason), "parts", strings.Join(plan.Parts, ","), "twins", strings.Join(plan.Drop, ","),
		"reference", ref.Source, "reference_seq", ref.Seq, "detail", plan.Detail)
	return &f
}

// ownerAuditFSM reads the arbiter's partition states of tableID (one
// partition, or all when partitionID is empty) within OwnerAuditFSMTimeout.
// Any failure, an arbiter without the read included, leaves the audit on this
// SNode's own inventory (clarification K3) and counts FSMUnavailable.
func (r *Role) ownerAuditFSM(ctx context.Context, tableID, partitionID string) (map[string]wire.PartitionState, bool) {
	ctx, cancel := context.WithTimeout(ctx, r.ownerAuditFSMTimeout())
	defer cancel()
	states, err := r.d.Client.GetPartitionStates(ctx, wire.PartitionStatesQuery{TableID: tableID, PartitionID: partitionID})
	if err != nil {
		r.audit.countFSMUnavailable()
		level := slog.LevelWarn
		if errors.Is(err, dataplane.ErrStage3ReadsUnavailable) {
			level = slog.LevelDebug
		}
		r.d.Logger.Log(ctx, level, "owner audit: the arbiter's partition states are unavailable; auditing against this SNode's own acknowledged inventory",
			"table", tableID, "partition", partitionID, "err", err)
		return nil, false
	}
	out := make(map[string]wire.PartitionState, len(states.Partitions))
	for _, p := range states.Partitions {
		out[p.PartitionID] = p
	}
	return out, true
}

// loadSafeTable makes sure the table's hg_safe storage is loaded before the
// audit reads system.parts. After a restart ClickHouse may accept queries while
// it still loads tables (async_load_databases), and system.parts lists no part
// of a table it has not loaded, which would read as every acknowledged part
// missing; a query on the table waits for its load. A table or database that
// does not exist yet has no parts.
func (r *Role) loadSafeTable(ctx context.Context, table string) error {
	err := r.d.Conn.Exec(ctx, fmt.Sprintf("SELECT 1 FROM %s.%s LIMIT 0", r.cfg.SafeDatabase, table))
	var exception *clickhouseproto.Exception
	if err == nil || (errors.As(err, &exception) && (exception.Code == int32(chproto.ErrUnknownTable) || exception.Code == int32(chproto.ErrUnknownDatabase))) {
		return nil
	}
	return fmt.Errorf("load %s.%s: %w", r.cfg.SafeDatabase, table, err)
}

func (r *Role) safePartitionParts(ctx context.Context, sch payloadexec.TableSchema, table, partitionID string) ([]partInfo, error) {
	parts, err := activeParts(ctx, r.d.Conn, r.cfg.SafeDatabase, table)
	if err != nil {
		return nil, err
	}
	return partsInLogicalPartition(parts, sch, partitionID), nil
}
