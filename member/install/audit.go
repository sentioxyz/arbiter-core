package install

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/housegate/housegate/pkg/replay/chexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/member/copyset"
)

// Audit audits every ready copy against its journal as a multiset (housegate
// spec 2026-10-14 §8.7): of parts with equal row hash and hash_of_all_files it
// keeps the journaled name (else the smallest) and drops the others with a local
// DROP PART; a journaled part that is missing or changed marks its partition
// Diverged; a part the journal does not list is scanned by name and left for the
// next Apply to adopt or repair (Plan S3-C2 K36). Trigger self_audit also scans
// every partition of every copy in full, and runs at most once per half
// SelfAuditInterval. Triggers startup and clickhouse_restart make
// Stats.AuditClean false until the audit completes; Apply runs a pending one
// itself before it installs anything. The audit covers the copies that are ready
// when it runs; a copy the copyset makes ready afterwards is unaudited
// (Stats.AuditClean false, TableState.Diverged, NeedsPass) until Apply audits it
// before its pass (Plan S3-C2 review fix 245-1). A quarantined table is not
// audited.
func (i *Installer) Audit(ctx context.Context, trigger string) error {
	if !slices.Contains(triggers, trigger) {
		return fmt.Errorf("install: unknown audit trigger %q (want %s)", trigger, strings.Join(triggers, ", "))
	}
	i.work.Lock()
	defer i.work.Unlock()
	return i.audit(ctx, trigger)
}

func (i *Installer) audit(ctx context.Context, trigger string) error {
	now := i.d.Now()
	i.mu.Lock()
	if trigger == TriggerSelfAudit && !i.lastSelfAudit.IsZero() && now.Sub(i.lastSelfAudit) < i.cfg.SelfAuditInterval/2 {
		last := i.lastSelfAudit
		i.mu.Unlock()
		i.d.Logger.Info("install: skipping a self-audit that follows the last one too closely", "last", last, "self_audit_interval", i.cfg.SelfAuditInterval)
		return nil
	}
	if trigger != TriggerSelfAudit {
		i.auditDone, i.auditTrigger = false, trigger
		i.audited = map[string]bool{}
	}
	i.auditRuns[trigger]++
	i.mu.Unlock()

	var errs []error
	for _, c := range i.readyCopies() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := i.auditCopy(ctx, trigger, c); err != nil {
			errs = append(errs, err)
			continue
		}
		i.markAudited(c)
	}
	if err := errors.Join(errs...); err != nil {
		i.d.Logger.Error("install: audit failed; installs wait for the next one", "trigger", trigger, "err", err)
		return err
	}
	i.mu.Lock()
	i.auditDone = true
	if trigger == TriggerSelfAudit {
		i.lastSelfAudit = now
	}
	i.mu.Unlock()
	i.d.Logger.Info("install: audit completed", "trigger", trigger, "clean", i.Stats().AuditClean)
	return nil
}

// auditUncovered audits, before an install pass, each ready copy that the audit
// since the start or the last ClickHouse restart did not cover: a copy the
// copyset made ready after that audit, such as one its first pass created again
// empty while the journal still lists its parts (Plan S3-C2 review fix 245-1).
// Each is reconciled as that audit would have (auditCopy under its trigger); the
// audit's run count does not change.
func (i *Installer) auditUncovered(ctx context.Context, ready []copyset.Copy) error {
	i.mu.Lock()
	trigger := i.auditTrigger
	var todo []copyset.Copy
	for _, c := range ready {
		if !i.audited[auditKey(c)] {
			todo = append(todo, c)
		}
	}
	i.mu.Unlock()
	var errs []error
	for _, c := range todo {
		if err := ctx.Err(); err != nil {
			return err
		}
		i.d.Logger.Info("install: auditing a copy the last audit did not cover", "trigger", trigger, "table", c.TableKey, "copy", c.Table)
		if err := i.auditCopy(ctx, trigger, c); err != nil {
			errs = append(errs, err)
			continue
		}
		i.markAudited(c)
	}
	return errors.Join(errs...)
}

// auditKey identifies one ready copy for the audit's coverage and the
// bootstrap probe: its owner, table key, incarnation, copy name and
// generation. A copy the copyset created again after its table vanished has a
// new generation, so it is unaudited (Stats.AuditClean false, NeedsPass) until
// Apply compared it with its journal, which marks the partitions whose
// journaled parts it lost Diverged and rebuilds them.
func auditKey(c copyset.Copy) string {
	return fmt.Sprintf("%d/%s#%d/%s@%d", c.Owner, c.TableKey, c.IncarnationSeq, c.Table, c.Generation)
}

func (i *Installer) markAudited(c copyset.Copy) {
	i.mu.Lock()
	i.audited[auditKey(c)] = true
	i.mu.Unlock()
}

func (i *Installer) auditCopy(ctx context.Context, trigger string, c copyset.Copy) error {
	if slices.Contains(i.foreignTables(c.Owner), c.TableKey) {
		return nil
	}
	tj := i.tableJournal(c.Owner, c)
	local, err := i.listCopy(ctx, c)
	if err != nil {
		return err
	}
	gates := &installGates{before: i.d.BeforeInstall, owner: c.Owner, seen: map[string]bool{}}
	rec, err := i.reconcileCopy(ctx, gates, c.Owner, c, tj, local)
	if err != nil {
		return err
	}
	marks := rec.diverged
	if trigger == TriggerSelfAudit {
		full, err := i.scanCopy(ctx, c, tj)
		if err != nil {
			return err
		}
		marks = append(marks, full...)
	}
	if err := i.persistDiverged(c.Owner, c, marks); err != nil {
		return err
	}
	i.mu.Lock()
	if len(rec.candidates) > 0 {
		i.pending[pendingKey(c.Owner, c.TableKey)] = len(rec.candidates)
	} else {
		delete(i.pending, pendingKey(c.Owner, c.TableKey))
	}
	i.mu.Unlock()
	for _, cand := range rec.candidates {
		i.d.Logger.Warn("install: the copy holds a part the journal does not list; the next install adopts or repairs it", "trigger", trigger, "table", c.TableKey, "copy", c.Table, "partition", cand.PartitionID, "part", cand.Name)
	}
	return nil
}

// reconciliation is one copy compared with its journal.
type reconciliation struct {
	local      []chparts.Part // the copy's non-empty active parts after the twin drops
	candidates []JournalPart  // unjournaled parts, scanned by name, one per (row hash, hash_of_all_files)
	diverged   []string       // partitions this comparison marked Diverged in tj
}

// reconcileCopy compares copy c's non-empty active parts with its journal tj
// (spec §8.7, K36). A journaled part that is missing or whose
// hash_of_all_files changed marks its partition Diverged in tj. The parts the
// journal does not list are scanned by name (chexec.ScanParts); one that
// equals a present journaled part in row hash and hash_of_all_files is a twin
// and is dropped with DROP PART, and of unjournaled twins the smallest name
// is kept. What remains is returned as candidates for adoption.
func (i *Installer) reconcileCopy(ctx context.Context, gates *installGates, owner uint64, c copyset.Copy, tj *TableJournal, local []chparts.Part) (reconciliation, error) {
	rec := reconciliation{}
	byName := make(map[string]chparts.Part, len(local))
	for _, p := range local {
		byName[p.Name] = p
	}
	type twinKey struct{ row, phys string }
	present := map[twinKey]bool{}
	journaled := map[string]bool{}
	for _, jp := range tj.Parts {
		journaled[jp.Name] = true
		lp, ok := byName[jp.Name]
		if ok && lp.PhysHash == jp.HashOfAllFiles && lp.Rows == jp.Rows {
			present[twinKey{jp.PartRowLtHash, jp.HashOfAllFiles}] = true
			continue
		}
		if tj.markDiverged(jp.PartitionID) {
			rec.diverged = append(rec.diverged, jp.PartitionID)
			i.d.Logger.Warn("install: a journaled part is missing or changed; the partition is Diverged", "table", c.TableKey, "copy", c.Table, "partition", jp.PartitionID, "part", jp.Name)
		}
	}
	var unjournaled []chparts.Part
	for _, p := range local {
		if !journaled[p.Name] {
			unjournaled = append(unjournaled, p)
		}
	}
	if len(unjournaled) == 0 {
		rec.local = local
		return rec, nil
	}
	names := partNames(unjournaled)
	scans, err := chexec.ScanParts(ctx, i.d.Conn, i.cfg.Pinned.ReplicaDB+"."+c.Table, c.Schema, names)
	if err != nil {
		return rec, fmt.Errorf("install: scan the unjournaled parts %v of %s.%s: %w", names, i.cfg.Pinned.ReplicaDB, c.Table, err)
	}
	rowHash := make(map[string]string, len(scans))
	for _, s := range scans {
		rowHash[s.PartName] = s.RowLtHash
	}
	slices.SortFunc(unjournaled, func(a, b chparts.Part) int { return strings.Compare(a.Name, b.Name) })
	var drop []chparts.Part
	for _, p := range unjournaled {
		k := twinKey{rowHash[p.Name], p.PhysHash}
		if present[k] {
			drop = append(drop, p)
			continue
		}
		present[k] = true
		rec.candidates = append(rec.candidates, JournalPart{
			Name: p.Name, PartitionID: chparts.LogicalPartitionID(c.Schema, p), PartRowLtHash: rowHash[p.Name], HashOfAllFiles: p.PhysHash, Rows: p.Rows,
		})
	}
	dropped := map[string]bool{}
	for _, p := range drop {
		if err := gates.check(ctx, databaseOf(c.TableKey)); err != nil {
			return rec, err
		}
		stmt := "ALTER TABLE " + qualified(i.cfg.Pinned.ReplicaDB, c.Table) + " DROP PART " + quoteLiteral(p.Name)
		if err := i.d.Conn.Exec(ctx, stmt); err != nil {
			return rec, fmt.Errorf("install: %s: %w", stmt, err)
		}
		dropped[p.Name] = true
		i.mu.Lock()
		i.droppedTwins++
		i.mu.Unlock()
		i.d.Logger.Warn("install: dropped an equal-hash twin from a copy", "owner", owner, "table", c.TableKey, "copy", c.Table, "part", p.Name, "hash_of_all_files", p.PhysHash)
	}
	rec.local = slices.DeleteFunc(slices.Clone(local), func(p chparts.Part) bool { return dropped[p.Name] })
	return rec, nil
}

// scanCopy is the self-audit's full scan (spec §8.5 step 6, §8.7): every
// partition of the copy that holds journaled parts and is not Diverged is
// scanned through chparts.PartitionContentScan, and a journaled part whose
// content no longer has its journaled row hash marks the partition Diverged.
func (i *Installer) scanCopy(ctx context.Context, c copyset.Copy, tj *TableJournal) ([]string, error) {
	want := make(map[string]string, len(tj.Parts))
	for _, jp := range tj.Parts {
		want[jp.Name] = jp.PartRowLtHash
	}
	var marks []string
	for _, id := range tj.partitions() {
		if tj.isDiverged(id) {
			continue
		}
		_, scanned, err := chparts.PartitionContentScan(ctx, i.d.Conn, i.cfg.Pinned.ReplicaDB, c.Table, c.Schema, id)
		if err != nil {
			return marks, fmt.Errorf("install: self-audit scan of %s.%s partition %s: %w", i.cfg.Pinned.ReplicaDB, c.Table, id, err)
		}
		for _, s := range scanned {
			h, ok := want[s.Name]
			if !ok || h == s.RowLtHash {
				continue
			}
			if tj.markDiverged(id) {
				marks = append(marks, id)
				i.d.Logger.Error("install: a journaled part's content changed; the partition is Diverged", "table", c.TableKey, "copy", c.Table, "partition", id, "part", s.Name)
			}
			break
		}
	}
	return marks, nil
}
