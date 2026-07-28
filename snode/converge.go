package snode

import (
	"context"
	"errors"
	"fmt"

	"housegate/housegate/pkg/replay/payloadexec"
)

// LookupPreparedStatement returns a prepared result only after any durable
// non-terminal intake record has converged to a truthful endpoint.
func (r *Role) LookupPreparedStatement(ctx context.Context, statementID string) (PreparedLocalResult, bool, error) {
	r.intakeMu.Lock()
	defer r.intakeMu.Unlock()

	rec, ok, err := r.journal.load(statementID)
	if err != nil {
		return PreparedLocalResult{}, false, fmt.Errorf("intake journal: %w", err)
	}
	if !ok {
		return PreparedLocalResult{}, false, nil
	}
	if rec.Lifecycle == LifecyclePreparing || rec.Lifecycle == LifecycleAbortPending {
		rec, err = r.convergeIntake(ctx, rec)
		if err != nil {
			return PreparedLocalResult{}, false, err
		}
	}
	switch rec.Lifecycle {
	case LifecycleUnsafeWritten, LifecycleRCBound:
		if rec.Result == nil {
			return PreparedLocalResult{}, false, fmt.Errorf("statement %s in %s has no prepared result", statementID, rec.Lifecycle)
		}
		return *rec.Result, true, nil
	case LifecycleCleaned:
		return PreparedLocalResult{}, false, nil
	default:
		return PreparedLocalResult{}, false, fmt.Errorf("statement %s has unknown intake lifecycle %q", statementID, rec.Lifecycle)
	}
}

func (r *Role) convergeIntake(ctx context.Context, rec intakeRecord) (intakeRecord, error) {
	if rec.ConvergeFailed != "" {
		return rec, fmt.Errorf("statement %s: %s: %w", rec.StatementID, rec.ConvergeFailed, ErrConvergenceForeignRows)
	}
	if rec.Lifecycle == LifecycleAbortPending {
		return r.runAbort(ctx, rec)
	}
	if rec.Lifecycle != LifecyclePreparing {
		return rec, nil
	}

	schema, err := r.schemaFor(rec.Envelope.TargetTableID)
	if err != nil {
		return rec, fmt.Errorf("converge schema: %w", err)
	}
	table := chTableName(schema.TableID)
	expected := make(map[string]bool, rec.ExpectedRowCount)
	for ordinal := uint64(0); ordinal < rec.ExpectedRowCount; ordinal++ {
		rowID := payloadexec.RowID(r.cfg.NetworkID, schema.TableID, rec.StatementID, ordinal)
		expected[string(rowID)] = true
	}
	current, err := activeParts(ctx, r.d.Conn, r.cfg.UnsafeDatabase, table)
	if err != nil {
		return rec, fmt.Errorf("converge list parts: %w", err)
	}

	attributed := make([]partInfo, 0)
	seenRowIDs := make(map[string]uint64, rec.ExpectedRowCount)
	var attributedRows uint64
	for _, part := range current {
		partitionID := logicalPartitionID(schema, part)
		if !containsString(rec.TouchedPartitions, partitionID) ||
			containsString(rec.PreWriteInventory[partitionID], part.Name) {
			continue
		}
		rowIDs, err := r.scanPartRowIDs(ctx, table, part.Name)
		if err != nil {
			return rec, fmt.Errorf("converge scan %s: %w", part.Name, err)
		}
		for _, rowID := range rowIDs {
			if !expected[rowID] {
				return r.persistConvergenceFailure(
					rec,
					fmt.Sprintf("part %s carries a row id outside the statement's derived set", part.Name),
					part.Name,
				)
			}
			seenRowIDs[rowID]++
		}
		attributed = append(attributed, part)
		attributedRows += uint64(len(rowIDs))
	}

	if attributedRows == rec.ExpectedRowCount && rec.ExpectedRowCount > 0 {
		for rowID := range expected {
			if seenRowIDs[rowID] != 1 {
				return r.persistConvergenceFailure(
					rec,
					"candidate parts do not contain each derived row id exactly once",
					"",
				)
			}
		}
		before := subtractParts(current, attributed)
		result, rc, err := r.assembleAfterWrite(ctx, rec.Envelope, schema, table, before, rec)
		if err != nil {
			return rec, fmt.Errorf("converge repair: %w", err)
		}
		rec.Lifecycle = LifecycleUnsafeWritten
		rec.Result = &result
		rec.RC = &rc
		if err := r.journal.save(rec); err != nil {
			return rec, fmt.Errorf("persist repaired record: %w", err)
		}
		return rec, nil
	}
	if attributedRows > rec.ExpectedRowCount {
		return r.persistConvergenceFailure(
			rec,
			"candidate parts contain more rows than the statement's expected row count",
			"",
		)
	}

	rec.Lifecycle = LifecycleAbortPending
	rec.Abort = &intakeAbort{
		PartNames: partInfoNames(attributed),
		Reason:    "partial write converged",
	}
	if err := r.journal.save(rec); err != nil {
		return rec, fmt.Errorf("persist abort intent: %w", err)
	}
	return r.runAbort(ctx, rec)
}

func (r *Role) persistConvergenceFailure(rec intakeRecord, detail, partName string) (intakeRecord, error) {
	rec.ConvergeFailed = detail
	if err := r.journal.save(rec); err != nil {
		return rec, fmt.Errorf("persist converge failure: %w", err)
	}
	if partName == "" {
		return rec, fmt.Errorf("statement %s: %w", rec.StatementID, ErrConvergenceForeignRows)
	}
	return rec, fmt.Errorf("statement %s part %s: %w", rec.StatementID, partName, ErrConvergenceForeignRows)
}

func (r *Role) runAbort(ctx context.Context, rec intakeRecord) (intakeRecord, error) {
	if rec.Abort == nil {
		return rec, fmt.Errorf("statement %s is AbortPending without abort details", rec.StatementID)
	}
	if len(rec.Abort.PartNames) == 0 {
		rec.Lifecycle = LifecycleCleaned
		if err := r.journal.save(rec); err != nil {
			return rec, fmt.Errorf("persist cleaned: %w", err)
		}
		return rec, nil
	}
	schema, err := r.schemaFor(rec.Envelope.TargetTableID)
	if err != nil {
		return rec, fmt.Errorf("abort schema: %w", err)
	}
	table := chTableName(schema.TableID)
	for _, name := range rec.Abort.PartNames {
		if err := r.validateAbortPart(rec, name); err != nil {
			return rec, err
		}
		err := r.exec(ctx, fmt.Sprintf(
			"ALTER TABLE %s.%s DROP PART '%s'",
			r.cfg.UnsafeDatabase,
			table,
			escapeSQLString(name),
		))
		if err != nil && !isMissingPartErr(err) {
			return rec, fmt.Errorf("abort drop %s: %w", name, err)
		}
		if err := r.excludeAbortedPart(rec, name); err != nil {
			return rec, fmt.Errorf("exclude aborted part %s: %w", name, err)
		}
	}
	rec.Lifecycle = LifecycleCleaned
	if err := r.journal.save(rec); err != nil {
		return rec, fmt.Errorf("persist cleaned: %w", err)
	}
	return rec, nil
}

func (r *Role) validateAbortPart(rec intakeRecord, name string) error {
	if rec.Result == nil {
		return nil
	}
	for _, part := range rec.Result.CandidateParts {
		if part.PartName == name {
			return nil
		}
	}
	return fmt.Errorf("abort part %s is not attributed to statement %s", name, rec.StatementID)
}

func (r *Role) excludeAbortedPart(rec intakeRecord, name string) error {
	if rec.Result == nil {
		return nil
	}
	for _, part := range rec.Result.CandidateParts {
		if part.PartName != name {
			continue
		}
		return r.state.RemoveUnpromotedPart(
			partitionKey{Table: part.TableID, Partition: part.PartitionID},
			part.PartName,
			part.PartRowLtHash,
		)
	}
	return nil
}

// convergeStartup resumes every durable non-terminal intake before the role
// starts its promotion subscription. Staged entry points also converge lazily,
// so correctness does not depend on callers invoking Run first.
func (r *Role) convergeStartup(ctx context.Context) error {
	r.intakeMu.Lock()
	defer r.intakeMu.Unlock()

	records, err := r.journal.list()
	if err != nil {
		return fmt.Errorf("list intake journal: %w", err)
	}
	for _, rec := range records {
		if rec.Lifecycle != LifecyclePreparing && rec.Lifecycle != LifecycleAbortPending {
			continue
		}
		if _, err := r.convergeIntake(ctx, rec); err != nil {
			if errors.Is(err, ErrConvergenceForeignRows) {
				r.d.Logger.Error(
					"intake convergence requires operator intervention",
					"statement_id", rec.StatementID,
					"error", err,
				)
				continue
			}
			return fmt.Errorf("converge intake %s: %w", rec.StatementID, err)
		}
	}
	return nil
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func subtractParts(all, removed []partInfo) []partInfo {
	remove := make(map[string]bool, len(removed))
	for _, part := range removed {
		remove[part.Name] = true
	}
	out := make([]partInfo, 0, len(all)-len(removed))
	for _, part := range all {
		if !remove[part.Name] {
			out = append(out, part)
		}
	}
	return out
}

func partInfoNames(parts []partInfo) []string {
	names := make([]string, 0, len(parts))
	for _, part := range parts {
		names = append(names, part.Name)
	}
	return names
}
