package objstore

import (
	"context"
	"fmt"
	"slices"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/member/gate"
)

// BackupRef names one backup and the key that reads it.
type BackupRef struct {
	Bucket      Bucket
	Owner       uint64
	Name        string
	Credentials Credentials
}

// GateInput is one partition to restore and gate (spec §8.5 with the
// object-store source of §8.6): the manifest S, table T by id and physical
// name, its schema, partition p in the logical form, the row hashes L of the
// local copy (empty for a drill and for a first install) and a fresh scratch.
type GateInput struct {
	Backup                 BackupRef
	Manifest               replay.SafeSnapshotManifest
	TableID, PhysicalTable string
	Schema                 payloadexec.TableSchema
	PartitionID            string
	Local                  []string
	Scratch                Scratch
}

// GateResult is RestoreAndGate's outcome. Complete is true only when every
// needed part was restored once, closure holds and the scratch holds exactly
// partition p; KeptParts are the scratch parts that satisfy Need, left in the
// scratch for ATTACH PARTITION ... FROM (plan S3-C2). Reason is DrillVerified
// when Complete, the first failed check otherwise, and empty when the plan is
// Diverged (nothing was restored).
type GateResult struct {
	Plan           gate.Plan
	Classification gate.Classification
	KeptParts      []string
	Complete       bool
	Reason         DrillReason
}

// manifestPartition is M (the row hashes S lists for (T, p)) and R (its root).
func manifestPartition(m replay.SafeSnapshotManifest, tableID, partitionID string) ([]string, string, error) {
	for _, t := range m.Tables {
		if t.TableID != tableID {
			continue
		}
		root, found := "", false
		for _, pc := range t.PartitionRoots {
			if pc.PartitionID == partitionID {
				root, found = pc.Root, true
			}
		}
		if !found {
			return nil, "", fmt.Errorf("objstore: manifest %s has no partition %s of table %s", m.SnapshotID, partitionID, tableID)
		}
		var hashes []string
		for _, p := range t.ActiveParts {
			if p.PartitionID == partitionID {
				hashes = append(hashes, p.PartRowLtHash)
			}
		}
		return hashes, root, nil
	}
	return nil, "", fmt.Errorf("objstore: manifest %s does not list table %s", m.SnapshotID, tableID)
}

// RestoreAndGate restores one partition of a backup into a fresh scratch and
// gates it against the manifest: gate.PlanPartition (Need = M - L), RESTORE
// TABLE ... AS <scratch> PARTITIONS ..., chexec.ScanAllParts over the
// scratch, gate.Classify in restore mode, DROP PART of every part not needed
// (a backup taken ahead of publication holds such parts; they are never
// Foreign), then every needed hash kept once, gate.Closure and
// gate.PartitionSet.
//
// RESTORE creates the scratch from the backed-up CREATE, so it keeps
// hg_safe's merge pin (max_bytes_to_merge_at_max_space_in_pool = 0) and no
// merge changes its parts between the restore, the scan and the DROP PARTs.
//
// A RESTORE that fails returns Reason DrillRestoreFailed together with the
// redacted error; any other error leaves Reason empty. A failed gate keeps
// its scratch for inspection; the caller drops a scratch it no longer needs.
func RestoreAndGate(ctx context.Context, conn clickhouse.Conn, in GateInput) (GateResult, error) {
	if err := in.Backup.Credentials.validate(); err != nil {
		return GateResult{}, err
	}
	if err := checkBackupName(in.Backup.Name); err != nil {
		return GateResult{}, err
	}
	m, root, err := manifestPartition(in.Manifest, in.TableID, in.PartitionID)
	if err != nil {
		return GateResult{}, err
	}
	plan, err := gate.PlanPartition(m, root, in.Local)
	if err != nil {
		return GateResult{}, fmt.Errorf("objstore: table %s partition %s: %w", in.TableID, in.PartitionID, err)
	}
	res := GateResult{Plan: plan}
	if plan.Diverged {
		return res, nil
	}
	if len(plan.Need) == 0 {
		if err := gate.Closure(root, in.Local, nil); err != nil {
			res.Reason = DrillClosureMismatch
			return res, nil
		}
		res.Complete, res.Reason = true, DrillVerified
		return res, nil
	}
	clause, err := PartitionClause(in.Schema, in.PartitionID)
	if err != nil {
		return GateResult{}, fmt.Errorf("objstore: table %s: %w", in.TableID, err)
	}
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+ident(in.Scratch.Database)); err != nil {
		return GateResult{}, fmt.Errorf("objstore: create database %s: %w", in.Scratch.Database, err)
	}
	restore := RestoreSQL(in.Backup.Bucket, in.Backup.Owner, in.Backup.Name, in.PhysicalTable, clause, in.Scratch, in.Backup.Credentials)
	if err := runAsync(ctx, conn, restore, in.Backup.Credentials, "RESTORED"); err != nil {
		res.Reason = DrillRestoreFailed
		return res, fmt.Errorf("objstore: restore %s partition %s of backup %s of indexer %d into %s: %w",
			in.TableID, in.PartitionID, in.Backup.Name, in.Backup.Owner, in.Scratch.Qualified(), err)
	}
	scans, err := chexec.ScanAllParts(ctx, conn, in.Scratch.Database+"."+in.Scratch.Table, in.Schema)
	if err != nil {
		return GateResult{}, fmt.Errorf("objstore: scan %s: %w", in.Scratch.Qualified(), err)
	}
	scratch := make([]gate.ScratchPart, 0, len(scans))
	for _, s := range scans {
		scratch = append(scratch, gate.ScratchPart{Name: s.PartName, RowLtHash: s.RowLtHash, Rows: s.RowCount})
	}
	res.Classification, err = gate.Classify(gate.Fetch{Need: plan.Need}, scratch)
	if err != nil {
		return GateResult{}, fmt.Errorf("objstore: classify %s: %w", in.Scratch.Qualified(), err)
	}
	for _, name := range res.Classification.Drop {
		if err := conn.Exec(ctx, "ALTER TABLE "+in.Scratch.Qualified()+" DROP PART "+literal(name)); err != nil {
			return GateResult{}, fmt.Errorf("objstore: drop part %s of %s: %w", name, in.Scratch.Qualified(), err)
		}
	}
	res.KeptParts = slices.Clone(res.Classification.Keep)
	if len(res.Classification.Kept) != len(plan.Need) {
		res.Reason = DrillNeedUnsatisfied
		return res, nil
	}
	if err := gate.Closure(root, in.Local, res.Classification.Kept); err != nil {
		res.Reason = DrillClosureMismatch
		return res, nil
	}
	remaining, err := chparts.ActiveParts(ctx, conn, in.Scratch.Database, in.Scratch.Table)
	if err != nil {
		return GateResult{}, fmt.Errorf("objstore: list the parts of %s: %w", in.Scratch.Qualified(), err)
	}
	local := make([]string, 0, len(remaining))
	for _, p := range remaining {
		local = append(local, chparts.LogicalPartitionID(in.Schema, p))
	}
	if err := gate.PartitionSet([]string{in.PartitionID}, local); err != nil {
		res.Reason = DrillPartitionSetMismatch
		return res, nil
	}
	res.Complete, res.Reason = true, DrillVerified
	return res, nil
}

// dropScratch drops a scratch table that is no longer needed.
func dropScratch(ctx context.Context, conn clickhouse.Conn, s Scratch) error {
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS "+s.Qualified()+" SYNC"); err != nil {
		return fmt.Errorf("objstore: drop scratch %s: %w", s.Qualified(), err)
	}
	return nil
}
