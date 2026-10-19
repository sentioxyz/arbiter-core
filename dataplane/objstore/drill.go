package objstore

import (
	"context"
	"errors"
	"fmt"
	"sort"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// SchemaSource answers a table's schema. Its answer is untrusted: the drill
// accepts it only when its payloadexec.TableSchemaHash equals the manifest's
// schema_hash.
type SchemaSource interface {
	TableSchema(ctx context.Context, tableID string) (payloadexec.TableSchema, error)
}

// Drill restores one backup of its owner, table by table and partition by
// partition, into scratch tables and gates each against the manifest the
// backup's index names (spec §16 P1 exit, K27).
type Drill struct {
	// Conn is a staging ClickHouse, logged in as si_restore.
	Conn      clickhouse.Conn
	Manifests ManifestSource
	Schemas   SchemaSource
	Bucket    Bucket
	Owner     uint64
	NetworkID string
	// Credentials is a read-only restore key.
	Credentials Credentials
	// Backup names the backup; "" selects the owner's latest.
	Backup  string
	Scratch *ScratchNamer
}

// DrillReason is a table's or a partition's outcome.
type DrillReason string

const (
	DrillVerified             DrillReason = "verified"
	DrillNotInManifest        DrillReason = "not_in_manifest"
	DrillSchemaHashMismatch   DrillReason = "schema_hash_mismatch"
	DrillRestoreFailed        DrillReason = "restore_failed"
	DrillNeedUnsatisfied      DrillReason = "need_unsatisfied"
	DrillClosureMismatch      DrillReason = "closure_mismatch"
	DrillPartitionSetMismatch DrillReason = "partition_set_mismatch"
)

// DrillPartition is one gated partition. Scratch names the scratch table a
// failed gate kept; a verified partition's scratch is dropped.
type DrillPartition struct {
	PartitionID string      `json:"partition_id"`
	Reason      DrillReason `json:"reason"`
	Need        int         `json:"need"`
	Kept        int         `json:"kept"`
	Dropped     int         `json:"dropped"`
	Scratch     string      `json:"scratch,omitempty"`
}

// DrillTable is one backed-up table: verified when every partition the
// manifest lists for it is verified.
type DrillTable struct {
	TableID        string           `json:"table_id"`
	PhysicalTable  string           `json:"physical_table"`
	IncarnationSeq uint64           `json:"incarnation_seq"`
	Reason         DrillReason      `json:"reason"`
	Partitions     []DrillPartition `json:"partitions"`
}

// DrillVerdict is a drill's outcome, printed as one JSON line.
type DrillVerdict struct {
	Format         string               `json:"format"`
	OwnerIndexerID uint64               `json:"owner_indexer_id"`
	Backup         string               `json:"backup"`
	SnapshotID     string               `json:"snapshot_id"`
	Entry          replay.TrackFrontier `json:"entry"`
	Tables         []DrillTable         `json:"tables"`
	Pass           bool                 `json:"pass"`
}

// drillPass is the pass rule: at least one table verified and every table
// verified except those the manifest does not list (K27).
func drillPass(tables []DrillTable) bool {
	verified := 0
	for _, t := range tables {
		switch t.Reason {
		case DrillVerified:
			verified++
		case DrillNotInManifest:
		default:
			return false
		}
	}
	return verified > 0
}

// Run picks the backup, takes the manifest S its index names (never the
// latest), and for every backed-up table that S lists checks the schema's
// hash against S's schema_hash and gates every partition S lists with
// RestoreAndGate and an empty local copy. The table ids come from S (the
// table whose ddl.CHTableName is the backed-up physical name), never from
// the index.
func (d *Drill) Run(ctx context.Context) (DrillVerdict, error) {
	if d.Conn == nil || d.Manifests == nil || d.Schemas == nil || d.Scratch == nil {
		return DrillVerdict{}, errors.New("objstore: a Drill needs a ClickHouse connection, a manifest source, a schema source and a scratch namer")
	}
	if d.Backup != "" {
		if err := checkBackupName(d.Backup); err != nil {
			return DrillVerdict{}, err
		}
	}
	indexes, err := ReadIndexes(ctx, d.Conn, d.Bucket, d.Owner, d.Credentials)
	if err != nil {
		return DrillVerdict{}, err
	}
	var idx *BackupIndex
	for i := range indexes {
		if d.Backup == "" || indexes[i].Name == d.Backup {
			idx = &indexes[i]
		}
	}
	under := d.Bucket.URL() + "/" + OwnerPrefix(d.Owner)
	if idx == nil && d.Backup != "" {
		return DrillVerdict{}, fmt.Errorf("objstore: backup %s of indexer %d has no index under %s", d.Backup, d.Owner, under)
	}
	if idx == nil {
		return DrillVerdict{}, fmt.Errorf("objstore: no backup of indexer %d under %s", d.Owner, under)
	}
	if idx.NetworkID != d.NetworkID {
		return DrillVerdict{}, fmt.Errorf("objstore: backup %s of indexer %d belongs to network %q, not %q", idx.Name, d.Owner, idx.NetworkID, d.NetworkID)
	}
	s, err := d.Manifests.Manifest(ctx, idx.SnapshotID)
	if err != nil {
		return DrillVerdict{}, err
	}
	if err := CheckManifest(idx.SnapshotID, s); err != nil {
		return DrillVerdict{}, err
	}
	entry, err := ownerEntry(s, d.Owner)
	if err != nil {
		return DrillVerdict{}, err
	}
	if entry != idx.Entry {
		return DrillVerdict{}, fmt.Errorf("objstore: backup %s names track frontier %+v of indexer %d, but manifest %s holds %+v", idx.Name, idx.Entry, d.Owner, s.SnapshotID, entry)
	}
	listed := make(map[string]replay.TableManifest, len(s.Tables))
	for _, t := range s.Tables {
		listed[ddl.CHTableName(t.TableID)] = t
	}
	ref := BackupRef{Bucket: d.Bucket, Owner: d.Owner, Name: idx.Name, Credentials: d.Credentials}
	verdict := DrillVerdict{Format: DrillVerdictFormat, OwnerIndexerID: d.Owner, Backup: idx.Name, SnapshotID: s.SnapshotID, Entry: entry, Tables: []DrillTable{}}
	backedUp := append([]BackupIndexTable(nil), idx.Tables...)
	sort.Slice(backedUp, func(i, j int) bool { return backedUp[i].PhysicalTable < backedUp[j].PhysicalTable })
	for _, bt := range backedUp {
		table := DrillTable{PhysicalTable: bt.PhysicalTable, IncarnationSeq: bt.IncarnationSeq, Partitions: []DrillPartition{}}
		tm, ok := listed[bt.PhysicalTable]
		if !ok {
			table.Reason = DrillNotInManifest
			verdict.Tables = append(verdict.Tables, table)
			continue
		}
		table.TableID = tm.TableID
		schema, err := d.Schemas.TableSchema(ctx, tm.TableID)
		if err != nil {
			return DrillVerdict{}, err
		}
		if payloadexec.TableSchemaHash(d.NetworkID, schema) != tm.SchemaHash {
			table.Reason = DrillSchemaHashMismatch
			verdict.Tables = append(verdict.Tables, table)
			continue
		}
		roots := append([]replay.PartitionCommitment(nil), tm.PartitionRoots...)
		sort.Slice(roots, func(i, j int) bool { return roots[i].PartitionID < roots[j].PartitionID })
		table.Reason = DrillVerified
		for _, pc := range roots {
			scratch := d.Scratch.Next()
			res, err := RestoreAndGate(ctx, d.Conn, GateInput{
				Backup: ref, Manifest: s, TableID: tm.TableID, PhysicalTable: bt.PhysicalTable,
				Schema: schema, PartitionID: pc.PartitionID, Scratch: scratch,
			})
			if err != nil && (errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || res.Reason != DrillRestoreFailed) {
				return DrillVerdict{}, err
			}
			part := DrillPartition{
				PartitionID: pc.PartitionID, Reason: res.Reason, Need: len(res.Plan.Need),
				Kept: len(res.Classification.Kept), Dropped: len(res.Classification.Drop),
			}
			if res.Complete {
				if err := dropScratch(ctx, d.Conn, scratch); err != nil {
					return DrillVerdict{}, err
				}
			} else {
				part.Scratch = scratch.Database + "." + scratch.Table
				if table.Reason == DrillVerified {
					table.Reason = res.Reason
				}
			}
			table.Partitions = append(table.Partitions, part)
		}
		verdict.Tables = append(verdict.Tables, table)
	}
	verdict.Pass = drillPass(verdict.Tables)
	return verdict, nil
}
