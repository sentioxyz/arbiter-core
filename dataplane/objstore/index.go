package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
)

// BackupIndex is the index object beside each backup,
// owner-<id>/<name>.index.json, written once after its BACKUP finished and
// never rewritten. The JSON field names are binding (plan S3-C1 contract
// §17).
type BackupIndex struct {
	Format         string     `json:"format"`
	NetworkID      string     `json:"network_id"`
	OwnerIndexerID uint64     `json:"owner_indexer_id"`
	Name           string     `json:"name"`
	Kind           BackupKind `json:"kind"`
	// Base names the backup an incremental backup is based on.
	Base        string `json:"base,omitempty"`
	ChainLength uint32 `json:"chain_length"`
	// StartedAt and FinishedAt are RFC 3339 UTC.
	StartedAt  string `json:"started_at"`
	FinishedAt string `json:"finished_at"`
	// SnapshotID is the latest published manifest when the backup started,
	// and Entry its track frontier of the owner: the manifest a drill gates
	// against (K27).
	SnapshotID string               `json:"snapshot_id"`
	Entry      replay.TrackFrontier `json:"entry"`
	Tables     []BackupIndexTable   `json:"tables"`
}

// BackupIndexTable is one hg_safe table of the backup.
type BackupIndexTable struct {
	PhysicalTable string `json:"physical_table"`
	// TableID is the id the manifest SnapshotID lists under this physical
	// name (ddl.CHTableName), empty when it lists none.
	TableID string `json:"table_id,omitempty"`
	// IncarnationSeq is the table's ddl.ParseIncarnationComment, 0 for a
	// genesis table.
	IncarnationSeq uint64            `json:"incarnation_seq"`
	Parts          []BackupIndexPart `json:"parts"`
}

// BackupIndexPart is one active part right before the BACKUP, its partition
// in the logical form (all, or p_<value>).
type BackupIndexPart struct {
	Name        string `json:"name"`
	PartitionID string `json:"partition_id"`
	PhysHash    string `json:"phys_hash"`
	Rows        uint64 `json:"rows"`
}

// decodeIndex decodes one index object strictly: an unknown field, a
// trailing value, another format, another owner or a name that is not the
// object's own refuses it.
func decodeIndex(file string, owner uint64, blob []byte) (BackupIndex, error) {
	var idx BackupIndex
	dec := json.NewDecoder(bytes.NewReader(blob))
	dec.DisallowUnknownFields()
	err := dec.Decode(&idx)
	if err == nil {
		var trailing any
		if dec.Decode(&trailing) != io.EOF {
			err = errors.New("trailing data after the object")
		}
	}
	if err == nil && idx.Format != BackupIndexFormat {
		err = fmt.Errorf("format %q", idx.Format)
	}
	if err == nil && idx.OwnerIndexerID != owner {
		err = fmt.Errorf("it names owner %d under %s", idx.OwnerIndexerID, OwnerPrefix(owner))
	}
	if err == nil && idx.Name+IndexSuffix != file {
		err = fmt.Errorf("it names backup %q", idx.Name)
	}
	if err == nil {
		err = checkBackupName(idx.Name)
	}
	if err != nil {
		return BackupIndex{}, fmt.Errorf("objstore: index %s is not a %s object: %v", file, BackupIndexFormat, err)
	}
	return idx, nil
}

// ReadIndexes reads every index object of owner, sorted by name (oldest
// first). An object that does not decode as a BackupIndexFormat object of
// this owner stops it.
func ReadIndexes(ctx context.Context, conn clickhouse.Conn, b Bucket, owner uint64, c Credentials) ([]BackupIndex, error) {
	if err := c.validate(); err != nil {
		return nil, err
	}
	rows, err := conn.Query(clickhouse.Context(ctx, clickhouse.WithSettings(listSettings)), indexReadSQL(b, owner, c))
	if err != nil {
		return nil, c.statementError("objstore: read the indexes of indexer %d", err, owner)
	}
	defer rows.Close()
	var out []BackupIndex
	for rows.Next() {
		var file, blob string
		if err := rows.Scan(&file, &blob); err != nil {
			return nil, c.statementError("objstore: read the indexes of indexer %d", err, owner)
		}
		idx, err := decodeIndex(file, owner, []byte(blob))
		if err != nil {
			return nil, err
		}
		out = append(out, idx)
	}
	if err := rows.Err(); err != nil {
		return nil, c.statementError("objstore: read the indexes of indexer %d", err, owner)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

// backupStarted reports whether any object exists below backup name's
// directory: a finished backup with or without an index, or the remains of
// one that failed.
func backupStarted(ctx context.Context, conn clickhouse.Conn, b Bucket, owner uint64, name string, c Credentials) (bool, error) {
	var n uint64
	if err := conn.QueryRow(clickhouse.Context(ctx, clickhouse.WithSettings(listSettings)), backupObjectsSQL(b, owner, name, c)).Scan(&n); err != nil {
		return false, c.statementError("objstore: list backup %s of indexer %d", err, name, owner)
	}
	return n > 0, nil
}

// writeIndex writes idx once; an existing object is refused, never
// overwritten.
func writeIndex(ctx context.Context, conn clickhouse.Conn, b Bucket, c Credentials, idx BackupIndex) error {
	doc, err := json.Marshal(idx)
	if err != nil {
		return fmt.Errorf("objstore: encode the index of backup %s: %w", idx.Name, err)
	}
	ctx = clickhouse.Context(ctx,
		clickhouse.WithSettings(indexWriteSettings),
		clickhouse.WithParameters(clickhouse.Parameters{"index": string(doc)}))
	if err := conn.Exec(ctx, indexWriteSQL(b, idx.OwnerIndexerID, idx.Name, c)); err != nil {
		return c.statementError("objstore: write the index of backup %s of indexer %d", err, idx.Name, idx.OwnerIndexerID)
	}
	return nil
}
