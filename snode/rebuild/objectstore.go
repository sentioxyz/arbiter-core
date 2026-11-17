package rebuild

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/objstore"
)

// ObjectStore is the rebuild's second source: one backup of the owner's
// hg_safe. Parts lists what the backup holds of one logical partition;
// RestorePartitions restores those partitions of one table into the fresh
// scratch table hg_promote.<scratch>, which it creates.
type ObjectStore interface {
	Parts(tableID, partitionID string) []BackupPart
	RestorePartitions(ctx context.Context, conn clickhouse.Conn, tableID string, partitionIDs []string, scratch string) error
}

// BackupPart is one part a backup holds: its name in the backed-up table, its
// hash_of_all_files and its row count.
type BackupPart struct {
	Name, HashOfAllFiles string
	Rows                 uint64
}

// promoteDatabase is the database of NewObjectStore's restore scratch: the
// SNode's promote database, where si_snode may create and drop tables.
const promoteDatabase = "hg_promote"

// NewObjectStore is the ObjectStore of one backup taken by Plan S3-C1's
// backup job (dataplane/objstore): it reads the backup's index through
// objstore.ReadIndexes and restores with objstore.RestoreSQLAt into
// hg_promote, the SNode's promote database. ref.Name names the backup; the
// index of that name must exist.
func NewObjectStore(ctx context.Context, conn clickhouse.Conn, ref objstore.BackupRef) (ObjectStore, error) {
	indexes, err := objstore.ReadIndexes(ctx, conn, ref.Bucket, ref.Owner, ref.Credentials)
	if err != nil {
		return nil, fmt.Errorf("rebuild: read the backup indexes of indexer %d: %w", ref.Owner, err)
	}
	for _, idx := range indexes {
		if idx.Name == ref.Name {
			return newIndexStore(ref, idx), nil
		}
	}
	return nil, fmt.Errorf("rebuild: indexer %d has no backup %s under %s", ref.Owner, ref.Name, ref.Bucket.IndexGlob(ref.Owner))
}

// indexStore is NewObjectStore's ObjectStore: the parts of one backup index,
// keyed by physical table and logical partition.
type indexStore struct {
	ref   objstore.BackupRef
	parts map[string]map[string][]BackupPart
}

func newIndexStore(ref objstore.BackupRef, idx objstore.BackupIndex) *indexStore {
	ref.Base = idx.Base
	s := &indexStore{ref: ref, parts: map[string]map[string][]BackupPart{}}
	for _, table := range idx.Tables {
		byPartition := s.parts[table.PhysicalTable]
		if byPartition == nil {
			byPartition = map[string][]BackupPart{}
			s.parts[table.PhysicalTable] = byPartition
		}
		for _, p := range table.Parts {
			byPartition[p.PartitionID] = append(byPartition[p.PartitionID], BackupPart{Name: p.Name, HashOfAllFiles: p.PhysHash, Rows: p.Rows})
		}
	}
	return s
}

func (s *indexStore) Parts(tableID, partitionID string) []BackupPart {
	return s.parts[ddl.CHTableName(tableID)][partitionID]
}

// RestorePartitions runs objstore.RestoreSQLAt synchronously: si_snode may not
// read system.backups, so the ASYNC form Plan S3-C1's jobs poll is not
// available to it, and a rebuild restores one partition per statement. The
// statement carries the key, so a failure names the backup, the table and
// the partitions, and ClickHouse's answer only with the secret redacted.
func (s *indexStore) RestorePartitions(ctx context.Context, conn clickhouse.Conn, tableID string, partitionIDs []string, scratch string) error {
	clause, err := partitionsClause(partitionIDs)
	if err != nil {
		return err
	}
	physical := ddl.CHTableName(tableID)
	// Credentials with a session token restore through a temporary named
	// collection; a static key pair through S3(url, key, secret), as before.
	endpoint, closeEndpoint, err := objstore.OpenEndpoint(ctx, conn, s.ref.Bucket, s.ref.Credentials)
	if err != nil {
		return fmt.Errorf("rebuild: restore %s partitions %v of backup %s: %w", tableID, partitionIDs, s.ref.Name, err)
	}
	defer func() {
		if err := closeEndpoint(); err != nil {
			slog.Warn("rebuild: could not drop a temporary named collection", "backup", s.ref.Name, "error", err.Error())
		}
	}()
	sql := objstore.RestoreSQLAt(endpoint, s.ref.Owner, s.ref.Name, s.ref.Base, physical, clause, objstore.Scratch{Database: promoteDatabase, Table: scratch})
	if err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("rebuild: restore %s partitions %v of backup %s into %s.%s: %s",
			tableID, partitionIDs, s.ref.Name, promoteDatabase, scratch, redactSecret(err.Error(), s.ref.Credentials))
	}
	return nil
}

// redactSecret replaces c's secret and session token, SQL-escaped and raw,
// in text: a server error may echo a fragment of the statement that carried
// it.
func redactSecret(text string, c objstore.Credentials) string {
	for _, v := range []string{c.SecretAccessKey, c.SessionToken} {
		if v == "" {
			continue
		}
		if escaped := chparts.EscapeSQLString(v); escaped != v {
			text = strings.ReplaceAll(text, escaped, "[redacted]")
		}
		text = strings.ReplaceAll(text, v, "[redacted]")
	}
	return text
}

// partitionsClause is objstore.PartitionClause for a list of logical
// partition ids, which carry the partitioning (all, or p_<value>), so it needs
// no schema (the ObjectStore interface passes none): nothing for an
// unpartitioned table, else PARTITIONS with each quoted value. For one id it
// is PartitionClause's text.
func partitionsClause(partitionIDs []string) (string, error) {
	if len(partitionIDs) == 1 && partitionIDs[0] == "all" {
		return "", nil
	}
	values := make([]string, 0, len(partitionIDs))
	for _, id := range partitionIDs {
		// The value may be empty: p_ is a String key's empty partition, which
		// '' selects (arbiter-core#84).
		value, ok := strings.CutPrefix(id, "p_")
		if !ok {
			return "", fmt.Errorf("rebuild: partition id %s does not use p_<value> form", id)
		}
		values = append(values, "'"+chparts.EscapeSQLString(value)+"'")
	}
	if len(values) == 0 {
		return "", fmt.Errorf("rebuild: no partition to restore")
	}
	return "PARTITIONS " + strings.Join(values, ", "), nil
}
