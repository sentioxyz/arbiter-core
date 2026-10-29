package rebuild

import (
	"context"
	"fmt"
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
// objstore.ReadIndexes and restores with objstore.RestoreSQL into
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

// RestorePartitions runs objstore.RestoreSQL synchronously: si_snode may not
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
	sql := objstore.RestoreSQL(s.ref.Bucket, s.ref.Owner, s.ref.Name, physical, clause, objstore.Scratch{Database: promoteDatabase, Table: scratch}, s.ref.Credentials)
	if err := conn.Exec(ctx, sql); err != nil {
		return fmt.Errorf("rebuild: restore %s partitions %v of backup %s into %s.%s: %s",
			tableID, partitionIDs, s.ref.Name, promoteDatabase, scratch, redactSecret(err.Error(), s.ref.Credentials))
	}
	return nil
}

// redactSecret replaces c's secret, SQL-escaped and raw, in text: a server
// error may echo a fragment of the statement that carried it.
func redactSecret(text string, c objstore.Credentials) string {
	if c.SecretAccessKey == "" {
		return text
	}
	if escaped := chparts.EscapeSQLString(c.SecretAccessKey); escaped != c.SecretAccessKey {
		text = strings.ReplaceAll(text, escaped, "[redacted]")
	}
	return strings.ReplaceAll(text, c.SecretAccessKey, "[redacted]")
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
		value, ok := strings.CutPrefix(id, "p_")
		if !ok || value == "" {
			return "", fmt.Errorf("rebuild: partition id %s does not use p_<value> form", id)
		}
		values = append(values, "'"+chparts.EscapeSQLString(value)+"'")
	}
	if len(values) == 0 {
		return "", fmt.Errorf("rebuild: no partition to restore")
	}
	return "PARTITIONS " + strings.Join(values, ", "), nil
}
