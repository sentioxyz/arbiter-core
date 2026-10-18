package objstore

import (
	"context"
	"errors"
	"fmt"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// Backuper takes one backup of its owner's hg_safe per Run: a full backup
// that starts a chain, or an incremental one on the chain's latest backup.
type Backuper struct {
	// Conn is the owner's source ClickHouse, logged in as si_backup.
	Conn      clickhouse.Conn
	Manifests ManifestSource
	Bucket    Bucket
	// Owner is the owner's contract-assigned SI indexer id.
	Owner       uint64
	NetworkID   string
	Credentials Credentials
	// MaxChainLinks bounds a chain: one full backup and at most
	// MaxChainLinks-1 incremental ones. 0 selects DefaultMaxChainLinks; else
	// 1 to MaxChainLinksLimit.
	MaxChainLinks uint32
	// Now is time.Now when nil.
	Now func() time.Time
}

// maxChainLinks is the configured chain length.
func maxChainLinks(n uint32) (uint32, error) {
	if n == 0 {
		return DefaultMaxChainLinks, nil
	}
	if n > MaxChainLinksLimit {
		return 0, fmt.Errorf("objstore: max chain links %d must be between 1 and 30", n)
	}
	return n, nil
}

// NextBackup is the kind, base and chain length of the backup after latest
// (nil when the owner has none): a full backup of chain length 1 when there
// is no backup yet or latest's chain is maxChainLinks long, else an
// incremental one on latest with its chain length plus one.
func NextBackup(latest *BackupIndex, maxChainLinks uint32) (BackupKind, string, uint32) {
	if latest == nil || latest.ChainLength >= maxChainLinks {
		return BackupFull, "", 1
	}
	return BackupIncremental, latest.Name, latest.ChainLength + 1
}

// Run takes the next backup and writes its index: the latest published
// manifest and the owner's entry in it, the owner's indexes, the kind
// (NextBackup), a refusal of a name that exists already, hg_safe's tables and
// their active parts, the BACKUP and, once it finished, the index object. It
// returns the index it wrote.
func (b *Backuper) Run(ctx context.Context) (BackupIndex, error) {
	if b.Conn == nil || b.Manifests == nil {
		return BackupIndex{}, errors.New("objstore: a Backuper needs a ClickHouse connection and a manifest source")
	}
	if err := b.Credentials.validate(); err != nil {
		return BackupIndex{}, err
	}
	links, err := maxChainLinks(b.MaxChainLinks)
	if err != nil {
		return BackupIndex{}, err
	}
	now := b.Now
	if now == nil {
		now = time.Now
	}
	started := now().UTC()
	s, err := b.Manifests.Latest(ctx)
	if err != nil {
		return BackupIndex{}, err
	}
	if err := CheckManifest(s.SnapshotID, s); err != nil {
		return BackupIndex{}, err
	}
	entry, err := ownerEntry(s, b.Owner)
	if err != nil {
		return BackupIndex{}, err
	}
	indexes, err := ReadIndexes(ctx, b.Conn, b.Bucket, b.Owner, b.Credentials)
	if err != nil {
		return BackupIndex{}, err
	}
	var latest *BackupIndex
	if len(indexes) > 0 {
		latest = &indexes[len(indexes)-1]
	}
	kind, base, chain := NextBackup(latest, links)
	name := BackupName(started, kind)
	if latest != nil && name <= latest.Name {
		if name == latest.Name {
			return BackupIndex{}, fmt.Errorf("objstore: backup %s of indexer %d exists already; a backup name is never reused", name, b.Owner)
		}
		return BackupIndex{}, fmt.Errorf("objstore: backup %s of indexer %d sorts before the latest backup %s; the chain is ordered by name, so check the clock", name, b.Owner, latest.Name)
	}
	exists, err := backupStarted(ctx, b.Conn, b.Bucket, b.Owner, name, b.Credentials)
	if err != nil {
		return BackupIndex{}, err
	}
	for _, idx := range indexes {
		exists = exists || idx.Name == name
	}
	if exists {
		return BackupIndex{}, fmt.Errorf("objstore: backup %s of indexer %d exists already; a backup name is never reused", name, b.Owner)
	}
	tables, err := b.safeTables(ctx, s.Tables)
	if err != nil {
		return BackupIndex{}, err
	}
	if err := runAsync(ctx, b.Conn, BackupSQL(b.Bucket, b.Owner, name, base, b.Credentials), b.Credentials, "BACKUP_CREATED"); err != nil {
		return BackupIndex{}, fmt.Errorf("objstore: backup %s of indexer %d: %w", name, b.Owner, err)
	}
	idx := BackupIndex{
		Format:         BackupIndexFormat,
		NetworkID:      b.NetworkID,
		OwnerIndexerID: b.Owner,
		Name:           name,
		Kind:           kind,
		Base:           base,
		ChainLength:    chain,
		StartedAt:      started.Format(time.RFC3339),
		FinishedAt:     now().UTC().Format(time.RFC3339),
		SnapshotID:     s.SnapshotID,
		Entry:          entry,
		Tables:         tables,
	}
	if err := writeIndex(ctx, b.Conn, b.Bucket, b.Credentials, idx); err != nil {
		return BackupIndex{}, err
	}
	return idx, nil
}

// safeTables lists hg_safe's tables (system.tables) and their active parts
// (chparts.ActiveParts), each named by the table id the manifest lists under
// its physical name.
func (b *Backuper) safeTables(ctx context.Context, listed []replay.TableManifest) ([]BackupIndexTable, error) {
	byPhysical := make(map[string]string, len(listed))
	for _, t := range listed {
		byPhysical[ddl.CHTableName(t.TableID)] = t.TableID
	}
	rows, err := b.Conn.Query(ctx, "SELECT name, comment, partition_key FROM system.tables WHERE database = ? ORDER BY name", SafeDatabase)
	if err != nil {
		return nil, fmt.Errorf("objstore: list the %s tables: %w", SafeDatabase, err)
	}
	type table struct{ name, comment, partitionKey string }
	var found []table
	for rows.Next() {
		var t table
		if err := rows.Scan(&t.name, &t.comment, &t.partitionKey); err != nil {
			rows.Close()
			return nil, fmt.Errorf("objstore: list the %s tables: %w", SafeDatabase, err)
		}
		found = append(found, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, fmt.Errorf("objstore: list the %s tables: %w", SafeDatabase, err)
	}
	out := make([]BackupIndexTable, 0, len(found))
	for _, t := range found {
		seq, _ := ddl.ParseIncarnationComment(t.comment)
		parts, err := chparts.ActiveParts(ctx, b.Conn, SafeDatabase, t.name)
		if err != nil {
			return nil, fmt.Errorf("objstore: list the parts of %s.%s: %w", SafeDatabase, t.name, err)
		}
		// Only PartitionBy decides the logical form: all for an unpartitioned
		// table, else p_<value>.
		sch := payloadexec.TableSchema{PartitionBy: t.partitionKey}
		entry := BackupIndexTable{PhysicalTable: t.name, TableID: byPhysical[t.name], IncarnationSeq: seq, Parts: []BackupIndexPart{}}
		for _, p := range parts {
			entry.Parts = append(entry.Parts, BackupIndexPart{Name: p.Name, PartitionID: chparts.LogicalPartitionID(sch, p), PhysHash: p.PhysHash, Rows: p.Rows})
		}
		out = append(out, entry)
	}
	return out, nil
}
