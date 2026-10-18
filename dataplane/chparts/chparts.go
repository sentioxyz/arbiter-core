// Package chparts lists, scans and drops the parts of one storage-integrity
// table through ClickHouse statements only (system.parts, chexec part scans,
// ALTER TABLE ... DROP PARTITION), so a caller needs no access to ClickHouse's
// filesystem. It holds the partition helpers the SNode's promotion and owner
// audit use, shared with the stage-3 restore-and-gate library and the member
// installer (housegate spec 2026-10-14 §8.5, §8.6, §15).
package chparts

import (
	"context"
	"fmt"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Part is one active part as system.parts lists it: name, partition_id,
// partition (PartitionValue), hash_of_all_files (PhysHash), path, rows and
// bytes_on_disk (Bytes).
type Part struct {
	Name           string
	PartitionID    string
	PartitionValue string
	PhysHash       string
	Path           string
	Rows           uint64
	Bytes          uint64
}

// ActiveParts lists the active parts of db.table, ordered by name. A table
// without active parts, or one that does not exist, gives a nil slice.
func ActiveParts(ctx context.Context, conn clickhouse.Conn, db, table string) ([]Part, error) {
	rows, err := conn.Query(ctx, `
		SELECT name, partition_id, partition, hash_of_all_files, path, rows, bytes_on_disk
		FROM system.parts
		WHERE database = ? AND table = ? AND active
		ORDER BY name`, db, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Part
	for rows.Next() {
		var p Part
		if err := rows.Scan(&p.Name, &p.PartitionID, &p.PartitionValue, &p.PhysHash, &p.Path, &p.Rows, &p.Bytes); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// LogicalPartitionID is the protocol's partition id of a part, the form the
// manifest and the arbiter use: "all" for an unpartitioned table, else "p_"
// followed by the part's partition value.
func LogicalPartitionID(sch payloadexec.TableSchema, p Part) string {
	if sch.PartitionBy == "" {
		return "all"
	}
	return "p_" + p.PartitionValue
}

// PartsInLogicalPartition keeps, in order, the parts whose logical partition
// id is partitionID. It never returns nil.
func PartsInLogicalPartition(parts []Part, sch payloadexec.TableSchema, partitionID string) []Part {
	out := make([]Part, 0, len(parts))
	for _, p := range parts {
		if LogicalPartitionID(sch, p) == partitionID {
			out = append(out, p)
		}
	}
	return out
}

// QuotePartition renders a logical partition id as the partition expression
// of an ALTER TABLE or RESTORE statement: tuple() for an unpartitioned table,
// else the single-quoted, escaped value.
func QuotePartition(sch payloadexec.TableSchema, partitionID string) (string, error) {
	if sch.PartitionBy == "" {
		if partitionID != "all" {
			return "", fmt.Errorf("unpartitioned table wants partition all, got %s", partitionID)
		}
		return "tuple()", nil
	}
	value, ok := strings.CutPrefix(partitionID, "p_")
	if !ok || value == "" {
		return "", fmt.Errorf("partition id %s does not use p_<value> form", partitionID)
	}
	return "'" + EscapeSQLString(value) + "'", nil
}

// EscapeSQLString escapes s for a single-quoted ClickHouse string literal.
func EscapeSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}

// DropPartitionIfPresent issues ALTER TABLE db.table DROP PARTITION for the
// logical partition partitionID when db.table has an active part in it, and
// reports whether it did. With no such part it issues nothing.
func DropPartitionIfPresent(ctx context.Context, conn clickhouse.Conn, db, table string, sch payloadexec.TableSchema, partitionID string) (bool, error) {
	parts, err := ActiveParts(ctx, conn, db, table)
	if err != nil {
		return false, err
	}
	if len(PartsInLogicalPartition(parts, sch, partitionID)) == 0 {
		return false, nil
	}
	partition, err := QuotePartition(sch, partitionID)
	if err != nil {
		return false, err
	}
	query := fmt.Sprintf("ALTER TABLE %s.%s DROP PARTITION %s", db, table, partition)
	if err := conn.Exec(ctx, query); err != nil {
		return false, fmt.Errorf("%s: %w", query, err)
	}
	return true, nil
}

// ScannedPart is one part of a partition scan: its name, its canonical row
// LtHash ("0x" + lowercase hex) and its row count.
type ScannedPart struct {
	Name      string
	RowLtHash string
	Rows      uint64
}

// PartitionContentScan scans every active part of one logical partition of
// db.table with chexec.ScanParts (the executor's row derivation) and returns
// the partition's content root, payloadexec.SumPartRowLtHashes of the parts'
// row LtHashes, with the parts in name order. A partition without parts has
// the zero root and no parts.
func PartitionContentScan(ctx context.Context, conn clickhouse.Conn, db, table string, sch payloadexec.TableSchema, partitionID string) (string, []ScannedPart, error) {
	parts, err := ActiveParts(ctx, conn, db, table)
	if err != nil {
		return "", nil, err
	}
	var names []string
	for _, p := range PartsInLogicalPartition(parts, sch, partitionID) {
		names = append(names, p.Name)
	}
	if len(names) == 0 {
		root, err := payloadexec.SumPartRowLtHashes(nil)
		return root, nil, err
	}
	scans, err := chexec.ScanParts(ctx, conn, db+"."+table, sch, names)
	if err != nil {
		return "", nil, err
	}
	hashes := make([]string, 0, len(scans))
	out := make([]ScannedPart, 0, len(scans))
	for _, s := range scans {
		hashes = append(hashes, s.RowLtHash)
		out = append(out, ScannedPart{Name: s.PartName, RowLtHash: s.RowLtHash, Rows: s.RowCount})
	}
	root, err := payloadexec.SumPartRowLtHashes(hashes)
	if err != nil {
		return "", nil, err
	}
	return root, out, nil
}
