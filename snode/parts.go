package snode

import (
	"context"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

type partInfo struct {
	Name           string
	PartitionID    string
	PartitionValue string
	PhysHash       string
	Path           string
	Rows           uint64
	Bytes          uint64
}

func activeParts(ctx context.Context, conn clickhouse.Conn, db, table string) ([]partInfo, error) {
	rows, err := conn.Query(ctx, `
		SELECT name, partition_id, partition, hash_of_all_files, path, rows, bytes_on_disk
		FROM system.parts
		WHERE database = ? AND table = ? AND active
		ORDER BY name`, db, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []partInfo
	for rows.Next() {
		var p partInfo
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

func diffParts(before, after []partInfo) []partInfo {
	seen := make(map[string]bool, len(before))
	for _, p := range before {
		seen[p.Name] = true
	}
	var out []partInfo
	for _, p := range after {
		if !seen[p.Name] {
			out = append(out, p)
		}
	}
	return out
}

func (r *Role) scanPartRowIDs(ctx context.Context, table, partName string) ([]string, error) {
	rows, err := r.d.Conn.Query(
		ctx,
		"SELECT _hg_row_id FROM "+r.cfg.UnsafeDatabase+"."+table+" WHERE _part = @part",
		clickhouse.Named("part", partName),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var rowID string
		if err := rows.Scan(&rowID); err != nil {
			return nil, err
		}
		out = append(out, rowID)
	}
	return out, rows.Err()
}

// CHTableName maps a logical storage-integrity table id to its physical
// ClickHouse table name; consumers must use this instead of copying the rule.
func CHTableName(tableID string) string {
	return ddl.CHTableName(tableID)
}
