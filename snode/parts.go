package snode

import (
	"context"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// partInfo is one active part as system.parts lists it (dataplane/chparts,
// shared with the stage-3 restore-and-gate library).
type partInfo = chparts.Part

func activeParts(ctx context.Context, conn clickhouse.Conn, db, table string) ([]partInfo, error) {
	return chparts.ActiveParts(ctx, conn, db, table)
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
