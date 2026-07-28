package snode

import (
	"context"
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func (r *Role) attachCandidatePart(ctx context.Context, table, promote, partName string) error {
	src, err := r.unsafePartPath(ctx, table, partName)
	if err != nil {
		return err
	}
	dst, err := r.promoteDetachedPath(ctx, table, partName)
	if err != nil {
		return err
	}
	if err := hardlinkDir(src, dst); err != nil {
		return fmt.Errorf("hardlink part %s: %w", partName, err)
	}
	return r.exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PART '%s'", promote, escapeSQLString(partName)))
}

func (r *Role) exec(ctx context.Context, query string) error {
	if err := r.d.Conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", query, err)
	}
	return nil
}

func (r *Role) dropPartitionIfPresent(ctx context.Context, db, table string, sch payloadexec.TableSchema, partitionID, partitionSQL string) error {
	parts, err := activeParts(ctx, r.d.Conn, db, table)
	if err != nil {
		return err
	}
	for _, p := range parts {
		if logicalPartitionID(sch, p) == partitionID {
			return r.exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s DROP PARTITION %s", db, table, partitionSQL))
		}
	}
	return nil
}

func (r *Role) unsafePartPath(ctx context.Context, table, partName string) (string, error) {
	var path string
	if err := r.d.Conn.QueryRow(ctx, `
		SELECT path
		FROM system.parts
		WHERE database = ? AND table = ? AND name = ? AND active
		LIMIT 1`, r.cfg.UnsafeDatabase, table, partName).Scan(&path); err != nil {
		return "", fmt.Errorf("unsafe part path %s: %w", partName, err)
	}
	return path, nil
}

func (r *Role) promoteDetachedPath(ctx context.Context, table, partName string) (string, error) {
	var root string
	if err := r.d.Conn.QueryRow(ctx, `
		SELECT data_paths[1]
		FROM system.tables
		WHERE database = ? AND name = ?`, r.cfg.PromoteDatabase, table).Scan(&root); err != nil {
		return "", fmt.Errorf("promote table path %s: %w", table, err)
	}
	return filepath.Join(root, "detached", partName), nil
}

func quotePartition(sch payloadexec.TableSchema, partitionID string) (string, error) {
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
	return "'" + escapeSQLString(value) + "'", nil
}

func escapeSQLString(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}

func candidateHashes(cmd arbiter.PromoteSafePartition) []string {
	hashes := make([]string, 0, len(cmd.CandidateParts))
	for _, p := range cmd.CandidateParts {
		hashes = append(hashes, p.PartRowLtHash)
	}
	sort.Strings(hashes)
	return hashes
}
