package snode

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

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
