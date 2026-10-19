package snode

import (
	"context"
	"fmt"
	"sort"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
)

func (r *Role) exec(ctx context.Context, query string) error {
	if err := r.d.Conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("%s: %w", query, err)
	}
	return nil
}

// dropPartitionIfPresent drops partitionID of db.table when it has an active
// part. partitionSQL is the caller's quotePartition(sch, partitionID), which
// chparts derives again; the signature is kept for the existing call sites.
func (r *Role) dropPartitionIfPresent(ctx context.Context, db, table string, sch payloadexec.TableSchema, partitionID, partitionSQL string) error {
	_, err := chparts.DropPartitionIfPresent(ctx, r.d.Conn, db, table, sch, partitionID)
	return err
}

func quotePartition(sch payloadexec.TableSchema, partitionID string) (string, error) {
	return chparts.QuotePartition(sch, partitionID)
}

func escapeSQLString(s string) string {
	return chparts.EscapeSQLString(s)
}

func candidateHashes(cmd arbiter.PromoteSafePartition) []string {
	hashes := make([]string, 0, len(cmd.CandidateParts))
	for _, p := range cmd.CandidateParts {
		hashes = append(hashes, p.PartRowLtHash)
	}
	sort.Strings(hashes)
	return hashes
}
