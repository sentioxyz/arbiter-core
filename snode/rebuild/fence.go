package rebuild

import (
	"context"
	"fmt"
	"slices"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// FenceResult is the fence's outcome for one owned table: its Keeper path,
// whether this node's replica was registered there, and whether the fence
// dropped it.
type FenceResult struct {
	TableID, Path    string
	Present, Dropped bool
}

// Fence is the Keeper fence of a same-identity owner rebuild (housegate spec
// 2026-10-14 §16 P4; plan S3-D clarification K54), run before the new
// ClickHouse creates any replica: the scoped root /sentio/<shard>/unsafe_inc
// must have no child (every incarnation is on the unscoped path before the
// purge switch, spec §8.8), and then, per table, p.NodeID's replica is either
// absent, or present and inactive and dropped with SYSTEM DROP REPLICA ...
// FROM ZKPATH. A present replica that is still active refuses the fence
// (ErrFenceReplicaActive): the old ClickHouse is not dead, and nothing of a
// later table is dropped. Tables are fenced in the order given.
func Fence(ctx context.Context, conn clickhouse.Conn, p ddl.Pinned, tableIDs []string) ([]FenceResult, error) {
	root := fmt.Sprintf("/sentio/%d/unsafe_inc", p.KeeperShardID)
	children, err := ddl.ScopedUnsafeRootChildren(ctx, conn, p)
	if err != nil {
		return nil, fmt.Errorf("rebuild %s: %w", PhaseFence, err)
	}
	if len(children) > 0 {
		return nil, refuse(ErrFenceScopedReplica, root, children)
	}
	out := make([]FenceResult, 0, len(tableIDs))
	for _, tableID := range tableIDs {
		res := FenceResult{TableID: tableID, Path: ddl.ZooKeeperPath(p, tableID)}
		replicas, err := ddl.KeeperReplicas(ctx, conn, p, tableID)
		if err != nil {
			return out, fmt.Errorf("rebuild %s: %w", PhaseFence, err)
		}
		if !slices.Contains(replicas, p.NodeID) {
			out = append(out, res)
			continue
		}
		res.Present = true
		active, err := ddl.ReplicaIsActive(ctx, conn, p, tableID, p.NodeID)
		if err != nil {
			return out, fmt.Errorf("rebuild %s: %w", PhaseFence, err)
		}
		if active {
			return out, refuse(ErrFenceReplicaActive, p.NodeID, tableID, replicaHost(ctx, conn, res.Path, p.NodeID))
		}
		if err := ddl.DropReplica(ctx, conn, p, tableID, p.NodeID); err != nil {
			if ddl.ReplicaActive(err) {
				// The old server came back between the check and the drop.
				return out, refuse(ErrFenceReplicaActive, p.NodeID, tableID, replicaHost(ctx, conn, res.Path, p.NodeID))
			}
			return out, fmt.Errorf("rebuild %s: %w", PhaseFence, err)
		}
		res.Dropped = true
		out = append(out, res)
	}
	return out, nil
}

// replicaHost is the host a replica registered under path (the host line of
// <path>/replicas/<replica>/host), for the operator; "an unknown host" when it
// cannot be read.
func replicaHost(ctx context.Context, conn clickhouse.Conn, path, replica string) string {
	var value string
	err := conn.QueryRow(ctx, `SELECT value FROM system.zookeeper WHERE path = ? AND name = 'host'`, path+"/replicas/"+replica).Scan(&value)
	if err != nil {
		return "an unknown host"
	}
	for _, line := range strings.Split(value, "\n") {
		if host, ok := strings.CutPrefix(line, "host: "); ok && host != "" {
			return host
		}
	}
	return "an unknown host"
}
