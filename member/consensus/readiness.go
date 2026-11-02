package consensus

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// ReplicaSyncTimeout bounds the SYSTEM SYNC REPLICA of one hg_member_unsafe
// table in ReadLocalReplicas; a replica that does not catch up in time is not
// ready at this pass.
const ReplicaSyncTimeout = 30 * time.Second

// LocalReplicas maps an hg_member_unsafe physical table name (ddl.CHTableName)
// to whether this member holds an active, synced replica of the incarnation
// PlanReadiness waits for, read from system.replicas after SYSTEM SYNC
// REPLICA as si_member (plan S3-E1 contract §42.3).
type LocalReplicas map[string]bool

// readinessTables lists the incarnations whose hg_member_unsafe replicas a
// member of g holds before it reports ready: the live incarnation of every
// table of g's database owned by g's owner, when it is Pending or Active.
// The reconciler creates no hg_member_unsafe replica from Retiring on and
// drops a Purging incarnation's once quiet (spec §8.8), so a member that
// joins while a table is Retiring or Purging holds none and neither holds
// readiness back.
func readinessTables(snap wire.TableRegistrySnapshot, g wire.DatabaseGroup) []wire.TableIncarnation {
	var out []wire.TableIncarnation
	for _, live := range liveTablesOf(snap, g.OwnerIndexerID, g.DatabaseID) {
		switch live.Status {
		case wire.TableStatusPending, wire.TableStatusActive:
			out = append(out, live)
		}
	}
	return out
}

// awaitingReadiness lists, in the snapshot's group order, the groups that
// name nodeID as a current member that is not Ready().
func awaitingReadiness(snap wire.TableRegistrySnapshot, nodeID string) []wire.DatabaseGroup {
	var out []wire.DatabaseGroup
	for _, g := range snap.Groups {
		if i := slices.IndexFunc(g.Members, func(m wire.GroupMember) bool { return m.NodeID == nodeID }); i >= 0 && !g.Members[i].Ready() {
			out = append(out, g)
		}
	}
	return out
}

// PlanReadiness plans nodeID's MemberReady messages (spec §6.4): one body per
// group that names the node as a current member that is not Ready(), at the
// group's current generation, once the live incarnation of every table of
// the group's database owned by its owner that is Pending or Active (at
// least one; a Retiring table is never waited for) has an active, synced
// local replica. local comes
// from ReadLocalReplicas over ReplicaPaths of the same snapshot, which checks
// each replica's Keeper path, so a replica of another incarnation of the same
// name never counts.
func PlanReadiness(snap wire.TableRegistrySnapshot, nodeID string, local LocalReplicas) []arbiter.MemberReadyBody {
	var out []arbiter.MemberReadyBody
	for _, g := range awaitingReadiness(snap, nodeID) {
		tables := readinessTables(snap, g)
		if len(tables) == 0 || slices.ContainsFunc(tables, func(inc wire.TableIncarnation) bool { return !local[ddl.CHTableName(inc.Key())] }) {
			continue
		}
		out = append(out, arbiter.MemberReadyBody{NodeID: nodeID, OwnerIndexerID: g.OwnerIndexerID, DatabaseID: g.DatabaseID, Generation: g.Generation})
	}
	return out
}

// ReplicaPaths maps the hg_member_unsafe table of every incarnation
// PlanReadiness waits for to that incarnation's Keeper path
// (ddl.ZooKeeperPathFor with tableset.KeeperScopeOf).
func ReplicaPaths(snap wire.TableRegistrySnapshot, nodeID string, keeperShardID uint32) map[string]string {
	paths := map[string]string{}
	for _, g := range awaitingReadiness(snap, nodeID) {
		for _, inc := range readinessTables(snap, g) {
			paths[ddl.CHTableName(inc.Key())] = ddl.ZooKeeperPathFor(keeperShardID, inc.Key(), tableset.KeeperScopeOf(inc))
		}
	}
	return paths
}

// codeUnknownTable is ClickHouse's UNKNOWN_TABLE.
const codeUnknownTable int32 = 60

// ReadLocalReplicas runs, as si_member, SYSTEM SYNC REPLICA on each table of
// paths in memberUnsafeDB (each bounded by ReplicaSyncTimeout) and then reads
// its system.replicas row: a table is synced when the row names its path and
// nodeID as the replica and the replica is neither read-only nor without a
// Keeper session. A missing table and a sync that fails or times out are
// false; a failed read of system.replicas is an error.
func ReadLocalReplicas(ctx context.Context, conn clickhouse.Conn, memberUnsafeDB, nodeID string, paths map[string]string) (LocalReplicas, error) {
	out := make(LocalReplicas, len(paths))
	for _, name := range slices.Sorted(maps.Keys(paths)) {
		out[name] = false
		syncCtx, cancel := context.WithTimeout(ctx, ReplicaSyncTimeout)
		err := conn.Exec(syncCtx, "SYSTEM SYNC REPLICA "+memberIdent(memberUnsafeDB)+"."+memberIdent(name))
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			continue
		}
		synced, err := replicaSynced(ctx, conn, memberUnsafeDB, name, paths[name], nodeID)
		if err != nil {
			return nil, err
		}
		out[name] = synced
	}
	return out, nil
}

func replicaSynced(ctx context.Context, conn clickhouse.Conn, database, table, path, nodeID string) (bool, error) {
	rows, err := conn.Query(ctx, "SELECT zookeeper_path, replica_name, is_readonly, is_session_expired FROM system.replicas WHERE database = ? AND table = ?", database, table)
	if err != nil {
		var exception *clickhouse.Exception
		if errors.As(err, &exception) && exception.Code == codeUnknownTable {
			return false, nil
		}
		return false, fmt.Errorf("member consensus: read system.replicas for %s.%s: %w", database, table, err)
	}
	defer rows.Close()
	synced := false
	for rows.Next() {
		var (
			zkPath, replica   string
			readOnly, expired uint8
		)
		if err := rows.Scan(&zkPath, &replica, &readOnly, &expired); err != nil {
			return false, fmt.Errorf("member consensus: scan system.replicas for %s.%s: %w", database, table, err)
		}
		synced = zkPath == path && replica == nodeID && readOnly == 0 && expired == 0
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("member consensus: read system.replicas for %s.%s: %w", database, table, err)
	}
	return synced, nil
}

// memberIdent backquotes an identifier with dataplane/ddl's rule.
func memberIdent(id string) string {
	id = strings.ReplaceAll(id, `\`, `\\`)
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}
