package ddl

import (
	"context"
	"slices"
	"sort"
	"strconv"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// ScopedKeeperPath names one incarnation path,
// /sentio/<shard>/unsafe_inc/<Table>/<IncarnationSeq>.
type ScopedKeeperPath struct {
	Table          string // the physical table name (CHTableName)
	IncarnationSeq uint64
}

// KeeperRoots is what one Keeper shard holds under the two SI roots
// (housegate spec 2026-10-14 §8.8), for the owner's sweeps and Keeper audit.
// Each list is sorted: Legacy by name, Scoped by (Table, IncarnationSeq),
// NonIncarnation by path.
type KeeperRoots struct {
	Legacy         []string           // children of /sentio/<shard>/unsafe
	Scoped         []ScopedKeeperPath // /sentio/<shard>/unsafe_inc/<t>/<seq>, seq a canonical decimal >= 1
	NonIncarnation []string           // every other replicated-table root under unsafe_inc, as a full path
}

// ListKeeperRoots lists the two SI Keeper roots of keeperShardID. Under the
// legacy root every child is one table's path. Under the incarnation-scoped
// root every child /sentio/<shard>/unsafe_inc/<t> holds incarnation paths:
// a child of it that is a canonical decimal >= 1 is one (Scoped); every other
// child, and <t> itself when it has a replicas child (a replicated table
// created directly at <t>), is NonIncarnation. Only these two levels are
// read. An absent root lists nothing.
func ListKeeperRoots(ctx context.Context, conn clickhouse.Conn, keeperShardID uint32) (KeeperRoots, error) {
	var out KeeperRoots
	legacy, err := keeperChildren(ctx, conn, keeperRootPath(keeperShardID, KeeperUnsafeRoot))
	if err != nil {
		return KeeperRoots{}, err
	}
	out.Legacy = legacy
	scopedRoot := keeperRootPath(keeperShardID, KeeperScopedUnsafeRoot)
	tables, err := keeperChildren(ctx, conn, scopedRoot)
	if err != nil {
		return KeeperRoots{}, err
	}
	for _, table := range tables {
		tablePath := scopedRoot + "/" + table
		children, err := keeperChildren(ctx, conn, tablePath)
		if err != nil {
			return KeeperRoots{}, err
		}
		if slices.Contains(children, "replicas") {
			out.NonIncarnation = append(out.NonIncarnation, tablePath)
		}
		for _, child := range children {
			if seq, ok := canonicalIncarnationSeq(child); ok {
				out.Scoped = append(out.Scoped, ScopedKeeperPath{Table: table, IncarnationSeq: seq})
				continue
			}
			out.NonIncarnation = append(out.NonIncarnation, tablePath+"/"+child)
		}
	}
	sort.Slice(out.Scoped, func(i, j int) bool {
		if out.Scoped[i].Table != out.Scoped[j].Table {
			return out.Scoped[i].Table < out.Scoped[j].Table
		}
		return out.Scoped[i].IncarnationSeq < out.Scoped[j].IncarnationSeq
	})
	sort.Strings(out.NonIncarnation)
	return out, nil
}

// canonicalIncarnationSeq parses an incarnation path's last element: a
// decimal >= 1 without sign or leading zero ("007", "+5" and "0" are not one),
// the rule ParseIncarnationComment applies to markers.
func canonicalIncarnationSeq(name string) (uint64, bool) {
	seq, err := strconv.ParseUint(name, 10, 64)
	if err != nil || seq == 0 || strconv.FormatUint(seq, 10) != name {
		return 0, false
	}
	return seq, true
}
