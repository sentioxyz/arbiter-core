// Package ddl renders and verifies the protocol-owned physical tables of the
// storage-integrity data plane (hg_unsafe / hg_safe / hg_promote). It is the
// single source of the D2 naming freeze and the D3 pinned engine settings.
package ddl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrPhysicalTableNameCollision reports a schema set whose frozen D2 mapping
// is not injective. Such a set would send distinct logical tables to the same
// hg_unsafe/hg_safe tables and the same Keeper replication path.
var ErrPhysicalTableNameCollision = errors.New("ddl: physical table name collision")

// CHTableName maps a logical storage-integrity table id (<database>.<table>)
// to its physical ClickHouse table name (D2 naming freeze). snode.CHTableName
// delegates here; do not copy the rule.
func CHTableName(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}

// ValidatePhysicalTableNames verifies the frozen D2 mapping across the whole
// authoritative schema set. Validation must happen before any role starts or
// any DDL is issued: per-table Intents cannot detect a collision with a sibling
// declaration after one of them has already created shared physical state.
func ValidatePhysicalTableNames(tables []payloadexec.TableSchema) error {
	owners := make(map[string]string, len(tables))
	for _, table := range tables {
		physical := CHTableName(table.TableID)
		if previous, ok := owners[physical]; ok {
			return fmt.Errorf("%w: logical table ids %q and %q both map to %q", ErrPhysicalTableNameCollision, previous, table.TableID, physical)
		}
		owners[physical] = table.TableID
	}
	return nil
}

// The two Keeper roots of the SI unsafe replicas (housegate spec 2026-10-14
// §8.8). An incarnation recorded before the stage-3 purge switch replicates
// under the legacy root, one recorded after it under the incarnation-scoped
// root. The roots are siblings under /sentio/<shard>, so no incarnation path
// is ever a descendant of a legacy table path, whose last replica's drop
// removes every child of it.
const (
	KeeperUnsafeRoot       = "unsafe"     // /sentio/<shard>/unsafe/<db__table>
	KeeperScopedUnsafeRoot = "unsafe_inc" // /sentio/<shard>/unsafe_inc/<db__table>/<incarnation_seq>
)

// KeeperScope selects one incarnation's Keeper path; the zero value is the legacy unscoped path.
type KeeperScope struct {
	// IncarnationSeq is the registry incarnation. It marks the tables'
	// COMMENT (IncarnationComment; 0 for a genesis table, which carries
	// none) and, when Scoped, names the incarnation path.
	IncarnationSeq uint64
	// Scoped selects /sentio/<shard>/unsafe_inc/<db__table>/<IncarnationSeq>,
	// the path of an incarnation the registry marks scoped_keeper_path.
	Scoped bool
}

// validate refuses a scoped path without an incarnation: seq 0 is the
// unmarked genesis incarnation, which never uses an incarnation path.
func (s KeeperScope) validate() error {
	if s.Scoped && s.IncarnationSeq == 0 {
		return errors.New("ddl: an incarnation-scoped Keeper path needs an incarnation seq of at least 1")
	}
	return nil
}

// ZooKeeperPathFor is the replication path of tableID's hg_unsafe (or
// hg_member_unsafe) table in scope s: /sentio/<shard>/unsafe/<CHTableName>
// unscoped, /sentio/<shard>/unsafe_inc/<CHTableName>/<IncarnationSeq> scoped.
// Every function that acts on a path refuses a scoped s with seq 0.
func ZooKeeperPathFor(keeperShardID uint32, tableID string, s KeeperScope) string {
	if s.Scoped {
		return fmt.Sprintf("/sentio/%d/%s/%s/%d", keeperShardID, KeeperScopedUnsafeRoot, CHTableName(tableID), s.IncarnationSeq)
	}
	return fmt.Sprintf("/sentio/%d/%s/%s", keeperShardID, KeeperUnsafeRoot, CHTableName(tableID))
}

// keeperRootPath is /sentio/<shard>/<root>.
func keeperRootPath(keeperShardID uint32, root string) string {
	return fmt.Sprintf("/sentio/%d/%s", keeperShardID, root)
}

// ZooKeeperPath is the anchored replication path of an hg_unsafe table on the
// legacy root: /sentio/<keeper_shard_id>/unsafe/<CHTableName>
// (ZooKeeperPathFor with the zero KeeperScope).
func ZooKeeperPath(p Pinned, tableID string) string {
	return ZooKeeperPathFor(p.KeeperShardID, tableID, KeeperScope{})
}

func quoteIdent(id string) string {
	id = strings.ReplaceAll(id, `\`, `\\`)
	id = strings.ReplaceAll(id, "`", "``")
	return "`" + id + "`"
}

func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
