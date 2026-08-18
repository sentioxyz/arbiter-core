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

// ZooKeeperPath is the anchored replication path of an hg_unsafe table:
// /sentio/<keeper_shard_id>/unsafe/<CHTableName>.
func ZooKeeperPath(p Pinned, tableID string) string {
	return fmt.Sprintf("/sentio/%d/unsafe/%s", p.KeeperShardID, CHTableName(tableID))
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
