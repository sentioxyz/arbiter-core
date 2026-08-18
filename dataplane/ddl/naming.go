// Package ddl renders and verifies the protocol-owned physical tables of the
// storage-integrity data plane (hg_unsafe / hg_safe / hg_promote). It is the
// single source of the D2 naming freeze and the D3 pinned engine settings.
package ddl

import (
	"fmt"
	"strings"
)

// CHTableName maps a logical storage-integrity table id (<database>.<table>)
// to its physical ClickHouse table name (D2 naming freeze). snode.CHTableName
// delegates here; do not copy the rule.
func CHTableName(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}

// ZooKeeperPath is the anchored replication path of an hg_unsafe table:
// /sentio/<keeper_shard_id>/unsafe/<CHTableName>.
func ZooKeeperPath(p Pinned, tableID string) string {
	return fmt.Sprintf("/sentio/%d/unsafe/%s", p.KeeperShardID, CHTableName(tableID))
}

func quoteIdent(id string) string {
	return "`" + strings.ReplaceAll(id, "`", "``") + "`"
}

func quoteLiteral(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `'`, `\'`)
	return "'" + s + "'"
}
