package ddl

import (
	"errors"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func goldenPinned() Pinned {
	return Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "node-1", KeeperShardID: 0}
}

func goldenSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
}

const goldenUnsafeDDL = "CREATE TABLE IF NOT EXISTS `hg_unsafe`.`db__t` (\n" +
	"    `_hg_row_id` FixedString(32),\n" +
	"    `p` String,\n" +
	"    `v` UInt64\n" +
	") ENGINE = ReplicatedMergeTree('/sentio/0/unsafe/db__t', 'node-1')\n" +
	"PARTITION BY `p`\n" +
	"ORDER BY (`p`, `_hg_row_id`)\n" +
	"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0"

const goldenSafeDDL = "CREATE TABLE IF NOT EXISTS `hg_safe`.`db__t` (\n" +
	"    `_hg_row_id` FixedString(32),\n" +
	"    `p` String,\n" +
	"    `v` UInt64\n" +
	") ENGINE = MergeTree\n" +
	"PARTITION BY `p`\n" +
	"ORDER BY (`p`, `_hg_row_id`)\n" +
	"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0"

func TestBuildDDL_GoldenStringPartitionedTable(t *testing.T) {
	unsafe, safe, err := BuildDDL(goldenPinned(), goldenSchema())
	if err != nil {
		t.Fatalf("BuildDDL: %v", err)
	}
	if unsafe != goldenUnsafeDDL {
		t.Fatalf("unsafe DDL:\n got: %s\nwant: %s", unsafe, goldenUnsafeDDL)
	}
	if safe != goldenSafeDDL {
		t.Fatalf("safe DDL:\n got: %s\nwant: %s", safe, goldenSafeDDL)
	}
}

func TestBuildDDL_UnpartitionedTableOrdersByRowIDOnly(t *testing.T) {
	sch := payloadexec.TableSchema{TableID: "db.u", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	unsafe, safe, err := BuildDDL(goldenPinned(), sch)
	if err != nil {
		t.Fatalf("BuildDDL: %v", err)
	}
	wantUnsafe := "CREATE TABLE IF NOT EXISTS `hg_unsafe`.`db__u` (\n" +
		"    `_hg_row_id` FixedString(32),\n" +
		"    `v` UInt64\n" +
		") ENGINE = ReplicatedMergeTree('/sentio/0/unsafe/db__u', 'node-1')\n" +
		"ORDER BY (`_hg_row_id`)\n" +
		"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0"
	if unsafe != wantUnsafe {
		t.Fatalf("unsafe DDL:\n got: %s\nwant: %s", unsafe, wantUnsafe)
	}
	wantSafe := "CREATE TABLE IF NOT EXISTS `hg_safe`.`db__u` (\n" +
		"    `_hg_row_id` FixedString(32),\n" +
		"    `v` UInt64\n" +
		") ENGINE = MergeTree\n" +
		"ORDER BY (`_hg_row_id`)\n" +
		"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0"
	if safe != wantSafe {
		t.Fatalf("safe DDL:\n got: %s\nwant: %s", safe, wantSafe)
	}
}

func TestBuildDDL_KeeperShardAndNodeIDLandInZKPath(t *testing.T) {
	p := goldenPinned()
	p.KeeperShardID = 3
	p.NodeID = "verifier-a'b"
	unsafe, _, err := BuildDDL(p, goldenSchema())
	if err != nil {
		t.Fatalf("BuildDDL: %v", err)
	}
	if want := "ENGINE = ReplicatedMergeTree('/sentio/3/unsafe/db__t', 'verifier-a\\'b')"; !contains(unsafe, want) {
		t.Fatalf("unsafe DDL missing %q:\n%s", want, unsafe)
	}
	if got := ZooKeeperPath(p, "db.t"); got != "/sentio/3/unsafe/db__t" {
		t.Fatalf("ZooKeeperPath = %q", got)
	}
}

func TestBuildDDL_RejectsPartitionFreezeViolations(t *testing.T) {
	cases := map[string]payloadexec.TableSchema{
		"expression": {TableID: "db.t", PartitionBy: "toYYYYMM(d)", Columns: []lthash.Column{{Name: "d", Type: "Date"}}},
		"non-string": {TableID: "db.t", PartitionBy: "n", Columns: []lthash.Column{{Name: "n", Type: "UInt64"}}},
		"undeclared": {TableID: "db.t", PartitionBy: "x", Columns: []lthash.Column{{Name: "p", Type: "String"}}},
	}
	for name, sch := range cases {
		t.Run(name, func(t *testing.T) {
			_, _, err := BuildDDL(goldenPinned(), sch)
			if !errors.Is(err, ErrPartitionFreeze) {
				t.Fatalf("err = %v, want ErrPartitionFreeze", err)
			}
		})
	}
}

func TestBuildDDL_RejectsRowIDColumnInDeclaredSchema(t *testing.T) {
	sch := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "_hg_row_id", Type: "FixedString(32)"}}}
	if _, _, err := BuildDDL(goldenPinned(), sch); err == nil {
		t.Fatal("declared _hg_row_id must be rejected")
	}
}

func TestIntents_MatchRenderedDDLShape(t *testing.T) {
	unsafe, safe, err := Intents(goldenPinned(), goldenSchema())
	if err != nil {
		t.Fatalf("Intents: %v", err)
	}
	if unsafe.Engine != "ReplicatedMergeTree" || unsafe.ZooKeeperPath != "/sentio/0/unsafe/db__t" || unsafe.ReplicaName != "node-1" {
		t.Fatalf("unsafe intent: %+v", unsafe)
	}
	if safe.Engine != "MergeTree" || safe.ZooKeeperPath != "" || len(safe.Settings) != 1 {
		t.Fatalf("safe intent: %+v", safe)
	}
	if unsafe.PartitionKey != "p" || len(unsafe.SortingKey) != 2 || unsafe.SortingKey[1] != "_hg_row_id" {
		t.Fatalf("unsafe keys: %+v", unsafe)
	}
	if unsafe.Columns[0].Name != "_hg_row_id" || unsafe.Columns[0].Type != "FixedString(32)" || len(unsafe.Columns) != 3 {
		t.Fatalf("unsafe columns: %+v", unsafe.Columns)
	}
	if unsafe.SQL() != goldenUnsafeDDL || safe.SQL() != goldenSafeDDL {
		t.Fatal("TableIntent.SQL must equal BuildDDL output")
	}
}

func TestCHTableName(t *testing.T) {
	if got := CHTableName("db.t"); got != "db__t" {
		t.Fatalf("CHTableName = %q", got)
	}
	if got := CHTableName("a.b.c"); got != "a__b__c" {
		t.Fatalf("CHTableName = %q", got)
	}
}

func contains(s, sub string) bool {
	return len(sub) == 0 || (len(s) >= len(sub) && indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
