package ddl

import (
	"context"
	"errors"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

type recordingExecConn struct {
	clickhouse.Conn
	execs []string
}

func (c *recordingExecConn) Exec(_ context.Context, query string, _ ...any) error {
	c.execs = append(c.execs, query)
	if strings.HasPrefix(query, "CREATE TABLE") {
		return errors.New("stop after recording unexpected table DDL")
	}
	return nil
}

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

const goldenPromoteDDL = "CREATE TABLE IF NOT EXISTS `hg_promote`.`db__t` (\n" +
	"    `_hg_row_id` FixedString(32),\n" +
	"    `p` String,\n" +
	"    `v` UInt64\n" +
	") ENGINE = MergeTree\n" +
	"PARTITION BY `p`\n" +
	"ORDER BY (`p`, `_hg_row_id`)\n" +
	"SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0"

func TestBuildDDL_GoldenStringPartitionedTable(t *testing.T) {
	unsafe, safe, promote, err := BuildDDL(goldenPinned(), goldenSchema())
	if err != nil {
		t.Fatalf("BuildDDL: %v", err)
	}
	if unsafe != goldenUnsafeDDL {
		t.Fatalf("unsafe DDL:\n got: %s\nwant: %s", unsafe, goldenUnsafeDDL)
	}
	if safe != goldenSafeDDL {
		t.Fatalf("safe DDL:\n got: %s\nwant: %s", safe, goldenSafeDDL)
	}
	if promote != goldenPromoteDDL {
		t.Fatalf("promote DDL:\n got: %s\nwant: %s", promote, goldenPromoteDDL)
	}
}

func TestBuildDDL_UnpartitionedTableOrdersByRowIDOnly(t *testing.T) {
	sch := payloadexec.TableSchema{TableID: "db.u", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	unsafe, safe, _, err := BuildDDL(goldenPinned(), sch)
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
	unsafe, _, _, err := BuildDDL(p, goldenSchema())
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
			_, _, _, err := BuildDDL(goldenPinned(), sch)
			if !errors.Is(err, ErrPartitionFreeze) {
				t.Fatalf("err = %v, want ErrPartitionFreeze", err)
			}
		})
	}
}

func TestBuildDDL_RejectsRowIDColumnInDeclaredSchema(t *testing.T) {
	sch := payloadexec.TableSchema{TableID: "db.t", Columns: []lthash.Column{{Name: "_hg_row_id", Type: "FixedString(32)"}}}
	if _, _, _, err := BuildDDL(goldenPinned(), sch); err == nil {
		t.Fatal("declared _hg_row_id must be rejected")
	}
}

func TestIntents_MatchRenderedDDLShape(t *testing.T) {
	unsafe, safe, promote, err := Intents(goldenPinned(), goldenSchema())
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
	if promote.Engine != "MergeTree" || promote.Database != "hg_promote" || promote.ZooKeeperPath != "" || len(promote.Settings) != 1 {
		t.Fatalf("promote intent: %+v", promote)
	}
	if unsafe.SQL() != goldenUnsafeDDL || safe.SQL() != goldenSafeDDL || promote.SQL() != goldenPromoteDDL {
		t.Fatal("TableIntent.SQL must equal BuildDDL output")
	}
}

func TestIntents_RejectsColumnTypeOutsideTheSIWhitelist(t *testing.T) {
	for name, schema := range map[string]payloadexec.TableSchema{
		// Nullable and Array stay out: Spec Q defers them to Phase 2, which also
		// bumps ExecutorProfileID. Date32 is deferred with them — the Native
		// decoder has no *proto.ColDate32 case, measured in Spec Q M-Date32.
		"nullable": {TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "Nullable(UInt64)"}}},
		"array":    {TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "Array(String)"}}},
		"date32":   {TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "Date32"}}},
		// Spec Q Q-D7 narrowed FixedString to the one width the Native lane
		// actually decodes. Widths ClickHouse and ch-go both accept are still
		// refused here, deliberately.
		"fixedstring_wrong_width": {TableID: "db.t", Columns: []lthash.Column{{Name: "v", Type: "FixedString(16)"}}},
		"ddl_injection": {TableID: "db.t", Columns: []lthash.Column{
			{Name: "v", Type: "String, injected UInt64"},
		}},
		"closes_column_list": {TableID: "db.t", Columns: []lthash.Column{
			{Name: "v", Type: "String) ENGINE = MergeTree ORDER BY tuple() --"},
		}},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, _, err := Intents(goldenPinned(), schema)
			if !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("Intents error = %v, want ErrUnsupportedColumnType", err)
			}
			if !contains(err.Error(), "table db.t: invalid DDL column declaration") {
				t.Fatalf("Intents error lacks table/DDL context: %v", err)
			}
			if _, _, _, err := BuildDDL(goldenPinned(), schema); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("BuildDDL error = %v, want ErrUnsupportedColumnType", err)
			}
		})
	}
}

// The partition freeze is checked before column types so an expression
// partition key keeps reporting ErrPartitionFreeze, which callers (ensure)
// treat as skip-with-warning rather than as a hard failure.
func TestIntents_PartitionFreezeOutranksColumnTypeRejection(t *testing.T) {
	schema := payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "toYYYYMM(d)",
		Columns:     []lthash.Column{{Name: "d", Type: "Date"}},
	}
	_, _, _, err := Intents(goldenPinned(), schema)
	if !errors.Is(err, ErrPartitionFreeze) {
		t.Fatalf("Intents error = %v, want ErrPartitionFreeze", err)
	}
}

func TestIntents_AcceptsEveryWhitelistedType(t *testing.T) {
	schema := payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"}, {Name: "f", Type: "FixedString(32)"},
			{Name: "b", Type: "Bool"}, {Name: "f32", Type: "Float32"}, {Name: "f64", Type: "Float64"},
			{Name: "u8", Type: "UInt8"}, {Name: "u16", Type: "UInt16"}, {Name: "u32", Type: "UInt32"}, {Name: "u64", Type: "UInt64"},
			{Name: "i8", Type: "Int8"}, {Name: "i16", Type: "Int16"}, {Name: "i32", Type: "Int32"}, {Name: "i64", Type: "Int64"},
			// Temporal families, restored by Spec Q Phase 1. They were always
			// supported by the Native decoder, the canonical row encoder and the
			// ClickHouse executor; only the declaration validator refused them.
			{Name: "d", Type: "Date"}, {Name: "dt", Type: "DateTime"}, {Name: "dttz", Type: "DateTime('UTC')"},
			{Name: "dt64", Type: "DateTime64(3)"}, {Name: "dt64tz", Type: "DateTime64(3, 'UTC')"},
		},
	}
	if _, _, _, err := Intents(goldenPinned(), schema); err != nil {
		t.Fatalf("Intents rejected the full whitelist: %v", err)
	}
}

func TestIntents_CanonicalizesAcceptedFixedStringSpellings(t *testing.T) {
	for name, typeName := range map[string]string{
		"leading_plus":   "FixedString(+32)",
		"leading_zeroes": "FixedString(0032)",
		"whitespace":     "FixedString(\t +0032 \n)",
	} {
		t.Run(name, func(t *testing.T) {
			schema := payloadexec.TableSchema{
				TableID: "db.t",
				Columns: []lthash.Column{{Name: "v", Type: typeName}},
			}
			unsafe, safe, promote, err := Intents(goldenPinned(), schema)
			if err != nil {
				t.Fatalf("Intents rejected accepted spelling %q: %v", typeName, err)
			}
			if schema.Columns[0].Type != typeName {
				t.Fatalf("Intents mutated caller schema type to %q, want original %q", schema.Columns[0].Type, typeName)
			}
			for intentName, intent := range map[string]TableIntent{
				"unsafe": unsafe, "safe": safe, "promote": promote,
			} {
				if got := intent.Columns[1].Type; got != "FixedString(32)" {
					t.Fatalf("%s intent column type = %q, want FixedString(32)", intentName, got)
				}
			}
			unsafeDDL, safeDDL, promoteDDL, err := BuildDDL(goldenPinned(), schema)
			if err != nil {
				t.Fatalf("BuildDDL rejected accepted spelling %q: %v", typeName, err)
			}
			for ddlName, ddl := range map[string]string{
				"unsafe": unsafeDDL, "safe": safeDDL, "promote": promoteDDL,
			} {
				if !strings.Contains(ddl, "`v` FixedString(32)") {
					t.Fatalf("%s DDL did not render canonical FixedString(32):\n%s", ddlName, ddl)
				}
			}
		})
	}
}

func TestEnsureProtocolTables_ValidatesAllSchemasBeforeAnyDDL(t *testing.T) {
	bad := payloadexec.TableSchema{
		TableID: "db.bad",
		Columns: []lthash.Column{{Name: "v", Type: "Nullable(UInt64)"}},
	}
	good := goldenSchema()
	good.TableID = "db.good"

	for name, schemas := range map[string][]payloadexec.TableSchema{
		"unsupported_then_valid": {bad, good},
		"valid_then_unsupported": {good, bad},
	} {
		t.Run(name, func(t *testing.T) {
			conn := &recordingExecConn{}
			err := EnsureProtocolTables(
				context.Background(),
				conn,
				goldenPinned(),
				schemas,
				ModeCreateAndVerify,
				nil,
			)
			if !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("EnsureProtocolTables error = %v, want ErrUnsupportedColumnType", err)
			}
			if len(conn.execs) != 0 {
				t.Fatalf("Exec called before all schemas validated: %q", conn.execs)
			}
		})
	}
}

func TestModeFromSchemaSource(t *testing.T) {
	for source, want := range map[SchemaSource]Mode{
		SchemaSourceNetworkState: ModeCreateAndVerify,
		SchemaSourceChain:        ModeCreateAndVerify,
		SchemaSourceClickHouse:   ModeVerifyOnly,
		SchemaSourceUnmanaged:    ModeOff,
	} {
		got, err := ModeFromSchemaSource(source)
		if err != nil || got != want {
			t.Fatalf("ModeFromSchemaSource(%q) = %v, %v; want %v, nil", source, got, err, want)
		}
	}
	for _, bad := range []SchemaSource{"", "CREATE", "network-state", "off"} {
		if _, err := ModeFromSchemaSource(bad); err == nil {
			t.Fatalf("ModeFromSchemaSource(%q) accepted an unknown schema source", bad)
		}
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

func TestQuoteIdent_EscapesClickHouseCStyleSequences(t *testing.T) {
	const input = "part\\n\\x41`tail"
	const want = "`part\\\\n\\\\x41``tail`"
	if got := quoteIdent(input); got != want {
		t.Fatalf("quoteIdent(%q) = %q, want %q", input, got, want)
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
