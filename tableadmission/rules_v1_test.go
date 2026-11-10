package tableadmission

import (
	"errors"
	"slices"
	"strings"
	"testing"
	"unicode"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

const rulesNet = "testnet-fsm"

func rulesHash(t *testing.T, tableID, partitionBy string, cols ...lthash.Column) string {
	t.Helper()
	return payloadexec.TableSchemaHash(rulesNet, payloadexec.TableSchema{TableID: tableID, PartitionBy: partitionBy, Columns: cols})
}

// TestAdmissionRulesV1Verdicts is the frozen verdict table of admission rules
// v1: one declaration per refusal code plus the admissible shapes. A change
// to any row changes how an already-committed AddTable replays, so it needs a
// new rules version, not an edit here.
func TestAdmissionRulesV1Verdicts(t *testing.T) {
	good := `{"table_id":"db.n","partition_by":"","columns":[{"name":"value","type":"Int64"}]}`
	goodHash := rulesHash(t, "db.n", "", lthash.Column{Name: "value", Type: "Int64"})
	for _, tc := range []struct {
		name, json, hash string
		want             RefusedCode // "" = admitted
	}{
		{"admitted", good, goodHash, ""},
		{"admitted without partition_by", `{"table_id":"db.n","columns":[{"name":"value","type":"Int64"}]}`, goodHash, ""},
		{"admitted with whitespace", "{ \"table_id\" : \"db.n\" ,\n \"columns\" : [ { \"type\" : \"Int64\" , \"name\" : \"value\" } ] }", goodHash, ""},
		{"admitted partitioned", `{"table_id":"db.n","partition_by":"p","columns":[{"name":"p","type":"String"},{"name":"ts","type":"DateTime64(6, 'UTC')"}]}`,
			rulesHash(t, "db.n", "p", lthash.Column{Name: "p", Type: "String"}, lthash.Column{Name: "ts", Type: "DateTime64(6, 'UTC')"}), ""},
		{"not json", `{`, goodHash, RefusedSchemaDecode},
		{"trailing data", good + `{}`, goodHash, RefusedSchemaDecode},
		{"top-level array", `[]`, goodHash, RefusedSchemaDecode},
		{"key case", `{"Table_ID":"db.n","columns":[{"name":"value","type":"Int64"}]}`, goodHash, RefusedSchemaDecode},
		{"unknown key", `{"table_id":"db.n","columns":[{"name":"value","type":"Int64"}],"engine":"x"}`, goodHash, RefusedSchemaDecode},
		{"duplicate key", `{"table_id":"db.n","table_id":"db.n","columns":[{"name":"value","type":"Int64"}]}`, goodHash, RefusedSchemaDecode},
		{"null table_id", `{"table_id":null,"columns":[{"name":"value","type":"Int64"}]}`, goodHash, RefusedSchemaDecode},
		{"null columns", `{"table_id":"db.n","columns":null}`, goodHash, RefusedNoColumns},
		{"column extra key", `{"table_id":"db.n","columns":[{"name":"value","type":"Int64","codec":"ZSTD"}]}`, goodHash, RefusedSchemaDecode},
		{"column key case", `{"table_id":"db.n","columns":[{"Name":"value","type":"Int64"}]}`, goodHash, RefusedSchemaDecode},
		{"invalid utf-8", "{\"table_id\":\"db.n\",\"columns\":[{\"name\":\"v\xff\",\"type\":\"Int64\"}]}", goodHash, RefusedSchemaDecode},
		{"table id mismatch", `{"table_id":"db.other","columns":[{"name":"value","type":"Int64"}]}`, goodHash, RefusedTableIDMismatch},
		{"no columns", `{"table_id":"db.n","columns":[]}`, goodHash, RefusedNoColumns},
		{"empty column name", `{"table_id":"db.n","columns":[{"name":" ","type":"Int64"}]}`, goodHash, RefusedColumnName},
		{"duplicate column name", `{"table_id":"db.n","columns":[{"name":"v","type":"Int64"},{"name":"v","type":"String"}]}`, goodHash, RefusedColumnName},
		{"reserved column", `{"table_id":"db.n","columns":[{"name":"_hg_row_id","type":"String"}]}`, goodHash, RefusedReservedColumn},
		{"unsupported type", `{"table_id":"db.n","columns":[{"name":"v","type":"Nullable(Int64)"}]}`, goodHash, RefusedColumnType},
		{"empty type", `{"table_id":"db.n","columns":[{"name":"v","type":""}]}`, goodHash, RefusedColumnType},
		{"non-UTC timezone", `{"table_id":"db.n","columns":[{"name":"v","type":"DateTime('Asia/Tokyo')"}]}`, goodHash, RefusedColumnType},
		{"non-canonical fixed string", `{"table_id":"db.n","columns":[{"name":"v","type":"FixedString( +032 )"}]}`, goodHash, RefusedNonCanonicalType},
		{"non-canonical datetime64", `{"table_id":"db.n","columns":[{"name":"v","type":"DateTime64(3,'UTC')"}]}`, goodHash, RefusedNonCanonicalType},
		{"partition expression", `{"table_id":"db.n","partition_by":"toYYYYMM(ts)","columns":[{"name":"ts","type":"DateTime"}]}`, goodHash, RefusedPartitionFreeze},
		{"partition non-string", `{"table_id":"db.n","partition_by":"v","columns":[{"name":"v","type":"Int64"}]}`, goodHash, RefusedPartitionFreeze},
		{"partition undeclared", `{"table_id":"db.n","partition_by":"p","columns":[{"name":"v","type":"Int64"}]}`, goodHash, RefusedPartitionFreeze},
		{"hash mismatch", good, "0xdeadbeef", RefusedSchemaHashMismatch},
		// Cross-column check order is frozen: names (empty/duplicate) across
		// every column, then the reserved row id column across every column,
		// then types across every column. Each row below carries two defects
		// that fall in different phases so the order, not just each check in
		// isolation, is pinned.
		{"column name checked before reserved column, across columns", `{"table_id":"db.n","columns":[{"name":"_hg_row_id","type":"String"},{"name":"","type":"Int64"}]}`,
			goodHash, RefusedColumnName},
		{"reserved column checked before types, across columns", `{"table_id":"db.n","columns":[{"name":"v","type":"Nullable(Int64)"},{"name":"_hg_row_id","type":"String"}]}`,
			goodHash, RefusedReservedColumn},
		{"first-column type verdict wins across columns (non-canonical before unsupported)",
			`{"table_id":"db.n","columns":[{"name":"a","type":"FixedString(032)"},{"name":"b","type":"Nullable(Int64)"}]}`, goodHash, RefusedNonCanonicalType},
		// A JSON-escaped object key still decodes to the exact lowercase key
		// name the strict decoder requires: encoding/json's tokenizer resolves
		// \uXXXX escapes before the strict key-set check ever sees the key.
		{"escaped key is still exactly table_id", "{\"\\u0074able_id\":\"db.n\",\"columns\":[{\"name\":\"value\",\"type\":\"Int64\"}]}", goodHash, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := AdmitDeclaredSchemaV1(rulesNet, "db.n", tc.hash, tc.json)
			switch {
			case tc.want == "" && got != nil:
				t.Fatalf("refused %+v, want admitted", got)
			case tc.want != "" && (got == nil || got.Code != tc.want):
				t.Fatalf("verdict = %+v, want %s", got, tc.want)
			case got != nil && (got.Detail == "" || strings.Contains(got.Detail, "payloadexec") || strings.Contains(got.Detail, "json:")):
				t.Fatalf("detail %q must be a non-empty arbiter-authored sentence", got.Detail)
			}
		})
	}
}

// TestAdmissionRulesV1RefusalDetailsAreASCII pins that a refusal detail built
// from a chain-supplied string is always ASCII, byte-for-byte, regardless of
// which Unicode version the running Go toolchain's strconv.IsPrint tables
// come from. %q's escaping decision for a printable-but-non-ASCII rune (or a
// rune only recently assigned a category) depends on those tables — e.g. a
// rune Unicode 17 knows as printable and an older toolchain does not would
// print raw under %q on one binary and escaped on another, so two replicas on
// different toolchains could record different RefusedReason bytes for the
// same committed AddTable. Every %+q call site (strconv.QuoteToASCII
// semantics: escape every non-ASCII rune) must therefore produce the exact
// same detail independent of toolchain, which this test pins literally.
func TestAdmissionRulesV1RefusalDetailsAreASCII(t *testing.T) {
	// U+1C89 CYRILLIC CAPITAL LETTER TJE was assigned in Unicode 14 (2021),
	// well after many toolchains' bundled Unicode tables; it is printable.
	const recentCodepoint = "Ᲊ"
	dup := `{"table_id":"db.n","columns":[{"name":"` + recentCodepoint + `","type":"Int64"},{"name":"` + recentCodepoint + `","type":"String"}]}`
	got := AdmitDeclaredSchemaV1(rulesNet, "db.n", "", dup)
	if got == nil || got.Code != RefusedColumnName {
		t.Fatalf("verdict = %+v, want %s", got, RefusedColumnName)
	}
	if want := "column 1: name \"\\u1c89\" duplicates column 0"; got.Detail != want {
		t.Fatalf("detail = %q, want %q", got.Detail, want)
	}
	if !isASCII(got.Detail) {
		t.Fatalf("detail %q is not pure ASCII", got.Detail)
	}

	// A valid-UTF-8, non-ASCII but ordinary printable rune (no invalid bytes
	// involved) must be escaped the same way.
	nonASCIIType := `{"table_id":"db.n","columns":[{"name":"v","type":"café"}]}`
	got = AdmitDeclaredSchemaV1(rulesNet, "db.n", "", nonASCIIType)
	if got == nil || got.Code != RefusedColumnType {
		t.Fatalf("verdict = %+v, want %s", got, RefusedColumnType)
	}
	if want := "column \"v\": type \"caf\\u00e9\" is not admitted by admission rules v1"; got.Detail != want {
		t.Fatalf("detail = %q, want %q", got.Detail, want)
	}
	if !isASCII(got.Detail) {
		t.Fatalf("detail %q is not pure ASCII", got.Detail)
	}
}

func isASCII(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] > unicode.MaxASCII {
			return false
		}
	}
	return true
}

// housegateAdmittedVectorsAtPin is housegate's AdmittedColumnTypeVectors at
// the version this module pins. A housegate bump that widens or narrows its
// profile changes that list and fails here, so the change is a deliberate
// admission-rules decision (a new rules version), never a silent change of
// replay. At this pin it is admission rules v2's set (Spec Q Phase 2
// revision): v1's types, the plain additions, LowCardinality(String) and
// Nullable over every plain vector.
var housegateAdmittedVectorsAtPin = func() []string {
	plain := []string{
		"String", "FixedString(32)", "Bool", "Float32", "Float64",
		"UInt8", "UInt16", "UInt32", "UInt64", "Int8", "Int16", "Int32", "Int64",
		"Date", "DateTime", "Date32", "UUID", "Int128", "Int256", "UInt128", "UInt256",
		"LowCardinality(String)",
		"DateTime('UTC')", "DateTime64(3)", "DateTime64(3, 'UTC')",
	}
	out := append([]string(nil), plain...)
	for _, p := range plain {
		if p != "LowCardinality(String)" {
			out = append(out, "Nullable("+p+")")
		}
	}
	return out
}()

// rulesProbes is the fixed probe list the rules are classified against the
// housegate profile on. narrowedProbes are the ones the rules refuse on
// purpose although housegate may admit them: non-UTC timezones, whose
// housegate verdict depends on the host tz database.
var rulesProbes = []string{
	"String", "string", "String ", "FixedString(32)", "FixedString( +032 )", "FixedString(16)", "FixedString(-32)",
	"Bool", "Float32", "UInt64", "Int8", "Date", "Date32", "DateTime", "DateTime( 'UTC' )", "DateTime('UTC')",
	"DateTime64(0)", "DateTime64(09)", "DateTime64(10)", "DateTime64(3,'UTC')", "DateTime64( 6 , 'UTC' )",
	"Nullable(Int64)", "LowCardinality(String)", "Decimal(18, 4)", "UUID", "Array(String)", "",
	"Int128", "Int256", "UInt128", "UInt256", "Int512", "uuid",
	"Nullable(String)", "Nullable( UInt64 )", "Nullable(FixedString(032))", "Nullable(DateTime64(3,'UTC'))",
	"Nullable(Nullable(String))", "Nullable(LowCardinality(String))", "LowCardinality(Nullable(String))",
	"LowCardinality(UInt64)", "Nullable(Decimal(18, 4))", "Nullable(Array(String))", "Nullable()",
	"DateTime('Asia/Tokyo')", "DateTime('Etc/UTC')", "DateTime64(3, 'Europe/Berlin')", "Nullable(DateTime('Asia/Tokyo'))",
}

var narrowedProbes = map[string]bool{
	"DateTime('Asia/Tokyo')": true, "DateTime('Etc/UTC')": true, "DateTime64(3, 'Europe/Berlin')": true,
	"Nullable(DateTime('Asia/Tokyo'))": true,
}

// TestAdmissionRulesV1MatchHousegateProfile pins the frozen v1 type rules
// against the housegate profile compiled into this binary. Every v1-admitted
// spelling must be one housegate resolves to itself (so every role can create
// and replay the table). On the probe list, v1 agrees with housegate wherever
// it admits; where v1 refuses and housegate admits, the probe is one v2 adds
// (or a narrowed timezone), never a type no rules version admits.
func TestAdmissionRulesV1MatchHousegateProfile(t *testing.T) {
	if got := payloadexec.AdmittedColumnTypeVectors(); !slices.Equal(got, housegateAdmittedVectorsAtPin) {
		t.Fatalf("housegate profile changed: AdmittedColumnTypeVectors() = %q; decide a new admission rules version before bumping", got)
	}
	for _, typ := range admittedColumnTypesV1 {
		profile, err := payloadexec.ResolveColumnProfile(typ)
		if err != nil || profile.Canonical != typ {
			t.Fatalf("v1 admits %q but housegate resolves it to %+v (%v)", typ, profile, err)
		}
	}
	for _, probe := range rulesProbes {
		canonical, v1ok := canonicalColumnTypeV1(probe)
		profile, err := payloadexec.ResolveColumnProfile(probe)
		hgok := err == nil
		if narrowedProbes[probe] {
			if v1ok {
				t.Fatalf("%q: v1 must refuse non-UTC timezones", probe)
			}
			continue
		}
		if v1ok && (!hgok || canonical != profile.Canonical) {
			t.Fatalf("%q: v1 (%q) admits what housegate resolves to %q (%v)", probe, canonical, profile.Canonical, err)
		}
		if !v1ok && hgok {
			if _, v2ok := canonicalColumnTypeV2(probe); !v2ok {
				t.Fatalf("%q: housegate admits it, but neither v1 nor v2 does", probe)
			}
		}
	}
	if admissionRowIDColumnV1 != ddl.RowIDColumn {
		t.Fatalf("row id column drifted: v1 %q, ddl %q", admissionRowIDColumnV1, ddl.RowIDColumn)
	}
	// The partition freeze must agree with the rule the data-plane roles
	// enforce before creating a table.
	for _, schema := range []payloadexec.TableSchema{
		{TableID: "db.n", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}}},
		{TableID: "db.n", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "UInt64"}}},
		{TableID: "db.n", PartitionBy: "toDate(p)", Columns: []lthash.Column{{Name: "p", Type: "String"}}},
		{TableID: "db.n", PartitionBy: "q", Columns: []lthash.Column{{Name: "p", Type: "String"}}},
	} {
		v1 := partitionByAdmittedV1(declaredSchemaV1{TableID: schema.TableID, PartitionBy: schema.PartitionBy, Columns: schema.Columns})
		if hg := ddl.ValidatePartitionFreeze(schema); v1 != (hg == nil) || (hg != nil && !errors.Is(hg, ddl.ErrPartitionFreeze)) {
			t.Fatalf("partition freeze disagrees for %+v: v1 %v, ddl %v", schema, v1, hg)
		}
	}
	// The hash and the physical name are protocol identity, delegated rather
	// than copied; pin one vector each so a bump that moves them fails here.
	if got := rulesHash(t, "db.n", "", lthash.Column{Name: "value", Type: "Int64"}); got != goldenV1SchemaHash {
		t.Fatalf("TableSchemaHash vector = %s, want %s", got, goldenV1SchemaHash)
	}
	if got := ddl.CHTableName("db.n"); got != "db__n" {
		t.Fatalf("CHTableName(db.n) = %s", got)
	}
}

// goldenV1SchemaHash is TableSchemaHash("testnet-fsm", {db.n, "", [value Int64]}).
const goldenV1SchemaHash = "0xc13cab0bfa5a5e6d5f1fbeaa844dc33604984a8c187c35934ebcc50a301f11de"

// TestDottedIDRefusalV1 pins the first rule's verdict and its detail bytes,
// which the registry records verbatim.
func TestDottedIDRefusalV1(t *testing.T) {
	if got := DottedIDRefusalV1("db", "t"); got != nil {
		t.Fatalf("undotted ids refused: %+v", got)
	}
	for _, tc := range []struct{ db, table, detail string }{
		{"a.b", "c", `database id "a.b" or table id "c" contains '.'`},
		{"a", "b.c", `database id "a" or table id "b.c" contains '.'`},
	} {
		got := DottedIDRefusalV1(tc.db, tc.table)
		if got == nil || got.Code != RefusedDottedID || got.Detail != tc.detail {
			t.Fatalf("DottedIDRefusalV1(%q, %q) = %+v, want %s %q", tc.db, tc.table, got, RefusedDottedID, tc.detail)
		}
	}
}

// TestRefusedCodesAreFrozen pins every code's replicated spelling.
func TestRefusedCodesAreFrozen(t *testing.T) {
	want := []RefusedCode{
		"dotted_id", "previous_incarnation_not_purged", "schema_decode", "table_id_mismatch",
		"no_columns", "column_name", "reserved_column", "column_type", "non_canonical_type",
		"partition_freeze", "schema_hash_mismatch", "physical_name_collision",
	}
	if got := RefusedCodes(); !slices.Equal(got, want) {
		t.Fatalf("RefusedCodes() = %q, want %q", got, want)
	}
	for _, c := range want {
		if !c.Known() {
			t.Fatalf("%q is not Known", c)
		}
	}
	if RefusedCode("duplicate_table").Known() || RefusedCode("").Known() {
		t.Fatal("an unlisted code is Known")
	}
}

// TestAdmittedColumnTypesV1ReturnsACopy pins that callers cannot widen the
// frozen set through the returned slice.
func TestAdmittedColumnTypesV1ReturnsACopy(t *testing.T) {
	got := AdmittedColumnTypesV1()
	if !slices.Equal(got, admittedColumnTypesV1) {
		t.Fatalf("AdmittedColumnTypesV1() = %q", got)
	}
	got[0] = "Nullable(String)"
	if admittedColumnTypesV1[0] != "String" {
		t.Fatal("AdmittedColumnTypesV1 exposed the frozen set")
	}
}
