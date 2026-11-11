package tableadmission

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func partitionedSchemaJSON(t *testing.T, partitionBy string, cols ...lthash.Column) (string, string) {
	t.Helper()
	type col struct {
		Name string `json:"name"`
		Type string `json:"type"`
	}
	doc := struct {
		TableID     string `json:"table_id"`
		PartitionBy string `json:"partition_by,omitempty"`
		Columns     []col  `json:"columns"`
	}{TableID: "db.n", PartitionBy: partitionBy}
	for _, c := range cols {
		doc.Columns = append(doc.Columns, col{c.Name, c.Type})
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw), rulesHash(t, "db.n", partitionBy, cols...)
}

// TestAdmissionRulesV3Verdicts is the frozen verdict table of admission rules
// v3. A change to any row changes how an already-committed AddTable replays
// under v3, so it needs a new rules version, not an edit here.
func TestAdmissionRulesV3Verdicts(t *testing.T) {
	id := lthash.Column{Name: "id", Type: "UInt64"}
	for _, tc := range []struct {
		key, column, typ string
		admitted         bool
	}{
		{"", "c", "String", true},
		{"c", "c", "String", true},
		{"select", "select", "String", true}, // the v1 freeze: any declared name
		{"c", "c", "UInt8", true}, {"c", "c", "Int64", true}, {"c", "c", "Date", true}, {"c", "c", "Date32", true},
		{"c", "c", "DateTime('UTC')", true}, {"c", "c", "DateTime64(0, 'UTC')", true}, {"c", "c", "DateTime64(9, 'UTC')", true},
		{"toYYYYMM(ts)", "ts", "DateTime('UTC')", true}, {"toYYYYMM(ts)", "ts", "DateTime64(3, 'UTC')", true},
		{"toYYYYMM(d)", "d", "Date32", true}, {"toYYYYMMDD(d)", "d", "Date", true},
		{"toDate(ts)", "ts", "DateTime('UTC')", true}, {"toStartOfMonth(d)", "d", "Date", true},
		{"toMonday(ts)", "ts", "DateTime('UTC')", true}, {"toStartOfDay(ts)", "ts", "DateTime('UTC')", true},
		{"intDiv(n, 2)", "n", "UInt32", true}, {"intDiv(n, 1000)", "n", "Int64", true},
		{"intDiv(n, 4294967295)", "n", "UInt32", true}, {"intDiv(n, 18446744073709551615)", "n", "UInt64", true},
		{"intDiv(n, 2147483647)", "n", "Int32", true}, {"intDiv(n, 9223372036854775807)", "n", "Int64", true},
		{"toYYYYMM(date)", "date", "Date", true}, {"intDiv(key, 1000)", "key", "UInt64", true},

		{"c", "c", "Bool", false}, {"c", "c", "Float64", false}, {"c", "c", "UUID", false}, {"c", "c", "FixedString(32)", false},
		{"c", "c", "Int128", false}, {"c", "c", "LowCardinality(String)", false}, {"c", "c", "Nullable(String)", false},
		{"c", "c", "Nullable(UInt64)", false}, {"c", "c", "DateTime", false}, {"c", "c", "DateTime64(3)", false},
		{"toDate(ts)", "ts", "DateTime", false}, {"toYYYYMM(ts)", "ts", "DateTime64(3)", false},
		{"toStartOfDay(d)", "d", "Date", false}, {"toDate(d)", "d", "Date32", false}, {"toMonday(ts)", "ts", "DateTime64(3, 'UTC')", false},
		{"toStartOfDay(ts)", "ts", "DateTime64(0, 'UTC')", false}, {"toYYYYMM(n)", "n", "UInt32", false},
		{"intDiv(n, 1000)", "n", "UInt16", false}, {"intDiv(n, 1)", "n", "UInt64", false}, {"intDiv(n, 0)", "n", "UInt64", false},
		{"intDiv(n, 4294967296)", "n", "UInt32", false}, {"intDiv(n, 2147483648)", "n", "Int32", false},
		{"intDiv(n, 9223372036854775808)", "n", "Int64", false}, {"intDiv(n, 01000)", "n", "UInt64", false},
		{"intDiv(n, 1000.)", "n", "UInt64", false}, {"intDiv(n,1000)", "n", "UInt64", false}, {"intDiv(n, -1000)", "n", "Int64", false},
		{"(toYYYYMM(ts))", "ts", "DateTime('UTC')", false}, {"(ts)", "ts", "DateTime('UTC')", false},
		{"toyyyymm(ts)", "ts", "DateTime('UTC')", false}, {"toYYYYMM( ts )", "ts", "DateTime('UTC')", false},
		{"toDate(ts, 'UTC')", "ts", "DateTime('UTC')", false}, {"toStartOfWeek(ts, 1)", "ts", "DateTime('UTC')", false},
		{"n % 16", "n", "UInt64", false}, {"modulo(n, 16)", "n", "UInt64", false}, {"tuple(c)", "c", "String", false},
		{"toYYYYMM(select)", "select", "DateTime('UTC')", false}, {"toYYYYMM(Values)", "Values", "DateTime('UTC')", false},
		{"from", "from", "UInt64", false}, {"`n`", "n", "UInt64", false}, {"toYYYYMM(`ts`)", "ts", "DateTime('UTC')", false},
		{"missing", "c", "UInt64", false}, {"toYYYYMM(missing)", "ts", "DateTime('UTC')", false},
	} {
		js, h := partitionedSchemaJSON(t, tc.key, id, lthash.Column{Name: tc.column, Type: tc.typ})
		if tc.column == "id" {
			js, h = partitionedSchemaJSON(t, tc.key, id)
		}
		got := AdmitDeclaredSchemaV3(rulesNet, "db.n", h, js)
		if tc.admitted && got != nil {
			t.Errorf("%q over %s: refused %+v, want admitted", tc.key, tc.typ, got)
		}
		if !tc.admitted {
			if got == nil || got.Code != RefusedPartitionFreeze {
				t.Errorf("%q over %s: verdict %+v, want partition_freeze", tc.key, tc.typ, got)
			} else if !strings.Contains(got.Detail, "admission rules v3") {
				t.Errorf("%q: detail %q does not name rules v3", tc.key, got.Detail)
			}
		}
	}
}

// TestAdmissionRulesV3WidensV2PartitionsOnly: on every schema v2 judges, v3
// gives v2's verdict byte for byte except a partition_freeze refusal that v3
// admits.
func TestAdmissionRulesV3WidensV2PartitionsOnly(t *testing.T) {
	cases := [][2]string{}
	for _, typ := range append(AdmittedColumnTypesV2(), "Decimal(18, 4)", "Nullable( Int64 )") {
		for _, key := range []string{"", "c", "toYYYYMM(c)", "intDiv(c, 1000)", "toDate(c)"} {
			js, h := partitionedSchemaJSON(t, key, lthash.Column{Name: "c", Type: typ})
			cases = append(cases, [2]string{js, h})
		}
	}
	cases = append(cases,
		[2]string{`{"table_id":"db.n","columns":[]}`, "0x00"},
		[2]string{`{"table_id":"db.other","columns":[{"name":"c","type":"String"}]}`, "0x00"},
		[2]string{`not json`, "0x00"},
		[2]string{`{"table_id":"db.n","columns":[{"name":"_hg_row_id","type":"String"}]}`, "0x00"},
		[2]string{`{"table_id":"db.n","columns":[{"name":"c","type":"String"}]}`, "0x00"},
	)
	for _, c := range cases {
		v2 := AdmitDeclaredSchemaV2(rulesNet, "db.n", c[1], c[0])
		v3 := AdmitDeclaredSchemaV3(rulesNet, "db.n", c[1], c[0])
		if v2 != nil && v2.Code == RefusedPartitionFreeze {
			if v3 != nil && v3.Code != RefusedPartitionFreeze {
				t.Errorf("%s: v2 %+v, v3 %+v", c[0], v2, v3)
			}
			continue
		}
		if fmt.Sprint(v2) != fmt.Sprint(v3) {
			t.Errorf("%s: v2 %+v, v3 %+v", c[0], v2, v3)
		}
	}
}

// TestAdmissionRulesV3MatchHousegate pins the frozen v3 partition rule to the
// partition-key parser of the housegate this binary is built with
// (payloadexec.ParsePartitionKey): on every candidate key over every v2 column
// type the two give the same verdict, so a v3 table is one every data-plane
// node can derive partitions for, and a node never derives partitions for a
// key v3 refuses.
func TestAdmissionRulesV3MatchHousegate(t *testing.T) {
	names := []string{"c", "ts", "_x", "x1", "date", "key", "true", "select", "Values", "NULL", "my col", "1a"}
	shapes := []string{
		"%s", "`%s`", "(%s)", "toYYYYMM(%s)", "toYYYYMMDD(%s)", "toDate(%s)", "toStartOfMonth(%s)", "toMonday(%s)",
		"toStartOfDay(%s)", "toStartOfWeek(%s)", "toyyyymm(%s)", "toYYYYMM( %s )", "(toYYYYMM(%s))", "toDate(%s, 'UTC')",
		"intDiv(%s, 0)", "intDiv(%s, 1)", "intDiv(%s, 2)", "intDiv(%s, 1000)", "intDiv(%s, 01000)", "intDiv(%s, 1000.)",
		"intDiv(%s, -1000)", "intDiv(%s,1000)", "intDiv(%s, 127)", "intDiv(%s, 65535)", "intDiv(%s, 2147483647)",
		"intDiv(%s, 2147483648)", "intDiv(%s, 4294967295)", "intDiv(%s, 4294967296)", "intDiv(%s, 9223372036854775807)",
		"intDiv(%s, 9223372036854775808)", "intDiv(%s, 18446744073709551615)", "intDiv(%s, 18446744073709551616)",
		"intDiv(%s, toUInt64(1000))", "%s %% 16", "modulo(%s, 16)", "tuple(%s)", "toYYYYMM(toDate(%s))",
	}
	checked, admitted := 0, 0
	for _, typ := range AdmittedColumnTypesV2() {
		for _, name := range names {
			for _, shape := range shapes {
				key := fmt.Sprintf(shape, name)
				schema := declaredSchemaV1{TableID: "db.n", PartitionBy: key, Columns: []lthash.Column{{Name: "id", Type: "UInt64"}, {Name: name, Type: typ}}}
				arbiter := partitionByAdmittedV3(schema)
				_, err := payloadexec.ParsePartitionKey(payloadexec.TableSchema{TableID: schema.TableID, PartitionBy: key, Columns: schema.Columns})
				if arbiter != (err == nil) {
					t.Errorf("%q over %s %s: rules v3 admitted=%v, housegate err=%v", key, name, typ, arbiter, err)
				}
				checked++
				if arbiter {
					admitted++
				}
			}
		}
	}
	for _, schema := range admittedPartitionKeysV3() {
		if !partitionByAdmittedV3(declaredSchemaV1{TableID: schema.TableID, PartitionBy: schema.PartitionBy, Columns: schema.Columns}) {
			t.Errorf("admittedPartitionKeysV3 lists %q over %s, which v3 refuses", schema.PartitionBy, schema.Columns[0].Type)
		}
	}
	if admitted < 100 || len(admittedPartitionKeysV3()) != 1+21+33+12 {
		t.Fatalf("checked %d keys, %d admitted, %d listed shapes", checked, admitted, len(admittedPartitionKeysV3()))
	}
}
