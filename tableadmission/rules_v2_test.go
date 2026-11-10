package tableadmission

import (
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// TestAdmissionRulesV2Verdicts is the frozen verdict table of admission rules
// v2. A change to any row changes how an already-committed AddTable replays
// under v2, so it needs a new rules version, not an edit here.
func TestAdmissionRulesV2Verdicts(t *testing.T) {
	one := func(typ string) (string, string) {
		return `{"table_id":"db.n","columns":[{"name":"v","type":"` + typ + `"}]}`, rulesHash(t, "db.n", "", lthash.Column{Name: "v", Type: typ})
	}
	type row struct {
		name, json, hash string
		want             RefusedCode
	}
	var rows []row
	for _, typ := range []string{
		"Date32", "UUID", "Int128", "Int256", "UInt128", "UInt256", "LowCardinality(String)",
		"Nullable(Int64)", "Nullable(String)", "Nullable(FixedString(32))", "Nullable(Bool)", "Nullable(Date32)",
		"Nullable(DateTime('UTC'))", "Nullable(DateTime64(9, 'UTC'))", "Nullable(UUID)", "Nullable(UInt256)",
		"String", "DateTime64(6, 'UTC')",
	} {
		js, h := one(typ)
		rows = append(rows, row{"admitted " + typ, js, h, ""})
	}
	for _, tc := range []struct {
		typ  string
		want RefusedCode
	}{
		{"Decimal(18, 4)", RefusedColumnType},
		{"Array(String)", RefusedColumnType},
		{"Map(String, String)", RefusedColumnType},
		{"Nullable(Nullable(String))", RefusedColumnType},
		{"Nullable(LowCardinality(String))", RefusedColumnType},
		{"LowCardinality(Nullable(String))", RefusedColumnType},
		{"LowCardinality(UInt64)", RefusedColumnType},
		{"Nullable(Decimal(18, 4))", RefusedColumnType},
		{"Nullable()", RefusedColumnType},
		{"Nullable(DateTime('Asia/Tokyo'))", RefusedColumnType},
		{"DateTime('Asia/Tokyo')", RefusedColumnType},
		{"Int512", RefusedColumnType},
		{"uuid", RefusedColumnType},
		{"Nullable( Int64 )", RefusedNonCanonicalType},
		{"Nullable(FixedString(032))", RefusedNonCanonicalType},
		{"Nullable(DateTime64(3,'UTC'))", RefusedNonCanonicalType},
		{"FixedString( +032 )", RefusedNonCanonicalType},
	} {
		js, h := one(tc.typ)
		rows = append(rows, row{"refused " + tc.typ, js, h, tc.want})
	}
	rows = append(rows,
		row{"partition on LowCardinality(String) stays frozen", `{"table_id":"db.n","partition_by":"p","columns":[{"name":"p","type":"LowCardinality(String)"}]}`,
			rulesHash(t, "db.n", "p", lthash.Column{Name: "p", Type: "LowCardinality(String)"}), RefusedPartitionFreeze},
		row{"partition on Nullable(String) stays frozen", `{"table_id":"db.n","partition_by":"p","columns":[{"name":"p","type":"Nullable(String)"}]}`,
			rulesHash(t, "db.n", "p", lthash.Column{Name: "p", Type: "Nullable(String)"}), RefusedPartitionFreeze},
		row{"partition expression stays frozen", `{"table_id":"db.n","partition_by":"toYYYYMM(d)","columns":[{"name":"d","type":"Date32"}]}`,
			rulesHash(t, "db.n", "toYYYYMM(d)", lthash.Column{Name: "d", Type: "Date32"}), RefusedPartitionFreeze},
		row{"admitted partitioned", `{"table_id":"db.n","partition_by":"p","columns":[{"name":"p","type":"String"},{"name":"u","type":"Nullable(UUID)"}]}`,
			rulesHash(t, "db.n", "p", lthash.Column{Name: "p", Type: "String"}, lthash.Column{Name: "u", Type: "Nullable(UUID)"}), ""},
		row{"hash mismatch", `{"table_id":"db.n","columns":[{"name":"v","type":"UUID"}]}`, "0xdeadbeef", RefusedSchemaHashMismatch},
		row{"reserved column before types", `{"table_id":"db.n","columns":[{"name":"v","type":"Array(String)"},{"name":"_hg_row_id","type":"String"}]}`, "", RefusedReservedColumn},
	)
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			got := AdmitDeclaredSchemaV2(rulesNet, "db.n", tc.hash, tc.json)
			switch {
			case tc.want == "" && got != nil:
				t.Fatalf("refused %+v, want admitted", got)
			case tc.want != "" && (got == nil || got.Code != tc.want):
				t.Fatalf("verdict = %+v, want %s", got, tc.want)
			case got != nil && (got.Detail == "" || strings.Contains(got.Detail, "payloadexec") || !isASCII(got.Detail)):
				t.Fatalf("detail %q must be a non-empty ASCII arbiter-authored sentence", got.Detail)
			}
		})
	}
	// Pinned detail bytes: they are replicated state.
	js, h := one("Array(String)")
	if got := AdmitDeclaredSchemaV2(rulesNet, "db.n", h, js); got == nil || got.Detail != `column "v": type "Array(String)" is not admitted by admission rules v2` {
		t.Fatalf("v2 type detail = %+v", got)
	}
	js, h = one("Nullable( Int64 )")
	if got := AdmitDeclaredSchemaV2(rulesNet, "db.n", h, js); got == nil || got.Detail != `column "v": type "Nullable( Int64 )" must be spelled "Nullable(Int64)"` {
		t.Fatalf("v2 non-canonical detail = %+v", got)
	}
}

// TestAdmissionRulesV2WidensV1Only pins the property the CREATE TABLE
// pre-check and the version raise rely on: v2 admits everything v1 admits
// with the same verdict and detail, so a host judging by a version that lags
// the arbiter's can only be stricter, never looser; and v2 differs from v1
// only on column types (every other refusal is v1's, byte for byte).
func TestAdmissionRulesV2WidensV1Only(t *testing.T) {
	for _, typ := range admittedColumnTypesV1 {
		if !columnTypeAdmittedV2(typ) {
			t.Fatalf("v2 does not admit v1 type %q", typ)
		}
	}
	for _, js := range []string{
		`{`, `{"table_id":"db.other","columns":[{"name":"v","type":"Int64"}]}`, `{"table_id":"db.n","columns":[]}`,
		`{"table_id":"db.n","columns":[{"name":"","type":"Int64"}]}`, `{"table_id":"db.n","columns":[{"name":"_hg_row_id","type":"String"}]}`,
		`{"table_id":"db.n","columns":[{"name":"v","type":"FixedString(032)"}]}`,
		`{"table_id":"db.n","partition_by":"v","columns":[{"name":"v","type":"Int64"}]}`,
		`{"table_id":"db.n","columns":[{"name":"v","type":"Int64"}]}`,
	} {
		v1, v2 := AdmitDeclaredSchemaV1(rulesNet, "db.n", "0x00", js), AdmitDeclaredSchemaV2(rulesNet, "db.n", "0x00", js)
		if (v1 == nil) != (v2 == nil) || (v1 != nil && *v1 != *v2) {
			t.Fatalf("%s: v1 %+v, v2 %+v", js, v1, v2)
		}
	}
}

// TestAdmissionRulesV2MatchHousegateProfile pins v2's type rules against the
// housegate profile compiled into this binary: every v2-admitted spelling is
// one housegate resolves to itself, so every role (payload executor, replay
// verifier, lthash, snapshot query) can create and replay the table, and on
// the probe list the two classify identically except the narrowed timezones.
func TestAdmissionRulesV2MatchHousegateProfile(t *testing.T) {
	for _, typ := range admittedColumnTypesV2 {
		profile, err := payloadexec.ResolveColumnProfile(typ)
		if err != nil || profile.Canonical != typ {
			t.Fatalf("v2 admits %q but housegate resolves it to %+v (%v)", typ, profile, err)
		}
	}
	for _, v := range housegateAdmittedVectorsAtPin {
		if !columnTypeAdmittedV2(v) {
			t.Fatalf("housegate vector %q is not admitted by v2", v)
		}
	}
	for _, probe := range rulesProbes {
		canonical, v2ok := canonicalColumnTypeV2(probe)
		profile, err := payloadexec.ResolveColumnProfile(probe)
		hgok := err == nil
		if narrowedProbes[probe] {
			if v2ok {
				t.Fatalf("%q: v2 must refuse non-UTC timezones", probe)
			}
			continue
		}
		if v2ok != hgok || (v2ok && canonical != profile.Canonical) {
			t.Fatalf("%q: v2 (%q, %v) and housegate (%q, %v) disagree", probe, canonical, v2ok, profile.Canonical, err)
		}
	}
}

// TestAdmitDeclaredSchemaSelectsByVersion pins the dispatch and the parameter
// rules the consensus update applies.
func TestAdmitDeclaredSchemaSelectsByVersion(t *testing.T) {
	js := `{"table_id":"db.n","columns":[{"name":"v","type":"UUID"}]}`
	h := rulesHash(t, "db.n", "", lthash.Column{Name: "v", Type: "UUID"})
	for _, tc := range []struct {
		param    uint32
		admitted bool
	}{{0, false}, {RulesV1, false}, {RulesV2, true}} {
		got := AdmitDeclaredSchema(tc.param, rulesNet, "db.n", h, js)
		if (got == nil) != tc.admitted {
			t.Fatalf("param %d: verdict %+v, want admitted=%v", tc.param, got, tc.admitted)
		}
	}
	if EffectiveRulesVersion(0) != RulesV1 || EffectiveRulesVersion(2) != RulesV2 {
		t.Fatal("EffectiveRulesVersion")
	}
	for param, valid := range map[uint32]bool{0: true, 1: false, 2: true, 3: false} {
		if ValidRulesVersionParam(param) != valid {
			t.Fatalf("ValidRulesVersionParam(%d) = %v", param, !valid)
		}
	}
	if LatestRulesVersion != RulesV2 {
		t.Fatal("LatestRulesVersion moved without a new rules version")
	}
}

// TestAdmittedColumnTypesV2ReturnsACopy pins that callers cannot widen the
// frozen set through the returned slice, and the set's exact size.
func TestAdmittedColumnTypesV2ReturnsACopy(t *testing.T) {
	got := AdmittedColumnTypesV2()
	if !slices.Equal(got, admittedColumnTypesV2) {
		t.Fatal("AdmittedColumnTypesV2() differs from the frozen set")
	}
	// v1's 36 spellings and 6 plain additions, LowCardinality(String), and
	// Nullable over each of the 42 plain ones.
	if len(got) != 36+6+1+42 {
		t.Fatalf("v2 admits %d spellings", len(got))
	}
	got[0] = "Array(String)"
	if admittedColumnTypesV2[0] != "String" {
		t.Fatal("AdmittedColumnTypesV2 exposed the frozen set")
	}
}
