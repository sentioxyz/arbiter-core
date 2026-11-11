package tableadmission

import (
	"slices"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// TestRulesFeatureSpelling pins the capability strings: arbiter's leader gate
// and arbiter-admin compare them byte for byte, and voters already report
// table_admission_rules_v2.
func TestRulesFeatureSpelling(t *testing.T) {
	if got := RulesFeature(RulesV2); got != "table_admission_rules_v2" {
		t.Fatalf("RulesFeature(RulesV2) = %q", got)
	}
	if got := RulesFeature(3); got != "table_admission_rules_v3" {
		t.Fatalf("RulesFeature(3) = %q", got)
	}
}

// TestDataPlaneFeaturesOfThisBinary pins that the housegate this module pins
// replays every rules-v2 type and parses every rules-v3 partition key, so
// every SNode and verifier built from it advertises both features.
func TestDataPlaneFeaturesOfThisBinary(t *testing.T) {
	if got := DataPlaneFeatures(); !slices.Equal(got, []string{"table_admission_rules_v2", "table_admission_rules_v3"}) {
		t.Fatalf("DataPlaneFeatures() = %v, want [table_admission_rules_v2 table_admission_rules_v3]", got)
	}
}

// TestDataPlaneFeaturesFollowTheProfile pins that the feature follows the
// compiled profile: one rules-v2 type the profile cannot replay withholds it,
// whatever the module version says.
func TestDataPlaneFeaturesFollowTheProfile(t *testing.T) {
	all := func(string) bool { return true }
	allKeys := func(payloadexec.TableSchema) bool { return true }
	if got := dataPlaneFeatures(all, allKeys); !slices.Equal(got, []string{"table_admission_rules_v2", "table_admission_rules_v3"}) {
		t.Fatalf("full profile: %v", got)
	}
	for _, missing := range []string{"Date32", "Nullable(UInt256)", "LowCardinality(String)", "String"} {
		got := dataPlaneFeatures(func(typ string) bool { return typ != missing }, allKeys)
		if len(got) != 0 {
			t.Fatalf("profile without %s: %v, want none", missing, got)
		}
	}
	if got := dataPlaneFeatures(columnTypeAdmittedV1, allKeys); len(got) != 0 {
		t.Fatalf("v1-only profile: %v, want none", got)
	}
	// A housegate whose parser lacks one v3 key shape withholds v3 only.
	for _, missing := range []string{"toMonday(c)", "intDiv(c, 18446744073709551615)", "c"} {
		got := dataPlaneFeatures(all, func(s payloadexec.TableSchema) bool { return s.PartitionBy != missing })
		if !slices.Equal(got, []string{"table_admission_rules_v2"}) {
			t.Fatalf("parser without %s: %v, want [table_admission_rules_v2]", missing, got)
		}
	}
	// The pre-v3 freeze (a bare String key only) withholds v3.
	freeze := func(s payloadexec.TableSchema) bool { return s.PartitionBy == "" || (s.PartitionBy == "c" && s.Columns[0].Type == "String") }
	if got := dataPlaneFeatures(all, freeze); !slices.Equal(got, []string{"table_admission_rules_v2"}) {
		t.Fatalf("pre-v3 parser: %v", got)
	}
}
