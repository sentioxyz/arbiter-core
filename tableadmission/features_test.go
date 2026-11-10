package tableadmission

import (
	"slices"
	"testing"
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

// TestDataPlaneFeaturesOfThisBinary pins that the housegate profile this
// module pins replays every rules-v2 type, so every SNode and verifier built
// from it advertises table_admission_rules_v2.
func TestDataPlaneFeaturesOfThisBinary(t *testing.T) {
	if got := DataPlaneFeatures(); !slices.Equal(got, []string{"table_admission_rules_v2"}) {
		t.Fatalf("DataPlaneFeatures() = %v, want [table_admission_rules_v2]", got)
	}
}

// TestDataPlaneFeaturesFollowTheProfile pins that the feature follows the
// compiled profile: one rules-v2 type the profile cannot replay withholds it,
// whatever the module version says.
func TestDataPlaneFeaturesFollowTheProfile(t *testing.T) {
	all := func(string) bool { return true }
	if got := dataPlaneFeatures(all); !slices.Equal(got, []string{"table_admission_rules_v2"}) {
		t.Fatalf("full profile: %v", got)
	}
	for _, missing := range []string{"Date32", "Nullable(UInt256)", "LowCardinality(String)", "String"} {
		got := dataPlaneFeatures(func(typ string) bool { return typ != missing })
		if len(got) != 0 {
			t.Fatalf("profile without %s: %v, want none", missing, got)
		}
	}
	if got := dataPlaneFeatures(columnTypeAdmittedV1); len(got) != 0 {
		t.Fatalf("v1-only profile: %v, want none", got)
	}
}
