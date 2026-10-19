package objstore

import "testing"

func TestDrillVerdictPassRule(t *testing.T) {
	v := func(reasons ...DrillReason) []DrillTable {
		out := make([]DrillTable, 0, len(reasons))
		for _, r := range reasons {
			out = append(out, DrillTable{Reason: r})
		}
		return out
	}
	for _, c := range []struct {
		name   string
		tables []DrillTable
		pass   bool
	}{
		{"no table", nil, false},
		{"only tables the manifest does not list", v(DrillNotInManifest, DrillNotInManifest), false},
		{"one verified", v(DrillVerified), true},
		{"verified and not listed", v(DrillNotInManifest, DrillVerified), true},
		{"a schema hash mismatch", v(DrillVerified, DrillSchemaHashMismatch), false},
		{"a failed restore", v(DrillVerified, DrillRestoreFailed), false},
		{"an unmet need", v(DrillNeedUnsatisfied, DrillVerified), false},
		{"a closure mismatch", v(DrillVerified, DrillClosureMismatch), false},
		{"a partition set mismatch", v(DrillPartitionSetMismatch), false},
		{"an empty reason", v(DrillVerified, ""), false},
	} {
		if got := drillPass(c.tables); got != c.pass {
			t.Errorf("%s: pass %v, want %v", c.name, got, c.pass)
		}
	}
}
