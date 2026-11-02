package consensus

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestMemberPurgeChecksEachHoldIndependently isolates each hold; stacking
// all three in one test can hide the removal of one of the three gates.
func TestMemberPurgeChecksEachHoldIndependently(t *testing.T) {
	for _, hold := range []string{"table", "replica", "unresolved", "corrupt ledger"} {
		t.Run(hold, func(t *testing.T) {
			h := newModeHarness(t, purgeSnapshot(), false)
			switch hold {
			case "table":
				h.held[3] = true
			case "replica":
				h.replicas[3] = []string{fixtureNode}
			case "unresolved":
				q, err := OpenQuiescence(h.stateDir)
				if err != nil {
					t.Fatal(err)
				}
				if err := q.Observe(41, []QuiescentTable{{TableID: "dex.pools", IncarnationSeq: 3}}); err != nil {
					t.Fatal(err)
				}
			case "corrupt ledger":
				if err := os.WriteFile(filepath.Join(h.stateDir, QuiescenceFile), []byte(`{"version":1}`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			h.mode.purgeStep(context.Background())
			if _, _, reports := h.sent(); len(reports) != 0 {
				t.Fatalf("reported %v while the sole remaining hold was %s", reports, hold)
			}
		})
	}
}
