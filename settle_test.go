package arbiter

import "testing"

// TestSettleTrackCommandValidate: a settle names a non-empty, strictly
// ascending list of promotions; Validate refuses anything else and never sorts
// (housegate spec 2026-10-14 §14; plan S3-D clarification K53).
func TestSettleTrackCommandValidate(t *testing.T) {
	for _, ok := range [][]uint64{{41}, {41, 42}, {0, 7, 9}} {
		cmd := SettleTrackCommand{IndexerID: 2, ExpectedGeneration: 3, PromotionSeqs: ok}
		if err := cmd.Validate(); err != nil {
			t.Fatalf("%v: %v", ok, err)
		}
	}
	for _, bad := range [][]uint64{nil, {}, {42, 41}, {41, 41}, {7, 9, 8}} {
		cmd := SettleTrackCommand{IndexerID: 2, ExpectedGeneration: 3, PromotionSeqs: bad}
		err := cmd.Validate()
		if err == nil || err.Error() != "settle track: promotion_seqs must be non-empty and strictly ascending" {
			t.Fatalf("%v: err = %v", bad, err)
		}
		if len(bad) == 3 && bad[2] != 8 {
			t.Fatalf("Validate sorted its input: %v", bad)
		}
	}
}
