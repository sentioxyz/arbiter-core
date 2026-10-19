package gate

import (
	"slices"
	"strings"
	"testing"
)

func part(name, rowLtHash string, rows uint64) ScratchPart {
	return ScratchPart{Name: name, RowLtHash: rowLtHash, Rows: rows}
}

// wantClassification compares field by field; nil and empty slices are equal.
func wantClassification(t *testing.T, got Classification, outcome Outcome, keep, drop, kept []string) {
	t.Helper()
	if got.Outcome != outcome || !slices.Equal(got.Keep, keep) || !slices.Equal(got.Drop, drop) || !slices.Equal(got.Kept, kept) {
		t.Fatalf("Classify = {%s keep %v drop %v kept %d}, want {%s keep %v drop %v kept %d}",
			got.Outcome, got.Keep, got.Drop, len(got.Kept), outcome, keep, drop, len(kept))
	}
}

func TestClassifyRestoreKeepsOnlyNeededPartsAndDropsTheRest(t *testing.T) {
	a, b, x := rh("a"), rh("b"), rh("x")
	need := []string{b, a, b}
	scratch := []ScratchPart{
		part("s1", a, 1), part("s2", x, 1), part("s3", upper(b), 1),
		part("s4", b, 1), part("s5", b, 1), part("s6", a, 1),
	}
	got, err := Classify(Fetch{Need: need}, scratch)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	// Each needed hash is kept once per occurrence, in the order the parts
	// are offered; the extra parts of a backup ahead of publication (s2, s5,
	// s6) are dropped, never Foreign.
	wantClassification(t, got, OutcomeKept, []string{"s1", "s3", "s4"}, []string{"s2", "s5", "s6"}, []string{a, b, b})

	got, err = Classify(Fetch{Need: need}, []ScratchPart{part("s6", a, 1), part("s1", a, 1), part("s4", b, 1), part("s3", b, 1)})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeKept, []string{"s6", "s4", "s3"}, []string{"s1"}, []string{a, b, b})

	got, err = Classify(Fetch{Need: need}, []ScratchPart{part("s1", x, 3)})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeKept, nil, []string{"s1"}, nil)

	got, err = Classify(Fetch{}, nil)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeKept, nil, nil, nil)

	if _, err := Classify(Fetch{Need: need}, []ScratchPart{part("s1", a, 1), part("s2", "0xnothex", 1)}); err == nil || !strings.HasPrefix(err.Error(), "gate: scratch: part row lthash 1: ") {
		t.Fatalf("a scratch part that is not an LtHash: %v", err)
	}
	if _, err := Classify(Fetch{Need: []string{a, ""}}, nil); err == nil || !strings.HasPrefix(err.Error(), "gate: need: part row lthash 1: ") {
		t.Fatalf("a need that is not an LtHash: %v", err)
	}
}

func TestClassifyKeepsANeededPullOnce(t *testing.T) {
	a, b := rh("a1", "a2", "a3"), rh("b")
	for _, need := range [][]string{{a}, {b, a}, {a, a}} {
		got, err := Classify(Fetch{Need: need, Single: &Expected{RowLtHash: upper(a), Rows: 3}}, []ScratchPart{part("p1", a, 3)})
		if err != nil {
			t.Fatalf("Classify: %v", err)
		}
		wantClassification(t, got, OutcomeKept, []string{"p1"}, nil, []string{a})
	}
	// A need that lists the hash twice still keeps one pulled part: two copies
	// in one scratch are the doubled insert, not two kept parts.
	got, err := Classify(Fetch{Need: []string{a, a}, Single: &Expected{RowLtHash: a, Rows: 3}}, []ScratchPart{part("p1", a, 3), part("p2", a, 3)})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeDoubled, nil, []string{"p1", "p2"}, nil)
	if _, err := Classify(Fetch{Need: []string{a}, Single: &Expected{RowLtHash: "0x", Rows: 3}}, nil); err == nil || !strings.HasPrefix(err.Error(), "gate: single-part fetch: part row lthash 0: ") {
		t.Fatalf("a single-part fetch whose hash is not an LtHash: %v", err)
	}
}

func TestClassifyZeroRowsIsARenameRace(t *testing.T) {
	a := rh("a1", "a2")
	single := &Expected{RowLtHash: a, Rows: 2}
	got, err := Classify(Fetch{Need: []string{a}, Single: single}, nil)
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeRenameRace, nil, nil, nil)
	got, err = Classify(Fetch{Need: []string{a}, Single: single}, []ScratchPart{part("p1", rh(), 0)})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeRenameRace, nil, []string{"p1"}, nil)
}

func TestClassifyPartsSummingToTheNeedAreASplit(t *testing.T) {
	whole := rh("r1", "r2", "r3")
	single := &Expected{RowLtHash: whole, Rows: 3}
	got, err := Classify(Fetch{Need: []string{whole}, Single: single}, []ScratchPart{part("p1", rh("r1", "r3"), 2), part("p2", rh("r2"), 1)})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeSplit, nil, []string{"p1", "p2"}, nil)
	got, err = Classify(Fetch{Need: []string{whole}, Single: single}, []ScratchPart{part("p1", rh("r1"), 1), part("p2", rh("r2"), 1), part("p3", rh("r3"), 1)})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeSplit, nil, []string{"p1", "p2", "p3"}, nil)
}

func TestClassifyDoubledRowsAreDoubled(t *testing.T) {
	a := rh("a1", "a2")
	single := &Expected{RowLtHash: a, Rows: 2}
	got, err := Classify(Fetch{Need: []string{a}, Single: single}, []ScratchPart{part("p1", a, 2), part("p2", a, 2)})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeDoubled, nil, []string{"p1", "p2"}, nil)
	// A retried insert whose two blocks landed in one part.
	got, err = Classify(Fetch{Need: []string{a}, Single: single}, []ScratchPart{part("p1", rh("a1", "a2", "a1", "a2"), 4)})
	if err != nil {
		t.Fatalf("Classify: %v", err)
	}
	wantClassification(t, got, OutcomeDoubled, nil, []string{"p1"}, nil)
}

func TestClassifyAnythingElseIsForeignAndDropsNothing(t *testing.T) {
	a, b := rh("a1", "a2"), rh("b")
	single := &Expected{RowLtHash: a, Rows: 2}
	for _, tc := range []struct {
		name    string
		need    []string
		scratch []ScratchPart
	}{
		{"another part", []string{a}, []ScratchPart{part("p1", b, 2)}},
		{"the right hash with other rows", []string{a}, []ScratchPart{part("p1", a, 3)}},
		{"the part and something else", []string{a}, []ScratchPart{part("p1", a, 2), part("p2", b, 1)}},
		{"a split with other rows", []string{a}, []ScratchPart{part("p1", rh("a1"), 1), part("p2", rh("a2"), 2)}},
		{"three copies", []string{a}, []ScratchPart{part("p1", a, 2), part("p2", a, 2), part("p3", a, 2)}},
		{"twice the hash with other rows", []string{a}, []ScratchPart{part("p1", a, 2), part("p2", a, 3)}},
		{"the pulled part when the need does not hold it", []string{b}, []ScratchPart{part("p1", a, 2)}},
		{"another needed part than the one pulled", []string{a, b}, []ScratchPart{part("p1", b, 1)}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Classify(Fetch{Need: tc.need, Single: single}, tc.scratch)
			if err != nil {
				t.Fatalf("Classify: %v", err)
			}
			wantClassification(t, got, OutcomeForeign, nil, nil, nil)
		})
	}
}
