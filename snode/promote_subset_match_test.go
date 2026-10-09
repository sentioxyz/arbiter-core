package snode

import (
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"

	"github.com/sentioxyz/arbiter-core"
)

// TestMatchShadowCandidates covers the pure multiset matcher.
func TestMatchShadowCandidates(t *testing.T) {
	hash := func(s string) string {
		h := lthash.New()
		h.Add([]byte(s))
		return accumulatorHex(h)
	}
	a, b, c := hash("a"), hash("b"), hash("c")
	ref := func(name, h string) arbiter.PartRef { return arbiter.PartRef{PartName: name, PartRowLtHash: h} }
	for _, tc := range []struct {
		name       string
		candidates []arbiter.PartRef
		shadow     []shadowPart
		keep, drop []string
		wantErr    string
	}{
		{"exact", []arbiter.PartRef{ref("u1", a)}, []shadowPart{{"s1", a}}, []string{"s1"}, nil, ""},
		{"subset", []arbiter.PartRef{ref("u1", a)}, []shadowPart{{"s1", a}, {"s2", b}}, []string{"s1"}, []string{"s2"}, ""},
		{"duplicate shadow content", []arbiter.PartRef{ref("u1", a)}, []shadowPart{{"s1", a}, {"s2", a}, {"s3", b}}, []string{"s1"}, []string{"s2", "s3"}, ""},
		{"duplicate candidates", []arbiter.PartRef{ref("u1", a), ref("u2", a)}, []shadowPart{{"s1", a}, {"s2", a}}, []string{"s1", "s2"}, nil, ""},
		{"duplicate candidates against duplicate shadow plus other", []arbiter.PartRef{ref("u1", a), ref("u2", a)}, []shadowPart{{"s1", a}, {"s2", a}, {"s3", b}}, []string{"s1", "s2"}, []string{"s3"}, ""},
		{"duplicate candidates short", []arbiter.PartRef{ref("u1", a), ref("u2", a)}, []shadowPart{{"s1", a}, {"s2", b}}, nil, nil, "not present in hg_unsafe"},
		{"zero-row shadow part", []arbiter.PartRef{ref("u1", a)}, []shadowPart{{"s1", a}, {"s2", ""}}, []string{"s1"}, []string{"s2"}, ""},
		{"missing", []arbiter.PartRef{ref("u1", a), ref("u2", c)}, []shadowPart{{"s1", a}, {"s2", b}}, nil, nil, "u2"},
		{"no shadow parts", []arbiter.PartRef{ref("u1", a)}, nil, nil, nil, "not present in hg_unsafe"},
		{"empty candidate hash", []arbiter.PartRef{ref("u1", "")}, []shadowPart{{"s1", a}}, nil, nil, "invalid part_row_lthash"},
		{"non-canonical hash spelling", []arbiter.PartRef{ref("u1", "0x"+strings.ToUpper(a[2:]))}, []shadowPart{{"s1", a}}, []string{"s1"}, nil, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			keep, drop, err := matchShadowCandidates(arbiter.PromoteSafePartition{CandidateParts: tc.candidates}, tc.shadow)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("err = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(keep, tc.keep) || !slices.Equal(drop, tc.drop) {
				t.Fatalf("keep %v drop %v, want keep %v drop %v", keep, drop, tc.keep, tc.drop)
			}
		})
	}
}
