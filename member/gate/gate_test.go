package gate

import (
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// rh is the row LtHash of a part whose rows are the given elements, in the
// executor's form ("0x" + lowercase hex of the 2048-byte accumulator).
func rh(elements ...string) string {
	acc := lthash.New()
	for _, e := range elements {
		acc.Add([]byte(e))
	}
	return "0x" + hex.EncodeToString(acc.Bytes())
}

// sum is the protocol's sum of row LtHashes (payloadexec.SumPartRowLtHashes).
func sum(t *testing.T, hashes ...string) string {
	t.Helper()
	s, err := payloadexec.SumPartRowLtHashes(hashes)
	if err != nil {
		t.Fatalf("sum: %v", err)
	}
	return s
}

// upper spells a hash with upper-case hex digits, which the gate accepts and
// compares in canonical form.
func upper(h string) string { return "0x" + strings.ToUpper(strings.TrimPrefix(h, "0x")) }

func sorted(hashes ...string) []string {
	out := slices.Clone(hashes)
	slices.Sort(out)
	return out
}

func TestPlanPartitionNeedIsManifestMinusLocal(t *testing.T) {
	a, b, c := rh("a"), rh("b1", "b2"), rh("c")
	m := []string{b, a, b, c}
	root := sum(t, m...)
	for _, tc := range []struct {
		name  string
		local []string
		need  []string
	}{
		{"empty local needs every part", nil, sorted(a, b, b, c)},
		{"one of two equal parts is local", []string{b}, sorted(a, b, c)},
		{"local in any order and spelling", []string{upper(c), a}, sorted(b, b)},
		{"local equal to the manifest needs nothing", []string{c, b, a, b}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := PlanPartition(m, upper(root), tc.local)
			if err != nil {
				t.Fatalf("PlanPartition: %v", err)
			}
			if p.Diverged || !slices.Equal(p.Need, tc.need) || p.Root != root {
				t.Fatalf("PlanPartition = {Diverged:%v Need:%d parts Root ok:%v}, want Need = %d parts, sorted, and the canonical root", p.Diverged, len(p.Need), p.Root == root, len(tc.need))
			}
		})
	}
	p, err := PlanPartition(nil, sum(t), nil)
	if err != nil || p.Diverged || len(p.Need) != 0 {
		t.Fatalf("an empty partition = %+v, %v; want nothing needed", p, err)
	}
}

func TestPlanPartitionIsDivergedWhenLocalIsNotASubMultiset(t *testing.T) {
	a, b := rh("a"), rh("b")
	m := []string{a, b, b}
	root := sum(t, m...)
	for _, tc := range []struct {
		name  string
		local []string
	}{
		{"a local part the manifest does not list", []string{rh("x")}},
		{"more copies of a part than the manifest lists", []string{b, b, b}},
		{"two copies of a part the manifest lists once", []string{a, a}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, err := PlanPartition(m, root, tc.local)
			if err != nil {
				t.Fatalf("PlanPartition: %v", err)
			}
			if !p.Diverged || p.Need != nil || p.Root != root {
				t.Fatalf("PlanPartition = {Diverged:%v Need:%d}, want Diverged with no Need", p.Diverged, len(p.Need))
			}
		})
	}
}

func TestPlanPartitionRefusesAManifestWhosePartsDoNotSumToItsRoot(t *testing.T) {
	a, b := rh("a"), rh("b")
	_, err := PlanPartition([]string{a, b}, a, nil)
	want := "gate: manifest parts sum to " + abbreviate(sum(t, a, b)) + ", not the partition root " + abbreviate(a)
	if err == nil || err.Error() != want {
		t.Fatalf("PlanPartition = %v, want %q", err, want)
	}
	if _, err := PlanPartition([]string{a, b}, a, []string{rh("x")}); err == nil || !strings.HasPrefix(err.Error(), "gate: manifest parts sum to ") {
		t.Fatalf("a bad manifest is refused before local is compared: %v", err)
	}
	if _, err := PlanPartition(nil, a, nil); err == nil || !strings.HasPrefix(err.Error(), "gate: manifest parts sum to ") {
		t.Fatalf("no part and a non-zero root: %v", err)
	}
	for _, tc := range []struct {
		name     string
		manifest []string
		root     string
		local    []string
		prefix   string
	}{
		{"manifest", []string{a, "0xzz"}, a, nil, "gate: manifest: part row lthash 1: "},
		{"short manifest part", []string{"0x00"}, a, nil, "gate: manifest: part row lthash 0: "},
		{"root", []string{a}, "", nil, "gate: partition root: "},
		{"local", []string{a}, a, []string{"not hex"}, "gate: local: part row lthash 0: "},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := PlanPartition(tc.manifest, tc.root, tc.local); err == nil || !strings.HasPrefix(err.Error(), tc.prefix) {
				t.Fatalf("PlanPartition = %v, want an error starting %q", err, tc.prefix)
			}
		})
	}
}

func TestClosureIsArithmetic(t *testing.T) {
	a, b, c := rh("a"), rh("b"), rh("c1", "c2")
	root := sum(t, a, b, c)
	for _, tc := range []struct {
		name        string
		local, kept []string
	}{
		{"local and kept", []string{a}, []string{c, b}},
		{"all local", []string{a, b, c}, nil},
		{"all kept, any order, any spelling", nil, []string{upper(c), a, b}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := Closure(root, tc.local, tc.kept); err != nil {
				t.Fatalf("Closure: %v", err)
			}
		})
	}
	err := Closure(root, []string{a}, []string{b})
	want := "gate: closure: 1 local and 1 kept parts sum to " + abbreviate(sum(t, a, b)) + ", the partition root is " + abbreviate(root)
	if err == nil || err.Error() != want {
		t.Fatalf("Closure missing a part = %v, want %q", err, want)
	}
	if err := Closure(sum(t, a, b), []string{a}, []string{b, b}); err == nil || !strings.HasPrefix(err.Error(), "gate: closure: 1 local and 2 kept parts sum to ") {
		t.Fatalf("a part kept twice must not close: %v", err)
	}
	if err := Closure(sum(t), nil, nil); err != nil {
		t.Fatalf("an empty partition closes on the zero root: %v", err)
	}
	if err := Closure(root, []string{a}, []string{"0x0"}); err == nil || !strings.HasPrefix(err.Error(), "gate: kept: part row lthash 0: ") {
		t.Fatalf("a bad kept hash: %v", err)
	}
}

func TestPartitionSetMustEqualTheManifests(t *testing.T) {
	manifest := []string{"p_b", "p_a"}
	for _, local := range [][]string{{"p_a", "p_b"}, {"p_b", "p_a", "p_a", "p_b"}} {
		if err := PartitionSet(manifest, local); err != nil {
			t.Fatalf("PartitionSet(%v, %v): %v", manifest, local, err)
		}
	}
	if err := PartitionSet(nil, nil); err != nil {
		t.Fatalf("two empty sets are equal: %v", err)
	}
	for _, tc := range []struct {
		local []string
		want  string
	}{
		{[]string{"p_a", "p_a"}, "gate: partition set [p_a] is not the manifest's [p_a p_b]"},
		{[]string{"p_c", "p_a", "p_b"}, "gate: partition set [p_a p_b p_c] is not the manifest's [p_a p_b]"},
		{nil, "gate: partition set [] is not the manifest's [p_a p_b]"},
	} {
		if err := PartitionSet(manifest, tc.local); err == nil || err.Error() != tc.want {
			t.Fatalf("PartitionSet(%v, %v) = %v, want %q", manifest, tc.local, err, tc.want)
		}
	}
}

// Invalid local input must not hide a corrupt published manifest. Moving
// local canonicalization ahead of the manifest sum check breaks this priority.
func TestPlanPartitionRefusesBadManifestBeforeMalformedLocal(t *testing.T) {
	a, b := rh("a"), rh("b")
	_, err := PlanPartition([]string{a, b}, a, []string{"not hex"})
	if err == nil || !strings.HasPrefix(err.Error(), "gate: manifest parts sum to ") {
		t.Fatalf("inconsistent manifest with malformed local returned %v; want manifest sum refusal", err)
	}
	// A valid manifest must still validate local, so ignoring local input
	// cannot satisfy the simultaneous-error assertion above.
	_, err = PlanPartition([]string{a}, a, []string{"not hex"})
	if err == nil || !strings.HasPrefix(err.Error(), "gate: local: part row lthash 0: ") {
		t.Fatalf("valid manifest with malformed local returned %v; want local refusal", err)
	}
}
