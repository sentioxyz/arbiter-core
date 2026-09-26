package ddl

import (
	"strings"
	"testing"
)

// TestParseIncarnationComment documents the exact accept/reject behavior of
// the hg_incarnation marker parser, since Task 5's leftover-drop decision
// depends on it. Every case here is intentional: the round-trip check in
// ParseIncarnationComment rejects any string whose canonical re-rendering
// does not match byte-for-byte (so "007" and "+5" are both foreign, not
// incarnation 7 / 5), and seq 0 is reserved for "no marker" so it can never
// round-trip either.
func TestParseIncarnationComment(t *testing.T) {
	cases := []struct {
		name    string
		comment string
		wantSeq uint64
		wantOK  bool
	}{
		{"round trip via IncarnationComment", IncarnationComment(7), 7, true},
		{"empty (genesis table)", "", 0, false},
		{"explicit zero is reserved for no marker", "hg_incarnation=0", 0, false},
		{"leading zero does not round-trip", "hg_incarnation=007", 0, false},
		{"leading plus does not round-trip", "hg_incarnation=+5", 0, false},
		{"overflow beyond uint64", "hg_incarnation=18446744073709551616", 0, false},
		{"trailing garbage", "hg_incarnation=7x", 0, false},
		{"foreign comment", "some_other_marker=5", 0, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			seq, ok := ParseIncarnationComment(c.comment)
			if seq != c.wantSeq || ok != c.wantOK {
				t.Fatalf("ParseIncarnationComment(%q) = (%d, %v), want (%d, %v)", c.comment, seq, ok, c.wantSeq, c.wantOK)
			}
		})
	}
}

func TestIncarnationComment(t *testing.T) {
	if got := IncarnationComment(7); got != "hg_incarnation=7" {
		t.Fatalf("IncarnationComment(7) = %q, want hg_incarnation=7", got)
	}
	seq, ok := ParseIncarnationComment(IncarnationComment(42))
	if !ok || seq != 42 {
		t.Fatalf("round trip through IncarnationComment(42) = (%d, %v), want (42, true)", seq, ok)
	}
}

// TestIncarnationIntents_RendersCommentClause is the golden check that
// IncarnationIntents' non-zero seq actually reaches the rendered DDL, and
// that seq 0 (genesis) renders none, matching today's unmarked golden DDL in
// build_test.go.
func TestIncarnationIntents_RendersCommentClause(t *testing.T) {
	p := goldenPinned()
	sch := goldenSchema()

	unsafe, safe, promote, err := IncarnationIntents(p, sch, 7)
	if err != nil {
		t.Fatalf("IncarnationIntents(seq=7): %v", err)
	}
	for _, intent := range []TableIntent{unsafe, safe, promote} {
		sql := intent.SQL()
		if !strings.HasSuffix(sql, "\nCOMMENT 'hg_incarnation=7'") {
			t.Fatalf("%s.%s SQL missing COMMENT clause: %s", intent.Database, intent.Table, sql)
		}
	}

	genesisUnsafe, genesisSafe, genesisPromote, err := IncarnationIntents(p, sch, 0)
	if err != nil {
		t.Fatalf("IncarnationIntents(seq=0): %v", err)
	}
	for _, intent := range []TableIntent{genesisUnsafe, genesisSafe, genesisPromote} {
		if strings.Contains(intent.SQL(), "COMMENT") {
			t.Fatalf("%s.%s genesis SQL must carry no COMMENT clause: %s", intent.Database, intent.Table, intent.SQL())
		}
	}
}
