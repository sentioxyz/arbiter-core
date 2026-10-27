package sources

import (
	"context"
	"regexp"
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// readerPartsColumns are the system.parts columns si_reader may read
// (contract §20.2).
var readerPartsColumns = []string{"database", "table", "name", "active", "rows", "partition_id", "hash_of_all_files"}

func pullSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID: "dex.swaps", PartitionBy: "p",
		Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
}

// TestOwnerPullFiltersOnPartOnly pins the owner pull's statements (housegate
// spec 2026-10-14 §8.6): the name is resolved on the owner by
// hash_of_all_files, the pull filters on _part only (never on the manifest's
// logical partition id, which is not ClickHouse's partition id), the
// collection is the only way to the owner (no host, user or password in
// SQL), only si_reader's system.parts columns are named, and a miss falls
// back to the parts of the needed partition (ClickHouse's partition_id,
// computed on the member's own server by a query that reads no table) and
// row count with the bound; the unsafe pulls never fall back.
func TestOwnerPullFiltersOnPartOnly(t *testing.T) {
	need := Need{
		TableKey: "dex.swaps", Table: "dex__swaps", PartitionID: "p_2026", PartName: "d589_1_1_0",
		PartPhysHash: "4adff322bb8fd41ea3d90f10b04424fb", PartRowLtHash: "0x01", RowCount: 3,
	}
	scratch := Scratch{Database: "hg_rstage", Table: "member-a_0011aabb_7"}
	resolve := "SELECT name, rows FROM remote(`si_owner_0`, database = 'system', table = 'parts') WHERE database = 'hg_safe' AND table = 'dex__swaps' AND active AND hash_of_all_files = '4adff322bb8fd41ea3d90f10b04424fb'"
	// partitionId('2026') is 3897267c32bd78b1a745b8b101ea06f3 on 25.8 and 26.3
	// (measured), the partition_id of every part whose partition value is 2026.
	partitionID := "SELECT partitionId('2026')"
	fallback := "SELECT name FROM remote(`si_owner_0`, database = 'system', table = 'parts') WHERE database = 'hg_safe' AND table = 'dex__swaps' AND active AND partition_id = '3897267c32bd78b1a745b8b101ea06f3' AND rows = 3 ORDER BY name LIMIT 8"
	pull := "INSERT INTO `hg_rstage`.`member-a_0011aabb_7` SELECT * FROM remote(`si_owner_0`, database = 'hg_safe', table = 'dex__swaps') WHERE _part = 'd589_4_4_0'"

	t.Run("resolved_by_hash", func(t *testing.T) {
		conn := &fakeConn{rows: func(query string, args []any) ([][]any, error) {
			switch query {
			case resolve:
				return [][]any{{"d589_9_9_0", uint64(3)}, {"d589_4_4_0", uint64(3)}}, nil
			case scratchPartsQuery:
				if !slices.Equal(args, []any{"hg_rstage", "member-a_0011aabb_7"}) {
					t.Errorf("scratch listing args %v", args)
				}
				return [][]any{{"d589_1_1_0"}}, nil
			}
			t.Fatalf("unexpected query %s", query)
			return nil, nil
		}}
		res := NewOwnerPull(conn, "si_owner_0", "").Fetch(context.Background(), need, pullSchema(), scratch)
		if res.Source != "owner_pull" || res.Outcome != OutcomeFetched || res.Err != nil || !slices.Equal(res.Parts, []string{"d589_1_1_0"}) {
			t.Fatalf("Fetch = %+v, want fetched [d589_1_1_0] from owner_pull", res)
		}
		want := []string{"query " + resolve, "exec " + pull, "query " + scratchPartsQuery}
		if got := conn.all(); !slices.Equal(got, want) {
			t.Fatalf("statements:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	})
	t.Run("missed_then_bounded_fallback", func(t *testing.T) {
		conn := &fakeConn{rows: func(query string, _ []any) ([][]any, error) {
			if query == partitionID {
				return [][]any{{"3897267c32bd78b1a745b8b101ea06f3"}}, nil
			}
			return nil, nil
		}}
		res := NewOwnerPull(conn, "si_owner_0", "hg_safe").Fetch(context.Background(), need, pullSchema(), scratch)
		if res.Outcome != OutcomeMissed || res.Err != nil || len(res.Parts) != 0 {
			t.Fatalf("Fetch = %+v, want missed", res)
		}
		if got, want := conn.all(), []string{"query " + resolve, "query " + partitionID, "query " + fallback}; !slices.Equal(got, want) {
			t.Fatalf("statements %q, want %q", got, want)
		}
	})
	t.Run("unpartitioned_fallback_reads_partition_all", func(t *testing.T) {
		conn := &fakeConn{}
		flat := pullSchema()
		flat.PartitionBy = ""
		unpartitioned := need
		unpartitioned.PartitionID = "all"
		res := NewOwnerPull(conn, "si_owner_0", "").Fetch(context.Background(), unpartitioned, flat, scratch)
		want := []string{"query " + resolve, "query " + strings.Replace(fallback, "'3897267c32bd78b1a745b8b101ea06f3'", "'all'", 1)}
		if res.Outcome != OutcomeMissed || !slices.Equal(conn.all(), want) {
			t.Fatalf("Fetch = %+v after %q, want missed after %q", res, conn.all(), want)
		}
	})
	t.Run("unsafe_pulls_never_fall_back", func(t *testing.T) {
		for _, p := range []*RemotePull{NewOwnerUnsafePull(&fakeConn{}, "si_owner_0", "hg_unsafe_x"), NewPeerUnsafePull(&fakeConn{}, "si_peer_verifier_1")} {
			res := p.Fetch(context.Background(), need, pullSchema(), scratch)
			statements := p.Conn.(*fakeConn).all()
			if res.Outcome != OutcomeMissed || len(statements) != 1 || !strings.Contains(statements[0], "hash_of_all_files") {
				t.Fatalf("%s: Fetch = %+v after %q, want missed after the hash lookup alone", p.Name(), res, statements)
			}
		}
	})
	t.Run("statement_shape", func(t *testing.T) {
		if strings.Contains(pull, need.PartitionID) || strings.Contains(pull, "_partition") || strings.Contains(pull, "PARTITION") {
			t.Fatalf("the pull names a partition: %s", pull)
		}
		word := regexp.MustCompile(`[a-z_]+`)
		for _, q := range []string{resolve, fallback} {
			where := q[strings.Index(q, " WHERE "):]
			selected := strings.TrimPrefix(q[:strings.Index(q, " FROM ")], "SELECT ")
			// Keywords are upper case, so every lower-case word is a column.
			for _, w := range append(word.FindAllString(stripLiterals(where), -1), word.FindAllString(selected, -1)...) {
				if !slices.Contains(readerPartsColumns, w) {
					t.Fatalf("%s names system.parts column %q outside si_reader's grant", q, w)
				}
			}
		}
		for _, q := range []string{resolve, fallback, pull} {
			for _, forbidden := range []string{"user", "password", "host", "port"} {
				if strings.Contains(strings.ToLower(stripLiterals(q)), forbidden+" =") {
					t.Fatalf("%s overrides %s", q, forbidden)
				}
			}
		}
		if strings.Contains(partitionID, " FROM ") || strings.Contains(partitionID, "remote(") {
			t.Fatalf("the partition id query reads a table: %s", partitionID)
		}
	})
	t.Run("refuses_a_collection_that_is_not_one_identifier", func(t *testing.T) {
		conn := &fakeConn{}
		res := NewOwnerPull(conn, "si_owner_0, user = 'default'", "").Fetch(context.Background(), need, pullSchema(), scratch)
		if res.Outcome != OutcomeRefused || res.Err == nil || !strings.Contains(res.Err.Error(), "must be one ClickHouse identifier") || len(conn.all()) != 0 {
			t.Fatalf("Fetch = %+v after %q, want refused before any statement", res, conn.all())
		}
	})
}

// TestOwnerPullFallbackListsAfterItsCursor pins how the row-hash fallback
// makes progress across fetches of one need (Plan S3-C2 Appendix B, review
// fix 245-3): a window of MaxFallbackCandidates that kept nothing leaves its
// last name as the need's cursor, the next listing starts after it (name >
// '<last>'), a short window (the partition's end) or a kept part clears the
// cursor so the next listing starts from the first candidate again, and
// each need, keyed by table, partition id, row hash and row count, keeps its
// own cursor.
func TestOwnerPullFallbackListsAfterItsCursor(t *testing.T) {
	pull := NewOwnerPull(&fakeConn{}, "si_owner_0", "")
	need := Need{TableKey: "dex.swaps", Table: "dex__swaps", PartitionID: "p_2026", PartRowLtHash: "0x01", RowCount: 3}
	from := "SELECT name FROM remote(`si_owner_0`, database = 'system', table = 'parts') WHERE database = 'hg_safe' AND table = 'dex__swaps' AND active AND partition_id = 'abc' AND rows = 3"
	if got, want := pull.fallbackQuery(need, "abc", "", 2), from+" ORDER BY name LIMIT 2"; got != want {
		t.Fatalf("first listing = %s, want %s", got, want)
	}
	if got, want := pull.fallbackQuery(need, "abc", "abc_4_4_0", 2), from+" AND name > 'abc_4_4_0' ORDER BY name LIMIT 2"; got != want {
		t.Fatalf("listing after the cursor = %s, want %s", got, want)
	}
	for _, tc := range []struct {
		listed []string
		limit  int
		want   string
	}{
		{[]string{"abc_1_1_0", "abc_4_4_0"}, 2, "abc_4_4_0"},
		{[]string{"abc_7_7_0"}, 2, ""},
		{nil, 2, ""},
	} {
		if got := nextFallbackCursor(tc.listed, tc.limit); got != tc.want {
			t.Fatalf("nextFallbackCursor(%v, %d) = %q, want %q", tc.listed, tc.limit, got, tc.want)
		}
	}
	key, other := fallbackKey(need, "abc"), fallbackKey(Need{Table: "dex__swaps", PartRowLtHash: "0x02", RowCount: 3}, "abc")
	pull.setCursor(key, "abc_4_4_0")
	if got := pull.cursor(key); got != "abc_4_4_0" {
		t.Fatalf("cursor = %q after a full window, want abc_4_4_0", got)
	}
	if got := pull.cursor(other); got != "" {
		t.Fatalf("another need's cursor = %q, want none", got)
	}
	pull.setCursor(key, "")
	if got := pull.cursor(key); got != "" {
		t.Fatalf("cursor = %q after the listing ended, want none", got)
	}
}

// stripLiterals removes single-quoted literals from a statement.
func stripLiterals(q string) string {
	return regexp.MustCompile(`'(?:[^'\\]|\\.)*'`).ReplaceAllString(q, "''")
}
