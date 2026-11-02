package sources

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// TestMemberHardlinkPureFakeConfinesDropsAndKeepsOneHash exercises the
// intended operations without claiming real ClickHouse hard-link mechanics.
func TestMemberHardlinkPureFakeConfinesDropsAndKeepsOneHash(t *testing.T) {
	conn := &fakeConn{}
	kept := map[string]string{"a": "wanted", "b": "foreign", "c": "wanted"}
	conn.rows = func(query string, args []any) ([][]any, error) {
		if strings.Contains(query, "hash_of_all_files = ?") {
			if !reflect.DeepEqual(args, []any{"hg_member_unsafe", "dex__pools", "wanted"}) {
				t.Fatalf("source lookup arguments %v", args)
			}
			return [][]any{{"source_part"}}, nil
		}
		if !reflect.DeepEqual(args, []any{"hg_rstage", "member_b_01234567_1"}) {
			t.Fatalf("scratch lookup arguments %v", args)
		}
		if strings.Contains(query, "name, hash_of_all_files") {
			return [][]any{{"a", "wanted"}, {"b", "foreign"}, {"c", "wanted"}}, nil
		}
		var rows [][]any
		for _, name := range []string{"a", "b", "c"} {
			if _, ok := kept[name]; ok {
				rows = append(rows, []any{name})
			}
		}
		return rows, nil
	}
	conn.exec = func(query string) error {
		if strings.HasSuffix(query, " DROP PART 'b'") {
			delete(kept, "b")
		} else if strings.HasSuffix(query, " DROP PART 'c'") {
			delete(kept, "c")
		}
		return nil
	}
	src := NewMemberUnsafeHardlink(conn, "hg_member_unsafe")
	got := src.Fetch(context.Background(), Need{Table: "dex__pools", PartPhysHash: "wanted", PartitionID: "p_a"}, testSchema("dex"), Scratch{Database: "hg_rstage", Table: "member_b_01234567_1"})
	if got.Outcome != OutcomeFetched || got.Err != nil || !reflect.DeepEqual(got.Parts, []string{"a"}) {
		t.Fatalf("fetch = %+v, remaining %+v", got, kept)
	}
	want := []string{
		"exec ALTER TABLE `hg_rstage`.`member_b_01234567_1` ATTACH PARTITION 'a' FROM `hg_member_unsafe`.`dex__pools`",
		"exec ALTER TABLE `hg_rstage`.`member_b_01234567_1` DROP PART 'b'",
		"exec ALTER TABLE `hg_rstage`.`member_b_01234567_1` DROP PART 'c'",
	}
	if got := conn.matching("exec "); !reflect.DeepEqual(got, want) {
		t.Fatalf("intended mutations %v, want %v", got, want)
	}
}

func TestMemberHardlinkPureFakeMissAndAttachError(t *testing.T) {
	for _, missing := range []bool{true, false} {
		t.Run(map[bool]string{true: "missing", false: "attach failure"}[missing], func(t *testing.T) {
			conn := &fakeConn{rows: func(string, []any) ([][]any, error) {
				if missing {
					return nil, nil
				}
				return [][]any{{"source_part"}}, nil
			}}
			conn.exec = func(string) error { return errors.New("attach refused") }
			got := NewMemberUnsafeHardlink(conn, "hg_member_unsafe").Fetch(context.Background(), Need{Table: "dex__pools", PartPhysHash: "wanted", PartitionID: "p_a"}, testSchema("dex"), Scratch{Database: "hg_rstage", Table: "member_b_01234567_1"})
			if missing {
				if got.Outcome != OutcomeMissed || got.Err != nil || len(conn.matching("exec ")) != 0 {
					t.Fatalf("missing part mutated scratch: %+v, %v", got, conn.all())
				}
			} else if got.Err == nil || got.Outcome == OutcomeFetched || len(conn.matching("exec ")) != 1 {
				t.Fatalf("attach failure not propagated: %+v, %v", got, conn.all())
			}
		})
	}
}
