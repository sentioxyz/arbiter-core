package chparts

import (
	"slices"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// partitionedSchema is a protocol table partitioned by a bare String column,
// the only partition key the P1c freeze admits.
func partitionedSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "chparts.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
}

func unpartitionedSchema() payloadexec.TableSchema {
	s := partitionedSchema()
	s.TableID = "chparts.u"
	s.PartitionBy = ""
	return s
}

func TestLogicalPartitionID(t *testing.T) {
	for _, tc := range []struct {
		name string
		sch  payloadexec.TableSchema
		part Part
		want string
	}{
		{"unpartitioned table", unpartitionedSchema(), Part{PartitionID: "all", PartitionValue: "tuple()"}, "all"},
		{"partitioned table", partitionedSchema(), Part{PartitionID: "4fd2cf8bb1b5f1ad19f5a1f6e2a2a6bd", PartitionValue: "2026-10"}, "p_2026-10"},
		{"the value is taken verbatim, not the ClickHouse partition_id", partitionedSchema(), Part{PartitionID: "all", PartitionValue: `a'b\c`}, `p_a'b\c`},
		{"an empty value", partitionedSchema(), Part{PartitionValue: ""}, "p_"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := LogicalPartitionID(tc.sch, tc.part); got != tc.want {
				t.Fatalf("LogicalPartitionID = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestQuotePartition(t *testing.T) {
	for _, tc := range []struct {
		name        string
		sch         payloadexec.TableSchema
		partitionID string
		want        string
		wantErr     string
	}{
		{"unpartitioned all", unpartitionedSchema(), "all", "tuple()", ""},
		{"unpartitioned refuses a value", unpartitionedSchema(), "p_a", "", "unpartitioned table wants partition all, got p_a"},
		{"partitioned value", partitionedSchema(), "p_a", "'a'", ""},
		{"partitioned value is escaped", partitionedSchema(), `p_a'b\c`, `'a\'b\\c'`, ""},
		{"partitioned refuses all", partitionedSchema(), "all", "", "partition id all does not use p_<value> form"},
		{"partitioned refuses an empty value", partitionedSchema(), "p_", "", "partition id p_ does not use p_<value> form"},
		{"partitioned refuses another prefix", partitionedSchema(), "q_a", "", "partition id q_a does not use p_<value> form"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := QuotePartition(tc.sch, tc.partitionID)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("QuotePartition = %q, %v; want error %q", got, err, tc.wantErr)
				}
				return
			}
			if err != nil || got != tc.want {
				t.Fatalf("QuotePartition = %q, %v; want %q", got, err, tc.want)
			}
		})
	}
}

func TestPartsInLogicalPartition(t *testing.T) {
	parts := []Part{
		{Name: "a_1_1_0", PartitionValue: "a"},
		{Name: "b_2_2_0", PartitionValue: "b"},
		{Name: "a_3_3_0", PartitionValue: "a"},
	}
	names := func(ps []Part) []string {
		out := make([]string, 0, len(ps))
		for _, p := range ps {
			out = append(out, p.Name)
		}
		return out
	}
	if got := names(PartsInLogicalPartition(parts, partitionedSchema(), "p_a")); !slices.Equal(got, []string{"a_1_1_0", "a_3_3_0"}) {
		t.Fatalf("p_a = %v, want [a_1_1_0 a_3_3_0] in input order", got)
	}
	if got := names(PartsInLogicalPartition(parts, partitionedSchema(), "p_b")); !slices.Equal(got, []string{"b_2_2_0"}) {
		t.Fatalf("p_b = %v, want [b_2_2_0]", got)
	}
	none := PartsInLogicalPartition(parts, partitionedSchema(), "p_c")
	if none == nil || len(none) != 0 {
		t.Fatalf("p_c = %#v, want an empty non-nil slice", none)
	}
	if got := names(PartsInLogicalPartition(parts, unpartitionedSchema(), "all")); !slices.Equal(got, []string{"a_1_1_0", "b_2_2_0", "a_3_3_0"}) {
		t.Fatalf("all of an unpartitioned table = %v, want every part", got)
	}
}

func TestEscapeSQLString(t *testing.T) {
	for in, want := range map[string]string{
		"plain":      "plain",
		`a'b`:        `a\'b`,
		`a\b`:        `a\\b`,
		`\'`:         `\\\'`,
		`all_1_1_0`:  `all_1_1_0`,
		`'; DROP --`: `\'; DROP --`,
	} {
		if got := EscapeSQLString(in); got != want {
			t.Fatalf("EscapeSQLString(%q) = %q, want %q", in, got, want)
		}
	}
}
