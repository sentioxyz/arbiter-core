package ddl

import (
	"reflect"
	"testing"
)

func TestParseMetadataIdentifiers_ClickHouseQuoting(t *testing.T) {
	tests := map[string]struct {
		input string
		want  []string
	}{
		"bare":               {input: "p, _hg_row_id", want: []string{"p", "_hg_row_id"}},
		"space":              {input: "`part key`, `_hg_row_id`", want: []string{"part key", "_hg_row_id"}},
		"comma":              {input: "`part,key`, `_hg_row_id`", want: []string{"part,key", "_hg_row_id"}},
		"backslash backtick": {input: "`part,\\`key`, _hg_row_id", want: []string{"part,`key", "_hg_row_id"}},
		"doubled backtick":   {input: "`part``key`, _hg_row_id", want: []string{"part`key", "_hg_row_id"}},
		"empty":              {input: "", want: nil},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := parseMetadataIdentifiers(tt.input)
			if err != nil {
				t.Fatalf("parseMetadataIdentifiers(%q): %v", tt.input, err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseMetadataIdentifiers(%q) = %#v, want %#v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseMetadataIdentifiers_RejectsMalformedLists(t *testing.T) {
	for _, input := range []string{"`unterminated", "`p` trailing", "p,,q", ",p"} {
		t.Run(input, func(t *testing.T) {
			if _, err := parseMetadataIdentifiers(input); err == nil {
				t.Fatalf("parseMetadataIdentifiers(%q) must fail", input)
			}
		})
	}
}
