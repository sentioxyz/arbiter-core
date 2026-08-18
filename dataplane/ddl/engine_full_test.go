package ddl

import (
	"reflect"
	"testing"
)

func TestParseEngineFullSettings_ReplicatedMergeTreePins(t *testing.T) {
	in := "ReplicatedMergeTree('/sentio/0/unsafe/db__t', 'node-1') PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 3000, max_parts_in_total = 100000, replicated_deduplication_window = 0, index_granularity = 8192"
	got, err := ParseEngineFullSettings(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := map[string]string{
		"max_bytes_to_merge_at_max_space_in_pool": "0",
		"parts_to_delay_insert":                   "1000",
		"parts_to_throw_insert":                   "3000",
		"max_parts_in_total":                      "100000",
		"replicated_deduplication_window":         "0",
		"index_granularity":                       "8192",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("settings = %v want %v", got, want)
	}
}

func TestParseEngineFullSettings_MergeTreeAndTamperedValue(t *testing.T) {
	in := "MergeTree PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, index_granularity = 8192"
	got, err := ParseEngineFullSettings(in)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got["max_bytes_to_merge_at_max_space_in_pool"] != "0" || got["index_granularity"] != "8192" || len(got) != 2 {
		t.Fatalf("settings = %v", got)
	}
	tampered := "ReplicatedMergeTree('/sentio/0/unsafe/db__t', 'node-1') PARTITION BY p ORDER BY (p, _hg_row_id) SETTINGS max_bytes_to_merge_at_max_space_in_pool = 0, parts_to_delay_insert = 1000, parts_to_throw_insert = 2999, max_parts_in_total = 100000, replicated_deduplication_window = 0, index_granularity = 8192"
	got, err = ParseEngineFullSettings(tampered)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got["parts_to_throw_insert"] != "2999" {
		t.Fatalf("parts_to_throw_insert = %q want 2999", got["parts_to_throw_insert"])
	}
}

func TestParseEngineFullSettings_NoSettingsClause(t *testing.T) {
	got, err := ParseEngineFullSettings("MergeTree ORDER BY _hg_row_id")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("settings = %v want empty", got)
	}
}

func TestParseEngineFullSettings_RejectsMalformedPair(t *testing.T) {
	if _, err := ParseEngineFullSettings("MergeTree ORDER BY x SETTINGS index_granularity"); err == nil {
		t.Fatal("malformed setting pair must error")
	}
}
