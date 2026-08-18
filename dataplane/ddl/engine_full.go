package ddl

import (
	"fmt"
	"strings"
)

const settingsClause = " SETTINGS "

// ParseEngineFullSettings extracts the per-table MergeTree setting overrides
// from system.tables.engine_full. ClickHouse renders them as the trailing
// "SETTINGS name = value, name = value" clause (it always appends
// index_granularity). Values are integers or quoted literals; the pinned
// settings compared by VerifyProtocolTable are all plain integers, so a
// comma-split on ", " is exact for them.
func ParseEngineFullSettings(engineFull string) (map[string]string, error) {
	out := map[string]string{}
	idx := strings.LastIndex(engineFull, settingsClause)
	if idx < 0 {
		return out, nil
	}
	tail := strings.TrimSpace(engineFull[idx+len(settingsClause):])
	if tail == "" {
		return out, nil
	}
	for _, pair := range strings.Split(tail, ", ") {
		name, value, ok := strings.Cut(pair, " = ")
		if !ok || strings.TrimSpace(name) == "" || strings.TrimSpace(value) == "" {
			return nil, fmt.Errorf("ddl: malformed engine_full setting %q in %q", pair, engineFull)
		}
		out[strings.TrimSpace(name)] = strings.TrimSpace(value)
	}
	return out, nil
}
