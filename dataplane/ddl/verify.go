package ddl

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
)

var (
	// ErrProtocolTableDrift reports an existing table that differs from the
	// pinned intent. Roles fail closed; v1 never auto-alters a drifted table.
	ErrProtocolTableDrift = errors.New("ddl: protocol table drift")
	// ErrProtocolTableMissing reports a table absent in verify-only mode.
	ErrProtocolTableMissing = errors.New("ddl: protocol table missing")
)

// VerifyProtocolTable compares live metadata against a pinned table intent.
// Every mismatch is joined into one error naming the table and field.
func VerifyProtocolTable(ctx context.Context, conn clickhouse.Conn, want TableIntent) error {
	qualified := want.Database + "." + want.Table
	var engine, engineFull, sortingKey, partitionKey string
	err := conn.QueryRow(ctx, `
		SELECT engine, engine_full, sorting_key, partition_key
		FROM system.tables WHERE database = ? AND name = ?`, want.Database, want.Table,
	).Scan(&engine, &engineFull, &sortingKey, &partitionKey)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("%w: %s", ErrProtocolTableMissing, qualified)
		}
		return fmt.Errorf("ddl: read system.tables for %s: %w", qualified, err)
	}
	var drifts []error
	drift := func(field, got, expected string) {
		drifts = append(drifts, fmt.Errorf("%w: %s %s: got %q want %q", ErrProtocolTableDrift, qualified, field, got, expected))
	}
	if engine != want.Engine {
		drift("engine", engine, want.Engine)
	}
	wantSortingKey := strings.Join(want.SortingKey, ", ")
	gotSortingKey, sortingErr := parseMetadataIdentifiers(sortingKey)
	if sortingErr != nil || !equalIdentifiers(gotSortingKey, want.SortingKey) {
		drift("sorting_key", sortingKey, wantSortingKey)
	}
	wantPartitionKey := []string(nil)
	if want.PartitionKey != "" {
		wantPartitionKey = []string{want.PartitionKey}
	}
	gotPartitionKey, partitionErr := parseMetadataIdentifiers(partitionKey)
	if partitionErr != nil || !equalIdentifiers(gotPartitionKey, wantPartitionKey) {
		drift("partition_key", partitionKey, want.PartitionKey)
	}

	columns, err := readColumns(ctx, conn, want.Database, want.Table)
	if err != nil {
		return err
	}
	if !equalColumns(columns, want.Columns) {
		drift("columns", formatColumns(columns), formatColumns(want.Columns))
	}

	overrides, err := ParseEngineFullSettings(engineFull)
	if err != nil {
		return fmt.Errorf("ddl: %s: %w", qualified, err)
	}
	for _, setting := range want.Settings {
		got, ok := overrides[setting.Name]
		if !ok {
			drift("setting "+setting.Name, "<missing explicit override>", setting.Value)
			continue
		}
		if got != setting.Value {
			drift("setting "+setting.Name, got, setting.Value)
		}
	}

	if want.Engine == EngineReplicatedMergeTree && engine == EngineReplicatedMergeTree {
		var zkPath, replica string
		err := conn.QueryRow(ctx, `SELECT zookeeper_path, replica_name FROM system.replicas WHERE database = ? AND table = ?`, want.Database, want.Table).Scan(&zkPath, &replica)
		if err != nil {
			return fmt.Errorf("ddl: read system.replicas for %s: %w", qualified, err)
		}
		if zkPath != want.ZooKeeperPath {
			drift("zookeeper_path", zkPath, want.ZooKeeperPath)
		}
		if replica != want.ReplicaName {
			drift("replica_name", replica, want.ReplicaName)
		}
	}
	return errors.Join(drifts...)
}

func readColumns(ctx context.Context, conn clickhouse.Conn, database, table string) ([]lthash.Column, error) {
	rows, err := conn.Query(ctx, `SELECT name, type FROM system.columns WHERE database = ? AND table = ? ORDER BY position`, database, table)
	if err != nil {
		return nil, fmt.Errorf("ddl: read system.columns for %s.%s: %w", database, table, err)
	}
	defer rows.Close()
	var columns []lthash.Column
	for rows.Next() {
		var column lthash.Column
		if err := rows.Scan(&column.Name, &column.Type); err != nil {
			return nil, fmt.Errorf("ddl: scan system.columns: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ddl: read system.columns rows: %w", err)
	}
	return columns, nil
}

func equalColumns(a, b []lthash.Column) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Name != b[i].Name || a[i].Type != b[i].Type {
			return false
		}
	}
	return true
}

func equalIdentifiers(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// parseMetadataIdentifiers canonicalizes the identifier lists ClickHouse
// renders in system.tables.sorting_key and partition_key. Simple identifiers
// are emitted bare; identifiers needing quoting use backticks, with either a
// backslash escape (ClickHouse's metadata rendering) or doubled backticks.
func parseMetadataIdentifiers(input string) ([]string, error) {
	input = strings.TrimSpace(input)
	if input == "" {
		return nil, nil
	}
	var identifiers []string
	for pos := 0; pos < len(input); {
		for pos < len(input) && isMetadataSpace(input[pos]) {
			pos++
		}
		if pos == len(input) || input[pos] == ',' {
			return nil, fmt.Errorf("ddl: empty identifier in metadata key %q", input)
		}

		var identifier string
		if input[pos] == '`' {
			pos++
			var b strings.Builder
			closed := false
			for pos < len(input) {
				switch input[pos] {
				case '\\':
					if pos+1 == len(input) {
						return nil, fmt.Errorf("ddl: trailing escape in metadata key %q", input)
					}
					escaped := input[pos+1]
					switch escaped {
					case 'a':
						b.WriteByte('\a')
					case 'b':
						b.WriteByte('\b')
					case 'e':
						b.WriteByte(0x1b)
					case 'f':
						b.WriteByte('\f')
					case 'n':
						b.WriteByte('\n')
					case 'r':
						b.WriteByte('\r')
					case 't':
						b.WriteByte('\t')
					case 'v':
						b.WriteByte('\v')
					case '0':
						b.WriteByte(0)
					case '\\', '`':
						b.WriteByte(escaped)
					case 'x':
						if pos+3 >= len(input) {
							return nil, fmt.Errorf("ddl: truncated hex escape in metadata key %q", input)
						}
						hi, okHi := metadataHexNibble(input[pos+2])
						lo, okLo := metadataHexNibble(input[pos+3])
						if !okHi || !okLo {
							return nil, fmt.Errorf("ddl: malformed hex escape in metadata key %q", input)
						}
						b.WriteByte(hi<<4 | lo)
						pos += 4
						continue
					default:
						return nil, fmt.Errorf("ddl: unsupported escape \\%c in metadata key %q", escaped, input)
					}
					pos += 2
				case '`':
					if pos+1 < len(input) && input[pos+1] == '`' {
						b.WriteByte('`')
						pos += 2
						continue
					}
					pos++
					closed = true
				default:
					b.WriteByte(input[pos])
					pos++
				}
				if closed {
					break
				}
			}
			if !closed {
				return nil, fmt.Errorf("ddl: unterminated quoted identifier in metadata key %q", input)
			}
			identifier = b.String()
			for pos < len(input) && isMetadataSpace(input[pos]) {
				pos++
			}
			if pos < len(input) && input[pos] != ',' {
				return nil, fmt.Errorf("ddl: trailing text after quoted identifier in metadata key %q", input)
			}
		} else {
			start := pos
			for pos < len(input) && input[pos] != ',' {
				pos++
			}
			identifier = strings.TrimSpace(input[start:pos])
			if !isBareMetadataIdentifier(identifier) {
				return nil, fmt.Errorf("ddl: malformed bare identifier %q in metadata key %q", identifier, input)
			}
		}
		identifiers = append(identifiers, identifier)

		if pos == len(input) {
			break
		}
		pos++
		for pos < len(input) && isMetadataSpace(input[pos]) {
			pos++
		}
		if pos == len(input) {
			return nil, fmt.Errorf("ddl: trailing comma in metadata key %q", input)
		}
	}
	return identifiers, nil
}

func isMetadataSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\r' || b == '\n'
}

// ClickHouse's unquoted identifier grammar is
// ^[a-zA-Z_][0-9a-zA-Z_]*$. Anything else in key metadata is an expression,
// not a bare column identifier, even if its text matches a quoted column name.
func isBareMetadataIdentifier(identifier string) bool {
	if identifier == "" || !isMetadataIdentifierStart(identifier[0]) {
		return false
	}
	for i := 1; i < len(identifier); i++ {
		if !isMetadataIdentifierStart(identifier[i]) && (identifier[i] < '0' || identifier[i] > '9') {
			return false
		}
	}
	return true
}

func isMetadataIdentifierStart(b byte) bool {
	return b == '_' || b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

func metadataHexNibble(b byte) (byte, bool) {
	switch {
	case b >= '0' && b <= '9':
		return b - '0', true
	case b >= 'a' && b <= 'f':
		return b - 'a' + 10, true
	case b >= 'A' && b <= 'F':
		return b - 'A' + 10, true
	default:
		return 0, false
	}
}

func formatColumns(columns []lthash.Column) string {
	parts := make([]string, 0, len(columns))
	for _, column := range columns {
		parts = append(parts, column.Name+" "+column.Type)
	}
	return strings.Join(parts, ", ")
}
