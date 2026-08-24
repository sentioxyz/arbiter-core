package ddl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrPartitionFreeze is returned for a declared schema outside the P1c
// partition-key freeze: partition_by must be empty or name a declared bare
// String column (no expressions, no other types).
var ErrPartitionFreeze = errors.New("ddl: partition_by must be a bare String column declared in the schema (P1c partition freeze: no expressions, no non-String keys)")

// TableIntent is the structured form of one protocol table. BuildDDL renders
// it and VerifyProtocolTable compares live metadata against it, so both sides
// share one definition.
type TableIntent struct {
	Database      string
	Table         string
	Engine        string
	ZooKeeperPath string // ReplicatedMergeTree only
	ReplicaName   string // ReplicatedMergeTree only
	Columns       []lthash.Column
	PartitionKey  string // "" for unpartitioned
	SortingKey    []string
	Settings      []PinnedSetting
}

// SQL renders the CREATE TABLE IF NOT EXISTS statement for the intent.
func (t TableIntent) SQL() string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s.%s (\n", quoteIdent(t.Database), quoteIdent(t.Table))
	for i, c := range t.Columns {
		sep := ","
		if i == len(t.Columns)-1 {
			sep = ""
		}
		fmt.Fprintf(&b, "    %s %s%s\n", quoteIdent(c.Name), c.Type, sep)
	}
	b.WriteString(") ENGINE = " + t.Engine)
	if t.Engine == EngineReplicatedMergeTree {
		fmt.Fprintf(&b, "(%s, %s)", quoteLiteral(t.ZooKeeperPath), quoteLiteral(t.ReplicaName))
	}
	b.WriteString("\n")
	if t.PartitionKey != "" {
		fmt.Fprintf(&b, "PARTITION BY %s\n", quoteIdent(t.PartitionKey))
	}
	keys := make([]string, 0, len(t.SortingKey))
	for _, k := range t.SortingKey {
		keys = append(keys, quoteIdent(k))
	}
	fmt.Fprintf(&b, "ORDER BY (%s)\n", strings.Join(keys, ", "))
	settings := make([]string, 0, len(t.Settings))
	for _, s := range t.Settings {
		settings = append(settings, s.Name+" = "+s.Value)
	}
	b.WriteString("SETTINGS " + strings.Join(settings, ", "))
	return b.String()
}

// Intents derives the hg_unsafe, hg_safe and hg_promote intents for one
// declared schema. Validation order is frozen: partition freeze (callers may
// skip such a declaration), then the reserved row-id column, then the column
// types. Every check runs before any caller can issue DDL.
func Intents(p Pinned, t payloadexec.TableSchema) (TableIntent, TableIntent, TableIntent, error) {
	if err := validatePartitionFreeze(t); err != nil {
		return TableIntent{}, TableIntent{}, TableIntent{}, fmt.Errorf("table %s: %w", t.TableID, err)
	}
	for _, c := range t.Columns {
		if c.Name == RowIDColumn {
			return TableIntent{}, TableIntent{}, TableIntent{}, fmt.Errorf("table %s: declared schema must not contain %s (the protocol injects it)", t.TableID, RowIDColumn)
		}
	}
	// Spec L D1: a declared type outside the storage-integrity profile's own
	// supported set is rejected before any CREATE. A type ClickHouse would
	// accept but the pinned replay executor cannot materialize diverges
	// silently; a type string that closes the column list would add a column
	// and permanently brick the role, because drift is fail-closed and
	// CREATE TABLE IF NOT EXISTS is a no-op against the existing table.
	canonical, err := payloadexec.CanonicalizeTableSchemaColumnTypes(t)
	if err != nil {
		return TableIntent{}, TableIntent{}, TableIntent{}, fmt.Errorf("table %s: invalid DDL column declaration: %w", t.TableID, err)
	}
	t = canonical
	cols := make([]lthash.Column, 0, len(t.Columns)+1)
	cols = append(cols, lthash.Column{Name: RowIDColumn, Type: RowIDType})
	cols = append(cols, t.Columns...)
	var sorting []string
	if t.PartitionBy != "" {
		sorting = append(sorting, t.PartitionBy)
	}
	sorting = append(sorting, RowIDColumn)
	table := CHTableName(t.TableID)
	unsafe := TableIntent{
		Database: p.UnsafeDB, Table: table, Engine: EngineReplicatedMergeTree,
		ZooKeeperPath: ZooKeeperPath(p, t.TableID), ReplicaName: p.NodeID,
		Columns: cols, PartitionKey: t.PartitionBy, SortingKey: sorting, Settings: UnsafeSettings(),
	}
	safe := TableIntent{
		Database: p.SafeDB, Table: table, Engine: EngineMergeTree,
		Columns: cols, PartitionKey: t.PartitionBy, SortingKey: sorting, Settings: SafeSettings(),
	}
	// Spec L D5: hg_promote is the promotion shadow. It was created ad hoc as
	// `CREATE TABLE ... AS hg_safe.<t>`, i.e. structurally identical to hg_safe
	// but in the promote database, so the intent is exactly that — now rendered
	// and verified like the other two instead of appearing at first promotion.
	promote := safe
	promote.Database = p.PromoteDB
	return unsafe, safe, promote, nil
}

// BuildDDL renders the three CREATE TABLE IF NOT EXISTS statements. Pure;
// golden tested.
func BuildDDL(p Pinned, t payloadexec.TableSchema) (string, string, string, error) {
	unsafe, safe, promote, err := Intents(p, t)
	if err != nil {
		return "", "", "", err
	}
	return unsafe.SQL(), safe.SQL(), promote.SQL(), nil
}

func validatePartitionFreeze(t payloadexec.TableSchema) error {
	if t.PartitionBy == "" {
		return nil
	}
	if strings.ContainsAny(t.PartitionBy, "()") {
		return fmt.Errorf("%w: got expression %q", ErrPartitionFreeze, t.PartitionBy)
	}
	for _, c := range t.Columns {
		if c.Name == t.PartitionBy {
			if c.Type != "String" {
				return fmt.Errorf("%w: column %q has type %s", ErrPartitionFreeze, t.PartitionBy, c.Type)
			}
			return nil
		}
	}
	return fmt.Errorf("%w: %q names no declared column", ErrPartitionFreeze, t.PartitionBy)
}
