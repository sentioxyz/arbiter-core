package ddl

import (
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrPartitionFreeze is returned for a declared schema whose partition key
// the compiled housegate cannot derive partitions for: partition_by must be
// empty or a key payloadexec.ParsePartitionKey admits (table admission rules
// v3: a bare String, integer, Date, Date32 or UTC DateTime/DateTime64 column,
// or an admitted partition expression).
var ErrPartitionFreeze = errors.New("ddl: partition_by is not a partition key this binary can replay (it must be empty or a key admitted by table admission rules v3)")

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
	// PartitionKey is the partition key as ClickHouse prints it in
	// system.tables.partition_key: "" for unpartitioned, a bare column name,
	// or an admitted partition expression (it contains a parenthesis).
	PartitionKey string
	SortingKey    []string
	Settings      []PinnedSetting
	// Comment marks the tables of one chain-origin registry incarnation
	// (IncarnationComment). Genesis tables carry none.
	Comment string
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
		fmt.Fprintf(&b, "PARTITION BY %s\n", partitionKeySQL(t.PartitionKey))
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
	if t.Comment != "" {
		b.WriteString("\nCOMMENT " + quoteLiteral(t.Comment))
	}
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
	// The sorting key leads with the partition key's source column: the
	// column itself for a bare key, the expression's argument otherwise.
	key, err := payloadexec.ParsePartitionKey(t)
	if err != nil {
		return TableIntent{}, TableIntent{}, TableIntent{}, fmt.Errorf("table %s: %w: %v", t.TableID, ErrPartitionFreeze, err)
	}
	var sorting []string
	if key.IsPartitioned() {
		sorting = append(sorting, key.Column())
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

// ValidatePartitionFreeze is the role-config entry point for the partition
// freeze (partition_by must be empty or a partition key the compiled
// housegate admits). Roles
// call it so a freeze violation fails both of them identically at startup,
// rather than bricking one and leaving the other silently without protocol
// tables for that table.
func ValidatePartitionFreeze(t payloadexec.TableSchema) error { return validatePartitionFreeze(t) }

func validatePartitionFreeze(t payloadexec.TableSchema) error {
	if _, err := payloadexec.ParsePartitionKey(t); err != nil {
		return fmt.Errorf("%w: %v", ErrPartitionFreeze, err)
	}
	return nil
}

// isPartitionExpression reports whether an admitted partition key is an
// expression rather than a bare column: only an expression has parentheses
// (payloadexec.ParsePartitionKey never admits a bare name with one).
func isPartitionExpression(key string) bool { return strings.ContainsAny(key, "()") }

// partitionKeySQL renders an admitted partition key: a bare column quoted, an
// expression verbatim (its column is a plain identifier that both supported
// ClickHouse versions print unquoted).
func partitionKeySQL(key string) string {
	if isPartitionExpression(key) {
		return key
	}
	return quoteIdent(key)
}
