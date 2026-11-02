package evidence

import (
	"context"
	"fmt"
	"slices"
	"sort"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// CHScanner recomputes byte-side part commitments from the replicated tables
// of one database of the replica's ClickHouse: the verifier's hg_unsafe, a
// member's hg_member_unsafe.
type CHScanner struct {
	networkID string
	tables    []payloadexec.TableSchema
	database  string
	conn      clickhouse.Conn
	registry  dataplane.RegistryView
}

// NewCHScanner builds a scanner of database's tables. tables is the genesis
// table set; with a registry that is enabled, each table resolves through it
// (dataplane.RegistrySchema), and only while it is disabled (or nil) do the
// genesis tables apply.
func NewCHScanner(networkID string, tables []payloadexec.TableSchema, database string, conn clickhouse.Conn, registry dataplane.RegistryView) *CHScanner {
	return &CHScanner{networkID: networkID, tables: slices.Clone(tables), database: database, conn: conn, registry: registry}
}

// Database is the database the scanner reads.
func (s *CHScanner) Database() string { return s.database }

// Registry is the registry view the scanner follows; nil for none.
func (s *CHScanner) Registry() dataplane.RegistryView { return s.registry }

// Scan recomputes the row LtHash for every requested active part.
func (s *CHScanner) Scan(ctx context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error) {
	byTable := map[string][]arbiter.PartRef{}
	tableOrder := []string{}
	for _, p := range parts {
		if p.PartName == "" {
			return nil, fmt.Errorf("scan request part without a name (table %s partition %s)", p.TableID, p.PartitionID)
		}
		if _, ok := byTable[p.TableID]; !ok {
			tableOrder = append(tableOrder, p.TableID)
		}
		byTable[p.TableID] = append(byTable[p.TableID], p)
	}
	sort.Strings(tableOrder)

	out := make([]arbiter.PartScan, 0, len(parts))
	for _, tableID := range tableOrder {
		sch, err := s.SchemaFor(tableID)
		if err != nil {
			return nil, err
		}
		refs := byTable[tableID]
		names := make([]string, 0, len(refs))
		for _, p := range refs {
			names = append(names, p.PartName)
		}
		qualified := s.database + "." + ddl.CHTableName(tableID)
		results, err := chexec.ScanParts(ctx, s.conn, qualified, sch, names)
		if err != nil {
			return nil, fmt.Errorf("scan table %s: %w", tableID, err)
		}
		byName := map[string]chexec.PartScanResult{}
		for _, r := range results {
			byName[r.PartName] = r
		}
		for _, p := range refs {
			r, ok := byName[p.PartName]
			if !ok {
				return nil, fmt.Errorf("part %s missing from scan results", p.PartName)
			}
			out = append(out, arbiter.PartScan{
				TableID:              p.TableID,
				PartitionID:          p.PartitionID,
				ClaimedPartRowLtHash: p.PartRowLtHash,
				ScannedPartRowLtHash: r.RowLtHash,
				LivePartName:         r.PartName,
			})
		}
	}
	return out, nil
}

// SchemaFor resolves the table a scan names, by the SNode's rule. With an
// enabled registry the key's live incarnation decides
// (dataplane.RegistrySchema, shared with the SNode): a genesis-origin one uses
// the configured schema, a chain-origin one its registry schema_json verified
// against its schema_hash, so a same-name recreation of a retired genesis
// table is scanned with its new schema. Only while the registry is disabled
// (or not followed) do the configured genesis tables apply.
func (s *CHScanner) SchemaFor(tableID string) (payloadexec.TableSchema, error) {
	if s.registry != nil {
		if snap, enabled := s.registry.View(); enabled {
			return dataplane.RegistrySchema(s.networkID, s.tables, snap, tableID)
		}
	}
	for _, t := range s.tables {
		if t.TableID == tableID {
			return t, nil
		}
	}
	return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
}
