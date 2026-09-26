package verifier

import (
	"context"
	"fmt"
	"sort"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// NewReplayCore assembles the real replay verifier for this verifier role.
func NewReplayCore(cfg Config, conn clickhouse.Conn, manifests replay.SnapshotStore, payloads replay.PayloadStore) (*replay.Verifier, error) {
	signer, err := payloadexec.NewEd25519Signer(cfg.ReplicaID, cfg.Ed25519Seed)
	if err != nil {
		return nil, fmt.Errorf("verifier signer: %w", err)
	}
	// NewDynamic: the configured tables are the genesis set; a table-set
	// transition job and the schemas a job carries extend it per job, so the
	// verifier needs no registry access (dynamic SI table set, sub-project 2b).
	payload := payloadexec.NewDynamic(cfg.NetworkID, chexec.NewMaterializer(cfg.NetworkID, conn), cfg.Tables...)
	// Query routes stay empty so the snapshot-query lane remains default-off.
	dispatcher, err := snapshotquery.NewCompositeExecutor(payload, nil)
	if err != nil {
		return nil, fmt.Errorf("verifier dispatcher: %w", err)
	}
	return &replay.Verifier{
		Snapshots:    manifests,
		Payloads:     payloads,
		Executor:     dispatcher,
		Signer:       signer,
		SchemaHashes: payloadexec.SchemaHashes{NetworkID: cfg.NetworkID, Tables: cfg.Tables},
	}, nil
}

// SnapshotQueryReplayCore adapts the query-only verifier to the role's
// dispatch port. Construction is explicit: callers that do not install a
// verified query route and historical policy leave the role default-off.
type SnapshotQueryReplayCore struct {
	verifier *snapshotquery.Verifier
}

// NewSnapshotQueryReplayCore installs no routes or authority itself. The
// caller must construct snapshotquery.Verifier with its immutable dispatcher,
// signer, and authenticated historical policy before injecting this core.
func NewSnapshotQueryReplayCore(v *snapshotquery.Verifier) (*SnapshotQueryReplayCore, error) {
	if v == nil {
		return nil, fmt.Errorf("snapshot query verifier is required")
	}
	return &SnapshotQueryReplayCore{verifier: v}, nil
}

// VerifySnapshotQuery forwards the exact trusted reference without deriving
// or normalising it. snapshotquery.Verifier owns the query-specific validation
// and receipt signing order.
func (c *SnapshotQueryReplayCore) VerifySnapshotQuery(ctx context.Context, job replay.SnapshotQueryJob, referenceID string) (replay.SnapshotQueryAttestation, error) {
	if c == nil || c.verifier == nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("snapshot query verifier is not configured")
	}
	return c.verifier.Verify(ctx, snapshotquery.VerifyRequest{Job: job, ReferenceID: referenceID})
}

// CHScanner recomputes byte-side part commitments from this verifier's ClickHouse.
type CHScanner struct {
	cfg      Config
	conn     clickhouse.Conn
	registry dataplane.RegistryView
}

// NewScanner builds a ClickHouse-backed byte-side scanner over the
// configured tables only.
func NewScanner(cfg Config, conn clickhouse.Conn) *CHScanner {
	return NewRegistryScanner(cfg, conn, nil)
}

// NewRegistryScanner builds a scanner that resolves tables through the table
// registry while it is enabled (nil registry: configured tables only).
func NewRegistryScanner(cfg Config, conn clickhouse.Conn, registry dataplane.RegistryView) *CHScanner {
	if cfg.UnsafeDatabase == "" {
		cfg.UnsafeDatabase = defaultUnsafeDatabase
	}
	return &CHScanner{cfg: cfg, conn: conn, registry: registry}
}

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
		sch, err := s.schemaFor(tableID)
		if err != nil {
			return nil, err
		}
		refs := byTable[tableID]
		names := make([]string, 0, len(refs))
		for _, p := range refs {
			names = append(names, p.PartName)
		}
		qualified := s.cfg.UnsafeDatabase + "." + ddl.CHTableName(tableID)
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

// schemaFor resolves the table a scan names, by the SNode's rule. With an
// enabled registry the key's live incarnation decides (registrySchema): a
// genesis-origin one uses the configured schema, a chain-origin one its
// registry schema_json verified against its schema_hash, so a same-name
// recreation of a retired genesis table is scanned with its new schema. Only
// while the registry is disabled (or not followed) do the configured genesis
// tables apply.
func (s *CHScanner) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	if s.registry != nil {
		if snap, enabled := s.registry.View(); enabled {
			return registrySchema(s.cfg.NetworkID, s.cfg.Tables, snap, tableID)
		}
	}
	if t, ok := genesisSchema(s.cfg.Tables, tableID); ok {
		return t, nil
	}
	return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
}
