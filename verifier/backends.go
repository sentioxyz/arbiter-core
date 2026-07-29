package verifier

import (
	"context"
	"fmt"
	"sort"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

// NewReplayCore assembles the real replay verifier for this verifier role.
func NewReplayCore(cfg Config, conn clickhouse.Conn, manifests replay.SnapshotStore, payloads replay.PayloadStore) (*replay.Verifier, error) {
	signer, err := payloadexec.NewEd25519Signer(cfg.ReplicaID, cfg.Ed25519Seed)
	if err != nil {
		return nil, fmt.Errorf("verifier signer: %w", err)
	}
	return &replay.Verifier{
		Snapshots: manifests,
		Payloads:  payloads,
		Executor:  payloadexec.NewWithMaterializer(cfg.NetworkID, chexec.NewMaterializer(cfg.NetworkID, conn), cfg.Tables...),
		Signer:    signer,
	}, nil
}

// CHScanner recomputes byte-side part commitments from this verifier's ClickHouse.
type CHScanner struct {
	cfg  Config
	conn clickhouse.Conn
}

// NewScanner builds a ClickHouse-backed byte-side scanner.
func NewScanner(cfg Config, conn clickhouse.Conn) *CHScanner {
	if cfg.UnsafeDatabase == "" {
		cfg.UnsafeDatabase = defaultUnsafeDatabase
	}
	return &CHScanner{cfg: cfg, conn: conn}
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
		qualified := s.cfg.UnsafeDatabase + "." + chTableName(tableID)
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

func (s *CHScanner) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	for _, t := range s.cfg.Tables {
		if t.TableID == tableID {
			return t, nil
		}
	}
	return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
}

func chTableName(tableID string) string {
	return strings.ReplaceAll(tableID, ".", "__")
}
