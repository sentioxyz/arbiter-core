package wire

import (
	"fmt"
	"slices"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

// TableIncarnationStatus mirrors arbiter fsm.TableIncarnationStatus: the same
// lowercase spellings, one per pb.TableIncarnationStatus value.
type TableIncarnationStatus string

const (
	TableStatusLegacy   TableIncarnationStatus = "legacy"
	TableStatusPending  TableIncarnationStatus = "pending"
	TableStatusRefused  TableIncarnationStatus = "refused"
	TableStatusActive   TableIncarnationStatus = "active"
	TableStatusRetiring TableIncarnationStatus = "retiring"
	TableStatusPurging  TableIncarnationStatus = "purging"
	TableStatusPurged   TableIncarnationStatus = "purged"
)

// TableOrigin mirrors arbiter fsm.TableOrigin.
type TableOrigin string

const (
	TableOriginGenesis TableOrigin = "genesis"
	TableOriginLegacy  TableOrigin = "legacy"
	TableOriginChain   TableOrigin = "chain"
)

var tableStatusFromPB = map[pb.TableIncarnationStatus]TableIncarnationStatus{
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_LEGACY:   TableStatusLegacy,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PENDING:  TableStatusPending,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_REFUSED:  TableStatusRefused,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE:   TableStatusActive,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_RETIRING: TableStatusRetiring,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGING:  TableStatusPurging,
	pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGED:   TableStatusPurged,
}

var tableOriginFromPB = map[pb.TableIncarnationOrigin]TableOrigin{
	pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_GENESIS: TableOriginGenesis,
	pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_LEGACY:  TableOriginLegacy,
	pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_CHAIN:   TableOriginChain,
}

// TableRegistryCursor mirrors pb.TableRegistryCursor.
type TableRegistryCursor struct {
	BlockNumber   uint64
	BlockHash     string
	LogIndex      uint64
	BlockComplete bool
}

// TableIncarnation mirrors pb.TableIncarnation (arbiter fsm.TableIncarnation).
type TableIncarnation struct {
	Seq            uint64
	DatabaseID     string
	TableID        string
	Origin         TableOrigin
	Status         TableIncarnationStatus
	Created        *arbiter.L2EventRef
	SchemaRef      *arbiter.L2EventRef
	SchemaVersion  uint32
	SchemaHash     string
	SchemaJSON     string
	RefusedReason  string
	RefusedCode    string
	Deleted        *arbiter.L2EventRef
	RetireReason   TableRetireReason
	AddBlockSeq    uint64
	RetireBlockSeq uint64
	PurgedBy       []string
	// OwnerIndexerID is the SI indexer owning the incarnation (housegate spec
	// 2026-10-10 D4); nil for one recorded before the signed-claims
	// activation, whose owner is the founding indexer (see Owner).
	OwnerIndexerID *uint64
}

// Key is the logical table id <database>.<table>.
func (t TableIncarnation) Key() string { return arbiter.TableKey(t.DatabaseID, t.TableID) }

// HasPhysicalTables reports whether data-plane nodes hold hg_* tables for the
// incarnation: Pending, Active, Retiring and Purging, as arbiter's
// fsm.TableIncarnation.hasPhysicalTables.
func (t TableIncarnation) HasPhysicalTables() bool {
	switch t.Status {
	case TableStatusPending, TableStatusActive, TableStatusRetiring, TableStatusPurging:
		return true
	}
	return false
}

// Schema decodes the registry's schema_json. A genesis incarnation carries no
// schema_json (its schema is configured statically), so it reports an error.
func (t TableIncarnation) Schema() (payloadexec.TableSchema, error) {
	if t.SchemaJSON == "" {
		return payloadexec.TableSchema{}, fmt.Errorf("table registry: incarnation %d of %s has no schema_json", t.Seq, t.Key())
	}
	return payloadexec.DecodeTableSchemaJSON(t.Key(), t.SchemaJSON)
}

// TableRegistrySnapshot mirrors pb.TableRegistrySnapshot: the whole committed
// registry at one version.
type TableRegistrySnapshot struct {
	Params       arbiter.TableRegistryParams
	Version      uint64
	Seeded       bool
	Cursor       TableRegistryCursor
	Incarnations []TableIncarnation
	// ClientLanes is the committed client-lane parameter; nil until activation.
	ClientLanes *arbiter.ClientLaneParams
	// SIIndexers is the committed si_indexers list sorted by IndexerID; nil
	// until the signed-claims activation.
	SIIndexers []arbiter.SIIndexerEntry
	// SeededIndexers lists, ascending, the SI indexers whose Legacy seed is
	// recorded; nil before the activation (Seeded keeps the founding flag).
	SeededIndexers []uint64
}

// Live mirrors arbiter's TableRegistryView.Live: the newest incarnation of key
// that is neither Purged nor Refused, else the newest one, else nil. The
// pointer aliases the snapshot's slice.
func (s TableRegistrySnapshot) Live(key string) *TableIncarnation {
	var newest *TableIncarnation
	for i := len(s.Incarnations) - 1; i >= 0; i-- {
		inc := &s.Incarnations[i]
		if inc.Key() != key {
			continue
		}
		if newest == nil {
			newest = inc
		}
		if inc.Status != TableStatusPurged && inc.Status != TableStatusRefused {
			return inc
		}
	}
	return newest
}

// ActiveTables returns the Active incarnations in seq order.
func (s TableRegistrySnapshot) ActiveTables() []TableIncarnation {
	var out []TableIncarnation
	for _, inc := range s.Incarnations {
		if inc.Status == TableStatusActive {
			out = append(out, inc)
		}
	}
	return out
}

// SignedClaimsActive reports whether the snapshot carries si_indexers, i.e.
// the signed-claims activation is committed (housegate spec 2026-10-10 §6.7).
func (s TableRegistrySnapshot) SignedClaimsActive() bool { return len(s.SIIndexers) > 0 }

// SIIndexer returns indexerID's enrolled entry.
func (s TableRegistrySnapshot) SIIndexer(indexerID uint64) (arbiter.SIIndexerEntry, bool) {
	for _, e := range s.SIIndexers {
		if e.IndexerID == indexerID {
			return e, true
		}
	}
	return arbiter.SIIndexerEntry{}, false
}

// Owner returns the SI indexer owning inc: its recorded OwnerIndexerID, or,
// for an incarnation recorded before the signed-claims activation (which
// names no owner), the founding indexer Params.SIIndexerID.
func (s TableRegistrySnapshot) Owner(inc TableIncarnation) uint64 {
	if inc.OwnerIndexerID != nil {
		return *inc.OwnerIndexerID
	}
	return s.Params.SIIndexerID
}

// signedClaimsFromPB decodes si_indexers and seeded_indexers. The arbiter
// keeps entries sorted and every seeded id enrolled; anything else is a
// corrupt view, refused like an unknown status.
func signedClaimsFromPB(m *pb.TableRegistrySnapshot) ([]arbiter.SIIndexerEntry, []uint64, error) {
	entries := SIIndexerEntriesFromPB(m.GetSiIndexers())
	for i, e := range entries {
		if err := e.Validate(); err != nil {
			return nil, nil, fmt.Errorf("table registry snapshot: %w", err)
		}
		if i > 0 && entries[i-1].IndexerID >= e.IndexerID {
			return nil, nil, fmt.Errorf("table registry snapshot: si_indexers must be sorted by indexer_id without duplicates")
		}
	}
	seeded := mapSlice(m.GetSeededIndexers(), func(id uint64) uint64 { return id })
	for i, id := range seeded {
		if i > 0 && seeded[i-1] >= id {
			return nil, nil, fmt.Errorf("table registry snapshot: seeded_indexers must be ascending without duplicates")
		}
		if !slices.ContainsFunc(entries, func(e arbiter.SIIndexerEntry) bool { return e.IndexerID == id }) {
			return nil, nil, fmt.Errorf("table registry snapshot: seeded indexer %d has no si_indexers entry", id)
		}
	}
	return entries, seeded, nil
}

// TableRegistrySnapshotFromPB decodes a snapshot. Every status and origin must
// be a known, specified value and every retire reason a known value; the
// incarnations must be numbered 1..n in order.
func TableRegistrySnapshotFromPB(m *pb.TableRegistrySnapshot) (TableRegistrySnapshot, error) {
	if m == nil {
		return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot is required")
	}
	out := TableRegistrySnapshot{Version: m.GetVersion(), Seeded: m.GetSeeded()}
	if p := TableRegistryParamsFromPB(m.GetParams()); p != nil {
		out.Params = *p
	}
	if c := m.GetCursor(); c != nil {
		out.Cursor = TableRegistryCursor{BlockNumber: c.GetBlockNumber(), BlockHash: c.GetBlockHash(), LogIndex: c.GetLogIndex(), BlockComplete: c.GetBlockComplete()}
	}
	if lanes := ClientLaneParamsFromPB(m.GetClientLanes()); lanes != nil {
		if err := lanes.Validate(); err != nil {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: %w", err)
		}
		out.ClientLanes = lanes
	}
	entries, seeded, err := signedClaimsFromPB(m)
	if err != nil {
		return TableRegistrySnapshot{}, err
	}
	out.SIIndexers, out.SeededIndexers = entries, seeded
	for i, inc := range m.GetIncarnations() {
		if inc.GetSeq() != uint64(i)+1 {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has seq %d", i+1, inc.GetSeq())
		}
		owner := cloneUint64(inc.OwnerIndexerId)
		if owner != nil {
			if _, ok := out.SIIndexer(*owner); !ok {
				return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d is owned by indexer %d, which has no si_indexers entry", inc.GetSeq(), *owner)
			}
		}
		status, ok := tableStatusFromPB[inc.GetStatus()]
		if !ok {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has unknown status %v", inc.GetSeq(), inc.GetStatus())
		}
		origin, ok := tableOriginFromPB[inc.GetOrigin()]
		if !ok {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has unknown origin %v", inc.GetSeq(), inc.GetOrigin())
		}
		reason := TableRetireReason(inc.GetRetireReason())
		switch reason {
		case TableRetireReasonUnspecified, TableRetireReasonTableDeleted, TableRetireReasonDatabaseDeleted:
		default:
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has unknown retire reason %v", inc.GetSeq(), inc.GetRetireReason())
		}
		out.Incarnations = append(out.Incarnations, TableIncarnation{
			Seq: inc.GetSeq(), DatabaseID: inc.GetDatabaseId(), TableID: inc.GetTableId(),
			Origin: origin, Status: status,
			Created: L2EventRefFromPB(inc.GetCreated()), SchemaRef: L2EventRefFromPB(inc.GetSchemaRef()),
			SchemaVersion: inc.GetSchemaVersion(), SchemaHash: inc.GetSchemaHash(), SchemaJSON: inc.GetSchemaJson(),
			RefusedReason: inc.GetRefusedReason(), RefusedCode: inc.GetRefusedCode(),
			Deleted: L2EventRefFromPB(inc.GetDeleted()), RetireReason: reason,
			AddBlockSeq: inc.GetAddBlockSeq(), RetireBlockSeq: inc.GetRetireBlockSeq(),
			PurgedBy:       mapSlice(inc.GetPurgedBy(), func(id string) string { return id }),
			OwnerIndexerID: owner,
		})
	}
	return out, nil
}

// TableRegistrySnapshotToPB returns an independent transport message. Fake
// arbiter servers in tests and hosts that re-serve a snapshot use it.
func TableRegistrySnapshotToPB(s TableRegistrySnapshot) *pb.TableRegistrySnapshot {
	params := s.Params
	out := &pb.TableRegistrySnapshot{
		Params: TableRegistryParamsToPB(&params), Version: s.Version, Seeded: s.Seeded,
		Cursor: &pb.TableRegistryCursor{BlockNumber: s.Cursor.BlockNumber, BlockHash: s.Cursor.BlockHash,
			LogIndex: s.Cursor.LogIndex, BlockComplete: s.Cursor.BlockComplete},
		ClientLanes:    ClientLaneParamsToPB(s.ClientLanes),
		SiIndexers:     SIIndexerEntriesToPB(s.SIIndexers),
		SeededIndexers: mapSlice(s.SeededIndexers, func(id uint64) uint64 { return id }),
	}
	for _, inc := range s.Incarnations {
		out.Incarnations = append(out.Incarnations, &pb.TableIncarnation{
			Seq: inc.Seq, DatabaseId: inc.DatabaseID, TableId: inc.TableID,
			Origin: tableOriginToPB(inc.Origin), Status: tableStatusToPB(inc.Status),
			Created: L2EventRefToPB(inc.Created), SchemaRef: L2EventRefToPB(inc.SchemaRef),
			SchemaVersion: inc.SchemaVersion, SchemaHash: inc.SchemaHash, SchemaJson: inc.SchemaJSON,
			RefusedReason: inc.RefusedReason, RefusedCode: inc.RefusedCode,
			Deleted: L2EventRefToPB(inc.Deleted), RetireReason: pb.TableRetireReason(inc.RetireReason),
			AddBlockSeq: inc.AddBlockSeq, RetireBlockSeq: inc.RetireBlockSeq,
			PurgedBy:       mapSlice(inc.PurgedBy, func(id string) string { return id }),
			OwnerIndexerId: cloneUint64(inc.OwnerIndexerID),
		})
	}
	return out
}

func tableStatusToPB(s TableIncarnationStatus) pb.TableIncarnationStatus {
	for k, v := range tableStatusFromPB {
		if v == s {
			return k
		}
	}
	return pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_UNSPECIFIED
}

func tableOriginToPB(o TableOrigin) pb.TableIncarnationOrigin {
	for k, v := range tableOriginFromPB {
		if v == o {
			return k
		}
	}
	return pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_UNSPECIFIED
}
