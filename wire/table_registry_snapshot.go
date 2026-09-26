package wire

import (
	"fmt"

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
	for i, inc := range m.GetIncarnations() {
		if inc.GetSeq() != uint64(i)+1 {
			return TableRegistrySnapshot{}, fmt.Errorf("table registry snapshot: incarnation %d has seq %d", i+1, inc.GetSeq())
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
			PurgedBy: mapSlice(inc.GetPurgedBy(), func(id string) string { return id }),
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
			PurgedBy: mapSlice(inc.PurgedBy, func(id string) string { return id }),
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
