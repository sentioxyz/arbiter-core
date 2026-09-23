package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

// TableRetireReason mirrors pb.TableRetireReason.
type TableRetireReason int32

const (
	TableRetireReasonUnspecified     TableRetireReason = 0
	TableRetireReasonTableDeleted    TableRetireReason = 1
	TableRetireReasonDatabaseDeleted TableRetireReason = 2
)

// SeedLegacyTables mirrors pb.SeedLegacyTablesCmd.
type SeedLegacyTables struct {
	AtBlock arbiter.L2BlockRef
	Tables  []arbiter.LegacyTable
}

// AddTable mirrors pb.AddTableCmd.
type AddTable struct {
	DatabaseID, TableID string
	Created, Schema     arbiter.L2EventRef
	SchemaVersion       uint32
	SchemaHash          string
	SchemaJSON          string
}

// RetireTables mirrors pb.RetireTablesCmd.
type RetireTables struct {
	DatabaseID string
	TableIDs   []string
	Deleted    arbiter.L2EventRef
	Reason     TableRetireReason
}

// AdvanceL2Cursor mirrors pb.AdvanceL2CursorCmd.
type AdvanceL2Cursor struct{ To arbiter.L2BlockRef }

// RecordTablePurged mirrors pb.RecordTablePurgedCmd.
type RecordTablePurged struct {
	NodeID         string
	IncarnationSeq uint64
}

// l2BlockRefValue adapts the pointer-returning L2BlockRefFromPB (defined in
// consensus.go for Task 2's optional-ref fields) to the value-typed fields
// these table-registry commands use; an absent ref decodes to the zero value.
func l2BlockRefValue(m *pb.L2BlockRef) arbiter.L2BlockRef {
	if v := L2BlockRefFromPB(m); v != nil {
		return *v
	}
	return arbiter.L2BlockRef{}
}

// l2EventRefValue is the L2EventRef counterpart of l2BlockRefValue.
func l2EventRefValue(m *pb.L2EventRef) arbiter.L2EventRef {
	if v := L2EventRefFromPB(m); v != nil {
		return *v
	}
	return arbiter.L2EventRef{}
}

func legacyTablesFromPB(in []*pb.LegacyTable) []arbiter.LegacyTable {
	return mapSlice(in, func(m *pb.LegacyTable) arbiter.LegacyTable {
		return arbiter.LegacyTable{DatabaseID: m.GetDatabaseId(), TableID: m.GetTableId(), Created: l2EventRefValue(m.GetCreated())}
	})
}

func legacyTablesToPB(in []arbiter.LegacyTable) []*pb.LegacyTable {
	return mapSlice(in, func(v arbiter.LegacyTable) *pb.LegacyTable {
		return &pb.LegacyTable{DatabaseId: v.DatabaseID, TableId: v.TableID, Created: L2EventRefToPB(&v.Created)}
	})
}
