package snode

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func rewindToPreparing(t *testing.T, role *Role, flat string) intakeRecord {
	t.Helper()
	rec, ok, err := role.journal.load(flat)
	if err != nil || !ok {
		t.Fatalf("load for rewind: ok=%v err=%v", ok, err)
	}
	rec.Lifecycle = LifecyclePreparing
	rec.Result = nil
	rec.RC = nil
	rec.Abort = nil
	rec.ConvergeFailed = ""
	overwriteIntakeRecordForTest(t, role.journal, rec)
	return rec
}

func TestConverge_CompleteWriteRepairsToUnsafeWritten(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\np1,2\n")
	req := stagedRequest(payload)
	want, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rewindToPreparing(t, role, want.StatementID)

	got, found, err := role.LookupPreparedStatement(ctx, want.StatementID)
	if err != nil || !found {
		t.Fatalf("lookup: found=%v err=%v", found, err)
	}
	if !reflect.DeepEqual(canonicalPrepared(want), canonicalPrepared(got)) {
		t.Fatalf("repaired result mismatch:\n%+v\n%+v", want, got)
	}
}

func TestConverge_PartialWriteCleansViaAbortPath(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\np1,2\n")
	req := stagedRequest(payload)
	res, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rec := rewindToPreparing(t, role, res.StatementID)
	dropPart(t, conn, role, schema, res.CandidateParts[0].PartName)

	_, found, err := role.LookupPreparedStatement(ctx, rec.StatementID)
	if err != nil || found {
		t.Fatalf("partial write must converge to no-trace: found=%v err=%v", found, err)
	}
	got, ok, err := role.journal.load(rec.StatementID)
	if err != nil || !ok || got.Lifecycle != LifecycleCleaned {
		t.Fatalf("must end Cleaned: ok=%v err=%v rec=%+v", ok, err, got)
	}
	if n := countActiveParts(t, conn, role, schema); n != 0 {
		t.Fatalf("all statement parts must be dropped, %d remain", n)
	}
	if _, err := role.PrepareLocalStatement(ctx, req, payload); err != nil {
		t.Fatalf("re-prepare after clean: %v", err)
	}
}

func TestConverge_ZeroWriteCleansViaAbortPath(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	req := stagedRequest(payload)
	res, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rewindToPreparing(t, role, res.StatementID)
	for _, name := range partNames(res.CandidateParts) {
		dropPart(t, conn, role, schema, name)
	}

	if _, found, err := role.LookupPreparedStatement(ctx, res.StatementID); err != nil || found {
		t.Fatalf("zero write must converge to no-trace: found=%v err=%v", found, err)
	}
	rec, _, _ := role.journal.load(res.StatementID)
	if rec.Lifecycle != LifecycleCleaned || rec.Abort == nil || len(rec.Abort.PartNames) != 0 {
		t.Fatalf("zero write must clean through an empty exact-part abort: %+v", rec)
	}
}

func TestConverge_ForeignRowIDFailsClosed(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	req := stagedRequest(payload)
	res, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rewindToPreparing(t, role, res.StatementID)
	mustExecIntake(t, conn, "INSERT INTO "+role.cfg.UnsafeDatabase+"."+CHTableName(schema.TableID)+
		" (_hg_row_id, p, v) VALUES ('"+strings.Repeat("A", 32)+"', 'p0', 9)")

	_, _, err = role.LookupPreparedStatement(ctx, res.StatementID)
	if !errors.Is(err, ErrConvergenceForeignRows) {
		t.Fatalf("want ErrConvergenceForeignRows, got %v", err)
	}
	rec, _, _ := role.journal.load(res.StatementID)
	if rec.ConvergeFailed == "" {
		t.Fatal("converge_failed marker must be persisted")
	}
	if _, err := role.PrepareLocalStatement(ctx, req, payload); !errors.Is(err, ErrConvergenceForeignRows) {
		t.Fatalf("prepare after failed convergence must refuse: %v", err)
	}
}

func TestConverge_StartupResumesAbortPending(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	req := stagedRequest(payload)
	res, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rec, _, _ := role.journal.load(res.StatementID)
	rec.Lifecycle = LifecycleAbortPending
	rec.Abort = &intakeAbort{PartNames: partNames(res.CandidateParts), Reason: "test crash"}
	overwriteIntakeRecordForTest(t, role.journal, rec)

	if err := role.convergeStartup(ctx); err != nil {
		t.Fatalf("startup convergence: %v", err)
	}
	got, _, _ := role.journal.load(res.StatementID)
	if got.Lifecycle != LifecycleCleaned {
		t.Fatalf("abort must resume to Cleaned: %+v", got)
	}
	if n := countActiveParts(t, conn, role, schema); n != 0 {
		t.Fatalf("parts must be dropped, %d remain", n)
	}
}

func dropPart(t *testing.T, conn interface {
	Exec(context.Context, string, ...any) error
}, role *Role, schema payloadexec.TableSchema, name string) {
	t.Helper()
	query := "ALTER TABLE " + role.cfg.UnsafeDatabase + "." + CHTableName(schema.TableID) +
		" DROP PART '" + escapeSQLString(name) + "'"
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("drop part %s: %v", name, err)
	}
}

func partNames(parts []arbiter.CandidatePart) []string {
	names := make([]string, 0, len(parts))
	for _, part := range parts {
		names = append(names, part.PartName)
	}
	return names
}

func canonicalPrepared(in PreparedLocalResult) PreparedLocalResult {
	out := in
	out.CandidateParts = append([]arbiter.CandidatePart(nil), in.CandidateParts...)
	out.PartitionNewPartSums = append([]arbiter.PartitionLtHashSum(nil), in.PartitionNewPartSums...)
	sort.Slice(out.CandidateParts, func(i, j int) bool {
		return out.CandidateParts[i].PartName < out.CandidateParts[j].PartName
	})
	sort.Slice(out.PartitionNewPartSums, func(i, j int) bool {
		if out.PartitionNewPartSums[i].TableID != out.PartitionNewPartSums[j].TableID {
			return out.PartitionNewPartSums[i].TableID < out.PartitionNewPartSums[j].TableID
		}
		return out.PartitionNewPartSums[i].PartitionID < out.PartitionNewPartSums[j].PartitionID
	})
	return out
}
