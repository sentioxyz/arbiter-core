package snode

import (
	"context"
	"errors"
	"reflect"
	"sort"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

type unexpectedQueryConn struct {
	clickhouse.Conn
}

func (unexpectedQueryConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return nil, errors.New("unexpected ClickHouse query before current schema binding validation")
}

func seedBoundIntakeRecord(t *testing.T, role *Role, lifecycle IntakeLifecycle) intakeRecord {
	t.Helper()
	payload := []byte("durable payload bytes")
	env := intakeEnvelope(payload)
	rec := intakeRecord{
		StatementID:     env.StatementID.Flat(),
		Lifecycle:       LifecyclePreparing,
		Envelope:        env,
		PayloadEncoding: env.PayloadFormat,
		Revision:        int(env.ClientRevision),
	}
	if err := role.journal.save(rec); err != nil {
		t.Fatalf("seed Preparing record: %v", err)
	}
	switch lifecycle {
	case LifecyclePreparing:
		return rec
	case LifecycleAbortPending:
		rec.Lifecycle = LifecycleAbortPending
		rec.Abort = &intakeAbort{Reason: "recovery fixture"}
	case LifecycleUnsafeWritten, LifecycleRCBound:
		rec.Lifecycle = LifecycleUnsafeWritten
		rec.Result = &PreparedLocalResult{
			StatementID:     rec.StatementID,
			PayloadRef:      env.PayloadRef,
			PayloadHash:     env.PayloadHash,
			PayloadLength:   env.PayloadLength,
			PayloadEncoding: env.PayloadFormat,
			Revision:        int(env.ClientRevision),
			Lifecycle:       LifecycleUnsafeWritten,
		}
		rec.RC = &arbiter.RCRecord{StatementID: env.StatementID}
	default:
		t.Fatalf("unsupported seed lifecycle %q", lifecycle)
	}
	if err := role.journal.save(rec); err != nil {
		t.Fatalf("seed %s record: %v", rec.Lifecycle, err)
	}
	if lifecycle == LifecycleRCBound {
		rec.Lifecycle = LifecycleRCBound
		rec.Result.Lifecycle = LifecycleRCBound
		if err := role.journal.save(rec); err != nil {
			t.Fatalf("seed RCBound record: %v", err)
		}
	}
	return rec
}

func driftedIntakeSchema(schema payloadexec.TableSchema) payloadexec.TableSchema {
	drifted := schema
	drifted.Columns = append([]lthash.Column(nil), schema.Columns...)
	drifted.Columns[1].Type = "UInt32"
	return drifted
}

func setRoleSchema(role *Role, schema payloadexec.TableSchema) {
	role.cfg.Tables = []payloadexec.TableSchema{schema}
	role.cfg.SchemaRoot = payloadexec.SchemaRoot(role.cfg.NetworkID, role.cfg.Tables)
}

func TestLookupPreparedStatement_RestartRecoveryRevalidatesCurrentSchema(t *testing.T) {
	for _, lifecycle := range []IntakeLifecycle{LifecycleUnsafeWritten, LifecycleRCBound} {
		t.Run(string(lifecycle), func(t *testing.T) {
			schema := intakeSchema()
			cfg := testConfigS(t)
			cfg.Tables = []payloadexec.TableSchema{schema}
			cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
			original, _ := newIntakeHarness(t, nil, cfg)
			rec := seedBoundIntakeRecord(t, original, lifecycle)

			restartedCfg := cfg
			restartedCfg.Tables = []payloadexec.TableSchema{driftedIntakeSchema(schema)}
			restartedCfg.SchemaRoot = payloadexec.SchemaRoot(restartedCfg.NetworkID, restartedCfg.Tables)
			restarted, _ := newIntakeHarness(t, nil, restartedCfg)

			got, found, err := restarted.LookupPreparedStatement(t.Context(), rec.StatementID)
			if !errors.Is(err, ErrSchemaHashMismatch) {
				t.Fatalf("restart recovery under schema drift must reject with ErrSchemaHashMismatch, got result=%+v found=%v err=%v", got, found, err)
			}
			if found {
				t.Fatal("a stale cached prepare must never be returned")
			}
		})
	}
}

func TestLookupPreparedStatement_ReplayRejectsUnknownTable(t *testing.T) {
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	original, _ := newIntakeHarness(t, nil, cfg)
	rec := seedBoundIntakeRecord(t, original, LifecycleUnsafeWritten)

	other := schema
	other.TableID = "db.other"
	restartedCfg := cfg
	restartedCfg.Tables = []payloadexec.TableSchema{other}
	restartedCfg.SchemaRoot = payloadexec.SchemaRoot(restartedCfg.NetworkID, restartedCfg.Tables)
	restarted, _ := newIntakeHarness(t, nil, restartedCfg)

	got, found, err := restarted.LookupPreparedStatement(t.Context(), rec.StatementID)
	if !errors.Is(err, ErrSchemaUnknown) {
		t.Fatalf("replay for a removed table must reject with ErrSchemaUnknown, got result=%+v found=%v err=%v", got, found, err)
	}
	if found {
		t.Fatal("an unknown-table cached prepare must never be returned")
	}
}

func TestRegisterPreparedClaim_RevalidatesCachedPrepareSchema(t *testing.T) {
	for _, lifecycle := range []IntakeLifecycle{LifecycleUnsafeWritten, LifecycleRCBound} {
		t.Run(string(lifecycle), func(t *testing.T) {
			schema := intakeSchema()
			cfg := testConfigS(t)
			cfg.Tables = []payloadexec.TableSchema{schema}
			cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
			role, claims := newIntakeHarness(t, nil, cfg)
			rec := seedBoundIntakeRecord(t, role, lifecycle)
			setRoleSchema(role, driftedIntakeSchema(schema))

			if _, err := role.RegisterPreparedClaim(t.Context(), rec.StatementID); !errors.Is(err, ErrSchemaHashMismatch) {
				t.Fatalf("claim under schema drift must reject with ErrSchemaHashMismatch, got %v", err)
			}
			if got := claims.count(); got != 0 {
				t.Fatalf("schema-invalid claim must not reach Arbiter, got %d calls", got)
			}
		})
	}
}

func TestLookupPreparedStatement_AbortPendingEmptyCleanupRemainsIdempotent(t *testing.T) {
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, nil, cfg)
	rec := seedBoundIntakeRecord(t, role, LifecycleAbortPending)

	if got, found, err := role.LookupPreparedStatement(t.Context(), rec.StatementID); err != nil || found {
		t.Fatalf("empty AbortPending recovery must converge to no prepared result: result=%+v found=%v err=%v", got, found, err)
	}
	stored, ok, err := role.journal.load(rec.StatementID)
	if err != nil || !ok || stored.Lifecycle != LifecycleCleaned {
		t.Fatalf("empty abort must be durably Cleaned: ok=%v err=%v record=%+v", ok, err, stored)
	}
}

func TestCurrentBindingCheckedBeforeNonTerminalConvergence(t *testing.T) {
	type operation struct {
		name      string
		lifecycle IntakeLifecycle
		run       func(context.Context, *Role, string) error
	}
	operations := []operation{
		{
			name:      "lookup Preparing",
			lifecycle: LifecyclePreparing,
			run: func(ctx context.Context, role *Role, statementID string) error {
				_, _, err := role.LookupPreparedStatement(ctx, statementID)
				return err
			},
		},
		{
			name:      "claim Preparing",
			lifecycle: LifecyclePreparing,
			run: func(ctx context.Context, role *Role, statementID string) error {
				_, err := role.RegisterPreparedClaim(ctx, statementID)
				return err
			},
		},
		{
			name:      "startup Preparing",
			lifecycle: LifecyclePreparing,
			run: func(ctx context.Context, role *Role, _ string) error {
				return role.convergeStartup(ctx)
			},
		},
		{
			name:      "lookup AbortPending",
			lifecycle: LifecycleAbortPending,
			run: func(ctx context.Context, role *Role, statementID string) error {
				_, _, err := role.LookupPreparedStatement(ctx, statementID)
				return err
			},
		},
		{
			name:      "claim AbortPending",
			lifecycle: LifecycleAbortPending,
			run: func(ctx context.Context, role *Role, statementID string) error {
				_, err := role.RegisterPreparedClaim(ctx, statementID)
				return err
			},
		},
		{
			name:      "abort AbortPending",
			lifecycle: LifecycleAbortPending,
			run: func(ctx context.Context, role *Role, statementID string) error {
				return role.AbortPreparedStatement(ctx, statementID, nil, "recovery")
			},
		},
		{
			name:      "startup AbortPending",
			lifecycle: LifecycleAbortPending,
			run: func(ctx context.Context, role *Role, _ string) error {
				return role.convergeStartup(ctx)
			},
		},
	}

	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			schema := intakeSchema()
			cfg := testConfigS(t)
			cfg.Tables = []payloadexec.TableSchema{schema}
			cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
			role, _ := newIntakeHarness(t, unexpectedQueryConn{}, cfg)
			rec := seedBoundIntakeRecord(t, role, op.lifecycle)
			setRoleSchema(role, driftedIntakeSchema(schema))

			if err := op.run(t.Context(), role, rec.StatementID); !errors.Is(err, ErrSchemaHashMismatch) {
				t.Fatalf("%s must reject schema drift before convergence, got %v", op.name, err)
			}
			stored, ok, err := role.journal.load(rec.StatementID)
			if err != nil || !ok || stored.Lifecycle != op.lifecycle {
				t.Fatalf("binding rejection must leave %s durable: ok=%v err=%v record=%+v", op.lifecycle, ok, err, stored)
			}
		})
	}
}

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

	payload := nativePayload(t, pv{"p0", 1}, pv{"p1", 2})
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

	payload := nativePayload(t, pv{"p0", 1}, pv{"p1", 2})
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

	payload := nativePayload(t, pv{"p0", 1})
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

	payload := nativePayload(t, pv{"p0", 1})
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

	payload := nativePayload(t, pv{"p0", 1})
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
