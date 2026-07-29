package snode

import (
	"context"
	"errors"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestRegisterPreparedClaim_AcceptedAndIdempotent(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, claims := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	prepared, err := role.PrepareLocalStatement(ctx, stagedRequest(payload), payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	out, err := role.RegisterPreparedClaim(ctx, prepared.StatementID)
	if err != nil || out.Category != ClaimAccepted || out.BoundSource != cfg.NodeID {
		t.Fatalf("register: %+v %v", out, err)
	}
	rec, _, _ := role.journal.load(prepared.StatementID)
	if rec.Lifecycle != LifecycleRCBound || rec.Result == nil || rec.Result.Lifecycle != LifecycleRCBound {
		t.Fatalf("lifecycle: %+v", rec)
	}
	if claims.count() != 1 {
		t.Fatalf("one result-claim call, got %d", claims.count())
	}

	out, err = role.RegisterPreparedClaim(ctx, prepared.StatementID)
	if err != nil || out.Category != ClaimAccepted {
		t.Fatalf("re-register: %+v %v", out, err)
	}
	if claims.count() != 2 {
		t.Fatalf("idempotent re-register must replay the claim, got %d calls", claims.count())
	}
}

func TestRegisterPreparedClaim_RequiresPrepared(t *testing.T) {
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)

	_, err := role.RegisterPreparedClaim(context.Background(), "never:1:x")
	if !errors.Is(err, ErrNotPrepared) {
		t.Fatalf("want ErrNotPrepared, got %v", err)
	}
}

func TestRegisterPreparedClaim_TerminalRejectKeepsUnsafeWritten(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, claims := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	prepared, err := role.PrepareLocalStatement(ctx, stagedRequest(payload), payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	claims.setErr(status.Error(codes.InvalidArgument, "conflicting result claim already parked"))
	out, err := role.RegisterPreparedClaim(ctx, prepared.StatementID)
	if err != nil || out.Category != ClaimTerminalReject || out.Reason == "" {
		t.Fatalf("terminal: %+v %v", out, err)
	}
	rec, _, _ := role.journal.load(prepared.StatementID)
	if rec.Lifecycle != LifecycleUnsafeWritten {
		t.Fatalf("terminal reject must leave abort decision to housegate: %+v", rec)
	}
}

func TestRegisterPreparedClaim_IndeterminateFailureIsUnknown(t *testing.T) {
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, claims := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	prepared, err := role.PrepareLocalStatement(context.Background(), stagedRequest(payload), payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	claims.setErr(status.Error(codes.Unavailable, "connection lost after apply"))
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	out, err := role.RegisterPreparedClaim(ctx, prepared.StatementID)
	if err != nil || out.Category != ClaimUnknown || out.Reason == "" {
		t.Fatalf("unknown: %+v %v", out, err)
	}
	rec, _, _ := role.journal.load(prepared.StatementID)
	if rec.Lifecycle != LifecycleUnsafeWritten {
		t.Fatalf("unknown result must remain retryable from UnsafeWritten: %+v", rec)
	}
}

func TestAbortPreparedStatement_DropsExactPartsAndCleans(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\np1,2\n")
	prepared, err := role.PrepareLocalStatement(ctx, stagedRequest(payload), payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	names := partNames(prepared.CandidateParts)
	if err := role.AbortPreparedStatement(ctx, prepared.StatementID, names, "terminal reject"); err != nil {
		t.Fatalf("abort: %v", err)
	}
	rec, _, _ := role.journal.load(prepared.StatementID)
	if rec.Lifecycle != LifecycleCleaned {
		t.Fatalf("must clean: %+v", rec)
	}
	if n := countActiveParts(t, conn, role, schema); n != 0 {
		t.Fatalf("parts remain: %d", n)
	}
	if err := role.AbortPreparedStatement(ctx, prepared.StatementID, names, "again"); err != nil {
		t.Fatalf("re-abort: %v", err)
	}
	if err := role.AbortPreparedStatement(ctx, "never:1:x", nil, "noop"); err != nil {
		t.Fatalf("empty abort: %v", err)
	}
}

func TestAbortPreparedStatement_PreparingConvergesBeforeAbort(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	prepared, err := role.PrepareLocalStatement(ctx, stagedRequest(payload), payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rewindToPreparing(t, role, prepared.StatementID)

	if err := role.AbortPreparedStatement(ctx, prepared.StatementID, nil, "terminal reject"); err == nil {
		t.Fatal("empty abort must not clean a complete write")
	}
	rec, _, _ := role.journal.load(prepared.StatementID)
	if rec.Lifecycle != LifecycleUnsafeWritten || rec.Result == nil {
		t.Fatalf("complete write must converge before abort validation: %+v", rec)
	}
	if _, found, err := role.LookupPreparedStatement(ctx, prepared.StatementID); err != nil || !found {
		t.Fatalf("prepared statement must remain discoverable: found=%v err=%v", found, err)
	}
}

func TestAbortPreparedStatement_PreparingRejectsUnattributedPart(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	prepared, err := role.PrepareLocalStatement(ctx, stagedRequest(payload), payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rewindToPreparing(t, role, prepared.StatementID)

	if err := role.AbortPreparedStatement(ctx, prepared.StatementID, []string{"foreign_part"}, "terminal reject"); err == nil {
		t.Fatal("unattributed abort part must be rejected")
	}
	rec, _, _ := role.journal.load(prepared.StatementID)
	if rec.Lifecycle != LifecycleUnsafeWritten || rec.Result == nil {
		t.Fatalf("complete write must converge before abort validation: %+v", rec)
	}
}

func TestSubmitLocalStatement_WrapperEquivalence(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	env := intakeEnvelope(payload)
	if err := role.SubmitLocalStatement(ctx, env, payload); err != nil {
		t.Fatalf("wrapper: %v", err)
	}
	rec, _, _ := role.journal.load(env.StatementID.Flat())
	if rec.Lifecycle != LifecycleRCBound {
		t.Fatalf("wrapper must land RCBound: %+v", rec)
	}
}
