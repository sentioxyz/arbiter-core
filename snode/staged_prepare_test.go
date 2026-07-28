package snode

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"housegate/housegate/pkg/replay/payloadexec"
)

const testEncoding = "csv-with-names-v1"

func stagedRequest(payload []byte) PrepareRequest {
	return PrepareRequest{
		Envelope:        intakeEnvelope(payload),
		PayloadEncoding: testEncoding,
		Revision:        54460,
	}
}

func TestPrepareLocalStatement_CleanPath(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, claims := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\np0,2\n")
	req := stagedRequest(payload)
	res, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if res.Lifecycle != LifecycleUnsafeWritten || res.SourceNode != cfg.NodeID {
		t.Fatalf("result header: %+v", res)
	}
	if res.PayloadRef != req.Envelope.PayloadRef ||
		res.PayloadHash != req.Envelope.PayloadHash ||
		res.PayloadLength != req.Envelope.PayloadLength ||
		res.PayloadEncoding != testEncoding ||
		res.Revision != 54460 {
		t.Fatalf("payload quintuple must echo verbatim: %+v", res)
	}
	if len(res.CandidateParts) == 0 || len(res.PartitionNewPartSums) == 0 || res.SourceClaimRoot == "" {
		t.Fatalf("claim material missing: %+v", res)
	}
	if claims.count() != 0 {
		t.Fatal("Prepare must not register the result claim")
	}
	rec, ok, err := role.journal.load(req.Envelope.StatementID.Flat())
	if err != nil || !ok || rec.Lifecycle != LifecycleUnsafeWritten || rec.RC == nil || rec.Result == nil {
		t.Fatalf("journal after prepare: ok=%v err=%v rec=%+v", ok, err, rec)
	}
}

func TestPrepareLocalStatement_IdempotentReplayAtUnsafeWritten(t *testing.T) {
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
	first, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	partsBefore := countActiveParts(t, conn, role, schema)
	second, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("second: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("replay must return the stored result verbatim:\n%+v\n%+v", first, second)
	}
	if got := countActiveParts(t, conn, role, schema); got != partsBefore {
		t.Fatalf("replay must not write again: parts %d -> %d", partsBefore, got)
	}
}

func TestPrepareLocalStatement_IdempotentReplayAtRCBound(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, claims := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)

	payload := []byte("p,v\np0,1\n")
	req := stagedRequest(payload)
	prepared, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if _, err := role.RegisterPreparedClaim(ctx, prepared.StatementID); err != nil {
		t.Fatalf("register: %v", err)
	}
	rec, _, _ := role.journal.load(prepared.StatementID)
	partsBefore := countActiveParts(t, conn, role, schema)

	replayed, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if rec.Result == nil || !reflect.DeepEqual(*rec.Result, replayed) {
		t.Fatalf("RCBound replay must return the durable result:\n%+v\n%+v", rec.Result, replayed)
	}
	if got := countActiveParts(t, conn, role, schema); got != partsBefore {
		t.Fatalf("RCBound replay must not write again: parts %d -> %d", partsBefore, got)
	}
	if claims.count() != 1 {
		t.Fatalf("Prepare replay must not register another claim, got %d calls", claims.count())
	}
}

func TestPrepareLocalStatement_ReplayAtPreparingConverges(t *testing.T) {
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
	first, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rewindToPreparing(t, role, first.StatementID)
	partsBefore := countActiveParts(t, conn, role, schema)

	repaired, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if !reflect.DeepEqual(canonicalPrepared(first), canonicalPrepared(repaired)) {
		t.Fatalf("Preparing replay must repair the durable result:\n%+v\n%+v", first, repaired)
	}
	if got := countActiveParts(t, conn, role, schema); got != partsBefore {
		t.Fatalf("Preparing replay must not write again: parts %d -> %d", partsBefore, got)
	}
}

func TestPrepareLocalStatement_ReplayAtAbortPendingCleansThenRetries(t *testing.T) {
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
	first, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	rec, _, _ := role.journal.load(first.StatementID)
	rec.Lifecycle = LifecycleAbortPending
	rec.Abort = &intakeAbort{PartNames: partNames(first.CandidateParts), Reason: "crash before abort"}
	overwriteIntakeRecordForTest(t, role.journal, rec)

	retried, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if retried.Lifecycle != LifecycleUnsafeWritten || len(retried.CandidateParts) == 0 {
		t.Fatalf("AbortPending replay must clean then prepare again: %+v", retried)
	}
	stored, _, _ := role.journal.load(first.StatementID)
	if stored.Lifecycle != LifecycleUnsafeWritten || stored.Abort != nil {
		t.Fatalf("fresh retry must replace the cleaned journal record: %+v", stored)
	}
}

func TestPrepareLocalStatement_TerminalValidation(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, _ := newIntakeHarness(t, conn, cfg)

	payload := []byte("p,v\np0,1\n")
	req := stagedRequest(payload)
	req.PayloadEncoding = "clickhouse-native-data-v1"
	if _, err := role.PrepareLocalStatement(ctx, req, payload); !errors.Is(err, ErrEncodingNotSupported) {
		t.Fatalf("want ErrEncodingNotSupported, got %v", err)
	}

	req = stagedRequest(payload)
	req.Revision = 0
	if _, err := role.PrepareLocalStatement(ctx, req, payload); err == nil || !strings.Contains(err.Error(), "revision") {
		t.Fatalf("want revision error, got %v", err)
	}

	req = stagedRequest(payload)
	if _, err := role.PrepareLocalStatement(ctx, req, []byte("p,v\np0,999\n")); !errors.Is(err, ErrPayloadMismatch) {
		t.Fatalf("want ErrPayloadMismatch, got %v", err)
	}

	if _, ok, _ := role.journal.load(req.Envelope.StatementID.Flat()); ok {
		t.Fatal("terminal validation must not create a journal record")
	}
}
