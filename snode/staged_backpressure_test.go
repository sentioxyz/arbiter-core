package snode

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func TestPrepareLocalStatement_RefusesAboveHardPartsLimitBeforeWriting(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.HardPartsPerPartition = 2
	role, claims := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)
	qualified := role.cfg.UnsafeDatabase + "." + CHTableName(schema.TableID)
	for i := 1; i <= 2; i++ {
		mustExecIntake(t, conn, fmt.Sprintf("INSERT INTO %s VALUES (unhex('%064x'), 'p0', %d)", qualified, i, i))
	}
	if got := countActiveParts(t, conn, role, schema); got != 2 {
		t.Fatalf("seed parts = %d want 2", got)
	}

	payload := nativePayload(t, pv{"p0", 3})
	req := stagedRequest(payload)
	_, err := role.PrepareLocalStatement(ctx, req, payload)
	if !errors.Is(err, ErrBackpressure) {
		t.Fatalf("err = %v, want ErrBackpressure", err)
	}
	if got := countActiveParts(t, conn, role, schema); got != 2 {
		t.Fatalf("refused prepare must not write: parts = %d", got)
	}
	if _, ok, journalErr := role.journal.load(req.Envelope.StatementID.Flat()); journalErr != nil || ok {
		t.Fatalf("refused prepare must not journal: ok=%v err=%v", ok, journalErr)
	}
	if claims.count() != 0 {
		t.Fatal("no claim may be registered")
	}

	other := nativePayload(t, pv{"p1", 1})
	env := intakeEnvelope(other)
	env.StatementID = arbiter.StatementID{ClientAccount: "0xacct", ClientSeq: 2, ClientNonce: "n"}
	if _, err := role.PrepareLocalStatement(ctx, PrepareRequest{Envelope: env, PayloadEncoding: testEncoding, Revision: 54460}, other); err != nil {
		t.Fatalf("prepare into p1: %v", err)
	}
}

// TestPrepareLocalStatement_DisableHardPartsSkipsTheSourceRefusal mirrors the
// setup above and changes only the two config fields and the expectation.
// DisableHardParts pins the limit at 0, so an unskipped check would refuse
// every partition (n >= 0 always holds); a successful prepare here proves the
// loop is skipped rather than merely under-triggered.
//
// This is a deliberate footgun made explicit (Spec O D2): with it set, inserts
// fail at ClickHouse's own parts_to_throw_insert instead of at the source.
func TestPrepareLocalStatement_DisableHardPartsSkipsTheSourceRefusal(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.DisableHardParts = true
	cfg.HardPartsPerPartition = 0
	role, claims := newIntakeHarness(t, conn, cfg)
	createIntakeTable(t, conn, role, schema)
	qualified := role.cfg.UnsafeDatabase + "." + CHTableName(schema.TableID)
	for i := 1; i <= 2; i++ {
		mustExecIntake(t, conn, fmt.Sprintf("INSERT INTO %s VALUES (unhex('%064x'), 'p0', %d)", qualified, i, i))
	}
	if got := countActiveParts(t, conn, role, schema); got != 2 {
		t.Fatalf("seed parts = %d want 2", got)
	}

	payload := nativePayload(t, pv{"p0", 3})
	req := stagedRequest(payload)
	res, err := role.PrepareLocalStatement(ctx, req, payload)
	if err != nil {
		t.Fatalf("an explicit hard-parts disable must not refuse at the source: %v", err)
	}
	if res.Lifecycle != LifecycleUnsafeWritten || len(res.CandidateParts) == 0 {
		t.Fatalf("disabled hard parts must still complete an ordinary prepare: %+v", res)
	}
	if got := countActiveParts(t, conn, role, schema); got != 3 {
		t.Fatalf("prepare must write one new part: parts = %d want 3", got)
	}
	rec, ok, journalErr := role.journal.load(req.Envelope.StatementID.Flat())
	if journalErr != nil || !ok || rec.Lifecycle != LifecycleUnsafeWritten {
		t.Fatalf("prepare must journal UnsafeWritten: ok=%v err=%v rec=%+v", ok, journalErr, rec)
	}
	if claims.count() != 0 {
		t.Fatal("Prepare must not register the result claim")
	}
	if role.cfg.HardPartsPerPartition != 0 {
		t.Fatalf("a disabled check must leave the limit visibly 0, got %d", role.cfg.HardPartsPerPartition)
	}
}
