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

	payload := []byte("p,v\np0,3\n")
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

	other := []byte("p,v\np1,1\n")
	env := intakeEnvelope(other)
	env.StatementID = arbiter.StatementID{ClientAccount: "0xacct", ClientSeq: 2, ClientNonce: "n"}
	if _, err := role.PrepareLocalStatement(ctx, PrepareRequest{Envelope: env, PayloadEncoding: testEncoding, Revision: 54460}, other); err != nil {
		t.Fatalf("prepare into p1: %v", err)
	}
}
