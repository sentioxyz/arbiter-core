package snode

import (
	"context"
	"reflect"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestSubmitLocalStatement_WritesSourceAndRegistersRC(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	role, claims := newIntakeHarness(t, conn, cfg)
	table := CHTableName(schema.TableID)
	db := role.cfg.UnsafeDatabase
	qualified := db + "." + table

	createIntakeTable(t, conn, role, schema)

	payload := nativePayload(t, pv{"p0", 1}, pv{"p0", 2})
	env := intakeEnvelope(payload)
	if err := role.SubmitLocalStatement(ctx, env, payload); err != nil {
		t.Fatalf("SubmitLocalStatement: %v", err)
	}

	assertSourceRows(t, ctx, conn, qualified, cfg.NetworkID, schema.TableID, env.StatementID.Flat())
	payloadReader, ok := role.d.Payloads.(replay.PayloadStore)
	if !ok {
		t.Fatalf("test payload spool does not implement replay.PayloadStore: %T", role.d.Payloads)
	}
	stored, err := payloadReader.GetPayload(ctx, env.PayloadRef)
	if err != nil {
		t.Fatalf("stored payload: %v", err)
	}
	if !reflect.DeepEqual(stored, payload) {
		t.Fatalf("stored payload = %q want %q", stored, payload)
	}
	rcs := claims.snapshot()
	if len(rcs) != 1 {
		t.Fatalf("registered RCs: %+v", rcs)
	}
	rc := rcs[0]
	info := activePartByName(t, ctx, conn, db, table, rc.CandidateParts[0].PartName)
	scans, err := chexec.ScanParts(ctx, conn, qualified, schema, []string{info.Name})
	if err != nil {
		t.Fatalf("scan parts: %v", err)
	}
	if len(rc.CandidateParts) != 1 || rc.SourceNode != cfg.NodeID {
		t.Fatalf("rc shape: %+v", rc)
	}
	part := rc.CandidateParts[0]
	if part.PartName != info.Name || part.RowCount != 2 || part.Bytes != info.Bytes ||
		part.PartPhysHash != info.PhysHash || part.PartRowLtHash != scans[0].RowLtHash {
		t.Fatalf("candidate part: %+v info=%+v scan=%+v", part, info, scans[0])
	}
	if len(rc.PartitionNewPartSums) != 1 || rc.PartitionNewPartSums[0].NewPartsLtHashSum != part.PartRowLtHash ||
		rc.PartitionNewPartSums[0].PartitionID != part.PartitionID {
		t.Fatalf("partition sums: %+v", rc.PartitionNewPartSums)
	}
	wantRoot := referenceSourceRoot(t, ctx, cfg, schema, env, payload)
	if rc.SourceClaimRoot != wantRoot {
		t.Fatalf("source root: got %s want %s", rc.SourceClaimRoot, wantRoot)
	}

	bad := env
	bad.PayloadHash = replay.DigestString("bad")
	if err := role.SubmitLocalStatement(ctx, bad, payload); err == nil {
		t.Fatal("payload hash mismatch must fail")
	}
	if got := rowCount(t, ctx, conn, qualified); got != 2 {
		t.Fatalf("row count after failed submit: %d", got)
	}
}
