package snode

import (
	"context"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

func TestHandleCleanup_HappyPath(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, env := seedPromotableStatement(t, ctx)
	part := promoteSeededPart(t, ctx, role, claims, signer, schema)
	cmd := cleanupCommand(schema.TableID, part)

	if err := role.handleCleanup(ctx, wire.CleanupToPB(cmd), mustSignCleanup(t, signer, cmd)); err != nil {
		t.Fatalf("handleCleanup: %v", err)
	}

	assertUnsafePartGone(t, ctx, role, schema.TableID, part.PartName)
	assertSourceRows(t, ctx, role.d.Conn, role.cfg.SafeDatabase+"."+CHTableName(schema.TableID), role.cfg.NetworkID, schema.TableID, env.StatementID.Flat())
	acks := claims.cleanupAcks()
	if len(acks) != 1 || acks[0].NodeID != role.cfg.NodeID || acks[0].PromotionSeq != 1 ||
		acks[0].TableID != schema.TableID || acks[0].PartitionID != part.PartitionID {
		t.Fatalf("cleanup acks: %+v", acks)
	}
}

func TestHandleCleanup_IdempotentMissingPartStillAcks(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := promoteSeededPart(t, ctx, role, claims, signer, schema)
	cmd := cleanupCommand(schema.TableID, part)
	jws := mustSignCleanup(t, signer, cmd)
	if err := role.handleCleanup(ctx, wire.CleanupToPB(cmd), jws); err != nil {
		t.Fatalf("first cleanup: %v", err)
	}

	if err := role.handleCleanup(ctx, wire.CleanupToPB(cmd), jws); err != nil {
		t.Fatalf("second cleanup: %v", err)
	}

	assertUnsafePartGone(t, ctx, role, schema.TableID, part.PartName)
	acks := claims.cleanupAcks()
	if len(acks) != 2 || acks[0] != acks[1] {
		t.Fatalf("cleanup acks: %+v", acks)
	}
}

func TestHandleCleanup_BadJWSRejected(t *testing.T) {
	ctx := context.Background()
	role, claims, _, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := cleanupCommand(schema.TableID, part)
	badSigner, err := authority.NewSignerFromHex(badAuthorityKeyHex)
	if err != nil {
		t.Fatalf("bad signer: %v", err)
	}

	if err := role.handleCleanup(ctx, wire.CleanupToPB(cmd), mustSignCleanup(t, badSigner, cmd)); err == nil {
		t.Fatal("bad cleanup authority accepted")
	}

	assertUnsafePartPresent(t, ctx, role, schema.TableID, part.PartName)
	if got := len(claims.cleanupAcks()); got != 0 {
		t.Fatalf("cleanup acks after bad jws = %d", got)
	}
}

func cleanupCommand(tableID string, part arbiter.CandidatePart) arbiter.UnsafeCleanup {
	return arbiter.UnsafeCleanup{
		TableID: tableID, PartitionID: part.PartitionID, PromotionSeq: 1,
		Parts: []arbiter.PartRef{{
			TableID: tableID, PartitionID: part.PartitionID,
			PartRowLtHash: part.PartRowLtHash, PartName: part.PartName,
		}},
	}
}

func assertUnsafePartPresent(t *testing.T, ctx context.Context, role *Role, tableID, partName string) {
	t.Helper()
	for _, p := range activePartsMust(t, ctx, role.d.Conn, role.cfg.UnsafeDatabase, CHTableName(tableID)) {
		if p.Name == partName {
			return
		}
	}
	t.Fatalf("unsafe part %s not found", partName)
}

func assertUnsafePartGone(t *testing.T, ctx context.Context, role *Role, tableID, partName string) {
	t.Helper()
	for _, p := range activePartsMust(t, ctx, role.d.Conn, role.cfg.UnsafeDatabase, CHTableName(tableID)) {
		if p.Name == partName {
			t.Fatalf("unsafe part %s still active", partName)
		}
	}
}
