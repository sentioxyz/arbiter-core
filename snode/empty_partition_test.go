package snode

import (
	"context"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// TestPromotion_EmptyStringPartitionIsAckedAndPromoted (arbiter-core#84): a
// statement whose String partition value is the empty string lands in the
// logical partition p_ (ClickHouse partition ''). Its promotion, delivered as
// the subscription delivers it, must be applied and acknowledged like any
// other: an SNode that cannot quote p_ never acknowledges it, and the
// unacknowledged promotion holds its track's safe prefix back for good.
func TestPromotion_EmptyStringPartitionIsAckedAndPromoted(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := promoteSchema()
	signer := mustPromoteSigner(t)
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.AuthorityAddresses = []string{signer.Address()}
	setUniqueDatabases(t, &cfg)
	role, claims := newIntakeHarness(t, conn, cfg)
	createSNodeTables(t, conn, role.cfg, schema)

	payload := nativePayload(t, pv{"", 1}, pv{"", 2})
	env := intakeEnvelope(payload)
	if err := role.SubmitLocalStatement(ctx, env, payload); err != nil {
		t.Fatalf("SubmitLocalStatement: %v", err)
	}
	rcs := claims.snapshot()
	if len(rcs) != 1 || len(rcs[0].CandidateParts) != 1 || rcs[0].CandidateParts[0].PartitionID != "p_" {
		t.Fatalf("source claim %+v: want one candidate part in partition p_", rcs)
	}
	part := rcs[0].CandidateParts[0]
	cmd := arbiter.PromoteSafePartition{
		TableID: schema.TableID, PartitionID: "p_", PromotionSeq: 1, BaseSafeSnapshotID: "genesis",
		CandidateParts: []arbiter.PartRef{{TableID: schema.TableID, PartitionID: "p_", PartRowLtHash: part.PartRowLtHash, PartName: part.PartName}},
	}
	promotion := &pb.PromotionCommand{
		Cmd:          &pb.PromotionCommand_Promote{Promote: wire.PromoteToPB(cmd)},
		AuthorityJws: mustSignPromotion(t, signer, cmd),
	}
	if err := role.dispatchPromotionCommand(ctx, promotion); err != nil {
		t.Fatalf("the empty-string partition's promotion failed (the subscription would redeliver it forever): %v", err)
	}

	acks := claims.promotionAcks()
	wantPost, err := lthashCombineHexAll("", []string{part.PartRowLtHash})
	if err != nil {
		t.Fatal(err)
	}
	if len(acks) != 1 || !acks[0].Applied || acks[0].PartitionID != "p_" || acks[0].PostPartitionCommitment != wantPost {
		t.Fatalf("acks = %+v, want one applied ack for p_", acks)
	}
	qualifiedSafe := role.cfg.SafeDatabase + "." + CHTableName(schema.TableID)
	assertSourceRows(t, ctx, role.d.Conn, qualifiedSafe, role.cfg.NetworkID, schema.TableID, env.StatementID.Flat())
	assertCompleteSafeInventory(t, ctx, role, schema, "p_", acks[0].SafePartitionParts)
	pk := partitionKey{Table: schema.TableID, Partition: "p_"}
	if got := role.state.Watermark(pk); got != 1 {
		t.Fatalf("watermark = %d, want 1", got)
	}
	assertNoPromotePartition(t, ctx, role, schema, "p_")

	// A settle of the same promotion answers from the journal (its shadow
	// drop quotes p_ too).
	settle := &pb.PromotionCommand{Cmd: promotion.Cmd, AuthorityJws: promotion.AuthorityJws, Settle: true}
	if err := role.dispatchPromotionCommand(ctx, settle); err != nil {
		t.Fatalf("settle of the p_ promotion: %v", err)
	}
}
