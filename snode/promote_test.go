package snode

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

func TestHandlePromote_HappyPath(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, env := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := arbiter.PromoteSafePartition{
		TableID:            schema.TableID,
		PartitionID:        part.PartitionID,
		PromotionSeq:       1,
		BaseSafeSnapshotID: "genesis",
		BasePartitionRoot:  "",
		CandidateParts: []arbiter.PartRef{{
			TableID: schema.TableID, PartitionID: part.PartitionID,
			PartRowLtHash: part.PartRowLtHash, PartName: part.PartName,
		}},
	}
	jws := mustSignPromotion(t, signer, cmd)

	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), jws); err != nil {
		t.Fatalf("handlePromote: %v", err)
	}

	qualifiedSafe := role.cfg.SafeDatabase + "." + CHTableName(schema.TableID)
	assertSourceRows(t, ctx, role.d.Conn, qualifiedSafe, role.cfg.NetworkID, schema.TableID, env.StatementID.Flat())
	acks := claims.promotionAcks()
	if len(acks) != 1 {
		t.Fatalf("acks: %+v", acks)
	}
	ack := acks[0]
	wantPost, err := lthashCombineHexAll("", []string{part.PartRowLtHash})
	if err != nil {
		t.Fatalf("want post: %v", err)
	}
	if !ack.Applied || ack.PromotionSeq != 1 || ack.PostPartitionCommitment != wantPost ||
		ack.TableID != schema.TableID || ack.PartitionID != part.PartitionID || ack.NodeID != role.cfg.NodeID {
		t.Fatalf("ack: %+v", ack)
	}
	assertExactSafePartMappings(t, ctx, role, schema, part, ack.Parts)
	pk := partitionKey{Table: schema.TableID, Partition: part.PartitionID}
	if got := role.state.Watermark(pk); got != 1 {
		t.Fatalf("watermark = %d", got)
	}
	if got, err := role.PromotedUnsafeParts(schema.TableID); err != nil || !reflect.DeepEqual(got, []string{part.PartName}) {
		t.Fatalf("PromotedUnsafeParts = %v %v, want [%s]", got, err, part.PartName)
	}
	base, snap := role.state.BaseRoot(pk)
	if base != wantPost || snap != "genesis" {
		t.Fatalf("base = (%s,%s), want (%s,genesis)", base, snap, wantPost)
	}
	if got := role.state.UnpromotedSum(pk); got != accumulatorHexZero() {
		t.Fatalf("unpromoted sum = %s, want zero", got)
	}
	assertNoPromotePartition(t, ctx, role, schema, part.PartitionID)
}

func TestHandlePromote_BadJWSRejected(t *testing.T) {
	ctx := context.Background()
	role, claims, _, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := arbiter.PromoteSafePartition{
		TableID: schema.TableID, PartitionID: part.PartitionID, PromotionSeq: 1,
		CandidateParts: []arbiter.PartRef{{
			TableID: schema.TableID, PartitionID: part.PartitionID,
			PartRowLtHash: part.PartRowLtHash, PartName: part.PartName,
		}},
	}
	badSigner, err := authority.NewSignerFromHex(badAuthorityKeyHex)
	if err != nil {
		t.Fatalf("bad signer: %v", err)
	}
	jws := mustSignPromotion(t, badSigner, cmd)

	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), jws); err == nil {
		t.Fatal("bad authority token accepted")
	}
	if got := len(claims.promotionAcks()); got != 0 {
		t.Fatalf("acks after bad jws = %d", got)
	}
	if got := rowCount(t, ctx, role.d.Conn, role.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 0 {
		t.Fatalf("safe rows after bad jws = %d", got)
	}
}

func TestHandlePromote_StaleSeqResendsPersistedAck(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := arbiter.PromoteSafePartition{
		TableID: schema.TableID, PartitionID: part.PartitionID, PromotionSeq: 1,
		CandidateParts: []arbiter.PartRef{{
			TableID: schema.TableID, PartitionID: part.PartitionID,
			PartRowLtHash: part.PartRowLtHash, PartName: part.PartName,
		}},
	}
	jws := mustSignPromotion(t, signer, cmd)
	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), jws); err != nil {
		t.Fatalf("first promote: %v", err)
	}
	table := CHTableName(schema.TableID)
	before := activePartsMust(t, ctx, role.d.Conn, role.cfg.SafeDatabase, table)

	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), jws); err != nil {
		t.Fatalf("duplicate promote: %v", err)
	}

	after := activePartsMust(t, ctx, role.d.Conn, role.cfg.SafeDatabase, table)
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("safe parts mutated on duplicate: before=%+v after=%+v", before, after)
	}
	acks := claims.promotionAcks()
	if len(acks) != 2 || !reflect.DeepEqual(acks[0], acks[1]) {
		t.Fatalf("duplicate did not resend identical ack: %+v", acks)
	}
}

func TestHandlePromote_PostReplaceFailureRestartRetryConverges(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	first := claims.snapshot()[0].CandidateParts[0]
	firstCmd := arbiter.PromoteSafePartition{
		TableID: schema.TableID, PartitionID: first.PartitionID, PromotionSeq: 1,
		BaseSafeSnapshotID: "genesis",
		CandidateParts: []arbiter.PartRef{{
			TableID: schema.TableID, PartitionID: first.PartitionID,
			PartRowLtHash: first.PartRowLtHash, PartName: first.PartName,
		}},
	}
	if err := role.handlePromote(ctx, wire.PromoteToPB(firstCmd), mustSignPromotion(t, signer, firstCmd)); err != nil {
		t.Fatalf("first promote: %v", err)
	}
	cleanup := cleanupCommand(schema.TableID, first)
	if err := role.handleCleanup(ctx, wire.CleanupToPB(cleanup), mustSignCleanup(t, signer, cleanup)); err != nil {
		t.Fatalf("first cleanup: %v", err)
	}

	secondPayload := nativePayload(t, pv{"p0", 3}, pv{"p0", 4})
	secondEnv := intakeEnvelope(secondPayload)
	secondEnv.StatementID.ClientSeq = 2
	secondEnv.StatementID.ClientNonce = "n2"
	secondEnv.PayloadRef = "payload-2.native"
	if err := role.SubmitLocalStatement(ctx, secondEnv, secondPayload); err != nil {
		t.Fatalf("second SubmitLocalStatement: %v", err)
	}
	second := claims.snapshot()[1].CandidateParts[0]
	baseRoot, _ := role.state.BaseRoot(partitionKey{Table: schema.TableID, Partition: second.PartitionID})
	secondCmd := arbiter.PromoteSafePartition{
		TableID: schema.TableID, PartitionID: second.PartitionID, PromotionSeq: 2,
		BaseSafeSnapshotID: "safe-1", BasePartitionRoot: baseRoot,
		CandidateParts: []arbiter.PartRef{{
			TableID: schema.TableID, PartitionID: second.PartitionID,
			PartRowLtHash: second.PartRowLtHash, PartName: second.PartName,
		}},
	}
	secondJWS := mustSignPromotion(t, signer, secondCmd)

	baseConn := role.d.Conn
	failing := &failAfterReplaceConn{Conn: baseConn}
	role.d.Conn = failing
	if err := role.handlePromote(ctx, wire.PromoteToPB(secondCmd), secondJWS); err == nil ||
		!strings.Contains(err.Error(), "injected post-REPLACE failure") {
		t.Fatalf("post-REPLACE failure = %v", err)
	}
	if !failing.replaced {
		t.Fatal("fault fired before REPLACE became visible")
	}
	if got := rowCount(t, ctx, baseConn, role.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 4 {
		t.Fatalf("safe rows after published-but-unfinalized promotion = %d, want 4", got)
	}
	if _, err := role.PromotedUnsafeParts(schema.TableID); err == nil {
		t.Fatal("unsafe_latest read state must fail closed while publication intent is unresolved")
	}
	if got := len(claims.promotionAcks()); got != 1 {
		t.Fatalf("promotion acks before recovery = %d, want only the first ack", got)
	}

	deps := role.d
	deps.Conn = baseConn
	restarted, err := New(role.cfg, deps)
	if err != nil {
		t.Fatalf("restart role: %v", err)
	}
	if _, err := restarted.PromotedUnsafeParts(schema.TableID); err == nil {
		t.Fatal("durable unresolved intent was lost across restart")
	}
	if err := restarted.handlePromote(ctx, wire.PromoteToPB(secondCmd), secondJWS); err != nil {
		t.Fatalf("retry after restart: %v", err)
	}
	if got := rowCount(t, ctx, baseConn, restarted.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 4 {
		t.Fatalf("safe rows after retry = %d, want 4 (no duplicate publication)", got)
	}
	if got, err := restarted.PromotedUnsafeParts(schema.TableID); err != nil || !reflect.DeepEqual(got, []string{second.PartName}) {
		t.Fatalf("PromotedUnsafeParts after recovery = %v %v, want [%s]", got, err, second.PartName)
	}
	acks := claims.promotionAcks()
	if len(acks) != 2 || !acks[1].Applied || acks[1].PromotionSeq != 2 {
		t.Fatalf("promotion acks after recovery = %+v", acks)
	}
	assertExactSafePartMappings(t, ctx, restarted, schema, second, acks[1].Parts)
}

type failAfterReplaceConn struct {
	clickhouse.Conn
	replaced bool
	failed   bool
}

func (c *failAfterReplaceConn) Exec(ctx context.Context, query string, args ...any) error {
	if c.replaced && !c.failed {
		c.failed = true
		return errors.New("injected post-REPLACE failure")
	}
	err := c.Conn.Exec(ctx, query, args...)
	if err == nil && strings.Contains(query, " REPLACE PARTITION ") {
		c.replaced = true
	}
	return err
}

func TestHandlePromote_BaseCASMismatchAcksNotApplied(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := arbiter.PromoteSafePartition{
		TableID:           schema.TableID,
		PartitionID:       part.PartitionID,
		PromotionSeq:      1,
		BasePartitionRoot: "0xwrong",
		CandidateParts: []arbiter.PartRef{{
			TableID: schema.TableID, PartitionID: part.PartitionID,
			PartRowLtHash: part.PartRowLtHash, PartName: part.PartName,
		}},
	}
	jws := mustSignPromotion(t, signer, cmd)

	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), jws); err != nil {
		t.Fatalf("handlePromote: %v", err)
	}

	acks := claims.promotionAcks()
	if len(acks) != 1 || acks[0].Applied || !strings.Contains(acks[0].Detail, "base") {
		t.Fatalf("base-mismatch ack: %+v", acks)
	}
	if got := rowCount(t, ctx, role.d.Conn, role.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 0 {
		t.Fatalf("safe rows after base mismatch = %d", got)
	}
	pk := partitionKey{Table: schema.TableID, Partition: part.PartitionID}
	if got := role.state.Watermark(pk); got != 1 {
		t.Fatalf("watermark = %d", got)
	}
}

// TestHandlePromote_ShadowClosureGateRejectsDivergentContent exercises the
// §8.2 closure gate: the shadow is published only if its physical partition
// content equals base ⊕ candidates. Here the candidate's claimed
// PartRowLtHash is corrupted, so the shadow's real content (the honest part)
// diverges from base ⊕ claimed and the gate must reject BEFORE REPLACE —
// nothing reaches hg_safe, no applied ack. This is the same assertion that
// stops a stray unverified part smuggled into the partition by a concurrent
// source write from riding a whole-partition ATTACH into hg_safe.
func TestHandlePromote_ShadowClosureGateRejectsDivergentContent(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	corrupt := lthashLie(part.PartRowLtHash)
	cmd := arbiter.PromoteSafePartition{
		TableID: schema.TableID, PartitionID: part.PartitionID, PromotionSeq: 1,
		BasePartitionRoot: "",
		CandidateParts: []arbiter.PartRef{{
			TableID: schema.TableID, PartitionID: part.PartitionID,
			PartRowLtHash: corrupt, PartName: part.PartName,
		}},
	}
	jws := mustSignPromotion(t, signer, cmd)

	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), jws); err == nil ||
		!strings.Contains(err.Error(), "shadow closure mismatch") {
		t.Fatalf("divergent shadow must be rejected by the closure gate, got %v", err)
	}
	if got := len(claims.promotionAcks()); got != 0 {
		t.Fatalf("divergent shadow produced %d acks", got)
	}
	if got := rowCount(t, ctx, role.d.Conn, role.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 0 {
		t.Fatalf("divergent shadow published %d safe rows", got)
	}
	assertNoPromotePartition(t, ctx, role, schema, part.PartitionID)
}

// lthashLie flips the first accumulator byte of a 0x-hex lthash, yielding a
// valid-shaped but wrong commitment.
func lthashLie(h string) string {
	raw := strings.TrimPrefix(h, "0x")
	b := []byte(raw)
	if b[0] == '0' {
		b[0] = '1'
	} else {
		b[0] = '0'
	}
	return "0x" + string(b)
}

func promoteSchema() payloadexec.TableSchema {
	return intakeSchema()
}
