package snode

import (
	"bytes"
	"context"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/lthash"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// execRecorderS is a clickhouse.Conn that records every Exec and succeeds,
// unless failWith set an error. Any other method is unimplemented (the
// embedded nil Conn panics).
type execRecorderS struct {
	clickhouse.Conn
	mu      sync.Mutex
	queries []string
	err     error
}

func (c *execRecorderS) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, query)
	return c.err
}

// failWith makes every later Exec return err after recording its query.
func (c *execRecorderS) failWith(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

func (c *execRecorderS) executed() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.queries)
}

// newRecordingSNodeS is indexer 1's SNode over a recording connection, with
// the fake arbiter that records what it sends.
func newRecordingSNodeS(t *testing.T) (*Role, *claimsFakeS, *execRecorderS) {
	t.Helper()
	fake, conn := &claimsFakeS{}, &execRecorderS{}
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake), Conn: conn,
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	return role, fake, conn
}

// handleCleanupS signs cleanup with the authority key and runs it through
// handleCleanup, as the promotion stream would deliver it.
func handleCleanupS(t *testing.T, role *Role, cleanup arbiter.UnsafeCleanup) error {
	t.Helper()
	jws, err := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex).SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	return role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), jws)
}

// partS is a part of db.t/all with a valid row LtHash; intake never saw it.
func partS(t *testing.T, name string) arbiter.PartRef {
	t.Helper()
	return arbiter.PartRef{TableID: "db.t", PartitionID: "all", PartRowLtHash: testAccumulatorHexS(t, "row of "+name), PartName: name}
}

// recordedPartS is partS after intake accounted its rows as unpromoted
// (AddUnpromotedPart, as assembleRC does for every candidate part).
func recordedPartS(t *testing.T, role *Role, name string) arbiter.PartRef {
	t.Helper()
	p := partS(t, name)
	if err := role.state.AddUnpromotedPart(partitionKey{Table: p.TableID, Partition: p.PartitionID}, p.PartName, p.PartRowLtHash); err != nil {
		t.Fatal(err)
	}
	return p
}

// applyPromotionS applies promotion seq of parts through the promotion
// handler's own journaling tail (no ClickHouse): the partition's watermark and
// stored acknowledgement become seq, the parts' hashes leave the unpromoted
// sum and their unsafe parts await cleanup.
func applyPromotionS(t *testing.T, role *Role, seq uint64, parts ...arbiter.PartRef) {
	t.Helper()
	landed := make([]arbiter.SafePartMapping, 0, len(parts))
	for _, p := range parts {
		landed = append(landed, arbiter.SafePartMapping{PartRowLtHash: p.PartRowLtHash, SafePartName: "safe_" + p.PartName, PartPhysHash: "0xphys"})
	}
	promotion := arbiter.PromoteSafePartition{TableID: "db.t", PartitionID: "all", PromotionSeq: seq,
		BaseSafeSnapshotID: "snap-base", CandidateParts: parts}
	k := partitionKey{Table: "db.t", Partition: "all"}
	if err := role.finishAppliedPromotion(context.Background(), k, promotion, "0xpost", safePartMappings{Candidates: landed, Partition: landed}); err != nil {
		t.Fatal(err)
	}
}

// unpromotedSumIsZeroS reports whether the partition's unpromoted accumulator
// is zero, the state in which the purge gate counts no unpromoted rows.
func unpromotedSumIsZeroS(t *testing.T, role *Role, k partitionKey) bool {
	t.Helper()
	acc, err := parseAccumulatorHex(role.state.UnpromotedSum(k))
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Equal(acc.Bytes(), lthash.New().Bytes())
}

func requireZeroSumS(t *testing.T, role *Role, k partitionKey) {
	t.Helper()
	if !unpromotedSumIsZeroS(t, role, k) {
		t.Fatalf("the unpromoted sum of %s/%s is not the zero accumulator", k.Table, k.Partition)
	}
}

// intakePartS is intake's record of one part: its row LtHash, "" once the part
// left the accounting, ok false when it was never recorded.
func intakePartS(role *Role, k partitionKey, name string) (string, bool) {
	role.state.mu.Lock()
	defer role.state.mu.Unlock()
	v, ok := role.state.s.IntakeParts[intakePartKey(k, name)]
	return v, ok
}

// TestRejectedPartCleanupIsEchoedAndSigned: a rejected-part cleanup
// (promotion_seq 0, rejected_block_seq 12) drops exactly the parts it names,
// although no promotion ever covered them, and the acknowledgement echoes
// rejected_block_seq inside the body the indexer key signs (housegate spec
// 2026-10-10 §7.5; plan S2-A contract §2, clarification C4).
func TestRejectedPartCleanupIsEchoedAndSigned(t *testing.T) {
	fake := &claimsFakeS{}
	conn := &execRecorderS{}
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake), Conn: conn,
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{
		{TableID: "db.t", PartitionID: "all", PartRowLtHash: "0xaa", PartName: "all_1_1_0"},
		{TableID: "db.t", PartitionID: "all", PartRowLtHash: "0xbb", PartName: "all_2_2_0"},
	}}
	jws, err := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex).SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), jws); err != nil {
		t.Fatal(err)
	}
	if got, want := conn.executed(), []string{
		"ALTER TABLE hg_unsafe.db__t DROP PART 'all_1_1_0'",
		"ALTER TABLE hg_unsafe.db__t DROP PART 'all_2_2_0'",
	}; !slices.Equal(got, want) {
		t.Fatalf("executed %q, want %q", got, want)
	}
	_, _, cleanups := fake.sent()
	if len(cleanups) != 1 {
		t.Fatalf("cleanup acknowledgements = %v", cleanups)
	}
	ack := wire.CleanupAckFromPB(cleanups[0])
	if want := (arbiter.CleanupAck{NodeID: authoritytest.SNodeNodeID1, TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12}); ack != want {
		t.Fatalf("ack = %+v, want %+v", ack, want)
	}
	ctx := authoritytest.Context()
	if err := authority.VerifySNodeMessage(authority.SNodeMessageCleanupAck, ctx, ack, cleanups[0].GetSourceJws(), authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
	unechoed := ack
	unechoed.RejectedBlockSeq = 0
	if err := authority.VerifySNodeMessage(authority.SNodeMessageCleanupAck, ctx, unechoed, cleanups[0].GetSourceJws(), authoritytest.IndexerAddr1); err == nil {
		t.Fatal("the indexer signature does not cover rejected_block_seq")
	}
}

// TestRejectedPartCleanupFollowsTheOwnerCheck: requireOwned applies to a
// rejected-part cleanup unchanged: another owner's table is ignored without a
// drop or an acknowledgement; this SNode's own table is cleaned and
// acknowledged with rejected_block_seq.
func TestRejectedPartCleanupFollowsTheOwnerCheck(t *testing.T) {
	fake := &claimsFakeS{}
	conn := &execRecorderS{}
	cfg := signingConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	view := newFakeRegistryS()
	view.setSnapshot(ownedSnapshotS(t, cfg))
	role, err := New(cfg, Deps{Client: startClaimsFakeS(t, fake), Conn: conn, Registry: view,
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	authoritySigner := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	for _, table := range []string{"db.theirs", "db.mine"} {
		cleanup := arbiter.UnsafeCleanup{TableID: table, PartitionID: "all", RejectedBlockSeq: 12,
			Parts: []arbiter.PartRef{{TableID: table, PartitionID: "all", PartRowLtHash: "0xaa", PartName: "all_1_1_0"}}}
		jws, err := authoritySigner.SignCleanup(cleanup)
		if err != nil {
			t.Fatal(err)
		}
		if err := role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), jws); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
	}
	if got, want := conn.executed(), []string{"ALTER TABLE hg_unsafe.db__mine DROP PART 'all_1_1_0'"}; !slices.Equal(got, want) {
		t.Fatalf("executed %q, want %q", got, want)
	}
	_, _, cleanups := fake.sent()
	if len(cleanups) != 1 || cleanups[0].GetTableId() != "db.mine" || cleanups[0].GetRejectedBlockSeq() != 12 {
		t.Fatalf("cleanup acknowledgements = %v", cleanups)
	}
}

// TestRejectedPartCleanupIgnoresThePartitionWatermark: cleanup never consults
// the partition watermark (plan S2-A clarification C4), unlike a promotion.
// Promotion 5 is applied through the promotion handler's own journaling tail,
// so the partition's watermark and stored acknowledgement are both 5; a
// rejected-part cleanup (promotion_seq 0) for the same partition still drops
// its parts and echoes rejected_block_seq. A `promotion_seq < watermark` guard,
// or one keyed on the stored acknowledgement, would skip it and leave the
// rejected rows in hg_unsafe. The promotion's own unsafe part keeps awaiting
// its cleanup, and once both are done every row intake accounted as unpromoted
// is accounted for: the promotion took its part out, the cleanup the rejected
// ones.
func TestRejectedPartCleanupIgnoresThePartitionWatermark(t *testing.T) {
	role, fake, conn := newRecordingSNodeS(t)
	k := partitionKey{Table: "db.t", Partition: "all"}
	promoted := recordedPartS(t, role, "all_9_9_0")
	first, second := recordedPartS(t, role, "all_1_1_0"), recordedPartS(t, role, "all_2_2_0")
	applyPromotionS(t, role, 5, promoted)
	if last, ok := role.state.LastAck(k); role.state.Watermark(k) != 5 || !ok || last.PromotionSeq != 5 {
		t.Fatalf("watermark = %d, stored ack = %+v (%v): promotion 5 must be applied", role.state.Watermark(k), last, ok)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{first, second}}
	if err := handleCleanupS(t, role, cleanup); err != nil {
		t.Fatal(err)
	}
	if got, want := conn.executed(), []string{
		"ALTER TABLE hg_unsafe.db__t DROP PART 'all_1_1_0'",
		"ALTER TABLE hg_unsafe.db__t DROP PART 'all_2_2_0'",
	}; !slices.Equal(got, want) {
		t.Fatalf("executed %q, want %q", got, want)
	}
	_, promotionAcks, cleanups := fake.sent()
	if len(promotionAcks) != 1 || promotionAcks[0].GetPromotionSeq() != 5 || len(cleanups) != 1 {
		t.Fatalf("acknowledgements: promotion %v, cleanup %v", promotionAcks, cleanups)
	}
	if got, want := wire.CleanupAckFromPB(cleanups[0]), (arbiter.CleanupAck{NodeID: authoritytest.SNodeNodeID1, TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12}); got != want {
		t.Fatalf("ack = %+v, want %+v", got, want)
	}
	if got := role.state.Watermark(k); got != 5 {
		t.Fatalf("watermark = %d after the cleanup, want 5", got)
	}
	if got, err := role.PromotedUnsafeParts("db.t"); err != nil || !slices.Equal(got, []string{"all_9_9_0"}) {
		t.Fatalf("promoted unsafe parts = %v, %v: the cleanup must not touch the promotion's own part", got, err)
	}
	requireZeroSumS(t, role, k)
}

// TestRejectedPartCleanupLeavesThePartitionQuiescent: intake accounted the
// parts of a rejected statement as unpromoted rows, and a rejected statement is
// never promoted, so its cleanup must take them out of that accounting.
// Otherwise the purge gate (tableQuiescent) and ForgetTable refuse the table
// for ever, and a re-incarnation under the same name stalls behind it.
func TestRejectedPartCleanupLeavesThePartitionQuiescent(t *testing.T) {
	role, fake, conn := newRecordingSNodeS(t)
	k := partitionKey{Table: "db.t", Partition: "all"}
	first, second := recordedPartS(t, role, "all_1_1_0"), recordedPartS(t, role, "all_2_2_0")
	if ok, err := role.tableQuiescent("db.t"); err != nil || ok {
		t.Fatalf("before the cleanup: quiescent = %v, %v; the rejected rows are unpromoted", ok, err)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{first, second}}
	if err := handleCleanupS(t, role, cleanup); err != nil {
		t.Fatal(err)
	}
	// Symptoms of one cause, each reported on its own.
	if !unpromotedSumIsZeroS(t, role, k) {
		t.Errorf("the unpromoted sum of %s/%s is not the zero accumulator", k.Table, k.Partition)
	}
	if ok, err := role.tableQuiescent("db.t"); err != nil || !ok {
		t.Errorf("after the cleanup: quiescent = %v, %v; the purge gate must be open", ok, err)
	}
	for _, p := range []arbiter.PartRef{first, second} {
		if recorded, ok := intakePartS(role, k, p.PartName); !ok || recorded != "" {
			t.Errorf("intake record of %s: present %v, removed marker %v; want both", p.PartName, ok, ok && recorded == "")
		}
	}
	if err := role.state.ForgetTable("db.t"); err != nil {
		t.Errorf("ForgetTable after the cleanup: %v", err)
	}
	if got := conn.executed(); len(got) != 2 {
		t.Fatalf("executed %q, want the two DROP PART statements", got)
	}
	if _, _, cleanups := fake.sent(); len(cleanups) != 1 || cleanups[0].GetRejectedBlockSeq() != 12 {
		t.Fatalf("cleanup acknowledgements = %v", cleanups)
	}
}

// TestRejectedPartCleanupRedeliveryIsANoOp: the subscription redelivers a
// cleanup until it is acknowledged, and ClickHouse answers NO_SUCH_DATA_PART to
// the second DROP of a part. The redelivered command leaves the accounting
// where the first one put it (no second subtraction), drops nothing that
// matters, and acknowledges again.
func TestRejectedPartCleanupRedeliveryIsANoOp(t *testing.T) {
	role, fake, conn := newRecordingSNodeS(t)
	k := partitionKey{Table: "db.t", Partition: "all"}
	first, second := recordedPartS(t, role, "all_1_1_0"), recordedPartS(t, role, "all_2_2_0")
	other := recordedPartS(t, role, "all_7_7_0") // another statement's part: the cleanup does not name it
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{first, second}}
	if err := handleCleanupS(t, role, cleanup); err != nil {
		t.Fatal(err)
	}
	want, err := lthashCombineHexAll("", []string{other.PartRowLtHash})
	if err != nil {
		t.Fatal(err)
	}
	if got := role.state.UnpromotedSum(k); got != want {
		t.Fatal("after the cleanup the unpromoted sum is not exactly the unnamed part's hash")
	}
	conn.failWith(&clickhouseproto.Exception{Code: int32(chproto.ErrNoSuchDataPart), Message: "part is absent"})
	if err := handleCleanupS(t, role, cleanup); err != nil {
		t.Fatalf("redelivered cleanup: %v", err)
	}
	if got := role.state.UnpromotedSum(k); got != want {
		t.Fatal("the redelivered cleanup changed the unpromoted sum")
	}
	if got := conn.executed(); len(got) != 4 {
		t.Fatalf("executed %q, want the two DROP PART statements twice", got)
	}
	_, _, cleanups := fake.sent()
	if len(cleanups) != 2 || wire.CleanupAckFromPB(cleanups[0]) != wire.CleanupAckFromPB(cleanups[1]) || cleanups[1].GetRejectedBlockSeq() != 12 {
		t.Fatalf("cleanup acknowledgements = %v, want the same one twice", cleanups)
	}
}

// TestRejectedPartCleanupRefusesAPartRecordedWithAnotherHash: intake recorded a
// part with another row LtHash than the command names, so the command does not
// describe that part. The cleanup fails closed before the first DROP, writes no
// state and sends no acknowledgement, like the abort path's hash mismatch.
func TestRejectedPartCleanupRefusesAPartRecordedWithAnotherHash(t *testing.T) {
	role, fake, conn := newRecordingSNodeS(t)
	k := partitionKey{Table: "db.t", Partition: "all"}
	good, bad := recordedPartS(t, role, "all_1_1_0"), recordedPartS(t, role, "all_2_2_0")
	before := role.state.UnpromotedSum(k)
	named := bad
	named.PartRowLtHash = testAccumulatorHexS(t, "another row")
	// The mismatching part is second: a check interleaved with the DROPs would
	// already have dropped the first.
	err := handleCleanupS(t, role, arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12,
		Parts: []arbiter.PartRef{good, named}})
	if err == nil || !strings.Contains(err.Error(), "all_2_2_0") {
		t.Fatalf("cleanup naming a part with another hash: err = %v, want a refusal naming all_2_2_0", err)
	}
	if got := conn.executed(); len(got) != 0 {
		t.Fatalf("executed %q before refusing", got)
	}
	if _, _, cleanups := fake.sent(); len(cleanups) != 0 {
		t.Fatalf("cleanup acknowledgements = %v, want none", cleanups)
	}
	if got := role.state.UnpromotedSum(k); got != before {
		t.Fatal("the refused cleanup changed the unpromoted sum")
	}
	for _, p := range []arbiter.PartRef{good, bad} {
		if recorded, ok := intakePartS(role, k, p.PartName); !ok || recorded != p.PartRowLtHash {
			t.Fatalf("the refused cleanup changed the intake record of %s", p.PartName)
		}
	}
}

// TestRejectedPartCleanupDropsAnUnrecordedPartWithoutTouchingTheSum: a part
// intake never recorded is dropped and acknowledged like any other, but there
// is nothing to take out of the accounting, and subtracting its hash from a sum
// that does not contain it would corrupt the sum.
func TestRejectedPartCleanupDropsAnUnrecordedPartWithoutTouchingTheSum(t *testing.T) {
	role, fake, conn := newRecordingSNodeS(t)
	k := partitionKey{Table: "db.t", Partition: "all"}
	other := recordedPartS(t, role, "all_7_7_0") // keeps the sum non-zero
	before := role.state.UnpromotedSum(k)
	unrecorded := partS(t, "all_1_1_0")
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{unrecorded}}
	if err := handleCleanupS(t, role, cleanup); err != nil {
		t.Fatal(err)
	}
	if got, want := conn.executed(), []string{"ALTER TABLE hg_unsafe.db__t DROP PART 'all_1_1_0'"}; !slices.Equal(got, want) {
		t.Fatalf("executed %q, want %q", got, want)
	}
	if _, _, cleanups := fake.sent(); len(cleanups) != 1 || cleanups[0].GetRejectedBlockSeq() != 12 {
		t.Fatalf("cleanup acknowledgements = %v", cleanups)
	}
	if got := role.state.UnpromotedSum(k); got != before {
		t.Fatal("dropping an unrecorded part changed the unpromoted sum")
	}
	if _, ok := intakePartS(role, k, unrecorded.PartName); ok {
		t.Fatal("an unrecorded part must not gain an intake record")
	}
	if recorded, ok := intakePartS(role, k, other.PartName); !ok || recorded != other.PartRowLtHash {
		t.Fatal("the cleanup changed the intake record of a part it does not name")
	}
}

// TestPromotionCleanupLeavesTheUnpromotedAccountingAlone guards the existing
// behaviour: a promotion's cleanup (rejected_block_seq 0) drops the promoted
// unsafe part and forgets its exclusion, and nothing else. The promotion
// already took the part's hash out of the unpromoted sum, so a second
// subtraction would corrupt it, and the intake record keeps the hash.
func TestPromotionCleanupLeavesTheUnpromotedAccountingAlone(t *testing.T) {
	role, fake, conn := newRecordingSNodeS(t)
	k := partitionKey{Table: "db.t", Partition: "all"}
	promoted := recordedPartS(t, role, "all_9_9_0")
	applyPromotionS(t, role, 5, promoted)
	requireZeroSumS(t, role, k)
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 5, Parts: []arbiter.PartRef{promoted}}
	if err := handleCleanupS(t, role, cleanup); err != nil {
		t.Fatal(err)
	}
	if got, want := conn.executed(), []string{"ALTER TABLE hg_unsafe.db__t DROP PART 'all_9_9_0'"}; !slices.Equal(got, want) {
		t.Fatalf("executed %q, want %q", got, want)
	}
	if got, err := role.PromotedUnsafeParts("db.t"); err != nil || len(got) != 0 {
		t.Fatalf("promoted unsafe parts = %v, %v: the promotion's cleanup must forget the exclusion", got, err)
	}
	requireZeroSumS(t, role, k)
	if recorded, ok := intakePartS(role, k, promoted.PartName); !ok || recorded != promoted.PartRowLtHash {
		t.Fatal("a promotion's cleanup changed the intake record of its part")
	}
	_, _, cleanups := fake.sent()
	if len(cleanups) != 1 || wire.CleanupAckFromPB(cleanups[0]) != (arbiter.CleanupAck{NodeID: authoritytest.SNodeNodeID1, PromotionSeq: 5, TableID: "db.t", PartitionID: "all"}) {
		t.Fatalf("cleanup acknowledgements = %v, want promotion 5's without rejected_block_seq", cleanups)
	}
}

// TestDispatchCleanupFailureNamesTheRejectedBlock: the redelivery log of a
// failed cleanup names rejected_block_seq. promotion_seq is 0 for every
// rejected-part cleanup, so without it the failures of different blocks of one
// table and partition cannot be told apart.
func TestDispatchCleanupFailureNamesTheRejectedBlock(t *testing.T) {
	var buf bytes.Buffer
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, &claimsFakeS{}), Conn: &execRecorderS{},
		Logger:      slog.New(slog.NewTextHandler(&buf, nil)),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	cleanup := wire.CleanupToPB(arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12,
		Parts: []arbiter.PartRef{{TableID: "db.t", PartitionID: "all", PartRowLtHash: "0xaa", PartName: "all_1_1_0"}}})
	err = role.dispatchPromotionCommand(context.Background(), &pb.PromotionCommand{
		Cmd:          &pb.PromotionCommand_Cleanup{Cleanup: cleanup},
		AuthorityJws: "not-a-jws",
	})
	if err == nil {
		t.Fatal("an unauthorized cleanup must fail")
	}
	logged := buf.String()
	for _, want := range []string{"level=ERROR", "unsafe cleanup failed", "promotion_seq=0", "rejected_block_seq=12", "table=db.t", "partition=all"} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log %q missing %q", logged, want)
		}
	}
}

// TestHandleCleanup_RejectedPartsAreDroppedWithoutAPromotion (ClickHouse): the
// candidate part of a statement that was never promoted leaves hg_unsafe on a
// cleanup with promotion_seq 0, and the acknowledgement names the rejected
// block (spec §7.5). Intake recorded the part under the very hash the
// cleanup names, so the cleanup also takes it out of the unpromoted accounting.
func TestHandleCleanup_RejectedPartsAreDroppedWithoutAPromotion(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := cleanupCommand(schema.TableID, part)
	cmd.PromotionSeq, cmd.RejectedBlockSeq = 0, 12
	k := partitionKey{Table: schema.TableID, Partition: part.PartitionID}
	if recorded, ok := intakePartS(role, k, part.PartName); !ok || recorded != part.PartRowLtHash {
		t.Fatalf("intake recorded %q (present %v), want the candidate's row lthash", recorded, ok)
	}
	if err := role.handleCleanup(ctx, wire.CleanupToPB(cmd), mustSignCleanup(t, signer, cmd)); err != nil {
		t.Fatalf("handleCleanup: %v", err)
	}
	assertUnsafePartGone(t, ctx, role, schema.TableID, part.PartName)
	acks := claims.cleanupAcks()
	if want := (arbiter.CleanupAck{NodeID: role.cfg.NodeID, TableID: schema.TableID, PartitionID: part.PartitionID, RejectedBlockSeq: 12}); len(acks) != 1 || acks[0] != want {
		t.Fatalf("cleanup acknowledgements = %+v, want [%+v]", acks, want)
	}
	requireZeroSumS(t, role, k)
	if recorded, ok := intakePartS(role, k, part.PartName); !ok || recorded != "" {
		t.Fatalf("intake record of %s: present %v, removed marker %v; want both", part.PartName, ok, ok && recorded == "")
	}
	if ok, err := role.tableQuiescent(schema.TableID); err != nil || !ok {
		t.Fatalf("quiescent = %v, %v after a rejected block's cleanup; the purge gate must be open", ok, err)
	}
}

// TestSNodeRegistrationAdvertisesTrackManifests: every SNode built from this
// release advertises track_manifests_v1, signed or not; the filter drops only
// signed_claims_v1, and only without a claim signer (plan S2-A contract §2).
func TestSNodeRegistrationAdvertisesTrackManifests(t *testing.T) {
	legacy, _, err := (&Role{cfg: Config{NodeID: "s1"}}).registrationRequests()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{arbiter.ClientLanesFeature, arbiter.TrackManifestsFeature}; !slices.Equal(legacy.GetFeatures(), want) {
		t.Fatalf("unsigned SNode features = %v, want %v", legacy.GetFeatures(), want)
	}
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, _ := fake.registrations()
	if len(regs) != 1 || !slices.Equal(regs[0].GetFeatures(), arbiter.LocalNodeFeatures()) {
		t.Fatalf("signing SNode registrations = %v, want features %v", regs, arbiter.LocalNodeFeatures())
	}
}
