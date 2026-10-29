package snode

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The SNode's settle answer (housegate spec 2026-10-14 §14 "Exits from the
// release refusals"; plan S3-D contract §34, clarification K53): a promotion
// streamed with PromotionCommand.settle is answered from durable state only
// and never applied.

// seedSettleCommand is the promotion of the seeded statement's one part at
// seq: what the leader would stream for it.
func seedSettleCommand(schema payloadexec.TableSchema, part arbiter.CandidatePart, seq uint64, baseRoot string) arbiter.PromoteSafePartition {
	return arbiter.PromoteSafePartition{
		TableID: schema.TableID, PartitionID: part.PartitionID, PromotionSeq: seq,
		BaseSafeSnapshotID: "genesis", BasePartitionRoot: baseRoot,
		CandidateParts: []arbiter.PartRef{{TableID: schema.TableID, PartitionID: part.PartitionID,
			PartRowLtHash: part.PartRowLtHash, PartName: part.PartName}},
	}
}

// settle delivers cmd through the subscription dispatch with settle set.
func settle(t *testing.T, ctx context.Context, role *Role, signer commandSigner, cmd arbiter.PromoteSafePartition) error {
	t.Helper()
	return role.dispatchPromotionCommand(ctx, wire.SettlePromotionCommandPB(cmd, mustSignPromotion(t, signer, cmd)))
}

func settleAnswers(role *Role) map[SettleOutcome]uint64 { return role.SettleStats().Answers }

func wantSettleAnswers(t *testing.T, role *Role, want map[SettleOutcome]uint64) {
	t.Helper()
	full := map[SettleOutcome]uint64{SettleJournaled: 0, SettleReconciled: 0, SettleNotApplied: 0, SettleUnanswerable: 0}
	for k, v := range want {
		full[k] = v
	}
	if got := settleAnswers(role); !reflect.DeepEqual(got, full) {
		t.Fatalf("settle answers = %v, want %v", got, full)
	}
}

// recordingConn records every statement it runs.
type recordingConn struct {
	clickhouse.Conn
	mu         sync.Mutex
	statements []string
}

func (c *recordingConn) Exec(ctx context.Context, query string, args ...any) error {
	c.mu.Lock()
	c.statements = append(c.statements, query)
	c.mu.Unlock()
	return c.Conn.Exec(ctx, query, args...)
}

func (c *recordingConn) executed() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.statements)
}

// failReplaceConn refuses the REPLACE PARTITION statement, so a promotion
// journals its intent and builds its shadow but never publishes.
type failReplaceConn struct{ clickhouse.Conn }

func (c failReplaceConn) Exec(ctx context.Context, query string, args ...any) error {
	if strings.Contains(query, " REPLACE PARTITION ") {
		return errors.New("injected pre-REPLACE failure")
	}
	return c.Conn.Exec(ctx, query, args...)
}

func TestSettleResendsTheJournaledAck(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := promoteSeededPart(t, ctx, role, claims, signer, schema)
	table := CHTableName(schema.TableID)
	before := activePartsMust(t, ctx, role.d.Conn, role.cfg.SafeDatabase, table)
	pk := partitionKey{Table: schema.TableID, Partition: part.PartitionID}
	stateBefore := cloneLocalState(role.state.s)

	if err := settle(t, ctx, role, signer, seedSettleCommand(schema, part, 1, "")); err != nil {
		t.Fatalf("settle: %v", err)
	}

	acks := claims.promotionAcks()
	if len(acks) != 2 || !reflect.DeepEqual(acks[0], acks[1]) || !acks[1].Applied {
		t.Fatalf("the settle did not resend the journaled acknowledgement: %+v", acks)
	}
	if after := activePartsMust(t, ctx, role.d.Conn, role.cfg.SafeDatabase, table); !reflect.DeepEqual(before, after) {
		t.Fatalf("hg_safe changed: %+v -> %+v", before, after)
	}
	if !reflect.DeepEqual(cloneLocalState(role.state.s), stateBefore) {
		t.Fatal("resending the journaled acknowledgement changed the journal")
	}
	if got := role.state.Watermark(pk); got != 1 {
		t.Fatalf("watermark = %d, want 1", got)
	}
	wantSettleAnswers(t, role, map[SettleOutcome]uint64{SettleJournaled: 1})
}

func TestSettleAnswersNotAppliedAndJournalsIt(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	pk := partitionKey{Table: schema.TableID, Partition: part.PartitionID}
	unpromoted := role.state.UnpromotedSum(pk)
	cmd := seedSettleCommand(schema, part, 1, "")

	if err := settle(t, ctx, role, signer, cmd); err != nil {
		t.Fatalf("settle: %v", err)
	}

	acks := claims.promotionAcks()
	want := arbiter.PromotionAck{NodeID: role.cfg.NodeID, PromotionSeq: 1, TableID: schema.TableID, PartitionID: part.PartitionID,
		Applied: false, Detail: "settle: promotion 1 was not applied on this SNode"}
	if len(acks) != 1 || !reflect.DeepEqual(acks[0], want) {
		t.Fatalf("acks = %+v, want [%+v]", acks, want)
	}
	if got := fmt.Sprintf(SettleNotAppliedDetail, 1); got != want.Detail {
		t.Fatalf("SettleNotAppliedDetail renders %q", got)
	}
	restarted, err := New(role.cfg, role.d)
	if err != nil {
		t.Fatal(err)
	}
	if got := restarted.state.Watermark(pk); got != 1 {
		t.Fatalf("journaled watermark = %d, want 1", got)
	}
	if ack, ok := restarted.state.LastAck(pk); !ok || !reflect.DeepEqual(ack, want) {
		t.Fatalf("journaled ack = %+v, %v", ack, ok)
	}
	if root, snap := restarted.state.BaseRoot(pk); root != "" || snap != "" {
		t.Fatalf("base = (%s, %s): a not-applied answer keeps the base", root, snap)
	}
	if got := restarted.state.UnpromotedSum(pk); got != unpromoted {
		t.Fatal("a not-applied answer must not take the candidates out of the unpromoted sum")
	}
	if got := rowCount(t, ctx, role.d.Conn, role.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 0 {
		t.Fatalf("hg_safe rows = %d, want 0", got)
	}
	wantSettleAnswers(t, role, map[SettleOutcome]uint64{SettleNotApplied: 1})

	if err := settle(t, ctx, restarted, signer, cmd); err != nil {
		t.Fatalf("second settle: %v", err)
	}
	if acks := claims.promotionAcks(); len(acks) != 2 || !reflect.DeepEqual(acks[1], want) {
		t.Fatalf("the second settle must resend the journaled not-applied ack: %+v", acks)
	}
	wantSettleAnswers(t, restarted, map[SettleOutcome]uint64{SettleJournaled: 1})
}

func TestSettleFinishesAPublishedIntent(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := seedSettleCommand(schema, part, 1, "")
	baseConn := role.d.Conn
	failing := &failAfterReplaceConn{Conn: baseConn}
	role.d.Conn = failing
	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), mustSignPromotion(t, signer, cmd), false); err == nil || !failing.replaced {
		t.Fatalf("the promotion must fail after its REPLACE became visible: %v", err)
	}
	deps := role.d
	deps.Conn = baseConn
	restarted, err := New(role.cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	pk := partitionKey{Table: schema.TableID, Partition: part.PartitionID}
	if _, ok := restarted.state.PendingPromotion(pk); !ok {
		t.Fatal("the fixture must leave a published, unfinalized intent")
	}

	if err := settle(t, ctx, restarted, signer, cmd); err != nil {
		t.Fatalf("settle: %v", err)
	}

	acks := claims.promotionAcks()
	if len(acks) != 1 || !acks[0].Applied || acks[0].PromotionSeq != 1 {
		t.Fatalf("acks = %+v, want one applied acknowledgement of promotion 1", acks)
	}
	if _, ok := restarted.state.PendingPromotion(pk); ok {
		t.Fatal("the settle must finalize the published intent")
	}
	if got := rowCount(t, ctx, baseConn, restarted.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 2 {
		t.Fatalf("hg_safe rows = %d, want the 2 published rows once", got)
	}
	assertCompleteSafeInventory(t, ctx, restarted, schema, part.PartitionID, acks[0].SafePartitionParts)
	wantSettleAnswers(t, restarted, map[SettleOutcome]uint64{SettleReconciled: 1})
}

func TestSettleRollsBackAnUnpublishedIntentAndAnswersNotApplied(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := seedSettleCommand(schema, part, 1, "")
	baseConn := role.d.Conn
	role.d.Conn = failReplaceConn{Conn: baseConn}
	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), mustSignPromotion(t, signer, cmd), false); err == nil ||
		!strings.Contains(err.Error(), "injected pre-REPLACE failure") {
		t.Fatalf("the promotion must fail at its REPLACE: %v", err)
	}
	role.d.Conn = baseConn
	pk := partitionKey{Table: schema.TableID, Partition: part.PartitionID}
	if _, ok := role.state.PendingPromotion(pk); !ok {
		t.Fatal("the fixture must leave an unpublished intent")
	}
	promote := activePartsMust(t, ctx, baseConn, role.cfg.PromoteDatabase, CHTableName(schema.TableID))
	if len(partsInLogicalPartition(promote, schema, part.PartitionID)) == 0 {
		t.Fatal("the fixture must leave the shadow in hg_promote")
	}

	if err := settle(t, ctx, role, signer, cmd); err != nil {
		t.Fatalf("settle: %v", err)
	}

	acks := claims.promotionAcks()
	if len(acks) != 1 || acks[0].Applied || acks[0].Detail != "settle: promotion 1 was not applied on this SNode" {
		t.Fatalf("acks = %+v", acks)
	}
	if _, ok := role.state.PendingPromotion(pk); ok {
		t.Fatal("the not-applied answer must remove the intent of its promotion")
	}
	assertNoPromotePartition(t, ctx, role, schema, part.PartitionID)
	if got := rowCount(t, ctx, baseConn, role.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 0 {
		t.Fatalf("hg_safe rows = %d, want 0", got)
	}
	if parts, err := role.PromotedUnsafeParts(schema.TableID); err != nil || parts != nil {
		t.Fatalf("PromotedUnsafeParts = %v, %v: with the intent gone unsafe_latest reads are available and nothing is excluded", parts, err)
	}
	wantSettleAnswers(t, role, map[SettleOutcome]uint64{SettleNotApplied: 1})
}

func TestSettleBelowTheWatermarkWithoutAnAckAnswersNothing(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	var logs bytes.Buffer
	role.d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	if err := settle(t, ctx, role, signer, seedSettleCommand(schema, part, 2, "")); err != nil {
		t.Fatalf("settle of promotion 2: %v", err)
	}
	if len(claims.promotionAcks()) != 1 {
		t.Fatalf("acks = %+v", claims.promotionAcks())
	}

	if err := settle(t, ctx, role, signer, seedSettleCommand(schema, part, 1, "")); err != nil {
		t.Fatalf("settle of promotion 1: %v", err)
	}

	if got := len(claims.promotionAcks()); got != 1 {
		t.Fatalf("a settle below the watermark with no stored acknowledgement must send nothing; acks = %d", got)
	}
	if !strings.Contains(logs.String(), "settle: promotion 1 is below this SNode's watermark 2 with no stored acknowledgement; nothing to answer") {
		t.Fatalf("missing warning; logs:\n%s", logs.String())
	}
	wantSettleAnswers(t, role, map[SettleOutcome]uint64{SettleNotApplied: 1, SettleUnanswerable: 1})
}

func TestSettleNeverBuildsAShadowOrTouchesHgSafe(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	table := CHTableName(schema.TableID)
	unsafeBefore := activePartsMust(t, ctx, role.d.Conn, role.cfg.UnsafeDatabase, table)
	recording := &recordingConn{Conn: role.d.Conn}
	role.d.Conn = recording

	if err := settle(t, ctx, role, signer, seedSettleCommand(schema, part, 1, "")); err != nil {
		t.Fatalf("settle: %v", err)
	}

	for _, statement := range recording.executed() {
		if strings.Contains(statement, "REPLACE PARTITION") || strings.Contains(statement, "ATTACH PARTITION") ||
			strings.Contains(statement, role.cfg.SafeDatabase+".") || strings.Contains(statement, role.cfg.UnsafeDatabase+".") {
			t.Fatalf("the settle ran %q: it never builds a shadow and never writes hg_safe or hg_unsafe", statement)
		}
	}
	pk := partitionKey{Table: schema.TableID, Partition: part.PartitionID}
	if _, ok := role.state.PendingPromotion(pk); ok {
		t.Fatal("the settle journaled a promotion intent")
	}
	if got := rowCount(t, ctx, recording.Conn, role.cfg.SafeDatabase+"."+table); got != 0 {
		t.Fatalf("hg_safe rows = %d", got)
	}
	if after := activePartsMust(t, ctx, recording.Conn, role.cfg.UnsafeDatabase, table); !reflect.DeepEqual(after, unsafeBefore) {
		t.Fatalf("hg_unsafe changed: %+v -> %+v", unsafeBefore, after)
	}
	assertNoPromotePartition(t, ctx, role, schema, part.PartitionID)
}

func TestPromoteWithoutSettleIsUnchanged(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := seedSettleCommand(schema, part, 1, "")
	plain := wire.PromotionCommandPB(cmd, mustSignPromotion(t, signer, cmd))
	if plain.GetSettle() {
		t.Fatal("PromotionCommandPB must leave settle unset")
	}

	if err := role.dispatchPromotionCommand(ctx, plain); err != nil {
		t.Fatalf("promote: %v", err)
	}

	acks := claims.promotionAcks()
	if len(acks) != 1 || !acks[0].Applied || acks[0].PromotionSeq != 1 || acks[0].Detail != "" {
		t.Fatalf("acks = %+v, want the applied acknowledgement of an ordinary promotion", acks)
	}
	assertExactSafePartMappings(t, ctx, role, schema, part, acks[0].Parts)
	assertCompleteSafeInventory(t, ctx, role, schema, part.PartitionID, acks[0].SafePartitionParts)
	if got := rowCount(t, ctx, role.d.Conn, role.cfg.SafeDatabase+"."+CHTableName(schema.TableID)); got != 2 {
		t.Fatalf("hg_safe rows = %d, want 2", got)
	}
	wantSettleAnswers(t, role, nil)
	if !role.SettleStats().LastSettleAt.IsZero() {
		t.Fatal("an ordinary promotion is not a settle answer")
	}
	stale := seedSettleCommand(schema, part, 2, "0xwrong")
	if err := role.dispatchPromotionCommand(ctx, wire.PromotionCommandPB(stale, mustSignPromotion(t, signer, stale))); err != nil {
		t.Fatalf("base CAS promote: %v", err)
	}
	if acks := claims.promotionAcks(); len(acks) != 2 || acks[1].Applied || !strings.HasPrefix(acks[1].Detail, "base CAS mismatch: local ") {
		t.Fatalf("acks = %+v, want the base CAS refusal of today's path", acks)
	}
	wantSettleAnswers(t, role, nil)
}

// TestSettleStatsCountEveryOutcome: the four outcomes are always present,
// zero included, the counts are a copy, and LastSettleAt is the clock at the
// last counted answer; a Role built without New has zero stats.
func TestSettleStatsCountEveryOutcome(t *testing.T) {
	at := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	r := &Role{d: Deps{Now: func() time.Time { return at }}}
	if got := r.SettleStats(); len(got.Answers) != 4 || !got.LastSettleAt.IsZero() {
		t.Fatalf("zero stats = %+v", got)
	}
	for _, o := range []SettleOutcome{SettleJournaled, SettleReconciled, SettleNotApplied, SettleNotApplied, SettleUnanswerable} {
		r.countSettle(o)
	}
	got := r.SettleStats()
	want := map[SettleOutcome]uint64{SettleJournaled: 1, SettleReconciled: 1, SettleNotApplied: 2, SettleUnanswerable: 1}
	if !reflect.DeepEqual(got.Answers, want) || !got.LastSettleAt.Equal(at) {
		t.Fatalf("stats = %+v, want %v at %s", got, want, at)
	}
	got.Answers[SettleJournaled] = 99
	if r.SettleStats().Answers[SettleJournaled] != 1 {
		t.Fatal("SettleStats must return a copy")
	}
	for o, label := range map[SettleOutcome]string{SettleJournaled: "journaled", SettleReconciled: "reconciled", SettleNotApplied: "not_applied", SettleUnanswerable: "unanswerable"} {
		if string(o) != label {
			t.Fatalf("outcome %q, want %q", o, label)
		}
	}
}
