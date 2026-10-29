package snode

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/sentioxyz/arbiter-core"
)

// The settle answer (housegate spec 2026-10-14 §14 "Exits from the release
// refusals"; plan S3-D contract §34, clarification K53). A quarantined track's
// issued promotions are never re-streamed (clarification C8), so one whose
// acknowledgement was lost would hold release refusal (iii) forever. A leader
// in settle mode streams exactly those promotions with
// PromotionCommand.settle set, and only to an SNode whose hello carried
// settle; this SNode answers each from durable state only. It never builds a
// shadow, never runs REPLACE and never writes hg_safe.

// SettleNotAppliedDetail is the Detail of the not-applied acknowledgement a
// settle answer sends for a promotion this SNode never applied; the arbiter
// consumes the promotion on it.
const SettleNotAppliedDetail = "settle: promotion %d was not applied on this SNode"

// SettleOutcome is how a settle-streamed promotion was answered.
type SettleOutcome string

const (
	// SettleJournaled: the journaled acknowledgement of the promotion was
	// sent again.
	SettleJournaled SettleOutcome = "journaled"
	// SettleReconciled: the promotion's published intent was finished as a
	// redelivered promotion finishes it, and its applied acknowledgement sent.
	SettleReconciled SettleOutcome = "reconciled"
	// SettleNotApplied: the promotion never reached hg_safe; a not-applied
	// acknowledgement was journaled and sent.
	SettleNotApplied SettleOutcome = "not_applied"
	// SettleUnanswerable: the promotion is at or below the partition's
	// watermark with no stored acknowledgement of it; nothing was sent.
	SettleUnanswerable SettleOutcome = "unanswerable"
)

// SettleStats is a point-in-time copy of the settle answers: one count per
// outcome, all four always present, and the clock at the last answer (zero
// before the first). An answer counts once it was sent (unanswerable: once it
// was decided), so a failed send counts nothing and its redelivery counts once.
type SettleStats struct {
	Answers      map[SettleOutcome]uint64
	LastSettleAt time.Time
}

// settleCounters is SettleStats behind a mutex; its zero value is ready.
type settleCounters struct {
	mu      sync.Mutex
	answers map[SettleOutcome]uint64
	last    time.Time
}

// SettleStats returns a copy of the settle answer counters.
func (r *Role) SettleStats() SettleStats {
	r.settle.mu.Lock()
	defer r.settle.mu.Unlock()
	out := SettleStats{Answers: map[SettleOutcome]uint64{SettleJournaled: 0, SettleReconciled: 0, SettleNotApplied: 0, SettleUnanswerable: 0},
		LastSettleAt: r.settle.last}
	maps.Copy(out.Answers, r.settle.answers)
	return out
}

func (r *Role) countSettle(o SettleOutcome) {
	now := r.now()
	r.settle.mu.Lock()
	defer r.settle.mu.Unlock()
	if r.settle.answers == nil {
		r.settle.answers = map[SettleOutcome]uint64{}
	}
	r.settle.answers[o]++
	r.settle.last = now
}

// answerSettle answers cmd, streamed with settle, under k's promotion lock and
// after handlePromote's authorization and ownership check, from durable state
// only, in this order: (1) at or below the watermark with cmd's stored
// acknowledgement: resend it (journaled); (2) at or below the watermark
// otherwise: nothing to answer (unanswerable); (3) a pending intent whose
// REPLACE is visible: finish it as a redelivered promotion does (reconciled);
// (4) otherwise, an intent whose REPLACE never became visible included: a
// journaled not-applied acknowledgement (not_applied).
func (r *Role) answerSettle(ctx context.Context, k partitionKey, cmd arbiter.PromoteSafePartition) error {
	if watermark := r.state.Watermark(k); cmd.PromotionSeq <= watermark {
		ack, ok := r.state.LastAck(k)
		if !ok || ack.PromotionSeq != cmd.PromotionSeq {
			r.d.Logger.Warn(fmt.Sprintf("settle: promotion %d is below this SNode's watermark %d with no stored acknowledgement; nothing to answer",
				cmd.PromotionSeq, watermark), "table", cmd.TableID, "partition", cmd.PartitionID)
			r.countSettle(SettleUnanswerable)
			return nil
		}
		if err := r.resendStoredAck(ctx, k, cmd, ack); err != nil {
			return fmt.Errorf("settle: promotion %d: %w", cmd.PromotionSeq, err)
		}
		r.countSettle(SettleJournaled)
		return nil
	}
	if intent, ok := r.state.PendingPromotion(k); ok {
		post, mappings, published, err := r.reconcilePromotionIntent(ctx, cmd, intent)
		if err != nil {
			return fmt.Errorf("settle: promotion %d recovery: %w", cmd.PromotionSeq, err)
		}
		if published {
			if err := r.finishAppliedPromotion(ctx, k, cmd, post, mappings); err != nil {
				return fmt.Errorf("settle: promotion %d: %w", cmd.PromotionSeq, err)
			}
			r.countSettle(SettleReconciled)
			return nil
		}
	}
	return r.settleNotApplied(ctx, k, cmd)
}

// settleNotApplied journals and sends the not-applied acknowledgement of cmd.
// The journal comes first, so a redelivery after a failed send resends it
// (journaled). A shadow of the partition left in hg_promote by an attempt
// that never published is dropped; a failure to drop it is logged and does
// not hold the answer back, because the next promotion's shadow build drops
// the partition first (prepareShadow).
func (r *Role) settleNotApplied(ctx context.Context, k partitionKey, cmd arbiter.PromoteSafePartition) error {
	baseRoot, baseSnapshotID := r.state.BaseRoot(k)
	ack := arbiter.PromotionAck{
		NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
		TableID: cmd.TableID, PartitionID: cmd.PartitionID,
		Applied: false, Detail: fmt.Sprintf(SettleNotAppliedDetail, cmd.PromotionSeq),
	}
	if err := r.state.RecordSettledNotApplied(k, cmd.PromotionSeq, ack, baseRoot, baseSnapshotID); err != nil {
		return fmt.Errorf("settle: journal the not-applied answer to promotion %d: %w", cmd.PromotionSeq, err)
	}
	if err := r.dropSettledShadow(ctx, cmd); err != nil {
		r.d.Logger.Warn("settle: the hg_promote shadow of a not-applied promotion could not be dropped; the next promotion of the partition drops it",
			"promotion_seq", cmd.PromotionSeq, "table", cmd.TableID, "partition", cmd.PartitionID, "err", err)
	}
	if err := r.sendAck(ctx, ack); err != nil {
		return fmt.Errorf("settle: promotion %d: %w", cmd.PromotionSeq, err)
	}
	r.countSettle(SettleNotApplied)
	return nil
}

func (r *Role) dropSettledShadow(ctx context.Context, cmd arbiter.PromoteSafePartition) error {
	if r.d.Conn == nil {
		return nil
	}
	sch, err := r.schemaFor(cmd.TableID)
	if err != nil {
		return err
	}
	partition, err := quotePartition(sch, cmd.PartitionID)
	if err != nil {
		return err
	}
	return r.dropPartitionIfPresent(ctx, r.cfg.PromoteDatabase, CHTableName(cmd.TableID), sch, cmd.PartitionID, partition)
}
