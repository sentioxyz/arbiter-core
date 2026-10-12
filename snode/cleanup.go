package snode

import (
	"context"
	"errors"
	"fmt"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

func (r *Role) handleCleanup(ctx context.Context, m *pb.UnsafeCleanup, jws string) error {
	cmd := wire.CleanupFromPB(m)
	if _, err := r.authority.AuthorizeCleanup(cmd, jws); err != nil {
		return fmt.Errorf("cleanup authority: %w", err)
	}
	if err := r.requireOwned(cmd.TableID); err != nil {
		r.d.Logger.Warn("ignoring a cleanup for a table this SNode does not own",
			"promotion_seq", cmd.PromotionSeq, "rejected_block_seq", cmd.RejectedBlockSeq, "table", cmd.TableID, "err", err)
		return nil
	}
	// A rejected-part cleanup (rejected_block_seq b, promotion_seq 0) names
	// candidate parts of a Rejected block that were never promoted (housegate
	// spec 2026-10-10 §7.5, plan S2-A clarification C4). Nothing here compares
	// promotion_seq with the partition watermark, so they are dropped like any
	// other cleanup. Fail closed before the first DROP when the command carries
	// a promotion_seq as well (a part a promotion applied has already left
	// UnpromotedSums, and the accounting below would subtract it a second time),
	// or when a part is not of the command's table and partition, has no name,
	// or was recorded by intake with another row LtHash than the command names;
	// the DROPs, the journaling and the acknowledgement then all use the
	// command's partition key. A promotion cleanup takes none of this: its
	// empty part_name still fails in the middle of the DROPs.
	if cmd.RejectedBlockSeq != 0 {
		k := partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}
		if err := r.state.CheckRejectedParts(k, cmd.PromotionSeq, cmd.Parts); err != nil {
			return fmt.Errorf("rejected-part cleanup for block %d: %w", cmd.RejectedBlockSeq, err)
		}
	}
	table := CHTableName(cmd.TableID)
	for _, p := range cmd.Parts {
		if p.PartName == "" {
			return fmt.Errorf("cleanup part for %s/%s has empty part_name", p.TableID, p.PartitionID)
		}
		err := r.exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s DROP PART '%s'",
			r.cfg.UnsafeDatabase, table, escapeSQLString(p.PartName)))
		if err != nil && !isCleanupGoneErr(err) {
			return fmt.Errorf("drop part %s: %w", p.PartName, err)
		}
	}
	// The acknowledgement echoes rejected_block_seq inside the signed body.
	ack := arbiter.CleanupAck{
		NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
		TableID: cmd.TableID, PartitionID: cmd.PartitionID,
		RejectedBlockSeq: cmd.RejectedBlockSeq,
	}
	// Every named part is gone now, and one state write journals the cleanup:
	// a rejected part's recorded LtHash leaves UnpromotedSums (after the DROPs,
	// so that a redelivery repairs a crash in between, and the purge gate and
	// the diagnostic source-claim root in view.go stop counting the dropped
	// rows), a promoted part leaves PromotedUnsafeParts (rejected parts never
	// entered it), and ack becomes owed to the arbiter. The purge gate
	// (registry.go) holds the table while ack is owed, so the write that
	// forgets what held the gate until now records what holds it from now on,
	// and nothing is owed for a cleanup whose DROPs did not all succeed. Only
	// the arbiter's acceptance of ack clears the entry (confirmCleanupAck);
	// until then the subscription redelivers the cleanup and
	// resendOwedCleanupAcks sends ack again in the background, from startup on.
	if err := r.state.RecordExecutedCleanup(cmd, ack); err != nil {
		return fmt.Errorf("journal cleanup: %w", err)
	}
	return r.confirmCleanupAck(ctx, ack)
}

func (r *Role) sendCleanupAck(ctx context.Context, ack arbiter.CleanupAck) error {
	req, err := r.cleanupAckRequest(ack)
	if err != nil {
		return err
	}
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).AckCleanup(ctx, req)
		return err
	})
}

// confirmCleanupAck sends ack and, once the arbiter has accepted it, clears its
// owed entry. When either fails the entry stays owed and ack is sent again
// later: the arbiter takes a repeated acknowledgement as a no-op (one for an
// already-removed pending cleanup, or for an unrecorded rejected partition).
func (r *Role) confirmCleanupAck(ctx context.Context, ack arbiter.CleanupAck) error {
	if err := r.sendCleanupAck(ctx, ack); err != nil {
		return err
	}
	if err := r.state.ConfirmCleanupAck(ack); err != nil {
		return fmt.Errorf("the arbiter accepted the cleanup acknowledgement, but clearing it from the owed acknowledgements failed: %w", err)
	}
	return nil
}

// resendOwedCleanupAcks is one pass of the background re-send: every owed
// cleanup acknowledgement is sent again, signed anew by cleanupAckRequest, and
// cleared once the arbiter accepts it. One resend interval bounds the whole
// pass, so an unreachable arbiter delays the next pass by at most that much,
// whatever the number of owed acknowledgements. A failed re-send is logged at
// warn level and stays owed, and its table's purge keeps waiting.
func (r *Role) resendOwedCleanupAcks(ctx context.Context) {
	owed := r.state.OwedCleanupAcks()
	if len(owed) == 0 {
		return
	}
	interval := r.cleanupAckResendInterval()
	pass, cancel := context.WithTimeout(ctx, interval)
	defer cancel()
	for _, ack := range owed {
		err := r.confirmCleanupAck(pass, ack)
		if ctx.Err() != nil {
			return // the role is stopping
		}
		if err != nil {
			r.d.Logger.Warn("re-sending an owed cleanup acknowledgement failed; its table's purge waits until the arbiter accepts it",
				"table", ack.TableID, "partition", ack.PartitionID,
				"promotion_seq", ack.PromotionSeq, "rejected_block_seq", ack.RejectedBlockSeq,
				"retry_in", interval, "err", err)
			continue
		}
		r.d.Logger.Info("the arbiter accepted an owed cleanup acknowledgement",
			"table", ack.TableID, "partition", ack.PartitionID,
			"promotion_seq", ack.PromotionSeq, "rejected_block_seq", ack.RejectedBlockSeq)
	}
}

// runCleanupAckResender re-sends owed cleanup acknowledgements until ctx ends:
// one pass at once, then one a resend interval after each pass ends. It never
// holds up the role: the role starts and serves while the arbiter is
// unreachable, and only the purges of tables with owed acknowledgements wait.
func (r *Role) runCleanupAckResender(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		r.resendOwedCleanupAcks(ctx)
		timer.Reset(r.cleanupAckResendInterval())
	}
}

// startCleanupAckResender runs runCleanupAckResender in the background under
// ctx; stop cancels it and waits for it to return.
func (r *Role) startCleanupAckResender(ctx context.Context) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		r.runCleanupAckResender(ctx)
	}()
	return func() {
		cancel()
		<-done
	}
}

// cleanupAckResendInterval is Config.CleanupAckResendInterval, or its default
// for a Role that New did not validate.
func (r *Role) cleanupAckResendInterval() time.Duration {
	if r.cfg.CleanupAckResendInterval > 0 {
		return r.cfg.CleanupAckResendInterval
	}
	return DefaultCleanupAckResendInterval
}

func isMissingPartErr(err error) bool {
	var exception *clickhouseproto.Exception
	return errors.As(err, &exception) && exception.Code == int32(chproto.ErrNoSuchDataPart)
}

// isCleanupGoneErr reports whether a cleanup's DROP PART found nothing left to
// drop: the part is gone (NO_SUCH_DATA_PART), or its whole table is
// (UNKNOWN_TABLE), as when this SNode purged the table before an earlier
// release had delivered the cleanup's acknowledgement. Only the cleanup path
// accepts UNKNOWN_TABLE; every other DROP keeps isMissingPartErr.
func isCleanupGoneErr(err error) bool {
	var exception *clickhouseproto.Exception
	return isMissingPartErr(err) || (errors.As(err, &exception) && exception.Code == int32(chproto.ErrUnknownTable))
}
