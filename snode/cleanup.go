package snode

import (
	"context"
	"errors"
	"fmt"

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
	// other cleanup. Fail closed before the first DROP when intake recorded one
	// of them with another row LtHash than the command names.
	rejected := cmd.RejectedBlockSeq != 0
	if rejected {
		if err := r.state.CheckRejectedParts(cmd.Parts); err != nil {
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
		if err != nil && !isMissingPartErr(err) {
			return fmt.Errorf("drop part %s: %w", p.PartName, err)
		}
	}
	// RecordCleanup below is a no-op for rejected parts: they never entered
	// PromotedUnsafeParts. Their recorded LtHash leaves UnpromotedSums instead,
	// after the DROPs so that a redelivery repairs a crash in between, and the
	// purge gate (registry.go) and the diagnostic source-claim root (view.go)
	// stop counting the dropped rows.
	if rejected {
		if err := r.state.RemoveRejectedParts(cmd.Parts); err != nil {
			return fmt.Errorf("journal rejected parts: %w", err)
		}
	}
	names := make([]string, 0, len(cmd.Parts))
	for _, p := range cmd.Parts {
		names = append(names, p.PartName)
	}
	if err := r.state.RecordCleanup(partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}, names); err != nil {
		return fmt.Errorf("journal cleanup: %w", err)
	}
	// The acknowledgement echoes rejected_block_seq inside the signed body.
	ack := arbiter.CleanupAck{
		NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
		TableID: cmd.TableID, PartitionID: cmd.PartitionID,
		RejectedBlockSeq: cmd.RejectedBlockSeq,
	}
	return r.sendCleanupAck(ctx, ack)
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

func isMissingPartErr(err error) bool {
	var exception *clickhouseproto.Exception
	return errors.As(err, &exception) && exception.Code == int32(chproto.ErrNoSuchDataPart)
}
