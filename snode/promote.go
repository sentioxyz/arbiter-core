package snode

import (
	"context"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

func (r *Role) handlePromote(ctx context.Context, m *pb.PromoteSafePartition, jws string) error {
	cmd := wire.PromoteFromPB(m)
	if _, err := r.authority.AuthorizePromotion(cmd, jws); err != nil {
		return fmt.Errorf("promote authority: %w", err)
	}
	k := partitionKey{Table: cmd.TableID, Partition: cmd.PartitionID}
	mu := r.promotionLock(k)
	mu.Lock()
	defer mu.Unlock()

	if cmd.PromotionSeq <= r.state.Watermark(k) {
		if ack, ok := r.state.LastAck(k); ok && ack.PromotionSeq == cmd.PromotionSeq {
			return r.sendAck(ctx, ack)
		}
		r.d.Logger.Warn("stale promotion below watermark with no stored ack", "seq", cmd.PromotionSeq)
		return nil
	}

	baseRoot, baseSnapshotID := r.state.BaseRoot(k)
	if cmd.BasePartitionRoot != baseRoot {
		ack := arbiter.PromotionAck{
			NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
			TableID: cmd.TableID, PartitionID: cmd.PartitionID,
			Applied: false, Detail: fmt.Sprintf("base CAS mismatch: local %s, command %s", baseRoot, cmd.BasePartitionRoot),
		}
		if err := r.state.RecordAck(k, cmd.PromotionSeq, ack, baseRoot, baseSnapshotID); err != nil {
			return err
		}
		return r.sendAck(ctx, ack)
	}

	post, mappings, err := r.buildAndReplace(ctx, cmd)
	if err != nil {
		return fmt.Errorf("promotion %d: %w", cmd.PromotionSeq, err)
	}
	ack := arbiter.PromotionAck{
		NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
		TableID: cmd.TableID, PartitionID: cmd.PartitionID,
		PostPartitionCommitment: post, Applied: true, Parts: mappings,
	}
	hashes := candidateHashes(cmd)
	if err := r.state.RecordAppliedPromotion(k, cmd.PromotionSeq, ack, post, cmd.BaseSafeSnapshotID, hashes); err != nil {
		return err
	}
	return r.sendAck(ctx, ack)
}

func (r *Role) sendAck(ctx context.Context, ack arbiter.PromotionAck) error {
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).AckPromotion(ctx, wire.PromotionAckToPB(ack))
		return err
	})
}
