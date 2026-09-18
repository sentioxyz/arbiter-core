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
			// A pre-upgrade ACK may be durable locally but not yet consumed by
			// Arbiter. Reconstruct its complete physical inventory without
			// REPLACE or subtracting the candidates from UnpromotedSums again.
			if ack.Applied && len(ack.SafePartitionParts) == 0 {
				if err := legacyAckMatchesCommand(ack, cmd); err != nil {
					return fmt.Errorf("refresh legacy promotion ack: %w", err)
				}
				sch, err := r.schemaFor(cmd.TableID)
				if err != nil {
					return err
				}
				table := CHTableName(cmd.TableID)
				mappings, err := r.safeMappings(ctx, r.cfg.SafeDatabase+"."+table, table, sch, cmd, ack.PostPartitionCommitment)
				if err != nil {
					return fmt.Errorf("refresh legacy promotion ack: %w", err)
				}
				ack.Parts, ack.SafePartitionParts = mappings.Candidates, mappings.Partition
				if err := r.state.RecordRefreshedAck(k, ack); err != nil {
					return fmt.Errorf("journal refreshed promotion ack: %w", err)
				}
			}
			return r.sendAck(ctx, ack)
		}
		r.d.Logger.Warn("stale promotion below watermark with no stored ack", "seq", cmd.PromotionSeq)
		return nil
	}
	if intent, ok := r.state.PendingPromotion(k); ok {
		post, mappings, published, err := r.reconcilePromotionIntent(ctx, cmd, intent)
		if err != nil {
			return fmt.Errorf("promotion %d recovery: %w", cmd.PromotionSeq, err)
		}
		if published {
			return r.finishAppliedPromotion(ctx, k, cmd, post, mappings)
		}
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
	return r.finishAppliedPromotion(ctx, k, cmd, post, mappings)
}

func legacyAckMatchesCommand(ack arbiter.PromotionAck, cmd arbiter.PromoteSafePartition) error {
	if ack.TableID != cmd.TableID || ack.PartitionID != cmd.PartitionID || ack.PromotionSeq != cmd.PromotionSeq {
		return fmt.Errorf("persisted ACK identity differs from retried command")
	}
	if len(ack.Parts) != len(cmd.CandidateParts) {
		return fmt.Errorf("persisted ACK candidate set differs from retried command")
	}
	hashes := make(map[string]bool, len(ack.Parts))
	for _, part := range ack.Parts {
		h, err := parseAccumulatorHex(part.PartRowLtHash)
		if err != nil || part.PartRowLtHash == "" || hashes[accumulatorHex(h)] {
			return fmt.Errorf("persisted ACK has invalid or duplicate candidate hashes")
		}
		hashes[accumulatorHex(h)] = true
	}
	for _, part := range cmd.CandidateParts {
		h, err := parseAccumulatorHex(part.PartRowLtHash)
		if err != nil || part.PartRowLtHash == "" || !hashes[accumulatorHex(h)] {
			return fmt.Errorf("persisted ACK candidate set differs from retried command")
		}
		delete(hashes, accumulatorHex(h))
	}
	post, err := lthashCombineHexAll(cmd.BasePartitionRoot, candidateHashes(cmd))
	if err != nil || post != ack.PostPartitionCommitment {
		return fmt.Errorf("persisted ACK post root differs from retried command closure")
	}
	return nil
}

func (r *Role) finishAppliedPromotion(ctx context.Context, k partitionKey, cmd arbiter.PromoteSafePartition, post string, mappings safePartMappings) error {
	ack := arbiter.PromotionAck{
		NodeID: r.cfg.NodeID, PromotionSeq: cmd.PromotionSeq,
		TableID: cmd.TableID, PartitionID: cmd.PartitionID,
		PostPartitionCommitment: post, Applied: true, Parts: mappings.Candidates,
		SafePartitionParts: mappings.Partition,
	}
	hashes := candidateHashes(cmd)
	unsafeParts := make([]string, 0, len(cmd.CandidateParts))
	for _, cp := range cmd.CandidateParts {
		unsafeParts = append(unsafeParts, cp.PartName)
	}
	if err := r.state.RecordAppliedPromotion(k, cmd.PromotionSeq, ack, post, cmd.BaseSafeSnapshotID, hashes, unsafeParts); err != nil {
		return fmt.Errorf("journal applied promotion: %w", err)
	}
	return r.sendAck(ctx, ack)
}

func (r *Role) sendAck(ctx context.Context, ack arbiter.PromotionAck) error {
	return r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).AckPromotion(ctx, wire.PromotionAckToPB(ack))
		return err
	})
}
