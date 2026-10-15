package dataplane

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core/wire"
)

// ErrStage3ReadsUnavailable is the answer of an arbiter that predates the
// stage-3 SafeState reads (housegate spec 2026-10-14 §15): it answers
// Unimplemented, which WithLeaderRetry returns at once. A caller falls back
// rather than retries; the owner audit, for one, then audits against the
// SNode's own inventory (plan S3-A clarification K3).
var ErrStage3ReadsUnavailable = errors.New("dataplane: the arbiter does not serve the stage-3 reads")

// GetPartitionStates reads the FSM's promotion state of q's partitions from
// the leader, behind one leader read barrier. It returns an error wrapping
// ErrStage3ReadsUnavailable from an arbiter without the read, any other gRPC
// refusal unchanged, and refuses an answer that names a partition outside q or
// one partition twice (SafeState reads are unauthenticated).
func (c *Client) GetPartitionStates(ctx context.Context, q wire.PartitionStatesQuery) (wire.PartitionStates, error) {
	var out wire.PartitionStates
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewSafeStateClient(conn).GetPartitionStates(ctx, wire.PartitionStatesQueryToPB(q))
		if err != nil {
			return err
		}
		out = wire.PartitionStatesFromPB(m)
		return nil
	})
	if err != nil {
		return wire.PartitionStates{}, stage3ReadError("GetPartitionStates", err)
	}
	if err := checkPartitionStates(q, out); err != nil {
		return wire.PartitionStates{}, err
	}
	return out, nil
}

// GetPromotionOutcomes reads one page of issued promotions from the leader,
// behind one leader read barrier, with GetPartitionStates' answers. It refuses
// an outcome at or before the cursor or out of ascending order, outside q's
// table or partition, carrying the command or the stored acknowledgement of
// another promotion, and more outcomes than q's limit.
func (c *Client) GetPromotionOutcomes(ctx context.Context, q wire.PromotionOutcomesQuery) (wire.PromotionOutcomes, error) {
	var out wire.PromotionOutcomes
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewSafeStateClient(conn).GetPromotionOutcomes(ctx, wire.PromotionOutcomesQueryToPB(q))
		if err != nil {
			return err
		}
		out = wire.PromotionOutcomesFromPB(m)
		return nil
	})
	if err != nil {
		return wire.PromotionOutcomes{}, stage3ReadError("GetPromotionOutcomes", err)
	}
	if err := checkPromotionOutcomes(q, out); err != nil {
		return wire.PromotionOutcomes{}, err
	}
	return out, nil
}

func stage3ReadError(method string, err error) error {
	if status.Code(err) == codes.Unimplemented {
		return fmt.Errorf("%w (%s: %s)", ErrStage3ReadsUnavailable, method, status.Convert(err).Message())
	}
	return err
}

// readScope names a query's scope in a refusal.
func readScope(table, partition string) string {
	switch {
	case table == "":
		return "every table"
	case partition == "":
		return table
	}
	return table + "/" + partition
}

func checkPartitionStates(q wire.PartitionStatesQuery, s wire.PartitionStates) error {
	seen := make(map[string]bool, len(s.Partitions))
	for _, p := range s.Partitions {
		if p.TableID != q.TableID || (q.PartitionID != "" && p.PartitionID != q.PartitionID) {
			return fmt.Errorf("dataplane: GetPartitionStates for %s answered partition %s/%s", readScope(q.TableID, q.PartitionID), p.TableID, p.PartitionID)
		}
		if seen[p.PartitionID] {
			return fmt.Errorf("dataplane: GetPartitionStates answered partition %s/%s twice", p.TableID, p.PartitionID)
		}
		seen[p.PartitionID] = true
	}
	return nil
}

func checkPromotionOutcomes(q wire.PromotionOutcomesQuery, o wire.PromotionOutcomes) error {
	if q.Limit != 0 && len(o.Outcomes) > int(q.Limit) {
		return fmt.Errorf("dataplane: GetPromotionOutcomes answered %d outcomes for a limit of %d", len(o.Outcomes), q.Limit)
	}
	previous := q.AfterPromotionSeq
	for _, out := range o.Outcomes {
		p := out.Promote
		switch {
		case out.PromotionSeq <= previous:
			return fmt.Errorf("dataplane: GetPromotionOutcomes answered promotion %d, which is not after %d", out.PromotionSeq, previous)
		case p.PromotionSeq != out.PromotionSeq:
			return fmt.Errorf("dataplane: GetPromotionOutcomes answered promotion %d carrying the command of promotion %d", out.PromotionSeq, p.PromotionSeq)
		case (q.TableID != "" && p.TableID != q.TableID) || (q.PartitionID != "" && p.PartitionID != q.PartitionID):
			return fmt.Errorf("dataplane: GetPromotionOutcomes for %s answered promotion %d of %s/%s", readScope(q.TableID, q.PartitionID), out.PromotionSeq, p.TableID, p.PartitionID)
		case out.Ack != nil && (out.Ack.PromotionSeq != out.PromotionSeq || out.Ack.TableID != p.TableID || out.Ack.PartitionID != p.PartitionID):
			return fmt.Errorf("dataplane: GetPromotionOutcomes answered promotion %d with the acknowledgement of %s/%s promotion %d", out.PromotionSeq, out.Ack.TableID, out.Ack.PartitionID, out.Ack.PromotionSeq)
		}
		previous = out.PromotionSeq
	}
	return nil
}
