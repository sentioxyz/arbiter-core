package dataplane

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
)

// ErrTableNotPurging is SubmitTablePurged's FAILED_PRECONDITION answer: the
// incarnation is not Purging yet. It is retried, never read as success.
var ErrTableNotPurging = errors.New("dataplane: incarnation is not purging yet")

// SubmitTablePurged reports that nodeID dropped its hg_* tables for one
// Purging incarnation. It returns nil on Ack (including the idempotent
// already-recorded and already-Purged answers), an error wrapping
// ErrTableNotPurging on the leader's FAILED_PRECONDITION, and the gRPC error
// otherwise (an arbiter without the RPC answers Unimplemented; the caller
// retries it like a transport error).
func (c *Client) SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error {
	var precondition error
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).SubmitTablePurged(ctx, &pb.RecordTablePurgedCmd{NodeId: nodeID, IncarnationSeq: incarnationSeq})
		if st, ok := leaderPrecondition(err); ok {
			precondition = fmt.Errorf("%w: %s", ErrTableNotPurging, st.Message())
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return precondition
}

// PurgeNodeSet returns the arbiter's current purge node set: every
// registered, non-evicted SNode and verifier, sorted ascending.
func (c *Client) PurgeNodeSet(ctx context.Context) ([]string, error) {
	var out []string
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewTableRegistryClient(conn).GetPurgeNodeSet(ctx, &emptypb.Empty{})
		if err != nil {
			return err
		}
		out = m.GetNodeIds()
		return nil
	})
	return out, err
}
