package dataplane

import (
	"context"
	"errors"
	"io"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type recvStream[T any] interface {
	Recv() (T, error)
}

func runSubscription[T any](
	ctx context.Context,
	c *Client,
	open func(context.Context, *grpc.ClientConn) (recvStream[T], error),
	deliver func(T) error,
) error {
	backoff := c.cfg.RetryBackoffMin
	preferred := ""
	for {
		delivered := false
		followHint := false
		for _, id := range c.candidateOrder(preferred) {
			if err := ctx.Err(); err != nil {
				return err
			}
			conn, err := c.conn(id)
			if err != nil {
				continue
			}
			stream, err := open(ctx, conn)
			if err != nil {
				if hint, ok := notLeaderHint(err); ok {
					if hint != "" && hint != id && c.hasPeer(hint) {
						preferred = hint
						followHint = true
						break
					}
					continue
				}
				if !retryableStatus(status.Code(err)) {
					return err
				}
				continue
			}
			c.setLeader(id)
			for {
				msg, err := stream.Recv()
				if err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}
					if hint, ok := notLeaderHint(err); ok {
						if hint != "" && hint != id && c.hasPeer(hint) {
							preferred = hint
							followHint = true
						}
					} else if !errors.Is(err, io.EOF) && !retryableStatus(status.Code(err)) {
						return err
					}
					break
				}
				delivered = true
				backoff = c.cfg.RetryBackoffMin
				_ = deliver(msg)
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if followHint {
				break
			}
		}
		if followHint {
			continue
		}
		if !delivered {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(backoff):
			}
			backoff *= 2
			if backoff > c.cfg.RetryBackoffMax {
				backoff = c.cfg.RetryBackoffMax
			}
		}
	}
}

func (c *Client) RunVerifierSubscription(ctx context.Context, replicaID string, onDispatch func(*pb.VerifierDispatch) error) error {
	return runSubscription(ctx, c,
		func(ctx context.Context, conn *grpc.ClientConn) (recvStream[*pb.VerifierDispatch], error) {
			return pb.NewVerifierGatewayClient(conn).SubscribeVerifierDispatch(ctx, &pb.VerifierHello{ReplicaId: replicaID})
		},
		onDispatch,
	)
}

func (c *Client) RunPromotionSubscription(ctx context.Context, nodeID string, onCommand func(*pb.PromotionCommand) error) error {
	return runSubscription(ctx, c,
		func(ctx context.Context, conn *grpc.ClientConn) (recvStream[*pb.PromotionCommand], error) {
			return pb.NewPromotionGatewayClient(conn).SubscribePromotions(ctx, &pb.SNodeHello{NodeId: nodeID})
		},
		onCommand,
	)
}
