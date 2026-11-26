package dataplane

import (
	"context"
	"errors"
	"io"

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
	pacer := newRetryPacer(c.cfg)
	preferred := ""
	for {
		delivered := false
		followHint := false
		limited := false
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
				if perPeerLimited(err) {
					preferred = id
					limited = true
					break
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
					} else if perPeerLimited(err) {
						// A server-streaming open is refused on the first
						// Recv: wait, then ask the same peer again.
						preferred = id
						limited = true
					} else if !errors.Is(err, io.EOF) && !retryableStatus(status.Code(err)) {
						return err
					}
					break
				}
				delivered = true
				pacer.reset()
				// A handler error does not end the stream: the arbiter re-dispatches
				// unrecorded work. Handlers own WARN-level reporting of their
				// failures (the verifier logs rejected evidence submissions, the
				// SNode logs failed promotion commands); this debug line keeps a
				// handler that forgot to from failing silently.
				if err := deliver(msg); err != nil && ctx.Err() == nil {
					c.cfg.Logger.Debug("dataplane subscription handler returned an error; continuing", "peer", id, "err", err)
				}
				if err := ctx.Err(); err != nil {
					return err
				}
			}
			if followHint || limited {
				break
			}
		}
		if limited {
			if err := pacer.wait(ctx); err != nil {
				return err
			}
			continue
		}
		// A hint is followed at once only once per round: two followers that
		// name each other during an election must not spin the loop. A stream
		// that delivered before it ended reconnects at once.
		if followHint && pacer.hop() {
			continue
		}
		if delivered && !followHint {
			continue
		}
		if err := pacer.wait(ctx); err != nil {
			return err
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

// RunPromotionSubscription subscribes nodeID to the leader's promotion
// stream. Its hello always carries settle: every SNode of this release
// answers PromotionCommand.settle from its journal (housegate spec 2026-10-14
// §14, P4), and a leader older than the field ignores it.
func (c *Client) RunPromotionSubscription(ctx context.Context, nodeID string, onCommand func(*pb.PromotionCommand) error) error {
	return runSubscription(ctx, c,
		func(ctx context.Context, conn *grpc.ClientConn) (recvStream[*pb.PromotionCommand], error) {
			return pb.NewPromotionGatewayClient(conn).SubscribePromotions(ctx, &pb.SNodeHello{NodeId: nodeID, Settle: true})
		},
		onCommand,
	)
}
