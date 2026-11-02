package dataplane

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/sentioxyz/arbiter-core/wire"
)

// ErrStage3SwitchOff is a stage-3 member RPC's answer before its switch (plan
// S3-E1 K67): the leader's FAILED_PRECONDITION without a NotLeader detail. A
// member reads it as "not yet": it retries at its interval and never pages.
var ErrStage3SwitchOff = errors.New("dataplane: the arbiter refuses this stage-3 command until its switch is on")

// SubmitMemberReady submits one signed MemberReady (Raft tag 40) to the
// leader. It returns nil on Ack (a repeat at the same generation is
// idempotent), an error wrapping ErrStage3SwitchOff with the leader's message
// on a FAILED_PRECONDITION without a NotLeader detail (before the placement
// switch), and the gRPC error otherwise.
func (c *Client) SubmitMemberReady(ctx context.Context, cmd wire.MemberReady) error {
	req := wire.MemberReadyToRequest(cmd)
	return c.submitStage3(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitMemberReady(ctx, req)
		return err
	})
}

// SubmitMemberCheckpoint submits one signed MemberCheckpoint (Raft tag 41) to
// the leader, with the answers of SubmitMemberReady (the switch is the
// checkpoints switch).
func (c *Client) SubmitMemberCheckpoint(ctx context.Context, cmd wire.MemberCheckpoint) error {
	req := wire.MemberCheckpointToRequest(cmd)
	return c.submitStage3(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewVerifierGatewayClient(conn).SubmitMemberCheckpoint(ctx, req)
		return err
	})
}

// submitStage3 runs call through WithLeaderRetry, which treats every
// FAILED_PRECONDITION as a leader miss; a leader's own precondition is
// intercepted here, so it ends the call instead of being retried.
func (c *Client) submitStage3(ctx context.Context, call func(ctx context.Context, conn *grpc.ClientConn) error) error {
	var switchOff error
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		err := call(ctx, conn)
		if st, ok := leaderPrecondition(err); ok {
			switchOff = fmt.Errorf("%w: %s", ErrStage3SwitchOff, st.Message())
			return nil
		}
		return err
	})
	if err != nil {
		return err
	}
	return switchOff
}
