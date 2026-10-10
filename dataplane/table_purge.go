package dataplane

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core/wire"
)

// ErrTableNotPurging is SubmitTablePurged's FAILED_PRECONDITION answer: the
// incarnation is not Purging yet. It is retried, never read as success.
var ErrTableNotPurging = errors.New("dataplane: incarnation is not purging yet")

// SubmitTablePurged reports, unsigned, that nodeID dropped its hg_* tables
// for one Purging incarnation. See SubmitTablePurgedReport for the answers.
func (c *Client) SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error {
	return c.SubmitTablePurgedReport(ctx, wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq})
}

// SubmitTablePurgedReport submits a purge report that may carry the
// reporter's signature (housegate spec 2026-10-10 §6.5). It returns nil on
// Ack (including the idempotent already-recorded and already-Purged
// answers), an error wrapping ErrTableNotPurging on the leader's
// FAILED_PRECONDITION, and the gRPC error otherwise (an arbiter without the
// RPC answers Unimplemented; the caller retries it like a transport error).
func (c *Client) SubmitTablePurgedReport(ctx context.Context, report wire.RecordTablePurged) error {
	var precondition error
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewPromotionGatewayClient(conn).SubmitTablePurged(ctx, wire.RecordTablePurgedToRequest(report))
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

// PurgeReporter is the tableset.Arbiter of a signing role: it signs every
// purge report before submitting it. A nil Sign reports unsigned, exactly
// like Client.
type PurgeReporter struct {
	Client *Client
	Sign   func(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error)
}

// SubmitTablePurged signs and submits one purge report.
func (p PurgeReporter) SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error {
	report := wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq}
	if p.Sign != nil {
		signed, err := p.Sign(nodeID, incarnationSeq)
		if err != nil {
			return fmt.Errorf("sign table purged report: %w", err)
		}
		if signed.NodeID != nodeID || signed.IncarnationSeq != incarnationSeq {
			return fmt.Errorf("sign table purged report: signed %s/%d, want %s/%d", signed.NodeID, signed.IncarnationSeq, nodeID, incarnationSeq)
		}
		report = signed
	}
	return p.Client.SubmitTablePurgedReport(ctx, report)
}

// PurgeNodeSet is Client.PurgeNodeSet.
func (p PurgeReporter) PurgeNodeSet(ctx context.Context) ([]string, error) {
	return p.Client.PurgeNodeSet(ctx)
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
