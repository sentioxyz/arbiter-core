package dataplane

import (
	"context"
	"errors"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// ErrManifestNotFound is the leader's NotFound for GetManifest: no published
// manifest has that snapshot id.
var ErrManifestNotFound = errors.New("dataplane: the arbiter has no manifest with that snapshot id")

// ErrBlockNotSealed is the leader's NotFound for GetBlockVerification: no
// block with that seq is sealed yet.
var ErrBlockNotSealed = errors.New("dataplane: no sealed block at that seq")

// GetSafeWatermark reads the published safe-state tip from the leader, behind
// one leader read barrier (plan S3-C2 contract §21.1). A follower's NotLeader
// is followed; any other refusal is returned unchanged.
func (c *Client) GetSafeWatermark(ctx context.Context) (arbiter.SafeWatermark, error) {
	var out arbiter.SafeWatermark
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewSafeStateClient(conn).GetSafeWatermark(ctx, &pb.GetSafeWatermarkRequest{})
		if err != nil {
			return err
		}
		out = arbiter.SafeWatermark{SnapshotID: m.GetSnapshotId(), SafeBlockSeq: m.GetSafeBlockSeq(), ManifestRoot: m.GetManifestRoot()}
		return nil
	})
	if err != nil {
		return arbiter.SafeWatermark{}, err
	}
	return out, nil
}

// GetManifest reads one published manifest by snapshot id from the leader. It
// returns the manifest as the arbiter sent it, unvalidated: the member's feed
// (member/manifestfeed.Verify) decides whether to accept it. NotFound becomes
// an error wrapping ErrManifestNotFound that names the id.
func (c *Client) GetManifest(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error) {
	if snapshotID == "" {
		return replay.SafeSnapshotManifest{}, errors.New("dataplane: GetManifest needs a snapshot id")
	}
	var out replay.SafeSnapshotManifest
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewSafeStateClient(conn).GetManifest(ctx, &pb.SnapshotRef{SnapshotId: snapshotID})
		if err != nil {
			return err
		}
		out = wire.ManifestFromPB(m)
		return nil
	})
	switch {
	case status.Code(err) == codes.NotFound:
		return replay.SafeSnapshotManifest{}, fmt.Errorf("%w: %s", ErrManifestNotFound, snapshotID)
	case err != nil:
		return replay.SafeSnapshotManifest{}, err
	}
	return out, nil
}

// GetBlockVerification reads one sealed block's verification state from the
// leader: its header, replay job, candidates, verifier set, evidence and
// verdict (plan S3-A contract §1, §4). NotFound becomes ErrBlockNotSealed, an
// arbiter without the read answers ErrStage3ReadsUnavailable, and an answer
// for another block is refused, because SafeState reads are unauthenticated.
func (c *Client) GetBlockVerification(ctx context.Context, blockSeq uint64) (wire.BlockVerification, error) {
	if blockSeq == 0 {
		return wire.BlockVerification{}, errors.New("dataplane: GetBlockVerification needs a block seq")
	}
	var msg *pb.BlockVerification
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewSafeStateClient(conn).GetBlockVerification(ctx, &pb.L3BlockRef{L3BlockSeq: blockSeq})
		if err != nil {
			return err
		}
		msg = m
		return nil
	})
	switch {
	case status.Code(err) == codes.NotFound:
		return wire.BlockVerification{}, fmt.Errorf("%w: %d", ErrBlockNotSealed, blockSeq)
	case err != nil:
		return wire.BlockVerification{}, stage3ReadError("GetBlockVerification", err)
	}
	v, err := wire.BlockVerificationFromPB(msg)
	if err != nil {
		return wire.BlockVerification{}, fmt.Errorf("dataplane: GetBlockVerification(%d): %w", blockSeq, err)
	}
	if v.Header.L3BlockSeq != blockSeq {
		return wire.BlockVerification{}, fmt.Errorf("dataplane: GetBlockVerification(%d) answered block %d", blockSeq, v.Header.L3BlockSeq)
	}
	return v, nil
}
