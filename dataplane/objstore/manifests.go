package objstore

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// ManifestSource answers published manifests, each already checked with
// CheckManifest.
type ManifestSource interface {
	// Latest is the manifest the safe watermark names.
	Latest(ctx context.Context) (replay.SafeSnapshotManifest, error)
	// Manifest is the manifest snapshotID names.
	Manifest(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error)
}

// NewArbiterManifests reads manifests from the arbiter through the existing
// SafeState GetSafeWatermark and GetManifest RPCs, following the leader
// (dataplane.Client.WithLeaderRetry). It caches nothing and checks every
// answer with CheckManifest.
func NewArbiterManifests(c *dataplane.Client) ManifestSource {
	return arbiterManifests{c: c}
}

type arbiterManifests struct {
	c *dataplane.Client
}

func (a arbiterManifests) Latest(ctx context.Context) (replay.SafeSnapshotManifest, error) {
	var id string
	if err := a.c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		wm, err := pb.NewSafeStateClient(conn).GetSafeWatermark(ctx, &pb.GetSafeWatermarkRequest{})
		if err != nil {
			return err
		}
		id = wm.GetSnapshotId()
		return nil
	}); err != nil {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("objstore: read the safe watermark: %w", err)
	}
	if id == "" {
		return replay.SafeSnapshotManifest{}, errors.New("objstore: the arbiter has published no manifest yet")
	}
	return a.Manifest(ctx, id)
}

func (a arbiterManifests) Manifest(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error) {
	if snapshotID == "" {
		return replay.SafeSnapshotManifest{}, errors.New("objstore: a manifest is read by its snapshot id, and the id is empty")
	}
	var m replay.SafeSnapshotManifest
	if err := a.c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewSafeStateClient(conn).GetManifest(ctx, &pb.SnapshotRef{SnapshotId: snapshotID})
		if err != nil {
			return err
		}
		m = wire.ManifestFromPB(resp)
		return nil
	}); err != nil {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("objstore: read manifest %s: %w", snapshotID, err)
	}
	if err := CheckManifest(snapshotID, m); err != nil {
		return replay.SafeSnapshotManifest{}, err
	}
	return m, nil
}

// CheckManifest is spec §8.4's accept rule for a manifest read by its id:
// the id equals the requested id and the manifest's own root, Validate
// (which recomputes every root) passes, and every partition root is the sum
// of its parts' row hashes (payloadexec.VerifyLedger).
func CheckManifest(requested string, m replay.SafeSnapshotManifest) error {
	if m.SnapshotID != requested {
		return fmt.Errorf("objstore: manifest %s answered for %s", m.SnapshotID, requested)
	}
	if m.SnapshotID != m.ManifestRoot {
		return fmt.Errorf("objstore: manifest %s is not its own root %s", m.SnapshotID, m.ManifestRoot)
	}
	if err := m.Validate(); err != nil {
		return fmt.Errorf("objstore: manifest %s: %w", m.SnapshotID, err)
	}
	if err := payloadexec.VerifyLedger(m); err != nil {
		return fmt.Errorf("objstore: manifest %s: %w", m.SnapshotID, err)
	}
	return nil
}

// ownerEntry is the owner's track frontier in m.
func ownerEntry(m replay.SafeSnapshotManifest, owner uint64) (replay.TrackFrontier, error) {
	entry, ok := replay.OwnerEntry(m, owner)
	if !ok {
		return replay.TrackFrontier{}, fmt.Errorf("objstore: manifest %s has no track frontier for indexer %d", m.SnapshotID, owner)
	}
	return entry, nil
}
