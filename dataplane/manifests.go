package dataplane

import (
	"context"
	"fmt"
	"sync"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
)

type ManifestStore struct {
	c *Client

	mu sync.Mutex
	m  map[string]replay.SafeSnapshotManifest
}

var _ replay.SnapshotStore = (*ManifestStore)(nil)

func NewManifestStore(c *Client) *ManifestStore {
	return &ManifestStore{c: c, m: make(map[string]replay.SafeSnapshotManifest)}
}

func (s *ManifestStore) GetSafeSnapshot(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error) {
	if snapshotID == "" {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("manifest store: empty snapshot id")
	}
	s.mu.Lock()
	if m, ok := s.m[snapshotID]; ok {
		s.mu.Unlock()
		return m, nil
	}
	s.mu.Unlock()

	var out replay.SafeSnapshotManifest
	if err := s.c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		resp, err := pb.NewSafeStateClient(conn).GetManifest(ctx, &pb.SnapshotRef{SnapshotId: snapshotID})
		if err != nil {
			return err
		}
		out = wire.ManifestFromPB(resp)
		return nil
	}); err != nil {
		return replay.SafeSnapshotManifest{}, err
	}
	if err := out.Validate(); err != nil {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("manifest %s failed validation: %w", snapshotID, err)
	}
	s.mu.Lock()
	s.m[snapshotID] = out
	s.mu.Unlock()
	return out, nil
}
