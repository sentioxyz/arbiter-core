package manifestfeed

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/dataplane"
)

// TestStoreServesAPreTracksManifest: the store is the replay base of the
// shadow and the genesis check, so it serves a manifest published before the
// tracks activation, which the follower would refuse as a watermark.
func TestStoreServesAPreTracksManifest(t *testing.T) {
	pre := sealManifest(t, "", 0, nil, "genesis-row")
	src := newFakeSource(pre)
	s := NewStore(src, 0)

	got, err := s.GetSafeSnapshot(context.Background(), pre.SnapshotID)

	if err != nil || !reflect.DeepEqual(got, pre) {
		t.Fatalf("GetSafeSnapshot(pre-tracks) = %+v, %v", got, err)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d, want 1", s.Len())
	}
}

// TestStoreEvictsTheLeastRecentlyUsedManifest: a hit refreshes a manifest, a
// miss beyond the bound evicts the least recently used one, and an evicted
// manifest is fetched again.
func TestStoreEvictsTheLeastRecentlyUsedManifest(t *testing.T) {
	chain := trackChain(t, "", 1, 3)
	a, b, c := chain[0], chain[1], chain[2]
	src := newFakeSource(chain...)
	s := NewStore(src, 2)
	ctx := context.Background()
	get := func(m replay.SafeSnapshotManifest) {
		t.Helper()
		if got, err := s.GetSafeSnapshot(ctx, m.SnapshotID); err != nil || got.SnapshotID != m.SnapshotID {
			t.Fatalf("GetSafeSnapshot(%d) = %s, %v", m.SafeBlockSeq, got.SnapshotID, err)
		}
	}

	get(a)
	get(b)
	get(a) // a is now the most recently used
	get(c) // evicts b
	if got, want := src.takeFetched(), idsOf(a, b, c); !slices.Equal(got, want) {
		t.Fatalf("fetched %v, want %v (the hit on a fetches nothing)", got, want)
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d, want 2", s.Len())
	}
	get(a)
	get(c)
	if got := src.takeFetched(); len(got) != 0 {
		t.Fatalf("a and c are cached, yet the store fetched %v", got)
	}
	get(b)
	if got, want := src.takeFetched(), idsOf(b); !slices.Equal(got, want) {
		t.Fatalf("fetched %v, want the evicted b again", got)
	}
}

// TestStoreKeepsOnlyVerifiedManifests: a refused or missing manifest is
// returned as an error and never cached, so the next read fetches it again;
// an empty id is refused before any fetch.
func TestStoreKeepsOnlyVerifiedManifests(t *testing.T) {
	good := sealManifest(t, "", 7, []replay.TrackFrontier{frontier(0, 7, 3)}, "a")
	tampered := good
	tampered.SafeBlockSeq = 8
	other := sealManifest(t, "", 9, []replay.TrackFrontier{frontier(0, 9, 4)}, "b")
	src := newFakeSource()
	src.manifests[good.SnapshotID] = tampered
	src.manifests["0xasked"] = other
	s := NewStore(src, 0)
	ctx := context.Background()

	for i := 0; i < 2; i++ {
		_, err := s.GetSafeSnapshot(ctx, good.SnapshotID)
		requireRefusal(t, err, fmt.Sprintf("manifestfeed: manifest %s: %v", good.SnapshotID, tampered.Validate()))
	}
	_, err := s.GetSafeSnapshot(ctx, "0xasked")
	requireRefusal(t, err, fmt.Sprintf("manifestfeed: GetManifest(0xasked) answered manifest %s", other.SnapshotID))
	if _, err := s.GetSafeSnapshot(ctx, "0xmissing"); !errors.Is(err, dataplane.ErrManifestNotFound) || errors.Is(err, ErrManifestRefused) {
		t.Fatalf("GetSafeSnapshot(missing) = %v, want ErrManifestNotFound", err)
	}
	if got, want := src.takeFetched(), []string{good.SnapshotID, good.SnapshotID, "0xasked", "0xmissing"}; !slices.Equal(got, want) {
		t.Fatalf("fetched %v, want %v (nothing refused is cached)", got, want)
	}
	if s.Len() != 0 {
		t.Fatalf("Len = %d, want 0", s.Len())
	}
	if _, err := s.GetSafeSnapshot(ctx, ""); err == nil || err.Error() != "manifestfeed: empty snapshot id" || len(src.takeFetched()) != 0 {
		t.Fatalf("GetSafeSnapshot(\"\") = %v, want the empty-id refusal before any fetch", err)
	}
	src.manifests[good.SnapshotID] = good
	if got, err := s.GetSafeSnapshot(ctx, good.SnapshotID); err != nil || !reflect.DeepEqual(got, good) || s.Len() != 1 {
		t.Fatalf("GetSafeSnapshot(good) = %v, Len %d", err, s.Len())
	}
}
