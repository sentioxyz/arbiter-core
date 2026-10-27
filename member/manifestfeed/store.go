package manifestfeed

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// Source is the arbiter read surface the feed uses; *dataplane.Client
// satisfies it. It has no by-block method, so the feed cannot call
// GetManifestByBlock (spec §8.4).
type Source interface {
	GetSafeWatermark(ctx context.Context) (arbiter.SafeWatermark, error)
	GetManifest(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error)
}

// DefaultCacheEntries is the Store's size when none is given (clarification
// K37).
const DefaultCacheEntries = 16

// Store is a bounded LRU of verified manifests in front of a Source. A miss
// fetches the manifest by id and keeps it only once Verify accepts it, so the
// store never serves a refused manifest. It is the member's
// replay.SnapshotStore: the shadow verifier's replays and the startup genesis
// check read through it. Callers must not modify a returned manifest.
type Store struct {
	src     Source
	entries int

	mu    sync.Mutex
	order *list.List // of string snapshot ids, most recently used first
	byID  map[string]*list.Element
	cache map[string]replay.SafeSnapshotManifest
}

var _ replay.SnapshotStore = (*Store)(nil)

// NewStore returns a Store of at most entries manifests (0 or less selects
// DefaultCacheEntries).
func NewStore(src Source, entries int) *Store {
	if entries <= 0 {
		entries = DefaultCacheEntries
	}
	return &Store{src: src, entries: entries, order: list.New(), byID: make(map[string]*list.Element), cache: make(map[string]replay.SafeSnapshotManifest)}
}

// GetSafeSnapshot returns the verified manifest snapshotID, from the cache or
// fetched through the Source. A fetch error is returned wrapped (so
// dataplane.ErrManifestNotFound stays matchable) and a refused manifest
// returns its Verify refusal; neither is cached.
func (s *Store) GetSafeSnapshot(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error) {
	if snapshotID == "" {
		return replay.SafeSnapshotManifest{}, errors.New("manifestfeed: empty snapshot id")
	}
	if m, ok := s.lookup(snapshotID); ok {
		return m, nil
	}
	m, err := s.src.GetManifest(ctx, snapshotID)
	if err != nil {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("manifestfeed: fetch manifest %s: %w", snapshotID, err)
	}
	if err := Verify(snapshotID, m); err != nil {
		return replay.SafeSnapshotManifest{}, err
	}
	s.insert(snapshotID, m)
	return m, nil
}

// Len is the number of manifests the store holds.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.order.Len()
}

func (s *Store) lookup(id string) (replay.SafeSnapshotManifest, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.byID[id]
	if !ok {
		return replay.SafeSnapshotManifest{}, false
	}
	s.order.MoveToFront(e)
	return s.cache[id], true
}

func (s *Store) insert(id string, m replay.SafeSnapshotManifest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.byID[id]; ok {
		s.order.MoveToFront(e)
		return
	}
	s.byID[id] = s.order.PushFront(id)
	s.cache[id] = m
	for s.order.Len() > s.entries {
		oldest := s.order.Back()
		s.order.Remove(oldest)
		evicted := oldest.Value.(string)
		delete(s.byID, evicted)
		delete(s.cache, evicted)
	}
}
