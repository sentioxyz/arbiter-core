package manifestfeed

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

const (
	// DefaultMaxWalk bounds the ParentSnapshotID hops from the watermark back
	// to the last accepted manifest; beyond it the follower accepts the
	// verified watermark manifest alone (a full fetch, clarification K37).
	DefaultMaxWalk = 64
	// DefaultPollInterval is how often Run reads the watermark.
	DefaultPollInterval = 5 * time.Second
	// MinPollInterval is the smallest PollInterval: every watermark read
	// crosses one leader read barrier (plan S3-C2 contract §20.1).
	MinPollInterval = time.Second
)

// Config bounds the follower; zero values select the defaults.
type Config struct {
	MaxWalk int
	// CacheEntries sizes the Store the caller builds for the follower
	// (NewStore(src, cfg.CacheEntries)); the follower itself keeps no cache.
	CacheEntries int
	PollInterval time.Duration
}

// Update is one accepted watermark manifest, demultiplexed per owner.
type Update struct {
	SnapshotID, ParentSnapshotID string
	SafeBlockSeq                 uint64
	Manifest                     replay.SafeSnapshotManifest
	// Entries holds replay.OwnerEntry of every indexer of
	// Manifest.TrackFrontiers.
	Entries map[uint64]replay.TrackFrontier
	// Changed lists, ascending, the indexers whose entry differs from the
	// previously accepted manifest's; all of them for the first.
	Changed []uint64
	// FullFetch reports that the manifest was accepted without a walk back to
	// the last accepted one: the first manifest, or a watermark more than
	// MaxWalk hops ahead.
	FullFetch bool
	// Walked is the number of ParentSnapshotID hops the walk followed.
	Walked int
}

// FollowerStats counts what the follower did; Last is the snapshot id of the
// last accepted manifest ("" before the first).
type FollowerStats struct {
	Accepted, Refused, FullFetches, UpdateFailures uint64
	Last                                           string
}

// Follower follows the safe watermark along the ParentSnapshotID chain.
type Follower struct {
	cfg   Config
	src   Source
	store *Store

	mu          sync.Mutex
	last        string
	lastEntries map[uint64]replay.TrackFrontier
	stats       FollowerStats
}

// NewFollower returns a follower that has accepted last with lastEntries, the
// state an installer journal records. With last "" (nothing accepted yet) the
// first poll accepts the verified watermark manifest alone, a full fetch;
// lastEntries, when given, still bind it: no owner's entry may go below them,
// and Changed is computed against them.
func NewFollower(cfg Config, src Source, store *Store, last string, lastEntries map[uint64]replay.TrackFrontier) (*Follower, error) {
	if src == nil || store == nil {
		return nil, errors.New("manifestfeed: NewFollower needs a source and a store")
	}
	if cfg.MaxWalk <= 0 {
		cfg.MaxWalk = DefaultMaxWalk
	}
	if cfg.CacheEntries <= 0 {
		cfg.CacheEntries = DefaultCacheEntries
	}
	switch {
	case cfg.PollInterval == 0:
		cfg.PollInterval = DefaultPollInterval
	case cfg.PollInterval < MinPollInterval:
		return nil, fmt.Errorf("manifestfeed: poll interval %s is below 1s", cfg.PollInterval)
	}
	f := &Follower{cfg: cfg, src: src, store: store, last: last, lastEntries: maps.Clone(lastEntries)}
	f.stats.Last = last
	return f, nil
}

// Poll reads the watermark once and accepts the manifest it names, if it
// moved: false when it did not (or no manifest is published yet). A refusal
// wraps ErrManifestRefused and leaves the follower where it was.
func (f *Follower) Poll(ctx context.Context) (Update, bool, error) {
	u, ok, err := f.next(ctx)
	if err != nil || !ok {
		return Update{}, false, err
	}
	f.accept(u)
	return u, true, nil
}

// Run polls immediately and then every PollInterval until ctx ends, handing
// each new update to onUpdate. An update is accepted only once onUpdate
// succeeds: an error is logged and counted, the follower stays at its last
// accepted manifest, and the next poll offers the update again (or a newer
// one), so the installer's journal stays the source of truth. Run returns
// ctx's error.
func (f *Follower) Run(ctx context.Context, onUpdate func(context.Context, Update) error) error {
	if onUpdate == nil {
		return errors.New("manifestfeed: Run needs an update callback")
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
		}
		f.step(ctx, onUpdate)
		timer.Reset(f.cfg.PollInterval)
	}
}

// Stats returns the follower's counters.
func (f *Follower) Stats() FollowerStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

func (f *Follower) step(ctx context.Context, onUpdate func(context.Context, Update) error) {
	u, ok, err := f.next(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Warn("manifest feed: poll failed; the next poll retries", "err", err)
		}
		return
	}
	if !ok {
		return
	}
	if err := onUpdate(ctx, u); err != nil {
		f.mu.Lock()
		f.stats.UpdateFailures++
		f.mu.Unlock()
		if ctx.Err() == nil {
			slog.Warn("manifest feed: applying an accepted manifest failed; the next poll offers it again",
				"snapshot_id", u.SnapshotID, "safe_block_seq", u.SafeBlockSeq, "err", err)
		}
		return
	}
	f.accept(u)
}

func (f *Follower) accept(u Update) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.last, f.lastEntries = u.SnapshotID, maps.Clone(u.Entries)
	f.stats.Accepted++
	if u.FullFetch {
		f.stats.FullFetches++
	}
	f.stats.Last = u.SnapshotID
}

// next computes the update the watermark names without accepting it.
func (f *Follower) next(ctx context.Context) (Update, bool, error) {
	f.mu.Lock()
	last, lastEntries := f.last, f.lastEntries
	f.mu.Unlock()

	u, ok, err := f.compute(ctx, last, lastEntries)
	if errors.Is(err, ErrManifestRefused) {
		f.mu.Lock()
		f.stats.Refused++
		f.mu.Unlock()
	}
	return u, ok, err
}

func (f *Follower) compute(ctx context.Context, last string, lastEntries map[uint64]replay.TrackFrontier) (Update, bool, error) {
	wm, err := f.src.GetSafeWatermark(ctx)
	if err != nil {
		return Update{}, false, fmt.Errorf("manifestfeed: read the safe watermark: %w", err)
	}
	if wm.SnapshotID == "" || wm.SnapshotID == last {
		return Update{}, false, nil
	}
	m, err := f.store.GetSafeSnapshot(ctx, wm.SnapshotID)
	if err != nil {
		return Update{}, false, err
	}
	if wm.ManifestRoot != m.ManifestRoot {
		return Update{}, false, refuse("manifestfeed: watermark names manifest_root %s, manifest %s has %s", wm.ManifestRoot, m.SnapshotID, m.ManifestRoot)
	}
	if len(m.TrackFrontiers) == 0 {
		return Update{}, false, refuse("manifestfeed: manifest %s carries no track_frontiers", m.SnapshotID)
	}
	u := Update{SnapshotID: m.SnapshotID, ParentSnapshotID: m.ParentSnapshotID, SafeBlockSeq: m.SafeBlockSeq, Manifest: m, FullFetch: last == ""}
	if last != "" {
		if u.Walked, u.FullFetch, err = f.walk(ctx, m, last); err != nil {
			return Update{}, false, err
		}
	}
	u.Entries = make(map[uint64]replay.TrackFrontier, len(m.TrackFrontiers))
	for _, tf := range m.TrackFrontiers {
		entry, _ := replay.OwnerEntry(m, tf.IndexerID)
		u.Entries[tf.IndexerID] = entry
	}
	// An owner's entry goes back when either component is below the last
	// accepted one; an owner missing from m counts as (0, 0).
	for _, owner := range slices.Sorted(maps.Keys(lastEntries)) {
		was, now := lastEntries[owner], u.Entries[owner]
		if now.SafeBlockSeq < was.SafeBlockSeq || now.AppliedPromotionSeq < was.AppliedPromotionSeq {
			return Update{}, false, refuse("manifestfeed: owner %d entry went back from (%d, %d) to (%d, %d) in manifest %s",
				owner, was.SafeBlockSeq, was.AppliedPromotionSeq, now.SafeBlockSeq, now.AppliedPromotionSeq, m.SnapshotID)
		}
	}
	for _, owner := range slices.Sorted(maps.Keys(u.Entries)) {
		if was, ok := lastEntries[owner]; !ok || was != u.Entries[owner] {
			u.Changed = append(u.Changed, owner)
		}
	}
	return u, true, nil
}

// walk follows ParentSnapshotID from the watermark manifest w back to last,
// fetching and verifying every manifest in between through the store. It
// gives up after MaxWalk hops, and the watermark is then accepted alone.
func (f *Follower) walk(ctx context.Context, w replay.SafeSnapshotManifest, last string) (int, bool, error) {
	cur := w
	for hops := 1; ; hops++ {
		parent := cur.ParentSnapshotID
		switch {
		case parent == last:
			return hops, false, nil
		case parent == "":
			return 0, false, refuse("manifestfeed: watermark %s does not descend from the last accepted manifest %s", w.SnapshotID, last)
		case hops >= f.cfg.MaxWalk:
			return hops, true, nil
		}
		next, err := f.store.GetSafeSnapshot(ctx, parent)
		if err != nil {
			return 0, false, err
		}
		cur = next
	}
}
