package manifestfeed

import (
	"context"
	"errors"
	"fmt"
	"net"
	"reflect"
	"slices"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

func newTestFollower(t *testing.T, cfg Config, src Source, last string, lastEntries map[uint64]replay.TrackFrontier) *Follower {
	t.Helper()
	f, err := NewFollower(cfg, src, NewStore(src, cfg.CacheEntries), last, lastEntries)
	if err != nil {
		t.Fatalf("NewFollower: %v", err)
	}
	return f
}

func mustPoll(t *testing.T, f *Follower) Update {
	t.Helper()
	u, ok, err := f.Poll(context.Background())
	if err != nil || !ok {
		t.Fatalf("Poll = %v, %v; want an update", ok, err)
	}
	return u
}

// TestFollowerRefusesAManifestWithoutTrackFrontiers: the member has no
// pre-tracks mode (clarification K37), so a watermark manifest without
// track_frontiers is refused and counted, and nothing is accepted. The
// refusal lasts only while that manifest is the watermark: a track-aware
// child is accepted, and a walk crosses a manifest without frontiers.
func TestFollowerRefusesAManifestWithoutTrackFrontiers(t *testing.T) {
	pre := sealManifest(t, "", 3, nil, "a")
	src := newFakeSource()
	src.publish(pre)
	f := newTestFollower(t, Config{}, src, "", nil)

	_, ok, err := f.Poll(context.Background())

	requireRefusal(t, err, fmt.Sprintf("manifestfeed: manifest %s carries no track_frontiers", pre.SnapshotID))
	if ok {
		t.Fatal("a refused poll must offer no update")
	}
	if st := f.Stats(); st.Refused != 1 || st.Accepted != 0 || st.Last != "" {
		t.Fatalf("stats = %+v, want one refusal and nothing accepted", st)
	}
	child := trackChain(t, pre.SnapshotID, 4, 1)[0]
	src.publish(child)
	if u := mustPoll(t, f); u.SnapshotID != child.SnapshotID || !u.FullFetch {
		t.Fatalf("update = %+v, want the track-aware child as a full fetch", u)
	}

	gap := sealManifest(t, child.SnapshotID, 5, nil, "b")
	src.publish(gap)
	_, ok, err = f.Poll(context.Background())
	requireRefusal(t, err, fmt.Sprintf("manifestfeed: manifest %s carries no track_frontiers", gap.SnapshotID))
	if ok || f.Stats().Last != child.SnapshotID {
		t.Fatalf("after the refusal: ok %v, stats %+v; want the follower at the child", ok, f.Stats())
	}
	next := trackChain(t, gap.SnapshotID, 6, 1)[0]
	src.publish(next)
	if u := mustPoll(t, f); u.SnapshotID != next.SnapshotID || u.FullFetch || u.Walked != 2 {
		t.Fatalf("update = %+v, want manifest 6 reached by a walk of 2 hops across the manifest without frontiers", u)
	}
}

// TestFollowerWalksTheParentChainBackToItsLastAcceptedManifest: a watermark
// four manifests ahead is reached by fetching and verifying the watermark and
// the three manifests between, never the last accepted one; an unmoved
// watermark costs no fetch; the next manifest is one hop.
func TestFollowerWalksTheParentChainBackToItsLastAcceptedManifest(t *testing.T) {
	chain := trackChain(t, "", 1, 6)
	src := newFakeSource(chain...)
	src.publish(chain[4])
	f := newTestFollower(t, Config{}, src, chain[0].SnapshotID, entriesOf(chain[0]))

	u := mustPoll(t, f)

	if u.SnapshotID != chain[4].SnapshotID || u.ParentSnapshotID != chain[3].SnapshotID || u.SafeBlockSeq != 5 || u.Walked != 4 || u.FullFetch {
		t.Fatalf("update = %+v, want manifest 5 reached by a walk of 4 hops", u)
	}
	if !reflect.DeepEqual(u.Manifest, chain[4]) {
		t.Fatal("the update must carry the watermark manifest")
	}
	if got, want := src.takeFetched(), idsOf(chain[4], chain[3], chain[2], chain[1]); !slices.Equal(got, want) {
		t.Fatalf("fetched %v, want %v", got, want)
	}
	if _, ok, err := f.Poll(context.Background()); ok || err != nil || len(src.takeFetched()) != 0 {
		t.Fatalf("an unmoved watermark = %v, %v; want no update and no fetch", ok, err)
	}
	src.publish(chain[5])
	if u := mustPoll(t, f); u.Walked != 1 || u.FullFetch || u.SnapshotID != chain[5].SnapshotID {
		t.Fatalf("update = %+v, want manifest 6 one hop ahead", u)
	}
	if st := f.Stats(); st.Accepted != 2 || st.FullFetches != 0 || st.Last != chain[5].SnapshotID {
		t.Fatalf("stats = %+v", st)
	}
}

// TestFollowerRefusesAWatermarkThatDoesNotDescend: a watermark whose chain
// ends before reaching the last accepted manifest is refused, the follower
// stays where it was, and the next descending watermark is accepted.
func TestFollowerRefusesAWatermarkThatDoesNotDescend(t *testing.T) {
	chain := trackChain(t, "", 1, 3)
	fork := trackChain(t, "", 20, 2)
	src := newFakeSource(chain...)
	src.add(fork...)
	src.publish(fork[1])
	f := newTestFollower(t, Config{}, src, chain[1].SnapshotID, entriesOf(chain[1]))

	_, ok, err := f.Poll(context.Background())

	requireRefusal(t, err, fmt.Sprintf("manifestfeed: watermark %s does not descend from the last accepted manifest %s", fork[1].SnapshotID, chain[1].SnapshotID))
	if ok || f.Stats().Last != chain[1].SnapshotID || f.Stats().Refused != 1 {
		t.Fatalf("after the refusal: ok %v, stats %+v", ok, f.Stats())
	}
	src.publish(chain[2])
	if u := mustPoll(t, f); u.SnapshotID != chain[2].SnapshotID || u.Walked != 1 {
		t.Fatalf("update = %+v, want manifest 3", u)
	}
}

// TestFollowerFallsBackToAFullFetchBeyondMaxWalk: a watermark exactly MaxWalk
// hops ahead is walked; one further ahead is accepted alone after MaxWalk
// fetches, counted as a full fetch.
func TestFollowerFallsBackToAFullFetchBeyondMaxWalk(t *testing.T) {
	chain := trackChain(t, "", 1, 8)
	src := newFakeSource(chain...)
	src.publish(chain[3])
	f := newTestFollower(t, Config{MaxWalk: 3}, src, chain[0].SnapshotID, entriesOf(chain[0]))

	if u := mustPoll(t, f); u.Walked != 3 || u.FullFetch {
		t.Fatalf("update = %+v, want a walk of exactly MaxWalk hops", u)
	}
	src.takeFetched()
	src.publish(chain[7])
	u := mustPoll(t, f)

	if u.SnapshotID != chain[7].SnapshotID || !u.FullFetch || u.Walked != 3 {
		t.Fatalf("update = %+v, want manifest 8 as a full fetch after 3 hops", u)
	}
	if got, want := src.takeFetched(), idsOf(chain[7], chain[6], chain[5]); !slices.Equal(got, want) {
		t.Fatalf("fetched %v, want %v (bounded by MaxWalk)", got, want)
	}
	if st := f.Stats(); st.FullFetches != 1 || st.Accepted != 2 {
		t.Fatalf("stats = %+v, want one full fetch of two accepted", st)
	}
}

// TestFollowerRefusesAnOwnerEntryThatGoesBack: after a walk and after a full
// fetch alike, no owner's (safe block seq, applied promotion seq) may go below
// the last accepted entries, and an owner may not disappear. A follower that
// starts with no manifest but with a journal's entries checks its first full
// fetch against them.
func TestFollowerRefusesAnOwnerEntryThatGoesBack(t *testing.T) {
	last := sealManifest(t, "", 10, []replay.TrackFrontier{frontier(0, 10, 5), frontier(1, 9, 4)}, "a")
	child := sealManifest(t, last.SnapshotID, 11, []replay.TrackFrontier{frontier(0, 11, 6), frontier(1, 9, 3)}, "b")
	mid := sealManifest(t, last.SnapshotID, 11, []replay.TrackFrontier{frontier(0, 11, 6), frontier(1, 9, 4)}, "c")
	far := sealManifest(t, mid.SnapshotID, 12, []replay.TrackFrontier{frontier(0, 9, 7), frontier(1, 9, 4)}, "d")
	gone := sealManifest(t, last.SnapshotID, 11, []replay.TrackFrontier{frontier(0, 11, 6)}, "e")
	for _, tc := range []struct {
		name      string
		watermark replay.SafeSnapshotManifest
		last      string
		maxWalk   int
		want      string
	}{
		{"after a walk", child, last.SnapshotID, 0, fmt.Sprintf("manifestfeed: owner 1 entry went back from (9, 4) to (9, 3) in manifest %s", child.SnapshotID)},
		{"after a full fetch", far, last.SnapshotID, 1, fmt.Sprintf("manifestfeed: owner 0 entry went back from (10, 5) to (9, 7) in manifest %s", far.SnapshotID)},
		{"an owner that disappears", gone, last.SnapshotID, 0, fmt.Sprintf("manifestfeed: owner 1 entry went back from (9, 4) to (0, 0) in manifest %s", gone.SnapshotID)},
		{"a first fetch against a journal's entries", far, "", 0, fmt.Sprintf("manifestfeed: owner 0 entry went back from (10, 5) to (9, 7) in manifest %s", far.SnapshotID)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := newFakeSource(last, mid)
			src.publish(tc.watermark)
			f := newTestFollower(t, Config{MaxWalk: tc.maxWalk}, src, tc.last, entriesOf(last))

			_, ok, err := f.Poll(context.Background())

			requireRefusal(t, err, tc.want)
			if ok || f.Stats().Last != tc.last || f.Stats().Accepted != 0 {
				t.Fatalf("the follower moved: ok %v, stats %+v", ok, f.Stats())
			}
		})
	}
}

// TestFollowerDemultiplexesOneWatermarkPerOwner: one watermark serves every
// owner: each update carries every owner's entry, and Changed names, in
// ascending order, the owners whose entry moved (all of them the first time).
func TestFollowerDemultiplexesOneWatermarkPerOwner(t *testing.T) {
	chain := trackChain(t, "", 1, 3)
	src := newFakeSource(chain...)
	src.publish(chain[0])
	f := newTestFollower(t, Config{}, src, "", nil)

	first := mustPoll(t, f)
	src.publish(chain[1])
	second := mustPoll(t, f)
	src.publish(chain[2])
	third := mustPoll(t, f)

	if !first.FullFetch || !slices.Equal(first.Changed, []uint64{0, 1, 2}) {
		t.Fatalf("first update: full fetch %v, changed %v; want a full fetch changing every owner", first.FullFetch, first.Changed)
	}
	for i, u := range []Update{first, second, third} {
		for owner := uint64(0); owner < 3; owner++ {
			want, _ := replay.OwnerEntry(chain[i], owner)
			if u.Entries[owner] != want {
				t.Fatalf("update %d: owner %d entry %+v, want %+v", i+1, owner, u.Entries[owner], want)
			}
		}
		if len(u.Entries) != 3 {
			t.Fatalf("update %d carries %d entries, want 3", i+1, len(u.Entries))
		}
	}
	if !slices.Equal(second.Changed, []uint64{0, 1}) || !slices.Equal(third.Changed, []uint64{0}) {
		t.Fatalf("changed: second %v, third %v; want [0 1] and [0]", second.Changed, third.Changed)
	}
}

// TestFollowerRefusesAWatermarkWhoseRootDiffersFromItsManifest: the watermark
// names the manifest root it published; a manifest with another root is
// refused even though the manifest verifies.
func TestFollowerRefusesAWatermarkWhoseRootDiffersFromItsManifest(t *testing.T) {
	m := trackChain(t, "", 1, 1)[0]
	src := newFakeSource()
	src.publish(m)
	src.watermark.ManifestRoot = "0xpublished-root"
	f := newTestFollower(t, Config{}, src, "", nil)

	_, _, err := f.Poll(context.Background())

	requireRefusal(t, err, fmt.Sprintf("manifestfeed: watermark names manifest_root 0xpublished-root, manifest %s has %s", m.SnapshotID, m.ManifestRoot))
}

// TestNewFollowerRefusesAPollIntervalBelowOneSecond: every poll crosses a
// leader read barrier, so the interval is at least a second; zero selects
// the default, and a follower needs its source and store.
func TestNewFollowerRefusesAPollIntervalBelowOneSecond(t *testing.T) {
	src := newFakeSource()
	if _, err := NewFollower(Config{PollInterval: 500 * time.Millisecond}, src, NewStore(src, 0), "", nil); err == nil || err.Error() != "manifestfeed: poll interval 500ms is below 1s" {
		t.Fatalf("NewFollower(500ms) = %v", err)
	}
	f, err := NewFollower(Config{}, src, NewStore(src, 0), "", nil)
	if err != nil || f.cfg.PollInterval != DefaultPollInterval || f.cfg.MaxWalk != DefaultMaxWalk || f.cfg.CacheEntries != DefaultCacheEntries {
		t.Fatalf("NewFollower(defaults) = %+v, %v", f, err)
	}
	if _, err := NewFollower(Config{}, nil, nil, "", nil); err == nil || err.Error() != "manifestfeed: NewFollower needs a source and a store" {
		t.Fatalf("NewFollower(nil) = %v", err)
	}
}

// TestFollowerRunOffersAFailedUpdateAgain: an update is accepted only once the
// callback succeeds; a failure is counted, the follower stays where it was,
// and the next poll offers the same update again.
func TestFollowerRunOffersAFailedUpdateAgain(t *testing.T) {
	chain := trackChain(t, "", 1, 2)
	src := newFakeSource(chain...)
	src.publish(chain[1])
	f := newTestFollower(t, Config{PollInterval: MinPollInterval}, src, chain[0].SnapshotID, entriesOf(chain[0]))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var offered []string

	err := f.Run(ctx, func(_ context.Context, u Update) error {
		offered = append(offered, u.SnapshotID)
		if len(offered) == 1 {
			return errors.New("installer: ClickHouse is restarting")
		}
		cancel()
		return nil
	})

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want the context's cancellation after the second offer", err)
	}
	if want := idsOf(chain[1], chain[1]); !slices.Equal(offered, want) {
		t.Fatalf("offered %v, want manifest 2 twice", offered)
	}
	if st := f.Stats(); st.UpdateFailures != 1 || st.Accepted != 1 || st.Last != chain[1].SnapshotID {
		t.Fatalf("stats = %+v, want one failure, then manifest 2 accepted", st)
	}
}

// byBlockGuard is a SafeState that serves the watermark and manifests from a
// fakeSource and fails the test on GetManifestByBlock; an interceptor records
// every method the client calls.
type byBlockGuard struct {
	pb.UnimplementedSafeStateServer
	t   *testing.T
	src *fakeSource
}

func (g *byBlockGuard) GetSafeWatermark(ctx context.Context, _ *pb.GetSafeWatermarkRequest) (*pb.SafeWatermark, error) {
	wm, _ := g.src.GetSafeWatermark(ctx)
	return &pb.SafeWatermark{SnapshotId: wm.SnapshotID, SafeBlockSeq: wm.SafeBlockSeq, ManifestRoot: wm.ManifestRoot}, nil
}

func (g *byBlockGuard) GetManifest(ctx context.Context, ref *pb.SnapshotRef) (*pb.SafeSnapshotManifest, error) {
	m, err := g.src.GetManifest(ctx, ref.GetSnapshotId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "unknown snapshot id")
	}
	return wire.ManifestToPB(m), nil
}

func (g *byBlockGuard) GetManifestByBlock(context.Context, *pb.BlockRef) (*pb.SafeSnapshotManifest, error) {
	g.t.Errorf("the feed called GetManifestByBlock (spec §8.4 forbids it)")
	return nil, status.Error(codes.FailedPrecondition, "GetManifestByBlock is forbidden to the member feed")
}

// TestFollowerNeverCallsGetManifestByBlock: Source has no by-block method,
// and a follower running through a real dataplane.Client across a full fetch,
// a walk, a refusal and a fetch beyond MaxWalk only ever calls
// GetSafeWatermark and GetManifest.
func TestFollowerNeverCallsGetManifestByBlock(t *testing.T) {
	src := reflect.TypeOf((*Source)(nil)).Elem()
	var names []string
	for i := 0; i < src.NumMethod(); i++ {
		names = append(names, src.Method(i).Name)
	}
	if !slices.Equal(names, []string{"GetManifest", "GetSafeWatermark"}) {
		t.Fatalf("Source methods = %v, want exactly GetManifest and GetSafeWatermark", names)
	}

	chain := trackChain(t, "", 1, 12)
	fake := newFakeSource(chain...)
	var mu sync.Mutex
	called := map[string]int{}
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		mu.Lock()
		called[info.FullMethod]++
		mu.Unlock()
		return handler(ctx, req)
	}))
	pb.RegisterSafeStateServer(srv, &byBlockGuard{t: t, src: fake})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	f := newTestFollower(t, Config{MaxWalk: 4}, client, "", nil)

	fake.publish(chain[0])
	mustPoll(t, f)
	fake.publish(chain[3])
	mustPoll(t, f)
	tampered := chain[4]
	tampered.SafeBlockSeq = 99
	fake.add(tampered)
	fake.mu.Lock()
	fake.watermark.SnapshotID, fake.watermark.ManifestRoot = tampered.SnapshotID, tampered.ManifestRoot
	fake.mu.Unlock()
	if _, _, err := f.Poll(context.Background()); !errors.Is(err, ErrManifestRefused) {
		t.Fatalf("Poll(tampered) = %v, want a refusal", err)
	}
	fake.add(chain[4])
	fake.publish(chain[11])
	if u := mustPoll(t, f); !u.FullFetch {
		t.Fatalf("update = %+v, want a full fetch beyond MaxWalk", u)
	}

	mu.Lock()
	defer mu.Unlock()
	for method := range called {
		if method != pb.SafeState_GetSafeWatermark_FullMethodName && method != pb.SafeState_GetManifest_FullMethodName {
			t.Fatalf("the feed called %s; calls %v", method, called)
		}
	}
	if called[pb.SafeState_GetSafeWatermark_FullMethodName] != 4 || called[pb.SafeState_GetManifest_FullMethodName] == 0 {
		t.Fatalf("calls %v, want four watermark reads and manifest reads", called)
	}
}
