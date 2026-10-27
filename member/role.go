// Package member is the stage-3 si-member role (housegate spec 2026-10-14
// §8; Plan S3-C2 contract §23.1): a process separate from the SNode that
// keeps one incarnation-scoped hg_replica copy of every SI table of every
// peer owner, filled only through the install gate from parts a verified
// manifest lists, and, when configured, shadow-verifies every sealed block of
// a peer owner. In P2 it has a node id and no keys: it never registers and
// makes only the read RPCs of contract §20.1, so it changes no consensus
// state. Role composes member/manifestfeed, member/copyset, member/sources,
// member/install and member/shadow in the startup order of contract §23.1.
package member

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/copyset"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/manifestfeed"
	"github.com/sentioxyz/arbiter-core/member/shadow"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

// copySet is what the role uses of *copyset.Reconciler.
type copySet interface {
	install.CopySet
	Reconcile(ctx context.Context) error
	Run(ctx context.Context) error
}

// installer is what the role uses of *install.Installer. Entry, Tables,
// Stats and NeedsPass are read while Apply or Audit runs.
type installer interface {
	Apply(ctx context.Context, u manifestfeed.Update) error
	Audit(ctx context.Context, trigger string) error
	DropStaleScratch(ctx context.Context) error
	Entry(owner uint64) (install.OwnerJournal, bool)
	Tables(owner uint64) []install.TableState
	Stats() install.Stats
	NeedsPass() bool
}

// shadowVerifier is what the role uses of *shadow.Verifier.
type shadowVerifier interface {
	Run(ctx context.Context) error
	Stats() shadow.Stats
}

// seams are the role's calls on its ClickHouse and the constructors of its
// parts. New installs the real ones; the package's tests replace them to
// check the startup order and the composition without a server.
type seams struct {
	ensureDatabases       func(ctx context.Context, conn clickhouse.Conn, m ddl.MemberPinned) error
	checkEngineFence      func(ctx context.Context, conn clickhouse.Conn, stageDB, probeTable string) error
	checkRemoteIsReadOnly func(ctx context.Context, conn clickhouse.Conn, collection string) error
	readOwnGrants         func(ctx context.Context, conn clickhouse.Conn) ([]sources.Grant, error)
	startedAt             func(ctx context.Context, conn clickhouse.Conn, now time.Time) (time.Time, error)
	watchRestarts         func(ctx context.Context, conn clickhouse.Conn, w *chrestart.Watcher)
	newCopySet            func(cfg copyset.Config, d copyset.Deps) (copySet, error)
	newInstaller          func(cfg install.Config, d install.Deps) (installer, error)
	newShadow             func(cfg shadow.Config, d shadow.Deps) (shadowVerifier, error)
}

func realSeams() seams {
	return seams{
		ensureDatabases:       ddl.EnsureMemberDatabases,
		checkEngineFence:      sources.CheckEngineFence,
		checkRemoteIsReadOnly: sources.CheckRemoteIsReadOnly,
		readOwnGrants:         sources.ReadOwnGrants,
		startedAt:             chrestart.StartedAt,
		watchRestarts:         func(ctx context.Context, conn clickhouse.Conn, w *chrestart.Watcher) { w.Run(ctx, conn) },
		newCopySet: func(cfg copyset.Config, d copyset.Deps) (copySet, error) {
			return copyset.New(cfg, d)
		},
		newInstaller: func(cfg install.Config, d install.Deps) (installer, error) {
			return install.New(cfg, d)
		},
		newShadow: func(cfg shadow.Config, d shadow.Deps) (shadowVerifier, error) {
			return shadow.New(cfg, d)
		},
	}
}

// parts are the components startup step (7) builds.
type parts struct {
	copies    copySet
	installer installer
	shadow    shadowVerifier // nil unless Config.Shadow
	follower  *manifestfeed.Follower
}

// Role is one si-member. New validates; Run starts it and returns when ctx
// ends or a part fails; Ready and Stats may be called at any time.
type Role struct {
	cfg   Config
	d     Deps
	log   *slog.Logger
	now   func() time.Time
	env   seams
	peers []Peer // ascending by IndexerID

	// installMu serializes Apply and Audit: the feed, the copy watch, the
	// restart watcher and the self-audit run on their own goroutines.
	installMu sync.Mutex

	mu               sync.Mutex
	parts            *parts
	audited          bool
	appliedOnce      bool
	grantsUnreadable bool
	// target is the newest update the follower offered (Task 29 verified it:
	// its id and root, its ledger, descent on a bounded walk (a full fetch
	// verifies the watermark alone), and no owner entry going back)
	// or, on a resume, the journaled manifest every peer is at. It is kept
	// whether or not its Apply succeeded, and the copy watch re-applies it,
	// never an older update: Installer.Apply commits and journals each owner
	// on its own, so after a pass that failed for one owner the others may be
	// at target already, and a pass of an older update would rebuild their
	// newer partitions away (Plan S3-C2 review fix 245-5). armed is set once a
	// pass succeeded or the journal named target; appliedCopies is the set of
	// ready copies the last successful pass saw. The copy watch acts only once
	// armed, when the ready copies differ from appliedCopies or the installer
	// reports work that needs no new manifest (NeedsPass, review fix 245-2);
	// before that the follower retries its failed update itself.
	target        *manifestfeed.Update
	armed         bool
	appliedCopies string
	published     map[uint64]replay.TrackFrontier
	behindSince   map[uint64]time.Time
}

// New validates cfg and d. It opens nothing and writes nothing.
func New(cfg Config, d Deps) (*Role, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if err := d.validate(cfg.Shadow); err != nil {
		return nil, err
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := d.Now
	if now == nil {
		now = time.Now
	}
	peers := slices.Clone(cfg.Peers)
	slices.SortFunc(peers, func(a, b Peer) int { return cmp.Compare(a.IndexerID, b.IndexerID) })
	return &Role{
		cfg: cfg, d: d, log: logger.With("role", "si-member", "node_id", cfg.NodeID), now: now, env: realSeams(), peers: peers,
		published: map[uint64]replay.TrackFrontier{}, behindSince: map[uint64]time.Time{},
	}, nil
}

// Run performs the startup of contract §23.1 in its binding order and then
// runs the copyset, the feed into the installer, the copy watch, the
// ClickHouse restart watcher, the self-audit ticker and, when configured,
// the shadow verifier. No step before (9) installs a part; step (8)'s first
// copyset pass only creates and drops copy tables. Run returns ctx's
// error when ctx ends, else the first startup refusal or the first failure
// of a part.
func (r *Role) Run(ctx context.Context) error {
	// (1) One process per state directory.
	lock, err := lockStateDir(r.cfg.StateDir)
	if err != nil {
		return err
	}
	defer func() { _ = lock.Close() }()

	// (2) The member databases never meet the protocol databases, and the
	// host is never a peer.
	if err := ddl.ValidateMemberPinned(r.cfg.Pinned, r.cfg.Protocol); err != nil {
		return err
	}
	for i, p := range r.peers {
		if p.IndexerID == r.cfg.HostIndexerID {
			return fmt.Errorf("member: %d is the host's own indexer; a member never copies its host", p.IndexerID)
		}
		if i > 0 && r.peers[i-1].IndexerID == p.IndexerID {
			return fmt.Errorf("member: peer indexer %d is listed twice", p.IndexerID)
		}
	}

	// (3) The two member databases, without ENGINE (K32), so that the fence
	// probe below has its database.
	if err := r.env.ensureDatabases(ctx, r.d.Conn, r.cfg.Pinned); err != nil {
		return fmt.Errorf("member: create the member databases: %w", err)
	}
	// (4) The engine fence is live for this user.
	if err := r.env.checkEngineFence(ctx, r.d.Conn, r.cfg.Pinned.StageDB, install.NodePrefix(r.cfg.NodeID)+"engine_probe"); err != nil {
		return err
	}
	// (5) remote() is read-only for this user through every collection it
	// names; system.grants agrees when this user may read it.
	for _, name := range r.collections() {
		if err := r.env.checkRemoteIsReadOnly(ctx, r.d.Conn, name); err != nil {
			return err
		}
	}
	grants, err := r.env.readOwnGrants(ctx, r.d.Conn)
	switch {
	case errors.Is(err, sources.ErrGrantsUnreadable):
		r.mu.Lock()
		r.grantsUnreadable = true
		r.mu.Unlock()
		r.log.Warn("member: system.grants is not readable by this user; the behavioural remote() probe stands alone", "err", err)
	case err != nil:
		return fmt.Errorf("member: read this user's grants: %w", err)
	default:
		if err := sources.CheckRemoteGrants(grants); err != nil {
			return err
		}
	}

	// (6) The registry, the node id, the peers' enrolment and the genesis
	// table set.
	if err := dataplane.WaitReady(ctx, r.d.Registry, dataplane.DefaultRegistryStartupTimeout); err != nil {
		return fmt.Errorf("member: table registry: %w", err)
	}
	snap, _ := r.d.Registry.View()
	purge, err := r.d.Client.PurgeNodeSet(ctx)
	if err != nil {
		return fmt.Errorf("member: read the purge node set: %w", err)
	}
	if err := copyset.CheckNodeIDUnused(snap, purge, r.cfg.NodeID); err != nil {
		return err
	}
	for _, p := range r.peers {
		if _, ok := snap.SIIndexer(p.IndexerID); !ok {
			return fmt.Errorf("member: peer indexer %d is not enrolled in si_indexers", p.IndexerID)
		}
	}
	store := manifestfeed.NewStore(r.d.Client, r.cfg.FeedCacheEntries)
	if err := r.checkGenesis(ctx, store); err != nil {
		return err
	}

	// (7) The parts, then the startup drop of this node's stage tables.
	bootNonce, err := install.NewBootNonce()
	if err != nil {
		return fmt.Errorf("member: boot nonce: %w", err)
	}
	p, err := r.build(store, bootNonce)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.parts = p
	r.mu.Unlock()
	if err := p.installer.DropStaleScratch(ctx); err != nil {
		return fmt.Errorf("member: drop this node's stage tables: %w", err)
	}

	// (8) The first copyset pass, then the startup audit over the copies it made
	// ready (Plan S3-C2 review fix 245-1): the reconciler reports no copy before
	// its first pass, so an audit ahead of it would cover nothing, and a copy
	// the pass creates again empty would never meet its journal. The pass
	// creates and drops copy tables and installs no part; a copy it cannot make
	// ready yet is audited by the installer before the pass that first sees it.
	// The ClickHouse start time is read first, so a restart during the pass or
	// the audit reruns the audit.
	baseline, err := r.env.startedAt(ctx, r.d.Conn, r.now())
	if err != nil {
		return fmt.Errorf("member: read the ClickHouse start time: %w", err)
	}
	if err := p.copies.Reconcile(ctx); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		r.log.Warn("member: first copyset pass failed; the copyset retries", "err", err)
	}
	if err := r.audit(ctx, p, install.TriggerStartup); err != nil {
		return fmt.Errorf("member: startup audit: %w", err)
	}
	r.mu.Lock()
	r.audited = true
	r.mu.Unlock()
	r.log.Info("member: started", "host_indexer_id", r.cfg.HostIndexerID, "peers", r.peerIDs(), "shadow", r.cfg.Shadow, "boot_nonce", bootNonce)

	// (9) Everything that writes copies, and the shadow.
	return r.runParts(ctx, p, store, baseline)
}

// collections lists every collection the member's user can reach: each
// peer's, then each pool collection.
func (r *Role) collections() []string {
	out := make([]string, 0, len(r.peers)+len(r.cfg.PoolCollections))
	for _, p := range r.peers {
		out = append(out, p.Collection)
	}
	return append(out, r.cfg.PoolCollections...)
}

func (r *Role) peerIDs() []uint64 {
	out := make([]uint64, len(r.peers))
	for i, p := range r.peers {
		out[i] = p.IndexerID
	}
	return out
}

// checkGenesis compares the configured genesis table set with the genesis
// manifest, read and verified through the feed's store.
func (r *Role) checkGenesis(ctx context.Context, store *manifestfeed.Store) error {
	m, err := store.GetSafeSnapshot(ctx, r.cfg.GenesisSnapshotID)
	if err != nil {
		return fmt.Errorf("member: read the genesis manifest %s: %w", r.cfg.GenesisSnapshotID, err)
	}
	recorded := make(map[string]string, len(m.Tables))
	for _, t := range m.Tables {
		recorded[t.TableID] = t.SchemaHash
	}
	want := make([]string, 0, len(r.cfg.Genesis))
	for _, t := range r.cfg.Genesis {
		want = append(want, t.TableID)
	}
	slices.Sort(want)
	if got := slices.Sorted(maps.Keys(recorded)); !slices.Equal(want, got) {
		return fmt.Errorf("member: genesis tables %v differ from the genesis manifest's %v", want, got)
	}
	schemas := slices.Clone(r.cfg.Genesis)
	slices.SortFunc(schemas, func(a, b payloadexec.TableSchema) int { return cmp.Compare(a.TableID, b.TableID) })
	for _, t := range schemas {
		if h := payloadexec.TableSchemaHash(r.cfg.NetworkID, t); h != recorded[t.TableID] {
			return fmt.Errorf("member: genesis table %s schema hashes to %s, the genesis manifest records %s", t.TableID, h, recorded[t.TableID])
		}
	}
	return nil
}

// build constructs the copyset, the installer and the shadow; none of their
// constructors writes anything.
func (r *Role) build(store *manifestfeed.Store, bootNonce string) (*parts, error) {
	owners := r.peerIDs()
	byOwner := make(map[uint64][]sources.Source, len(r.peers))
	pulls := make(map[uint64]sources.Source, len(r.peers))
	for _, p := range r.peers {
		// The P2 source order (contract §22.3): owner pull, peer unsafe
		// pulls, restore.
		list := []sources.Source{sources.NewOwnerPull(r.d.Conn, p.Collection, p.SafeDatabase)}
		for _, name := range r.cfg.PoolCollections {
			list = append(list, sources.NewPeerUnsafePull(r.d.Conn, name))
		}
		if r.cfg.Restore != nil {
			list = append(list, sources.NewRestore(r.d.Conn, *r.cfg.Restore, p.IndexerID))
		}
		byOwner[p.IndexerID] = list
		pulls[p.IndexerID] = sources.NewOwnerUnsafePull(r.d.Conn, p.Collection, p.UnsafeDatabase)
	}
	// copyset.Deps.Dropped stays nil: the installer ignores the journal of a
	// copy the copyset dropped (it reads only entries that match a ready
	// copy's incarnation), so a drop needs nothing from it.
	copies, err := r.env.newCopySet(copyset.Config{
		Pinned: r.cfg.Pinned, Protocol: r.cfg.Protocol, NetworkID: r.cfg.NetworkID, Genesis: r.cfg.Genesis,
		Owners: owners, HostIndexerID: r.cfg.HostIndexerID,
	}, copyset.Deps{Conn: r.d.Conn, Registry: r.d.Registry, Logger: r.log})
	if err != nil {
		return nil, fmt.Errorf("member: copyset: %w", err)
	}
	inst, err := r.env.newInstaller(install.Config{
		Pinned: r.cfg.Pinned, NetworkID: r.cfg.NetworkID, StateDir: r.cfg.StateDir, BootNonce: bootNonce,
		DiskBudgetBytes: r.cfg.DiskBudgetBytes, StopInstallsRatio: r.cfg.StopInstallsRatio, SelfAuditInterval: r.cfg.SelfAuditInterval,
	}, install.Deps{
		Conn: r.d.Conn, Copies: copies,
		Sources:     func(owner uint64) []sources.Source { return byOwner[owner] },
		AfterAttach: r.d.AfterAttach, Logger: r.log, Now: r.now,
	})
	if err != nil {
		return nil, fmt.Errorf("member: installer: %w", err)
	}
	p := &parts{copies: copies, installer: inst}
	if r.cfg.Shadow {
		v, err := r.env.newShadow(shadow.Config{
			NodeID: r.cfg.NodeID, BootNonce: bootNonce, NetworkID: r.cfg.NetworkID, StateDir: r.cfg.StateDir,
			Pinned: r.cfg.Pinned, Genesis: r.cfg.Genesis, Owners: owners, HostIndexerID: r.cfg.HostIndexerID,
		}, shadow.Deps{
			Blocks: r.d.Client, Watermark: r.d.Client, Snapshots: store, Payloads: r.d.Payloads, Conn: r.d.Conn,
			UnsafePull: func(owner uint64) sources.Source { return pulls[owner] },
			Logger:     r.log, Now: r.now,
		})
		if err != nil {
			return nil, fmt.Errorf("member: shadow: %w", err)
		}
		p.shadow = v
	}
	return p, nil
}

// audit runs one installer audit; Apply and Audit never overlap.
func (r *Role) audit(ctx context.Context, p *parts, trigger string) error {
	r.installMu.Lock()
	defer r.installMu.Unlock()
	return p.installer.Audit(ctx, trigger)
}

// runParts is startup step (9). Step (8) ran the first copyset pass, so the
// first install pass sees every copy the registry asks for.
func (r *Role) runParts(parent context.Context, p *parts, store *manifestfeed.Store, baseline time.Time) error {
	follower, err := r.newFollower(parent, p, store)
	if err != nil {
		return err
	}
	r.mu.Lock()
	p.follower = follower
	r.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	failed := make(chan error, 6)
	var wg sync.WaitGroup
	start := func(name string, run func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := run(ctx); err != nil && ctx.Err() == nil {
				failed <- fmt.Errorf("member: %s: %w", name, err)
			}
		}()
	}
	start("copyset", p.copies.Run)
	start("feed", func(ctx context.Context) error {
		return follower.Run(ctx, func(ctx context.Context, u manifestfeed.Update) error { return r.onUpdate(ctx, p, u) })
	})
	start("copy watch", func(ctx context.Context) error { return r.watchCopies(ctx, p) })
	start("restart watcher", func(ctx context.Context) error {
		r.env.watchRestarts(ctx, r.d.Conn, &chrestart.Watcher{
			Interval: r.cfg.PollInterval, Skew: chrestart.DefaultRestartSkew, Now: r.now, Baseline: baseline,
			OnRestart: func(ctx context.Context, previous, current time.Time) {
				r.log.Warn("member: ClickHouse restarted; auditing the copies", "previous_start", previous, "current_start", current)
				if err := r.audit(ctx, p, install.TriggerClickHouseRestart); err != nil && ctx.Err() == nil {
					r.log.Warn("member: restart audit failed; the next restart or self-audit reruns it", "err", err)
				}
			},
		})
		return nil
	})
	start("self-audit", func(ctx context.Context) error { return r.selfAudit(ctx, p) })
	if p.shadow != nil {
		start("shadow", p.shadow.Run)
	}
	select {
	case <-ctx.Done():
	case err = <-failed:
	}
	cancel()
	wg.Wait()
	if err != nil {
		return err
	}
	return parent.Err()
}

// newFollower starts the feed where the journal is. When every peer's journal
// entry names one manifest, the follower walks from it and that manifest,
// read through the store, is the target the copy watch re-applies until the
// follower offers a newer one. Otherwise the follower accepts the verified
// watermark manifest alone, and the journaled entries still refuse an owner
// entry that goes back.
func (r *Role) newFollower(ctx context.Context, p *parts, store *manifestfeed.Store) (*manifestfeed.Follower, error) {
	entries := make(map[uint64]replay.TrackFrontier, len(r.peers))
	last, agreed := "", true
	for _, peer := range r.peers {
		j, ok := p.installer.Entry(peer.IndexerID)
		if !ok {
			agreed = false
			continue
		}
		entries[peer.IndexerID] = j.Entry
		switch {
		case last == "":
			last = j.SnapshotID
		case j.SnapshotID != last:
			agreed = false
		}
	}
	if !agreed {
		last = ""
	}
	if last != "" {
		m, err := store.GetSafeSnapshot(ctx, last)
		if err != nil {
			r.log.Warn("member: the journaled manifest is not readable; the feed starts from the watermark", "snapshot_id", last, "err", err)
			last = ""
		} else {
			u := manifestfeed.Update{SnapshotID: m.SnapshotID, ParentSnapshotID: m.ParentSnapshotID, SafeBlockSeq: m.SafeBlockSeq, Manifest: m,
				Entries: make(map[uint64]replay.TrackFrontier, len(m.TrackFrontiers))}
			for _, f := range m.TrackFrontiers {
				u.Entries[f.IndexerID], _ = replay.OwnerEntry(m, f.IndexerID)
			}
			r.observe(u)
			r.mu.Lock()
			r.target, r.armed = &u, true
			r.mu.Unlock()
		}
	}
	return manifestfeed.NewFollower(manifestfeed.Config{
		MaxWalk: r.cfg.FeedMaxWalk, CacheEntries: r.cfg.FeedCacheEntries, PollInterval: r.cfg.PollInterval,
	}, r.d.Client, store, last, entries)
}

// onUpdate installs one update the follower offers. The update becomes the
// target before its pass runs, unless the target is newer: a pass that fails
// may have committed some owners already, and the copy watch must retry the
// update they are at (Plan S3-C2 review fix 245-5). On an error the follower
// keeps its last accepted manifest and offers the update again at its next
// poll (contract §21.4).
func (r *Role) onUpdate(ctx context.Context, p *parts, u manifestfeed.Update) error {
	r.observe(u)
	r.installMu.Lock()
	defer r.installMu.Unlock()
	r.mu.Lock()
	if r.target == nil || u.SafeBlockSeq >= r.target.SafeBlockSeq {
		r.target = &u
	}
	r.mu.Unlock()
	copies := copiesKey(p.copies.Copies())
	if err := p.installer.Apply(ctx, u); err != nil {
		return err
	}
	r.mu.Lock()
	r.armed, r.appliedCopies, r.appliedOnce = true, copies, true
	r.mu.Unlock()
	return nil
}

// watchCopies re-applies the target, the newest update the follower offered
// (Plan S3-C2 review fix 245-5), whenever the ready copies differ from those the
// last successful pass saw or the installer reports work that needs no new
// manifest (NeedsPass: a pending or uncovered audit, a Diverged partition, an
// unjournaled part; review fix 245-2), so a copy that becomes ready between
// publications is filled and the damage a startup, restart or self-audit found
// is repaired while the watermark stands still (Apply is idempotent, contract
// §22.3), and keeps the peers' behind clocks.
func (r *Role) watchCopies(ctx context.Context, p *parts) error {
	t := time.NewTicker(r.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		r.reapply(ctx, p)
		r.refreshBehind(p)
	}
}

func (r *Role) reapply(ctx context.Context, p *parts) {
	r.installMu.Lock()
	defer r.installMu.Unlock()
	r.mu.Lock()
	u, armed, applied := r.target, r.armed, r.appliedCopies
	r.mu.Unlock()
	if u == nil || !armed {
		return
	}
	copies := copiesKey(p.copies.Copies())
	if copies == applied && !p.installer.NeedsPass() {
		return
	}
	if err := p.installer.Apply(ctx, *u); err != nil {
		if ctx.Err() == nil {
			r.log.Warn("member: install pass for changed copies or outstanding repairs failed; the next check retries", "snapshot_id", u.SnapshotID, "err", err)
		}
		return
	}
	r.mu.Lock()
	r.appliedCopies, r.appliedOnce = copies, true
	r.mu.Unlock()
}

// selfAudit runs the rate-limited self-audit every SelfAuditInterval.
func (r *Role) selfAudit(ctx context.Context, p *parts) error {
	t := time.NewTicker(r.cfg.SelfAuditInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
		if err := r.audit(ctx, p, install.TriggerSelfAudit); err != nil && ctx.Err() == nil {
			r.log.Warn("member: self-audit failed; the next interval reruns it", "err", err)
		}
	}
}

// observe records the entries a manifest publishes for the peers.
func (r *Role) observe(u manifestfeed.Update) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, peer := range r.peers {
		if e, ok := u.Entries[peer.IndexerID]; ok {
			r.published[peer.IndexerID] = e
		}
	}
}

// refreshBehind starts or clears each peer's behind clock.
func (r *Role) refreshBehind(p *parts) {
	now := r.now()
	entries := make(map[uint64]replay.TrackFrontier, len(r.peers))
	for _, peer := range r.peers {
		if j, ok := p.installer.Entry(peer.IndexerID); ok {
			entries[peer.IndexerID] = j.Entry
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, peer := range r.peers {
		id := peer.IndexerID
		published, seen := r.published[id]
		switch {
		case !seen || entries[id] == published:
			delete(r.behindSince, id)
		case r.behindSince[id].IsZero():
			r.behindSince[id] = now
		}
	}
}

// copiesKey identifies a set of ready copies.
func copiesKey(copies []copyset.Copy) string {
	keys := make([]string, 0, len(copies))
	for _, c := range copies {
		if c.Ready {
			keys = append(keys, c.TableKey+"#"+strconv.FormatUint(c.IncarnationSeq, 10))
		}
	}
	slices.Sort(keys)
	return strings.Join(keys, ",")
}

// Ready reports whether the startup audit ran, the installer reports its
// copies clean and every peer's copies reached a manifest: the peer has a
// journal entry, or an update was applied in this process and the peer has
// no ready copy (the installer journals only owners with a copy).
func (r *Role) Ready() bool {
	r.mu.Lock()
	p, audited, appliedOnce := r.parts, r.audited, r.appliedOnce
	r.mu.Unlock()
	if p == nil || !audited || !p.installer.Stats().AuditClean {
		return false
	}
	owners := make(map[uint64]bool)
	for _, c := range p.copies.Copies() {
		if c.Ready {
			owners[c.Owner] = true
		}
	}
	for _, peer := range r.peers {
		if _, ok := p.installer.Entry(peer.IndexerID); ok {
			continue
		}
		if appliedOnce && !owners[peer.IndexerID] {
			continue
		}
		return false
	}
	return true
}
