package member

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/manifestfeed"
	"github.com/sentioxyz/arbiter-core/member/serve"
)

// servingManifestEntries is the size of the serving view's own manifest
// store: the latest published manifest and the one before it.
const servingManifestEntries = 2

// serving is the role's serving side (Plan S3-C4 contract §30.6): a
// serve.Server over the install journal and the arbiter's latest published
// manifest. A role without Config.Serve has none.
type serving struct {
	server  *serve.Server
	metrics *serve.Metrics
	copies  *servedCopies
}

// newServing builds the serving side of cfg.Serve; without it, nil and no
// error. New calls it; the install journal is bound by attachServing once
// the installer exists.
func newServing(cfg Config, d Deps) (*serving, error) {
	if cfg.Serve == nil {
		return nil, nil
	}
	if d.Client == nil {
		return nil, errors.New("member: Config.Serve needs Deps.Client to read the published manifest")
	}
	owners := make([]uint64, 0, len(cfg.Peers))
	for _, p := range cfg.Peers {
		owners = append(owners, p.IndexerID)
	}
	slices.Sort(owners)
	copies := &servedCopies{owners: owners}
	metrics := serve.NewMetrics()
	server, err := serve.New(*cfg.Serve, serve.Deps{
		Registry:  d.Registry,
		Published: publishedFeed{watermark: d.Client, store: manifestfeed.NewStore(d.Client, servingManifestEntries)},
		Copies:    copies,
		Now:       d.Now,
		Metrics:   metrics,
		Logger:    d.Logger,
		NodeID:    cfg.NodeID,
		StateDir:  cfg.StateDir,
	})
	if err != nil {
		return nil, fmt.Errorf("member: %w", err)
	}
	return &serving{server: server, metrics: metrics, copies: copies}, nil
}

// installHooks are the role's install.Deps.BeforeInstall and AfterInstall:
// the serving server's, or nil and nil without Config.Serve (P2's installer).
func (r *Role) installHooks() (func(ctx context.Context, owner uint64, database string) error, func(owner uint64, database string)) {
	if r.serving == nil {
		return nil, nil
	}
	return r.serving.server.BeforeInstall, r.serving.server.AfterInstall
}

// attachServing binds the installer's journal to the serving view. The role
// calls it once the installer is built, before startup step (9).
func (r *Role) attachServing(j installJournal) {
	if r.serving != nil {
		r.serving.copies.attach(j)
	}
}

// runServing runs the serving server's polls and owner probes until ctx
// ends, as one of the components of startup step (9); without Config.Serve
// it returns nil at once.
func (r *Role) runServing(ctx context.Context) error {
	if r.serving == nil {
		return nil
	}
	return r.serving.server.Run(ctx)
}

// ServeHandler is the serving endpoint (contract §30.3), nil without
// Config.Serve. It answers from New on; before startup step (9) its view
// lists no database.
func (r *Role) ServeHandler() http.Handler {
	if r.serving == nil {
		return nil
	}
	return r.serving.server.Handler()
}

// Serving is the serving view, empty without Config.Serve.
func (r *Role) Serving() serve.ServingView {
	if r.serving == nil {
		return serve.ServingView{}
	}
	return r.serving.server.Serving()
}

// ServeMetrics are the serving series (contract §30.5), nil without
// Config.Serve; sentio-node registers them on the si-member process's
// registry.
func (r *Role) ServeMetrics() *serve.Metrics {
	if r.serving == nil {
		return nil
	}
	return r.serving.metrics
}

// installJournal is the part of the installer the serving view reads.
type installJournal interface {
	Entry(owner uint64) (install.OwnerJournal, bool)
	Tables(owner uint64) []install.TableState
	Stats() install.Stats
}

// servedCopies adapts the install journal to serve.Copies: one database per
// database part of a ready copy's table key, its copy at the owner's journal
// entry. A table whose journal is not at that entry is excluded; Foreign
// takes precedence over Diverged.
type servedCopies struct {
	owners []uint64 // the configured peers, ascending

	mu      sync.Mutex
	journal installJournal
}

func (c *servedCopies) attach(j installJournal) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.journal = j
}

func (c *servedCopies) installer() installJournal {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.journal
}

func (c *servedCopies) Databases() []serve.OwnerDatabase {
	j := c.installer()
	if j == nil {
		return nil
	}
	seen := map[serve.OwnerDatabase]bool{}
	var out []serve.OwnerDatabase
	for _, owner := range c.owners {
		for _, t := range j.Tables(owner) {
			database, _, ok := strings.Cut(t.TableKey, ".")
			key := serve.OwnerDatabase{Owner: owner, Database: database}
			if ok && database != "" && !seen[key] {
				seen[key] = true
				out = append(out, key)
			}
		}
	}
	slices.SortFunc(out, func(a, b serve.OwnerDatabase) int {
		return cmp.Or(cmp.Compare(a.Database, b.Database), cmp.Compare(a.Owner, b.Owner))
	})
	return out
}

func (c *servedCopies) Database(owner uint64, database string) (serve.DatabaseCopy, bool) {
	j := c.installer()
	if j == nil {
		return serve.DatabaseCopy{}, false
	}
	entry, ok := j.Entry(owner)
	if !ok {
		return serve.DatabaseCopy{}, false
	}
	out := serve.DatabaseCopy{SnapshotID: entry.SnapshotID, Entry: entry.Entry, AuditClean: j.Stats().AuditClean}
	for _, t := range j.Tables(owner) {
		if db, _, _ := strings.Cut(t.TableKey, "."); db != database {
			continue
		}
		condition := serve.ConditionInstalled
		switch {
		case t.Foreign:
			condition = serve.ConditionForeign
		case t.Diverged:
			condition = serve.ConditionDiverged
		case t.SnapshotID != entry.SnapshotID || t.Entry != entry.Entry:
			condition = serve.ConditionExcluded
		}
		out.Tables = append(out.Tables, serve.TableCopy{
			TableID: t.TableKey, IncarnationSeq: t.IncarnationSeq, CopyTable: t.Copy, SchemaHash: t.SchemaHash, Condition: condition,
		})
	}
	slices.SortFunc(out.Tables, func(a, b serve.TableCopy) int {
		return cmp.Or(cmp.Compare(a.TableID, b.TableID), cmp.Compare(a.IncarnationSeq, b.IncarnationSeq))
	})
	return out, len(out.Tables) > 0
}

// publishedFeed reads the latest published manifest for the serving view:
// the arbiter's watermark in every call, the manifest through a store that
// keeps it only once manifestfeed.Verify accepted it (the feed's rule, Plan
// S3-C2 contract §21.4), with the watermark's root and track_frontiers.
type publishedFeed struct {
	watermark interface {
		GetSafeWatermark(ctx context.Context) (arbiter.SafeWatermark, error)
	}
	store interface {
		GetSafeSnapshot(ctx context.Context, snapshotID string) (replay.SafeSnapshotManifest, error)
	}
}

func (p publishedFeed) Latest(ctx context.Context) (replay.SafeSnapshotManifest, error) {
	w, err := p.watermark.GetSafeWatermark(ctx)
	if err != nil {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("member serving: safe watermark: %w", err)
	}
	if w.SnapshotID == "" {
		return replay.SafeSnapshotManifest{}, errors.New("member serving: the arbiter has published no manifest yet")
	}
	m, err := p.store.GetSafeSnapshot(ctx, w.SnapshotID)
	if err != nil {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("member serving: manifest %s: %w", w.SnapshotID, err)
	}
	if m.ManifestRoot != w.ManifestRoot {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("member serving: watermark names manifest_root %s, manifest %s has %s", w.ManifestRoot, m.SnapshotID, m.ManifestRoot)
	}
	if len(m.TrackFrontiers) == 0 {
		return replay.SafeSnapshotManifest{}, fmt.Errorf("member serving: manifest %s carries no track_frontiers", m.SnapshotID)
	}
	return m, nil
}
