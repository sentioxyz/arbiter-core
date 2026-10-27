// Package copyset is the stage-3 member's copy reconciler (housegate spec
// 2026-10-14 §8.3, §8.8; plan S3-C2 K31). It keeps exactly one MergeTree copy
// in hg_replica for every Pending or Active incarnation owned by an SI
// indexer in its group scope, drops a copy from Retiring on, and drops every
// other hg_replica table: copies of Purged, Refused and Legacy incarnations,
// of owners outside the scope, and tables whose name and incarnation comment
// name no such copy. Copies are regenerable local state on no Keeper path,
// so the reconciler never reports a purge, never delays one, and never opens
// the host SNode's protocol databases, hg_rstage or hg_member_unsafe. In P2
// the scope is static: every enrolled SI indexer but the host.
package copyset

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// Config fixes one reconciler.
type Config struct {
	// Pinned names the member databases, the node id and the storage policy.
	Pinned ddl.MemberPinned
	// Protocol is the host SNode's {hg_unsafe, hg_safe, hg_promote}; no
	// member database may be one of them (ddl.ValidateMemberPinned).
	Protocol ddl.Pinned
	// NetworkID is the network every chain schema hashes under.
	NetworkID string
	// Genesis supplies the schemas of genesis-origin incarnations, which
	// carry no schema_json.
	Genesis []payloadexec.TableSchema
	// Owners is the static group scope of P2: ascending, unique and never
	// the host.
	Owners []uint64
	// HostIndexerID is the SI indexer whose host runs this member.
	HostIndexerID uint64
	// Interval is the steady-state pass cadence (0 = ddl.DefaultReconcileInterval).
	Interval time.Duration
}

// Deps are the reconciler's collaborators.
type Deps struct {
	// Conn is the member's ClickHouse, as si_member.
	Conn clickhouse.Conn
	// Registry is the table-registry follower.
	Registry dataplane.RegistryView
	Logger   *slog.Logger
	// Dropped, when set, runs after each table this reconciler dropped, so
	// the installer forgets that copy's journal. Table is always set; Owner,
	// TableKey, IncarnationSeq and Status are set when the registry
	// attributes the table. It must be idempotent. A failure is retried at
	// the start of every later pass, and until it succeeds no copy of that
	// name is reported ready.
	Dropped func(ctx context.Context, c Copy) error
}

// Copy is one copy the reconciler keeps or dropped.
type Copy struct {
	Owner          uint64
	TableKey       string
	IncarnationSeq uint64
	// Table is the copy's name in ReplicaDB (ddl.CopyTableName).
	Table  string
	Status wire.TableIncarnationStatus
	Schema payloadexec.TableSchema
	// Generation numbers the ClickHouse table that holds the copy: it changes
	// whenever a pass creates the table, also when a table this reconciler
	// already reported vanished and is created again empty, and stays while
	// the table stays. The installer's audit coverage and the role's copy key
	// include it, so a recreated copy is unaudited, and not ready to serve,
	// until it was compared with its journal again.
	Generation uint64
	// Ready is true once the copy exists and verified in the last pass.
	Ready bool
}

// Stats is a point-in-time copy of the reconciler's counters.
type Stats struct {
	// Copies is the number of ready copies.
	Copies int
	// Dropped counts copies dropped from Retiring on; Unknown counts every
	// other hg_replica table dropped.
	Dropped, Unknown uint64
	// Failures counts failed copy reconciles by table key (by
	// <ReplicaDB>.<table> for a drop the registry cannot attribute).
	Failures map[string]uint64
}

// store is the reconciler's view of the member's ClickHouse; tests replace
// it.
type store interface {
	list(ctx context.Context) ([]ddl.LocalTable, error)
	ensure(ctx context.Context, schema payloadexec.TableSchema, incarnationSeq uint64) error
	drop(ctx context.Context, table string) error
}

type clickHouseStore struct {
	conn clickhouse.Conn
	m    ddl.MemberPinned
}

func (s clickHouseStore) list(ctx context.Context) ([]ddl.LocalTable, error) {
	return ddl.ListMemberTables(ctx, s.conn, s.m)
}

func (s clickHouseStore) ensure(ctx context.Context, schema payloadexec.TableSchema, incarnationSeq uint64) error {
	return ddl.EnsureCopy(ctx, s.conn, s.m, schema, incarnationSeq)
}

func (s clickHouseStore) drop(ctx context.Context, table string) error {
	return ddl.DropCopyTable(ctx, s.conn, s.m, table)
}

var errRegistryNotAnswered = errors.New("copyset: the table registry has not answered yet")

// Reconciler reconciles one member's copies. Passes are serialized; Copies,
// Copy, Stats and Changed are safe for concurrent use.
type Reconciler struct {
	cfg    Config
	d      Deps
	store  store
	owners map[uint64]bool

	passMu sync.Mutex

	mu       sync.Mutex
	copies   map[string]Copy // table key -> desired copy at the last pass
	gens     uint64          // the last Generation handed out
	ready    []string        // sorted names@generations of the ready copies
	changed  chan struct{}
	owed     map[string]Copy // copy name -> a drop whose Dropped hook failed
	dropped  uint64
	unknown  uint64
	failures map[string]uint64
}

// New validates cfg and returns a reconciler on d.Conn.
func New(cfg Config, d Deps) (*Reconciler, error) {
	if d.Conn == nil {
		return nil, errors.New("copyset: clickhouse connection is required")
	}
	return newReconciler(cfg, d, clickHouseStore{conn: d.Conn, m: cfg.Pinned})
}

func newReconciler(cfg Config, d Deps, s store) (*Reconciler, error) {
	if d.Registry == nil {
		return nil, errors.New("copyset: a table registry view is required")
	}
	if err := ddl.ValidateMemberPinned(cfg.Pinned, cfg.Protocol); err != nil {
		return nil, err
	}
	if cfg.NetworkID == "" {
		return nil, errors.New("copyset: NetworkID is required")
	}
	if len(cfg.Owners) == 0 {
		return nil, errors.New("copyset: Owners is empty: a member copies at least one peer owner")
	}
	owners := make(map[uint64]bool, len(cfg.Owners))
	for i, owner := range cfg.Owners {
		if i > 0 && cfg.Owners[i-1] >= owner {
			return nil, fmt.Errorf("copyset: Owners must be ascending without duplicates, got %v", cfg.Owners)
		}
		if owner == cfg.HostIndexerID {
			return nil, fmt.Errorf("copyset: owner %d is the host's own indexer", owner)
		}
		owners[owner] = true
	}
	if err := ddl.ValidatePhysicalTableNames(cfg.Genesis); err != nil {
		return nil, err
	}
	if cfg.Interval <= 0 {
		cfg.Interval = ddl.DefaultReconcileInterval
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	cfg.Owners = slices.Clone(cfg.Owners)
	cfg.Genesis = slices.Clone(cfg.Genesis)
	return &Reconciler{
		cfg: cfg, d: d, store: s, owners: owners,
		copies: map[string]Copy{}, changed: make(chan struct{}),
		owed: map[string]Copy{}, failures: map[string]uint64{},
	}, nil
}

// Run reconciles at once once the registry answered, then again on every
// accepted registry version and every Interval, backing off after a failed
// pass with ddl.ReconcileBackoff. A failed pass is logged and retried; Run
// returns only ctx's error.
func (r *Reconciler) Run(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.d.Registry.Ready():
	}
	consecutive := 0
	for {
		// Arm the registry wake channel before the pass, so a version
		// accepted during it starts the next one.
		registryChanged := r.d.Registry.Changed()
		delay := r.cfg.Interval
		if err := r.Reconcile(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			consecutive++
			delay = ddl.ReconcileBackoff(consecutive, r.cfg.Interval)
			r.d.Logger.Warn("copy reconcile pass failed; retrying", "consecutive_failures", consecutive, "retry_in", delay, "err", err)
		} else {
			consecutive = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-registryChanged:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// Reconcile runs one pass. With the registry disabled no copy is kept: none
// is created or reported ready and nothing is dropped, since only the
// registry can tell a copy from an unknown table. Otherwise every hg_replica
// table that is not the copy of a Pending or Active incarnation of an owner
// in scope, under that copy's name and comment, is dropped (a copy of a
// Retiring or Purging incarnation counted Dropped, any other table Unknown),
// then every such copy is created if missing and verified. A failure of one
// copy does not stop the others; the pass returns them joined.
func (r *Reconciler) Reconcile(ctx context.Context) error {
	r.passMu.Lock()
	defer r.passMu.Unlock()
	select {
	case <-r.d.Registry.Ready():
	default:
		return errRegistryNotAnswered
	}
	snap, enabled := r.d.Registry.View()
	if !enabled {
		r.publish(map[string]Copy{})
		return nil
	}
	var errs []error
	for _, c := range r.owedDrops() {
		if err := r.notifyDropped(ctx, c); err != nil {
			errs = append(errs, err)
		}
	}
	local, err := r.store.list(ctx)
	if err != nil {
		return errors.Join(append(errs, fmt.Errorf("copyset: list member tables: %w", err))...)
	}
	sort.Slice(local, func(i, j int) bool {
		if local[i].Table != local[j].Table {
			return local[i].Table < local[j].Table
		}
		return local[i].Database < local[j].Database
	})

	desired, schemaErrs := r.desired(snap)
	present := map[string]bool{}
	for _, lt := range local {
		if lt.Database != r.cfg.Pinned.ReplicaDB {
			continue
		}
		c, v, reason := r.classify(snap, lt)
		if v == keep {
			present[lt.Table] = true
			continue
		}
		if err := r.dropTable(ctx, c, v, reason); err != nil {
			errs = append(errs, err)
		}
	}

	for _, key := range sortedKeys(desired) {
		c := desired[key]
		if err := schemaErrs[key]; err != nil {
			errs = append(errs, r.fail(key, fmt.Errorf("copyset: copy %s of %s (incarnation %d): %w", c.Table, key, c.IncarnationSeq, err)))
			continue
		}
		if r.isOwed(c.Table) {
			errs = append(errs, r.fail(key, fmt.Errorf("copyset: copy %s of %s waits until the installer forgets the dropped table of that name", c.Table, key)))
			continue
		}
		if err := r.store.ensure(ctx, c.Schema, c.IncarnationSeq); err != nil {
			errs = append(errs, r.fail(key, fmt.Errorf("copyset: copy %s of %s (incarnation %d): %w", c.Table, key, c.IncarnationSeq, err)))
			continue
		}
		c.Generation = r.generation(c, present[c.Table])
		if !present[c.Table] {
			r.d.Logger.Info("created copy", "table", key, "incarnation", c.IncarnationSeq, "owner", c.Owner, "copy", c.Table, "generation", c.Generation)
		}
		c.Ready = true
		desired[key] = c
	}
	r.publish(desired)
	return errors.Join(errs...)
}

// generation is copy c's Generation after a pass that found its table (present)
// or created it. A table that was there keeps the generation the last pass gave
// it; one this pass created gets a new one. A table dropped in the moment
// between this pass's listing and its CREATE TABLE IF NOT EXISTS is created
// again under its old generation; the journaled parts it lacks are then found
// by the next manifest's pass or the self-audit.
func (r *Reconciler) generation(c Copy, present bool) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if prev, ok := r.copies[c.TableKey]; ok && present && prev.Table == c.Table && prev.Generation != 0 {
		return prev.Generation
	}
	r.gens++
	return r.gens
}

// desired is every Pending or Active incarnation of an owner in scope, by
// table key, with its resolved schema; a schema that does not resolve is
// reported per key.
func (r *Reconciler) desired(snap wire.TableRegistrySnapshot) (map[string]Copy, map[string]error) {
	out := map[string]Copy{}
	errs := map[string]error{}
	for _, inc := range snap.Incarnations {
		owner := snap.Owner(inc)
		if !r.owners[owner] {
			continue
		}
		if inc.Status != wire.TableStatusPending && inc.Status != wire.TableStatusActive {
			continue
		}
		key := inc.Key()
		c := Copy{Owner: owner, TableKey: key, IncarnationSeq: inc.Seq, Table: ddl.CopyTableName(key, inc.Seq), Status: inc.Status}
		schema, err := dataplane.IncarnationSchema(r.cfg.NetworkID, r.cfg.Genesis, inc)
		if err != nil {
			errs[key] = err
		} else {
			c.Schema = schema
		}
		out[key] = c
	}
	return out, errs
}

type verdict int

const (
	keep    verdict = iota // the copy of a Pending or Active incarnation in scope
	retired                // the copy of a Retiring or Purging incarnation in scope
	unknown                // anything else
)

// classify judges one hg_replica table by its incarnation comment and name.
func (r *Reconciler) classify(snap wire.TableRegistrySnapshot, lt ddl.LocalTable) (Copy, verdict, string) {
	seq, ok := ddl.ParseIncarnationComment(lt.Comment)
	if !ok {
		return Copy{Table: lt.Table}, unknown, "no incarnation comment"
	}
	inc := incarnation(snap, seq)
	if inc == nil {
		return Copy{Table: lt.Table, IncarnationSeq: seq}, unknown, "an incarnation the registry does not record"
	}
	if ddl.CopyTableName(inc.Key(), seq) != lt.Table {
		return Copy{Table: lt.Table}, unknown, "its comment names another table's incarnation"
	}
	c := Copy{Owner: snap.Owner(*inc), TableKey: inc.Key(), IncarnationSeq: seq, Table: lt.Table, Status: inc.Status}
	if !r.owners[c.Owner] {
		return c, unknown, "an owner outside this member's scope"
	}
	switch inc.Status {
	case wire.TableStatusPending, wire.TableStatusActive:
		return c, keep, ""
	case wire.TableStatusRetiring, wire.TableStatusPurging:
		return c, retired, ""
	default:
		return c, unknown, "an incarnation that holds no tables (" + string(inc.Status) + ")"
	}
}

// dropTable drops one hg_replica table, counts it and tells the installer.
func (r *Reconciler) dropTable(ctx context.Context, c Copy, v verdict, reason string) error {
	failureKey := c.TableKey
	if failureKey == "" || v == unknown {
		failureKey = r.cfg.Pinned.ReplicaDB + "." + c.Table
	}
	if err := r.store.drop(ctx, c.Table); err != nil {
		return r.fail(failureKey, fmt.Errorf("copyset: drop %s.%s: %w", r.cfg.Pinned.ReplicaDB, c.Table, err))
	}
	r.mu.Lock()
	if v == retired {
		r.dropped++
	} else {
		r.unknown++
	}
	r.mu.Unlock()
	if v == retired {
		r.d.Logger.Info("dropped copy from Retiring on", "table", c.TableKey, "incarnation", c.IncarnationSeq, "status", c.Status, "copy", c.Table)
	} else {
		r.d.Logger.Warn("dropped an hg_replica table that is no copy this member keeps", "table", c.Table, "reason", reason)
	}
	return r.notifyDropped(ctx, c)
}

// notifyDropped runs the Dropped hook; a failure is owed and retried.
func (r *Reconciler) notifyDropped(ctx context.Context, c Copy) error {
	if r.d.Dropped == nil {
		return nil
	}
	err := r.d.Dropped(ctx, c)
	r.mu.Lock()
	if err != nil {
		r.owed[c.Table] = c
	} else {
		delete(r.owed, c.Table)
	}
	r.mu.Unlock()
	if err != nil {
		return fmt.Errorf("copyset: forget dropped table %s.%s: %w", r.cfg.Pinned.ReplicaDB, c.Table, err)
	}
	return nil
}

func (r *Reconciler) owedDrops() []Copy {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Copy, 0, len(r.owed))
	for _, name := range sortedKeys(r.owed) {
		out = append(out, r.owed[name])
	}
	return out
}

func (r *Reconciler) isOwed(table string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.owed[table]
	return ok
}

func (r *Reconciler) fail(key string, err error) error {
	r.mu.Lock()
	r.failures[key]++
	n := r.failures[key]
	r.mu.Unlock()
	r.d.Logger.Warn("copy reconcile failed", "table", key, "failures", n, "err", err)
	return err
}

// publish records the pass's copies and signals a change of the ready set,
// a copy created again under a new generation included.
func (r *Reconciler) publish(copies map[string]Copy) {
	var ready []string
	for _, c := range copies {
		if c.Ready {
			ready = append(ready, c.Table+"@"+strconv.FormatUint(c.Generation, 10))
		}
	}
	sort.Strings(ready)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.copies = copies
	if !slices.Equal(ready, r.ready) {
		r.ready = ready
		close(r.changed)
		r.changed = make(chan struct{})
	}
}

// Copies returns the ready copies, sorted by table key.
func (r *Reconciler) Copies() []Copy {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []Copy
	for _, key := range sortedKeys(r.copies) {
		if c := r.copies[key]; c.Ready {
			out = append(out, c)
		}
	}
	return out
}

// Copy returns the copy the last pass desired for tableKey, ready or not.
func (r *Reconciler) Copy(tableKey string) (Copy, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	c, ok := r.copies[tableKey]
	return c, ok
}

// Changed returns a channel closed when the set of ready copies, or the
// generation of one of them, changes after this call, so the role re-runs the installer for a copy that became ready
// between publications.
func (r *Reconciler) Changed() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.changed
}

// Stats returns a copy of the reconciler's counters.
func (r *Reconciler) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := Stats{Copies: len(r.ready), Dropped: r.dropped, Unknown: r.unknown, Failures: make(map[string]uint64, len(r.failures))}
	for key, n := range r.failures {
		out.Failures[key] = n
	}
	return out
}

// incarnation returns the incarnation numbered seq, or nil.
func incarnation(snap wire.TableRegistrySnapshot, seq uint64) *wire.TableIncarnation {
	for i := range snap.Incarnations {
		if snap.Incarnations[i].Seq == seq {
			return &snap.Incarnations[i]
		}
	}
	return nil
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
