// Package tableset is the level-triggered table-set reconciler of the
// dynamic storage-integrity table set (sub-project 4 §7). Each data-plane role
// runs one: it derives the desired hg_unsafe / hg_safe / hg_promote tables
// from the arbiter's table registry (or, while the registry is disabled, from
// the configured genesis set), compares them with ClickHouse and closes the
// difference: it creates and verifies the tables of Pending, Active and
// Retiring incarnations, drops those of Purging incarnations and reports
// SubmitTablePurged, drops leftovers of Purged, Refused and Legacy keys, and
// only reports protocol tables the registry does not know.
package tableset

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// State is one table's reconciler state, the label of the "tables per state"
// metric.
type State string

const (
	// StateCreating: the tables should exist but are not verified yet.
	StateCreating State = "creating"
	// StateReady: all three tables of the current incarnation exist and verify.
	StateReady State = "ready"
	// StateWaitingQuiescence: Purging, but the role's quiescence hook still
	// reports promotion or cleanup work for the table.
	StateWaitingQuiescence State = "waiting_quiescence"
	// StatePurging: dropping the tables or reporting the purge.
	StatePurging State = "purging"
	// StatePurgeReported: this node's purge is recorded by the arbiter.
	StatePurgeReported State = "purge_reported"
)

// Arbiter is the slice of *dataplane.Client the reconciler calls.
type Arbiter interface {
	SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error
	PurgeNodeSet(ctx context.Context) ([]string, error)
}

// Config fixes one reconciler.
type Config struct {
	// Pinned names the protocol databases, the keeper shard and this node's
	// replica name (the SNode node id or the verifier replica id).
	Pinned ddl.Pinned
	// Genesis is the configured genesis table set. While the registry is
	// disabled it is the whole desired set; afterwards it supplies the
	// schemas of genesis-origin incarnations, which carry no schema_json.
	Genesis []payloadexec.TableSchema
	// Interval is the steady-state pass cadence (0 = ddl.DefaultReconcileInterval);
	// it also caps the per-table failure backoff.
	Interval time.Duration
	// SweepDecommissioned makes this node remove, after its own drop,
	// every Keeper replica of a purged table that is not in the arbiter's
	// purge node set. Only the source SNode sets it.
	SweepDecommissioned bool
}

// Deps are the reconciler's collaborators.
type Deps struct {
	Conn clickhouse.Conn
	// Registry is the registry follower. Nil means the registry is never
	// followed: only the genesis set is reconciled.
	Registry dataplane.RegistryView
	// Arbiter receives purge reports. Required when Registry is set.
	Arbiter Arbiter
	// Quiescent, when set, gates a purge: it reports whether the role has no
	// promotion or unsafe cleanup work left that references tableID.
	Quiescent func(tableID string) (bool, error)
	// Dropped, when set, runs after this node dropped tableID's protocol
	// tables: in a purge, after the drop and before SubmitTablePurged (a
	// failure fails the purge step, which is retried with backoff and not
	// reported), and after every leftover drop (a failure is retried before
	// the key's tables are created again). It lets the role forget
	// per-table state, so a later incarnation under the same name (spec D9)
	// starts from an empty ledger. It must be idempotent.
	Dropped func(ctx context.Context, tableID string) error
	Logger  *slog.Logger
}

// Stats is a point-in-time copy of the reconciler's metrics.
type Stats struct {
	States        map[State]int
	Failures      map[string]uint64
	Unknown       []string
	LeftoverDrops uint64
}

type tableStatus struct {
	state       State
	seq         uint64
	failures    int
	nextAttempt time.Time
}

// Reconciler reconciles one node's protocol tables. Reconcile passes are
// serialized; Ready, Stats, Trigger and Wake are safe for concurrent use.
type Reconciler struct {
	cfg     Config
	d       Deps
	genesis map[string]payloadexec.TableSchema
	now     func() time.Time

	passMu sync.Mutex

	mu            sync.Mutex
	tables        map[string]*tableStatus
	failures      map[string]uint64
	unknown       []string
	leftoverDrops uint64
	// owed names the keys whose last Dropped hook call failed; their tables
	// are not created again until the hook succeeds. In memory only: a
	// restart forgets it.
	owed map[string]bool
	// early carries the early-pass signal Wake exposes. Trigger sends on it
	// after clearing every backoff; WaitReady sends on it and keeps the
	// backoffs (P9).
	early    chan struct{}
	passDone chan struct{}
}

// New validates cfg and returns a reconciler.
func New(cfg Config, d Deps) (*Reconciler, error) {
	if d.Conn == nil {
		return nil, errors.New("tableset: clickhouse connection is required")
	}
	if cfg.Pinned.UnsafeDB == "" || cfg.Pinned.SafeDB == "" || cfg.Pinned.PromoteDB == "" || cfg.Pinned.NodeID == "" {
		return nil, errors.New("tableset: Pinned needs UnsafeDB, SafeDB, PromoteDB and NodeID")
	}
	if d.Registry != nil && d.Arbiter == nil {
		return nil, errors.New("tableset: an arbiter client is required to follow the registry")
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
	genesis := make(map[string]payloadexec.TableSchema, len(cfg.Genesis))
	for _, t := range cfg.Genesis {
		genesis[t.TableID] = t
	}
	return &Reconciler{
		cfg: cfg, d: d, genesis: genesis, now: time.Now,
		tables: map[string]*tableStatus{}, failures: map[string]uint64{}, owed: map[string]bool{},
		early: make(chan struct{}, 1), passDone: make(chan struct{}),
	}, nil
}

// Ready reports whether all three tables of tableID's current incarnation
// exist and verify. With the registry enabled the verified incarnation must
// be the key's live one, so tables of an earlier incarnation never count.
func (r *Reconciler) Ready(tableID string) bool {
	if r == nil {
		return false
	}
	var snap wire.TableRegistrySnapshot
	enabled := false
	if r.d.Registry != nil {
		snap, enabled = r.d.Registry.View()
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.readyLocked(tableID, snap, enabled)
}

func (r *Reconciler) readyLocked(tableID string, snap wire.TableRegistrySnapshot, enabled bool) bool {
	ts := r.tables[tableID]
	if ts == nil || ts.state != StateReady {
		return false
	}
	if !enabled {
		return true
	}
	live := snap.Live(tableID)
	if live == nil || !live.HasPhysicalTables() || live.Status == wire.TableStatusPurging {
		return false
	}
	// Tables verified while the registry was disabled are recorded as the
	// unmarked genesis incarnation (seq 0). They are the key's genesis-origin
	// incarnation, so enabling the registry does not make them briefly
	// not-Ready (R-F7b); a chain recreation of the key never matches them.
	return live.Seq == ts.seq || (ts.seq == 0 && live.Origin == wire.TableOriginGenesis)
}

// Trigger requests an early pass and clears every per-table backoff.
func (r *Reconciler) Trigger() {
	if r == nil {
		return
	}
	r.mu.Lock()
	for _, ts := range r.tables {
		ts.nextAttempt = time.Time{}
	}
	r.mu.Unlock()
	r.wake()
}

// wake requests an early pass without touching any per-table backoff: a
// table that is backing off stays skipped until its retry is due.
func (r *Reconciler) wake() {
	select {
	case r.early <- struct{}{}:
	default:
	}
}

// Wake returns the channels that start an early pass: the registry's next
// accepted version, and trigger, which fires on Trigger and on WaitReady. A
// nil reconciler returns nil channels.
func (r *Reconciler) Wake() (registry <-chan struct{}, trigger <-chan struct{}) {
	if r == nil {
		return nil, nil
	}
	if r.d.Registry != nil {
		registry = r.d.Registry.Changed()
	}
	return registry, r.early
}

// NextDelay is how long the caller's loop may sleep before the next pass:
// the interval, shortened to the earliest per-table retry.
func (r *Reconciler) NextDelay() time.Duration {
	if r == nil {
		return ddl.DefaultReconcileInterval
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	delay := r.cfg.Interval
	now := r.now()
	for _, ts := range r.tables {
		if ts.nextAttempt.IsZero() {
			continue
		}
		if d := ts.nextAttempt.Sub(now); d < delay {
			delay = max(d, 0)
		}
	}
	return delay
}

// WaitReady wakes the reconcile loop for an early pass and waits up to
// timeout for every table in tableIDs to be Ready. It reports whether they all
// are. Unlike Trigger it keeps every per-table backoff (P9): a caller that
// waits repeatedly, such as the verifier's add gate on every redelivery, must
// not turn a failing table's backoff into a retry at its own cadence.
func (r *Reconciler) WaitReady(ctx context.Context, tableIDs []string, timeout time.Duration) bool {
	if r == nil {
		return false
	}
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	r.wake()
	for {
		var snap wire.TableRegistrySnapshot
		enabled := false
		if r.d.Registry != nil {
			snap, enabled = r.d.Registry.View()
		}
		r.mu.Lock()
		done := r.passDone
		ready := true
		for _, id := range tableIDs {
			if !r.readyLocked(id, snap, enabled) {
				ready = false
				break
			}
		}
		r.mu.Unlock()
		if ready {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-deadline.C:
			return false
		case <-done:
		}
	}
}

// Stats returns a copy of the reconciler's metrics.
func (r *Reconciler) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := Stats{States: map[State]int{}, Failures: map[string]uint64{}, Unknown: slices.Clone(r.unknown), LeftoverDrops: r.leftoverDrops}
	for _, ts := range r.tables {
		out.States[ts.state]++
	}
	for id, n := range r.failures {
		out.Failures[id] = n
	}
	return out
}

// Reconcile runs one pass. genesisMode governs genesis-origin tables (the
// role's schema-source mode at startup, ddl.ModeVerifyOnly afterwards);
// chain-origin tables are always created and verified. It returns fatal
// errors (drift, a verify-only table missing, an unattributable table) and
// any failure of a genesis table, which the role's existing retry budget
// handles; every other per-table failure is recorded, backed off and retried
// by a later pass.
func (r *Reconciler) Reconcile(ctx context.Context, genesisMode ddl.Mode) error {
	r.passMu.Lock()
	defer r.passMu.Unlock()
	defer r.finishPass()
	var snap wire.TableRegistrySnapshot
	enabled := false
	if r.d.Registry != nil {
		snap, enabled = r.d.Registry.View()
	}
	if !enabled {
		return r.reconcileGenesis(ctx, genesisMode)
	}
	return r.reconcileRegistry(ctx, snap, genesisMode)
}

func (r *Reconciler) finishPass() {
	r.mu.Lock()
	defer r.mu.Unlock()
	close(r.passDone)
	r.passDone = make(chan struct{})
}

// reconcileGenesis is the registry-disabled pass: exactly the configured
// genesis set, in its configured mode; nothing is ever dropped.
func (r *Reconciler) reconcileGenesis(ctx context.Context, mode ddl.Mode) error {
	if mode == ddl.ModeOff {
		return nil
	}
	var errs []error
	desired := map[string]bool{}
	for _, schema := range r.cfg.Genesis {
		desired[ddl.CHTableName(schema.TableID)] = true
		if err := ddl.EnsureTable(ctx, r.d.Conn, r.cfg.Pinned, schema, 0, mode); err != nil {
			r.fail(schema.TableID, 0, StateCreating, err)
			errs = append(errs, err)
			continue
		}
		r.setState(schema.TableID, 0, StateReady)
	}
	if err := r.reportUnknown(ctx, desired); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

type target struct {
	inc    wire.TableIncarnation
	schema payloadexec.TableSchema
	mode   ddl.Mode
	marker uint64
}

func (r *Reconciler) reconcileRegistry(ctx context.Context, snap wire.TableRegistrySnapshot, genesisMode ddl.Mode) error {
	present := map[string]target{}                // physical name -> table to create and verify
	wanted := map[string]bool{}                   // physical names a Pending/Active/Retiring key desires
	purging := map[string]wire.TableIncarnation{} // physical name -> incarnation to purge
	history := map[string][]string{}              // physical name -> keys whose live incarnation is Purged/Refused/Legacy
	seen := map[string]bool{}
	var fatal, genesisErrs []error
	for _, inc := range snap.Incarnations {
		key := inc.Key()
		if seen[key] {
			continue
		}
		seen[key] = true
		live := snap.Live(key)
		physical := ddl.CHTableName(key)
		switch {
		case live.Status == wire.TableStatusPurging:
			purging[physical] = *live
		case live.HasPhysicalTables():
			wanted[physical] = true
			// A chain table that is backing off is skipped before its schema
			// is decoded, so a deterministic decode failure backs off too. A
			// genesis table keeps the role's own retry budget (its failures
			// are returned), so it is never skipped.
			if live.Origin != wire.TableOriginGenesis && !r.due(key) {
				continue
			}
			t, err := r.targetFor(*live, genesisMode)
			if err != nil {
				r.fail(key, live.Seq, StateCreating, err)
				if live.Origin == wire.TableOriginGenesis {
					genesisErrs = append(genesisErrs, err)
				}
				continue
			}
			present[physical] = t
		default:
			// A retired key has no tables to converge: forget its status, so
			// neither an expired backoff nor its last state outlives it.
			r.forget(key)
			history[physical] = append(history[physical], key)
		}
	}
	r.forgetAbsent(seen)

	local, err := ddl.ListProtocolTables(ctx, r.d.Conn, r.cfg.Pinned)
	if err != nil {
		return err
	}
	comments := map[string][]string{}
	unsafeLocal := map[string]bool{} // physical names with a local hg_unsafe table
	for _, lt := range local {
		comments[lt.Table] = append(comments[lt.Table], lt.Comment)
		if lt.Database == r.cfg.Pinned.UnsafeDB {
			unsafeLocal[lt.Table] = true
		}
	}

	for _, physical := range sortedKeys(present) {
		t := present[physical]
		if err := r.ensurePresent(ctx, snap, t, comments[physical], unsafeLocal[physical]); err != nil {
			switch {
			case ddl.FatalReconcileError(err):
				fatal = append(fatal, err)
			case t.inc.Origin == wire.TableOriginGenesis:
				genesisErrs = append(genesisErrs, err)
			}
		}
	}
	for _, physical := range sortedKeys(purging) {
		inc := purging[physical]
		if wanted[physical] {
			// D2 naming is not injective: another key that is Pending,
			// Active or Retiring desires this physical name. Arbiter
			// admission prevents the collision; never drop its tables, and
			// never report a purge whose drop did not happen.
			r.setState(inc.Key(), inc.Seq, StatePurging)
			r.d.Logger.Error("purging table shares its physical name with a live table; not dropping or reporting it",
				"table", inc.Key(), "incarnation", inc.Seq, "physical", physical)
			continue
		}
		r.purge(ctx, inc, len(comments[physical]) > 0)
	}
	desired := maps.Clone(wanted)
	for physical := range purging {
		desired[physical] = true
	}
	// Local tables under a retired key are leftovers only when every marker
	// attributes them to an incarnation of such a key (R-F1, P8). Any other
	// marker is never dropped: it is reported as unknown, and not fatal,
	// because nothing needs creating under a retired key.
	accounted := maps.Clone(desired) // physical names that are not unknown
	skipSweep := maps.Clone(desired) // physical names the Keeper sweep must not drop
	for _, physical := range sortedKeys(comments) {
		keys, ok := history[physical]
		if desired[physical] || !ok {
			continue
		}
		if !attributable(snap, keys, comments[physical]) {
			skipSweep[physical] = true
			continue
		}
		accounted[physical] = true
		r.dropLeftover(ctx, keys[0])
	}
	if r.cfg.SweepDecommissioned {
		r.sweepPurgedKeeperPaths(ctx, history, skipSweep)
	}
	if err := r.reportUnknown(ctx, accounted); err != nil {
		fatal = append(fatal, err)
	}
	return errors.Join(append(fatal, genesisErrs...)...)
}

func (r *Reconciler) targetFor(inc wire.TableIncarnation, genesisMode ddl.Mode) (target, error) {
	if inc.Origin == wire.TableOriginGenesis {
		schema, ok := r.genesis[inc.Key()]
		if !ok {
			return target{}, fmt.Errorf("tableset: genesis table %s is not in the configured genesis set", inc.Key())
		}
		return target{inc: inc, schema: schema, mode: genesisMode}, nil
	}
	schema, err := inc.Schema()
	if err != nil {
		return target{}, err
	}
	return target{inc: inc, schema: schema, mode: ddl.ModeCreateAndVerify, marker: inc.Seq}, nil
}

// ensurePresent creates and verifies one table. The expected table comment
// is empty for a genesis table and IncarnationComment(seq) for a chain one. A
// local table of a chain table with another comment is a leftover when the
// registry records the incarnation it belongs to as Purged or Refused
// (retiredLeftover); it is dropped before the create. Any other comment is
// drift. hasUnsafe reports whether a local hg_unsafe table of that name
// exists; before a chain incarnation's hg_unsafe is created, its Keeper path
// must hold no decommissioned replica (clearDecommissioned).
func (r *Reconciler) ensurePresent(ctx context.Context, snap wire.TableRegistrySnapshot, t target, comments []string, hasUnsafe bool) error {
	key := t.inc.Key()
	expected := ""
	if t.marker != 0 {
		expected = ddl.IncarnationComment(t.marker)
	}
	leftover := false
	for _, comment := range comments {
		if comment == expected {
			continue
		}
		if t.marker == 0 || !retiredLeftover(snap, key, comment, t.inc.Seq) {
			err := fmt.Errorf("%w: %s tables carry comment %q, want %q (incarnation %d)", ddl.ErrProtocolTableDrift, key, comment, expected, t.inc.Seq)
			r.fail(key, t.inc.Seq, StateCreating, err)
			return err
		}
		leftover = true
	}
	if leftover {
		if err := ddl.DropTable(ctx, r.d.Conn, r.cfg.Pinned, key); err != nil {
			r.fail(key, t.inc.Seq, StateCreating, err)
			return err
		}
		r.countLeftover(key, "earlier incarnation")
		hasUnsafe = false
		if err := r.notifyDropped(ctx, key); err != nil {
			r.fail(key, t.inc.Seq, StateCreating, err)
			return err
		}
	} else if r.isOwed(key) {
		// An earlier leftover drop's hook failed: retry it before the key's
		// tables exist again.
		if err := r.notifyDropped(ctx, key); err != nil {
			r.fail(key, t.inc.Seq, StateCreating, err)
			return err
		}
	}
	if t.marker != 0 && !hasUnsafe {
		if err := r.clearDecommissioned(ctx, key); err != nil {
			r.fail(key, t.inc.Seq, StateCreating, err)
			return err
		}
	}
	if err := ddl.EnsureTable(ctx, r.d.Conn, r.cfg.Pinned, t.schema, t.marker, t.mode); err != nil {
		r.fail(key, t.inc.Seq, StateCreating, err)
		return err
	}
	r.setState(key, t.inc.Seq, StateReady)
	return nil
}

// ownerOf maps a table comment to the incarnation seq it marks: the marked
// seq, or for an unmarked table the key's genesis incarnation; 0 when the
// comment names no incarnation of key. A comment that is neither empty nor a
// canonical marker (ddl.ParseIncarnationComment, which rejects "007" and the
// like) names none, and neither does a marker of another key's incarnation.
func ownerOf(snap wire.TableRegistrySnapshot, key, comment string) uint64 {
	if comment == "" {
		for _, inc := range snap.Incarnations {
			if inc.Key() == key && inc.Origin == wire.TableOriginGenesis {
				return inc.Seq
			}
		}
		return 0
	}
	seq, ok := ddl.ParseIncarnationComment(comment)
	if !ok {
		return 0
	}
	if inc := incarnation(snap, seq); inc == nil || inc.Key() != key {
		return 0
	}
	return seq
}

// earlierRetired reports whether owner is an earlier incarnation of key that
// the registry records as Purged or Refused.
func earlierRetired(snap wire.TableRegistrySnapshot, key string, owner, current uint64) bool {
	if owner == 0 || owner >= current {
		return false
	}
	inc := incarnation(snap, owner)
	return inc != nil && inc.Key() == key && (inc.Status == wire.TableStatusPurged || inc.Status == wire.TableStatusRefused)
}

// retiredLeftover reports whether a local table under key's physical name
// with comment is a leftover the registry has retired, so it may be dropped
// before key's incarnation current is created:
//   - an unmarked table whose owner is key's genesis incarnation, earlier than
//     current and Purged or Refused;
//   - a canonical marker naming a Purged or Refused incarnation of any key with
//     the same D2 physical name (key itself, or a colliding key such as a
//     Purged a__b.c under a newly admitted a.b__c, left on a node that was
//     evicted before the purge and rejoined).
//
// An unparseable or unknown marker, and a marker of a live incarnation of any
// key, is not a leftover.
func retiredLeftover(snap wire.TableRegistrySnapshot, key, comment string, current uint64) bool {
	if comment == "" {
		return earlierRetired(snap, key, ownerOf(snap, key, comment), current)
	}
	seq, ok := ddl.ParseIncarnationComment(comment)
	if !ok {
		return false
	}
	inc := incarnation(snap, seq)
	return inc != nil && inc.Seq != current && ddl.CHTableName(inc.Key()) == ddl.CHTableName(key) &&
		(inc.Status == wire.TableStatusPurged || inc.Status == wire.TableStatusRefused)
}

// attributable reports whether every comment attributes its table to an
// incarnation of one of keys (the retired keys sharing one physical name).
func attributable(snap wire.TableRegistrySnapshot, keys, comments []string) bool {
	for _, comment := range comments {
		if !slices.ContainsFunc(keys, func(key string) bool { return ownerOf(snap, key, comment) != 0 }) {
			return false
		}
	}
	return true
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

// purge drops one Purging incarnation's tables and reports it. hasLocal is
// false when no local table of that name exists (the drop is then a Keeper
// check only).
func (r *Reconciler) purge(ctx context.Context, inc wire.TableIncarnation, hasLocal bool) {
	key := inc.Key()
	if slices.Contains(inc.PurgedBy, r.cfg.Pinned.NodeID) {
		r.setState(key, inc.Seq, StatePurgeReported)
		return
	}
	if !r.due(key) {
		return
	}
	if r.d.Quiescent != nil {
		quiet, err := r.d.Quiescent(key)
		if err != nil {
			r.fail(key, inc.Seq, StateWaitingQuiescence, err)
			return
		}
		if !quiet {
			r.setState(key, inc.Seq, StateWaitingQuiescence)
			return
		}
	}
	r.setState(key, inc.Seq, StatePurging)
	if err := ddl.DropTable(ctx, r.d.Conn, r.cfg.Pinned, key); err != nil {
		r.fail(key, inc.Seq, StatePurging, err)
		return
	}
	if hasLocal {
		r.d.Logger.Info("dropped purging table", "table", key, "incarnation", inc.Seq)
	}
	if err := r.notifyDropped(ctx, key); err != nil {
		r.fail(key, inc.Seq, StatePurging, err)
		return
	}
	if r.cfg.SweepDecommissioned {
		if err := r.sweep(ctx, key); err != nil {
			r.fail(key, inc.Seq, StatePurging, err)
			return
		}
	}
	if err := r.d.Arbiter.SubmitTablePurged(ctx, r.cfg.Pinned.NodeID, inc.Seq); err != nil {
		if status.Code(err) == codes.Unimplemented {
			err = fmt.Errorf("arbiter does not implement SubmitTablePurged yet: %w", err)
		}
		r.fail(key, inc.Seq, StatePurging, err)
		return
	}
	r.setState(key, inc.Seq, StatePurgeReported)
	r.d.Logger.Info("reported table purged", "table", key, "incarnation", inc.Seq, "node", r.cfg.Pinned.NodeID)
}

// sweep removes every replica under key's Keeper path that is neither this
// node nor in the arbiter's purge node set.
func (r *Reconciler) sweep(ctx context.Context, key string) error {
	replicas, err := ddl.KeeperReplicas(ctx, r.d.Conn, r.cfg.Pinned, key)
	if err != nil || len(replicas) == 0 {
		return err
	}
	nodes, err := r.d.Arbiter.PurgeNodeSet(ctx)
	if err != nil {
		return fmt.Errorf("tableset: purge node set: %w", err)
	}
	for _, replica := range replicas {
		if replica == r.cfg.Pinned.NodeID || slices.Contains(nodes, replica) {
			continue
		}
		if err := ddl.DropReplica(ctx, r.d.Conn, r.cfg.Pinned, key, replica); err != nil {
			return err
		}
		r.d.Logger.Warn("dropped decommissioned replica", "table", key, "replica", replica)
	}
	return nil
}

// clearDecommissioned runs before this node creates a chain incarnation's
// hg_unsafe table that it does not hold locally. A replica under the key's
// Keeper path that is neither this node nor in the arbiter's purge node set
// belongs to a decommissioned node, left behind when that node's eviction
// completed an earlier incarnation's purge after the source's sweep. Joining
// that path would fail with a different structure, or clone the evicted
// replica's parts into the new incarnation. The source SNode
// (SweepDecommissioned) removes such replicas (ClickHouse refuses an active
// one, which fails the step); every other node refuses the create until the
// source has removed them. An absent path lists no replica.
func (r *Reconciler) clearDecommissioned(ctx context.Context, key string) error {
	replicas, err := ddl.KeeperReplicas(ctx, r.d.Conn, r.cfg.Pinned, key)
	if err != nil {
		return err
	}
	var nodes, blocking []string
	fetched := false
	for _, replica := range replicas {
		if replica == r.cfg.Pinned.NodeID {
			continue
		}
		if !fetched {
			if nodes, err = r.d.Arbiter.PurgeNodeSet(ctx); err != nil {
				return fmt.Errorf("tableset: purge node set: %w", err)
			}
			fetched = true
		}
		if slices.Contains(nodes, replica) {
			continue
		}
		if !r.cfg.SweepDecommissioned {
			blocking = append(blocking, replica)
			continue
		}
		if err := ddl.DropReplica(ctx, r.d.Conn, r.cfg.Pinned, key, replica); err != nil {
			return err
		}
		r.d.Logger.Warn("dropped decommissioned replica before create", "table", key, "replica", replica)
	}
	if len(blocking) > 0 {
		return fmt.Errorf("tableset: keeper path of %s holds decommissioned replica(s) %v; not creating until the source SNode removes them", key, blocking)
	}
	return nil
}

// notifyDropped runs the Dropped hook for key. A failure is remembered, so
// ensurePresent retries it before key's tables are created again.
func (r *Reconciler) notifyDropped(ctx context.Context, key string) error {
	if r.d.Dropped == nil {
		return nil
	}
	err := r.d.Dropped(ctx, key)
	r.mu.Lock()
	if err != nil {
		r.owed[key] = true
	} else {
		delete(r.owed, key)
	}
	r.mu.Unlock()
	if err != nil {
		return fmt.Errorf("tableset: forget dropped table %s: %w", key, err)
	}
	return nil
}

func (r *Reconciler) isOwed(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.owed[key]
}

// sweepPurgedKeeperPaths catches a Keeper path that survived its purge
// because a node was evicted after this node's sweep: for every key whose
// live incarnation has no tables but whose path still exists, drop this
// node's own stranded replica and sweep the rest. skip names the physical
// tables it must leave alone: the desired ones and retired ones whose local
// tables carry an unattributable marker.
func (r *Reconciler) sweepPurgedKeeperPaths(ctx context.Context, history map[string][]string, skip map[string]bool) {
	paths, err := ddl.KeeperUnsafeTables(ctx, r.d.Conn, r.cfg.Pinned)
	if err != nil {
		r.d.Logger.Warn("list keeper table paths", "err", err)
		return
	}
	for _, physical := range paths {
		keys, ok := history[physical]
		if !ok || skip[physical] {
			continue
		}
		key := keys[0]
		if err := ddl.DropTable(ctx, r.d.Conn, r.cfg.Pinned, key); err != nil {
			r.d.Logger.Warn("drop stranded replica", "table", key, "err", err)
			continue
		}
		if err := r.sweep(ctx, key); err != nil {
			r.d.Logger.Warn("sweep surviving keeper path", "table", key, "err", err)
		}
	}
}

func (r *Reconciler) dropLeftover(ctx context.Context, key string) {
	if err := ddl.DropTable(ctx, r.d.Conn, r.cfg.Pinned, key); err != nil {
		r.d.Logger.Warn("drop leftover protocol tables", "table", key, "err", err)
		r.mu.Lock()
		r.failures[key]++
		r.mu.Unlock()
		return
	}
	r.countLeftover(key, "retired key")
	if err := r.notifyDropped(ctx, key); err != nil {
		r.d.Logger.Warn("forget dropped leftover", "table", key, "err", err)
		r.mu.Lock()
		r.failures[key]++
		r.mu.Unlock()
	}
}

func (r *Reconciler) countLeftover(key, why string) {
	r.mu.Lock()
	r.leftoverDrops++
	r.mu.Unlock()
	r.d.Logger.Warn("dropped leftover protocol tables", "table", key, "reason", why)
}

// reportUnknown logs and records the local protocol tables whose physical
// name accounted does not hold: neither desired nor an attributable leftover
// of a retired key. They are never dropped.
func (r *Reconciler) reportUnknown(ctx context.Context, accounted map[string]bool) error {
	local, err := ddl.ListProtocolTables(ctx, r.d.Conn, r.cfg.Pinned)
	if err != nil {
		return err
	}
	var unknown []string
	for _, lt := range local {
		if accounted[lt.Table] {
			continue
		}
		name := lt.Database + "." + lt.Table
		if !slices.Contains(unknown, name) {
			unknown = append(unknown, name)
		}
	}
	sort.Strings(unknown)
	r.mu.Lock()
	previous := r.unknown
	r.unknown = unknown
	r.mu.Unlock()
	if !slices.Equal(previous, unknown) && len(unknown) > 0 {
		r.d.Logger.Warn("unknown protocol tables (reported, never dropped)", "tables", unknown)
	}
	return nil
}

func (r *Reconciler) due(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	ts := r.tables[key]
	return ts == nil || ts.nextAttempt.IsZero() || !r.now().Before(ts.nextAttempt)
}

func (r *Reconciler) setState(key string, seq uint64, state State) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ts := r.tables[key]
	if ts == nil || ts.seq != seq {
		ts = &tableStatus{seq: seq}
		r.tables[key] = ts
	}
	ts.state = state
	// A non-failing transition ends any backoff: a nextAttempt left behind
	// once expired would pin NextDelay at 0 and spin the loop. Only a
	// converged table resets the consecutive-failure count.
	ts.nextAttempt = time.Time{}
	if state == StateReady || state == StatePurgeReported {
		ts.failures = 0
	}
}

// forget drops key's status.
func (r *Reconciler) forget(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.tables, key)
}

func (r *Reconciler) fail(key string, seq uint64, state State, err error) {
	r.mu.Lock()
	ts := r.tables[key]
	if ts == nil || ts.seq != seq {
		ts = &tableStatus{seq: seq}
		r.tables[key] = ts
	}
	ts.state = state
	ts.failures++
	ts.nextAttempt = r.now().Add(ddl.ReconcileBackoff(ts.failures, r.cfg.Interval))
	r.failures[key]++
	failures := ts.failures
	r.mu.Unlock()
	r.d.Logger.Warn("table reconcile failed", "table", key, "incarnation", seq, "state", state, "consecutive_failures", failures, "err", err)
}

// forgetAbsent drops the status of keys no longer in the registry snapshot.
func (r *Reconciler) forgetAbsent(seen map[string]bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key := range r.tables {
		if !seen[key] {
			delete(r.tables, key)
		}
	}
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}
