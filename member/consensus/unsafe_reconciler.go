package consensus

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// codeReplicaAlreadyExists is ClickHouse's REPLICA_ALREADY_EXISTS.
const codeReplicaAlreadyExists = 253

// ErrReplicaAlreadyExists stops the reconciler for one table: a replica
// named after this member already exists at the table's Keeper path, and
// si_member_ddl holds no SYSTEM DROP REPLICA to remove it.
var ErrReplicaAlreadyExists = errors.New("member consensus: replica already exists")

// replicaExistsError is ErrReplicaAlreadyExists with its table, replica and
// path.
type replicaExistsError struct{ table, replica, path string }

func (e replicaExistsError) Error() string {
	return fmt.Sprintf("member consensus: hg_member_unsafe.%s: replica %s already exists at %s; not dropping it (si_member_ddl holds no SYSTEM DROP REPLICA); the owner's sweep removes it after an eviction", e.table, e.replica, e.path)
}

func (e replicaExistsError) Unwrap() error { return ErrReplicaAlreadyExists }

// ReconcilerDeps are the reconciler's collaborators.
type ReconcilerDeps struct {
	DDLConn  clickhouse.Conn /* si_member_ddl */
	Registry dataplane.RegistryView
	Arbiter  tableset.Arbiter /* PurgeNodeSet only */
	Logger   *slog.Logger
}

// ReconcilerStats is a point-in-time copy of the reconciler's metrics.
type ReconcilerStats struct {
	// Replicas counts this member's hg_member_unsafe replicas of known
	// incarnations after the last pass.
	Replicas int
	// ReplicaAlreadyExists counts the tables the reconciler stopped for.
	ReplicaAlreadyExists uint64
	// Failures counts failed creates and drops per table key.
	Failures map[string]uint64
}

// UnsafeReconciler creates and drops the member's hg_member_unsafe replicas
// for the groups it belongs to (spec §8.3, §8.8). It is the only code that
// runs as si_member_ddl, and it issues exactly CREATE DATABASE IF NOT
// EXISTS (no ENGINE), CREATE TABLE IF NOT EXISTS of ddl.MemberUnsafeIntent,
// DROP TABLE IF EXISTS … SYNC of its own replicas, and reads system.tables;
// never SYSTEM DROP REPLICA.
type UnsafeReconciler struct {
	cfg    ReconcilerConfig
	d      ReconcilerDeps
	log    *slog.Logger
	pinned ddl.MemberPinned

	passMu sync.Mutex

	mu       sync.Mutex
	replicas int
	exists   uint64
	failures map[string]uint64
	// stopped names, as "<key>@<seq>", every incarnation whose create met
	// REPLICA_ALREADY_EXISTS; it is not created again by this process.
	stopped map[string]bool
	// unattributed names the local tables already reported as not this
	// member's replica of a known incarnation.
	unattributed map[string]bool
}

// NewUnsafeReconciler validates cfg and d.
func NewUnsafeReconciler(cfg ReconcilerConfig, d ReconcilerDeps) (*UnsafeReconciler, error) {
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if d.DDLConn == nil || d.Registry == nil || d.Arbiter == nil {
		return nil, errors.New("member consensus: the reconciler needs its si_member_ddl connection, the registry and the arbiter")
	}
	log := d.Logger
	if log == nil {
		log = slog.Default()
	}
	return &UnsafeReconciler{
		cfg: cfg, d: d, log: log, pinned: cfg.pinned(),
		failures: map[string]uint64{}, stopped: map[string]bool{}, unattributed: map[string]bool{},
	}, nil
}

// Run reconciles at every registry change and every Interval until ctx
// ends; a failed pass is retried with ddl.ReconcileBackoff.
func (r *UnsafeReconciler) Run(ctx context.Context) error {
	failures := 0
	for {
		changed := r.d.Registry.Changed()
		err := r.reconcile(ctx)
		delay := r.cfg.Interval
		switch {
		case ctx.Err() != nil:
			return ctx.Err()
		case err != nil:
			failures++
			delay = ddl.ReconcileBackoff(failures, r.cfg.Interval)
			r.log.Warn("member hg_member_unsafe reconcile failed; retrying", "consecutive_failures", failures, "retry_in", delay, "err", err)
		default:
			failures = 0
		}
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		case <-changed:
			timer.Stop()
		}
	}
}

// Stats returns a copy of the reconciler's metrics.
func (r *UnsafeReconciler) Stats() ReconcilerStats {
	r.mu.Lock()
	defer r.mu.Unlock()
	return ReconcilerStats{Replicas: r.replicas, ReplicaAlreadyExists: r.exists, Failures: maps.Clone(r.failures)}
}

// localReplica is one table of hg_member_unsafe as si_member_ddl sees it in
// system.tables.
type localReplica struct {
	name, comment, engineFull string
}

// reconcile is one pass. A replica is wanted while the member is placed in
// the incarnation's group (a current member, or Leaving and not released)
// and the incarnation is Pending, Active or Retiring; it is created only
// while CreateReplicas is set, the member is in the purge node set, the
// purge switch is on and the incarnation is Pending or Active. Every other
// replica the member holds of a known incarnation (Purging, a released
// Leaving record, no longer placed, not physical) is dropped once its table
// is quiet. A table that is not this member's replica of a known incarnation
// at that incarnation's Keeper path is reported and never dropped.
func (r *UnsafeReconciler) reconcile(ctx context.Context) error {
	r.passMu.Lock()
	defer r.passMu.Unlock()
	snap, enabled := r.d.Registry.View()
	if !enabled {
		return nil
	}
	local, err := r.listLocal(ctx)
	if err != nil {
		return err
	}
	byName := make(map[string]localReplica, len(local))
	for _, l := range local {
		byName[l.name] = l
	}
	purgeOn := snap.Stage3 != nil && snap.Stage3.Purge
	createOK := false
	if r.cfg.CreateReplicas && purgeOn {
		nodes, err := r.d.Arbiter.PurgeNodeSet(ctx)
		if err != nil {
			return fmt.Errorf("member consensus: purge node set: %w", err)
		}
		createOK = slices.Contains(nodes, r.cfg.NodeID)
	}
	ledger, ledgerErr := ReadQuiescence(r.cfg.StateDir)

	var errs []error
	held := 0
	attributed := map[string]bool{}
	for _, inc := range snap.Incarnations {
		key := inc.Key()
		name := ddl.CHTableName(key)
		path := ddl.ZooKeeperPathFor(r.cfg.KeeperShardID, key, tableset.KeeperScopeOf(inc))
		placed := r.placed(snap, inc)
		l, present := byName[name]
		if !present {
			creatable := inc.Status == wire.TableStatusPending || inc.Status == wire.TableStatusActive
			if !createOK || !placed || !creatable || r.isStopped(key, inc.Seq) {
				continue
			}
			if err := r.create(ctx, inc, path); err != nil {
				r.fail(key)
				errs = append(errs, err)
				continue
			}
			byName[name] = localReplica{name: name, comment: ddl.IncarnationComment(inc.Seq), engineFull: engineHead(path, r.cfg.NodeID)}
			attributed[name] = true
			held++
			r.log.Info("member hg_member_unsafe replica created", "table", name, "incarnation", inc.Seq, "zookeeper_path", path)
			continue
		}
		if seq, ok := ddl.ParseIncarnationComment(l.comment); !ok || seq != inc.Seq {
			continue // another incarnation's table holds the name (or none's)
		}
		if !strings.HasPrefix(l.engineFull, engineHead(path, r.cfg.NodeID)) {
			continue // not at this incarnation's Keeper path: reported below
		}
		attributed[name] = true
		wanted := placed && (inc.Status == wire.TableStatusPending || inc.Status == wire.TableStatusActive || inc.Status == wire.TableStatusRetiring)
		switch {
		case wanted:
			held++
		case ledgerErr != nil:
			errs = append(errs, fmt.Errorf("member consensus: keep %s.%s: %w", r.pinned.MemberUnsafeDB, name, ledgerErr))
			held++
		case !ledger.Quiet(key, inc.Seq):
			r.log.Debug("member hg_member_unsafe replica waits for quiescence", "table", name, "incarnation", inc.Seq)
			held++
		default:
			if err := r.drop(ctx, name); err != nil {
				r.fail(key)
				errs = append(errs, err)
				held++
				continue
			}
			delete(byName, name)
			r.log.Info("member hg_member_unsafe replica dropped", "table", name, "incarnation", inc.Seq, "status", inc.Status, "placed", placed)
		}
	}
	for _, l := range local {
		if attributed[l.name] {
			continue
		}
		r.mu.Lock()
		reported := r.unattributed[l.name]
		r.unattributed[l.name] = true
		r.mu.Unlock()
		if !reported {
			r.log.Warn("hg_member_unsafe table is not this member's replica of a known incarnation at its Keeper path; leaving it for an operator",
				"table", l.name, "comment", l.comment, "engine_full", l.engineFull)
		}
	}
	r.mu.Lock()
	r.replicas = held
	r.mu.Unlock()
	return errors.Join(errs...)
}

// placed reports whether the member is in inc's group: a current member, or
// Leaving and not released.
func (r *UnsafeReconciler) placed(snap wire.TableRegistrySnapshot, inc wire.TableIncarnation) bool {
	g, ok := snap.Group(snap.Owner(inc), inc.DatabaseID)
	if !ok {
		return false
	}
	for _, m := range g.Members {
		if m.NodeID == r.cfg.NodeID {
			return true
		}
	}
	for _, l := range g.Leaving {
		if l.NodeID == r.cfg.NodeID && !l.Released {
			return true
		}
	}
	return false
}

// create makes the replica of inc at path. REPLICA_ALREADY_EXISTS stops
// the incarnation for this process (ErrReplicaAlreadyExists): the replica
// named after this member is never dropped from here.
func (r *UnsafeReconciler) create(ctx context.Context, inc wire.TableIncarnation, path string) error {
	key := inc.Key()
	schema, err := dataplane.IncarnationSchema(r.cfg.NetworkID, r.cfg.Tables, inc)
	if err != nil {
		return fmt.Errorf("member consensus: %s: %w", key, err)
	}
	intent, err := ddl.MemberUnsafeIntent(r.pinned, schema, inc.Seq, path)
	if err != nil {
		return fmt.Errorf("member consensus: %s: %w", key, err)
	}
	if err := r.d.DDLConn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(r.pinned.MemberUnsafeDB)); err != nil {
		return fmt.Errorf("member consensus: create database %s: %w", r.pinned.MemberUnsafeDB, err)
	}
	if err := r.d.DDLConn.Exec(ctx, intent.SQL()); err != nil {
		var ex *clickhouse.Exception
		if errors.As(err, &ex) && ex.Code == codeReplicaAlreadyExists {
			stop := replicaExistsError{table: intent.Table, replica: r.cfg.NodeID, path: path}
			r.mu.Lock()
			r.stopped[stopKey(key, inc.Seq)] = true
			r.exists++
			r.mu.Unlock()
			r.log.Error(stop.Error(), "table", intent.Table, "incarnation", inc.Seq, "zookeeper_path", path)
			return stop
		}
		return fmt.Errorf("member consensus: create %s.%s: %w", intent.Database, intent.Table, err)
	}
	return nil
}

// drop removes one of the member's own replicas: DROP TABLE … SYNC removes
// the local table and this member's replica from Keeper.
func (r *UnsafeReconciler) drop(ctx context.Context, name string) error {
	q := fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC", quoteIdent(r.pinned.MemberUnsafeDB), quoteIdent(name))
	if err := r.d.DDLConn.Exec(ctx, q); err != nil {
		return fmt.Errorf("member consensus: drop %s.%s: %w", r.pinned.MemberUnsafeDB, name, err)
	}
	return nil
}

// listLocal reads hg_member_unsafe's tables from system.tables, the one
// system table si_member_ddl can read.
func (r *UnsafeReconciler) listLocal(ctx context.Context) ([]localReplica, error) {
	rows, err := r.d.DDLConn.Query(ctx, `SELECT name, comment, engine_full FROM system.tables WHERE database = ? ORDER BY name`, r.pinned.MemberUnsafeDB)
	if err != nil {
		return nil, fmt.Errorf("member consensus: list %s: %w", r.pinned.MemberUnsafeDB, err)
	}
	defer rows.Close()
	var out []localReplica
	for rows.Next() {
		var l localReplica
		if err := rows.Scan(&l.name, &l.comment, &l.engineFull); err != nil {
			return nil, fmt.Errorf("member consensus: list %s: %w", r.pinned.MemberUnsafeDB, err)
		}
		out = append(out, l)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("member consensus: list %s: %w", r.pinned.MemberUnsafeDB, err)
	}
	return out, nil
}

func (r *UnsafeReconciler) isStopped(key string, seq uint64) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.stopped[stopKey(key, seq)]
}

func (r *UnsafeReconciler) fail(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failures[key]++
}

func stopKey(key string, seq uint64) string { return fmt.Sprintf("%s@%d", key, seq) }

// engineHead is how system.tables.engine_full starts for a replica at path
// named replica: ClickHouse renders the two engine arguments as quoted
// literals.
func engineHead(path, replica string) string {
	return "ReplicatedMergeTree(" + quoteLiteral(path) + ", " + quoteLiteral(replica) + ")"
}

func quoteIdent(id string) string {
	return "`" + strings.NewReplacer(`\`, `\\`, "`", "``").Replace(id) + "`"
}

func quoteLiteral(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}
