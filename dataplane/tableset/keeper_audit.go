package tableset

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// DefaultKeeperAuditInterval paces the Keeper audit when
// Config.KeeperAuditInterval is 0.
const DefaultKeeperAuditInterval = 10 * time.Minute

// KeeperFindingReason is why the Keeper audit reports a path.
type KeeperFindingReason string

const (
	// KeeperFindingUnknownReplica: a replica on one of the owner's Keeper
	// paths that is not the owner, a node of the network pool, a current or
	// past member of the incarnation's group, or one of its purge members
	// (wire.TableRegistrySnapshot.KnownKeeperReplica over NetworkPool of the
	// purge node set): another owner's SNode and a registered member of
	// another group are reported too.
	KeeperFindingUnknownReplica KeeperFindingReason = "unknown_replica"
	// KeeperFindingNonIncarnationRoot: a replicated-table root under the
	// incarnation-scoped root that is not the path of a scoped_keeper_path
	// incarnation of its table (ddl.KeeperRoots.NonIncarnation, or a
	// canonical seq that names no such incarnation).
	KeeperFindingNonIncarnationRoot KeeperFindingReason = "non_incarnation_root"
)

// KeeperFinding is one report of the Keeper audit.
type KeeperFinding struct {
	Path    string
	Replica string // empty for a non-incarnation root
	Reason  KeeperFindingReason
}

// keeperFindingOf is a finding with the table key its log line names.
type keeperFindingOf struct {
	KeeperFinding
	table string
}

// auditKeeperIfDue runs the owner SNode's Keeper audit (housegate spec
// 2026-10-14 §8.2: the owner's sweep alerts on any replica on its paths that
// is not the owner, a pool verifier or a current or past member, and on any
// replicated-table root under the SI roots that is not an incarnation path)
// when Config.KeeperAuditInterval has passed since the last one that
// completed. It is report-only: it drops nothing, whatever it finds; the
// sweeps keep their own rules. Each finding is logged when it first appears;
// Stats carries the last audit's findings. A failed audit is logged and
// retried at the next pass.
func (r *Reconciler) auditKeeperIfDue(ctx context.Context, snap wire.TableRegistrySnapshot) {
	if !r.cfg.SweepDecommissioned {
		return
	}
	now := r.now()
	r.mu.Lock()
	due := r.lastKeeperAudit.IsZero() || !now.Before(r.lastKeeperAudit.Add(r.cfg.KeeperAuditInterval))
	r.mu.Unlock()
	if !due {
		return
	}
	found, err := r.auditKeeper(ctx, snap)
	if err != nil {
		r.d.Logger.Warn("keeper audit failed; the next pass retries it", "err", err)
		return
	}
	var findings []KeeperFinding
	for _, f := range found {
		findings = append(findings, f.KeeperFinding)
	}
	r.mu.Lock()
	previous := r.keeperFindings
	r.keeperFindings = findings
	r.keeperAuditRuns++
	r.lastKeeperAudit = now
	r.mu.Unlock()
	for _, f := range found {
		if slices.Contains(previous, f.KeeperFinding) {
			continue
		}
		switch f.Reason {
		case KeeperFindingUnknownReplica:
			r.d.Logger.Warn("keeper replica on an owner path is not the owner, a pool verifier or a current or past member",
				"table", f.table, "path", f.Path, "replica", f.Replica)
		case KeeperFindingNonIncarnationRoot:
			r.d.Logger.Warn("replicated-table root under the SI keeper roots is not an incarnation path", "path", f.Path)
		}
	}
}

// auditKeeper lists both SI Keeper roots of this node's shard and returns the
// findings, sorted by path then replica. This node's paths are the legacy
// path of every incarnation it owns that is not scoped (a legacy path is
// judged against every such incarnation of its physical name) and the
// incarnation path of every scoped incarnation it owns; another owner's
// paths are that owner's to audit. Every root under unsafe_inc that is no
// incarnation path is reported whoever owns its name.
func (r *Reconciler) auditKeeper(ctx context.Context, snap wire.TableRegistrySnapshot) ([]keeperFindingOf, error) {
	shard := r.cfg.Pinned.KeeperShardID
	roots, err := ddl.ListKeeperRoots(ctx, r.d.Conn, shard)
	if err != nil {
		return nil, err
	}
	legacyIncs := map[string][]wire.TableIncarnation{}
	for _, inc := range snap.Incarnations {
		if inc.ScopedKeeperPath || !r.owns(snap, inc) {
			continue
		}
		physical := ddl.CHTableName(inc.Key())
		legacyIncs[physical] = append(legacyIncs[physical], inc)
	}
	var pool []string
	fetched := false
	var out []keeperFindingOf
	check := func(key string, s ddl.KeeperScope, incs []wire.TableIncarnation) error {
		replicas, err := ddl.KeeperReplicasAt(ctx, r.d.Conn, shard, key, s)
		if err != nil {
			return err
		}
		for _, replica := range replicas {
			if replica == r.cfg.Pinned.NodeID {
				continue
			}
			if !fetched {
				nodes, err := r.d.Arbiter.PurgeNodeSet(ctx)
				if err != nil {
					return fmt.Errorf("tableset: purge node set: %w", err)
				}
				// The purge node set lists every registered member too;
				// only its network pool is known on every path.
				pool, fetched = snap.NetworkPool(nodes), true
			}
			if slices.ContainsFunc(incs, func(inc wire.TableIncarnation) bool { return snap.KnownKeeperReplica(inc, pool, replica) }) {
				continue
			}
			out = append(out, keeperFindingOf{
				KeeperFinding: KeeperFinding{Path: ddl.ZooKeeperPathFor(shard, key, s), Replica: replica, Reason: KeeperFindingUnknownReplica},
				table:         key,
			})
		}
		return nil
	}
	for _, physical := range roots.Legacy {
		incs := legacyIncs[physical]
		if len(incs) == 0 {
			continue
		}
		if err := check(incs[len(incs)-1].Key(), ddl.KeeperScope{}, incs); err != nil {
			return nil, err
		}
	}
	for _, path := range roots.Scoped {
		inc := incarnation(snap, path.IncarnationSeq)
		if inc == nil || !inc.ScopedKeeperPath || ddl.CHTableName(inc.Key()) != path.Table {
			out = append(out, keeperFindingOf{KeeperFinding: KeeperFinding{
				Path:   fmt.Sprintf("/sentio/%d/%s/%s/%d", shard, ddl.KeeperScopedUnsafeRoot, path.Table, path.IncarnationSeq),
				Reason: KeeperFindingNonIncarnationRoot,
			}})
			continue
		}
		if !r.owns(snap, *inc) {
			continue
		}
		if err := check(inc.Key(), KeeperScopeOf(*inc), []wire.TableIncarnation{*inc}); err != nil {
			return nil, err
		}
	}
	for _, path := range roots.NonIncarnation {
		out = append(out, keeperFindingOf{KeeperFinding: KeeperFinding{Path: path, Reason: KeeperFindingNonIncarnationRoot}})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Path != out[j].Path {
			return out[i].Path < out[j].Path
		}
		return out[i].Replica < out[j].Replica
	})
	return out, nil
}
