package consensus

import (
	"context"
	"errors"
	"fmt"
	"slices"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// purgeProbe is what the purge reports read of the member's ClickHouse as
// si_member: whether it still holds an incarnation's hg_member_unsafe table,
// and the replicas on the incarnation's own Keeper path.
type purgeProbe interface {
	MemberTableHeld(ctx context.Context, inc wire.TableIncarnation) (bool, error)
	KeeperReplicas(ctx context.Context, inc wire.TableIncarnation) ([]string, error)
}

// chPurgeProbe is the purgeProbe of a running member.
type chPurgeProbe struct {
	conn           clickhouse.Conn
	memberUnsafeDB string
	keeperShardID  uint32
}

// MemberTableHeld reports whether memberUnsafeDB holds inc's table: a table
// of its physical name whose COMMENT does not name another incarnation (the
// member DDL marks every hg_member_unsafe table with ddl.IncarnationComment;
// a table without a readable comment counts as held).
func (p chPurgeProbe) MemberTableHeld(ctx context.Context, inc wire.TableIncarnation) (bool, error) {
	name := ddl.CHTableName(inc.Key())
	rows, err := p.conn.Query(ctx, "SELECT comment FROM system.tables WHERE database = ? AND name = ?", p.memberUnsafeDB, name)
	if err != nil {
		return false, fmt.Errorf("member consensus: read system.tables for %s.%s: %w", p.memberUnsafeDB, name, err)
	}
	defer rows.Close()
	held := false
	for rows.Next() {
		var comment string
		if err := rows.Scan(&comment); err != nil {
			return false, fmt.Errorf("member consensus: scan system.tables for %s.%s: %w", p.memberUnsafeDB, name, err)
		}
		seq, ok := ddl.ParseIncarnationComment(comment)
		held = !ok || seq == inc.Seq
	}
	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("member consensus: read system.tables for %s.%s: %w", p.memberUnsafeDB, name, err)
	}
	return held, nil
}

// KeeperReplicas lists the replicas on inc's own Keeper path, unscoped or
// incarnation-scoped (tableset.KeeperScopeOf); none when it does not exist.
func (p chPurgeProbe) KeeperReplicas(ctx context.Context, inc wire.TableIncarnation) ([]string, error) {
	return ddl.KeeperReplicasAt(ctx, p.conn, p.keeperShardID, inc.Key(), tableset.KeeperScopeOf(inc))
}

// purgeReporter submits one signed purge report (dataplane.PurgeReporter).
type purgeReporter interface {
	SubmitTablePurged(ctx context.Context, nodeID string, incarnationSeq uint64) error
}

// signTablePurged signs a purge report as the verifier message table_purged
// under the member's ed25519 key, the key of its verifiers entry
// (authority.VerifierMessageTablePurged, the verifier's precedent).
func (m *Mode) signTablePurged(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error) {
	sig, err := authority.SignVerifierMessage(m.priv, authority.VerifierMessageTablePurged, m.msgCtx,
		authority.TablePurgedBody{NodeID: nodeID, IncarnationSeq: incarnationSeq})
	if err != nil {
		return wire.RecordTablePurged{}, err
	}
	return wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq, Ed25519Signature: sig}, nil
}

// purgeStep reports every Purging incarnation that lists the node in
// PurgeMembers and not yet in PurgedBy (spec §8.8; plan S3-E1 K60) once the
// member no longer holds its hg_member_unsafe table, its own replica is gone
// from the incarnation's Keeper path, and the quiescence ledger names no
// unresolved block of it. The reconciler process drops the table, as
// si_member_ddl, only once the table is quiet, so the order is drop, then
// report. An incarnation the member never held passes all three at once and
// is reported at the first step that sees it Purging. A report the leader
// answers "not purging yet", or any failed report, is retried at the next
// step.
func (m *Mode) purgeStep(ctx context.Context) {
	snap, enabled := m.d.Registry.View()
	if !enabled {
		return
	}
	var due []wire.TableIncarnation
	for _, inc := range snap.Incarnations {
		if inc.Status == wire.TableStatusPurging && slices.Contains(inc.PurgeMembers, m.cfg.NodeID) &&
			!slices.Contains(inc.PurgedBy, m.cfg.NodeID) && !m.purgeReported(inc.Seq) {
			due = append(due, inc)
		}
	}
	if len(due) == 0 {
		return
	}
	ledger, err := ReadQuiescence(m.cfg.StateDir)
	if err != nil {
		m.log.Warn("member consensus: the quiescence ledger is unreadable; no purge is reported until it is", "err", err)
		return
	}
	for _, inc := range due {
		if ctx.Err() != nil {
			return
		}
		held, err := m.env.probe.MemberTableHeld(ctx, inc)
		if err != nil {
			m.log.Warn("member consensus: purge check failed; the next step retries", "table", inc.Key(), "incarnation", inc.Seq, "err", err)
			continue
		}
		if held {
			continue
		}
		replicas, err := m.env.probe.KeeperReplicas(ctx, inc)
		if err != nil {
			m.log.Warn("member consensus: purge check failed; the next step retries", "table", inc.Key(), "incarnation", inc.Seq, "err", err)
			continue
		}
		if slices.Contains(replicas, m.cfg.NodeID) || !ledger.Quiet(inc.Key(), inc.Seq) {
			continue
		}
		switch err := m.env.reporter.SubmitTablePurged(ctx, m.cfg.NodeID, inc.Seq); {
		case err == nil:
			m.markPurgeReported(inc.Seq)
			m.metrics.purgeReports.Inc()
			m.log.Info("member consensus: reported a purged incarnation", "table", inc.Key(), "incarnation", inc.Seq)
		case errors.Is(err, dataplane.ErrTableNotPurging):
			m.log.Info("member consensus: the arbiter does not see the incarnation purging yet; retrying", "table", inc.Key(), "incarnation", inc.Seq)
		case ctx.Err() == nil:
			m.log.Warn("member consensus: purge report failed; the next step retries", "table", inc.Key(), "incarnation", inc.Seq, "err", err)
		}
	}
}

func (m *Mode) purgeReported(seq uint64) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.reported[seq]
}

func (m *Mode) markPurgeReported(seq uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.reported[seq] = true
}
