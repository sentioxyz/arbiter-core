package ddl

import (
	"errors"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// DefaultReconcileMaxFailures bounds consecutive transient reconcile failures
// before the role gives up and exits.
const DefaultReconcileMaxFailures = 5

// reconcileBackoffMin is the first retry delay after a transient failure.
const reconcileBackoffMin = time.Second

// FatalReconcileError reports whether a reconcile error describes a deployment
// fact rather than a temporary failure. Retrying cannot repair these errors, so
// a role must fail closed when it sees one.
func FatalReconcileError(err error) bool {
	return errors.Is(err, ErrProtocolTableDrift) ||
		errors.Is(err, ErrProtocolTableMissing) ||
		errors.Is(err, ErrPhysicalTableNameCollision) ||
		errors.Is(err, ErrPartitionFreeze) ||
		errors.Is(err, payloadexec.ErrUnsupportedColumnType)
}

// UnservableTableError reports whether a reconcile error says that this
// binary cannot build the declaration's protocol DDL at all: the partition
// key or a column type is outside what it replays. It is a fact about one
// table and this binary, not about the node's ClickHouse, so a reconciler
// marks that table locally unservable (never Ready) and keeps serving the
// others (arbiter-core#76); a binary that can build it picks it up on a
// later pass. It is a subset of FatalReconcileError, which still decides for
// configured genesis tables and every other error.
func UnservableTableError(err error) bool {
	return errors.Is(err, ErrPartitionFreeze) || errors.Is(err, payloadexec.ErrUnsupportedColumnType)
}

// ReconcileBackoff returns the delay before retry number consecutiveFailures.
// It doubles from one second and is capped by the steady-state interval.
func ReconcileBackoff(consecutiveFailures int, interval time.Duration) time.Duration {
	if interval <= reconcileBackoffMin {
		return interval
	}
	if consecutiveFailures < 1 {
		consecutiveFailures = 1
	}
	backoff := reconcileBackoffMin
	for i := 1; i < consecutiveFailures; i++ {
		// Checking against half of the cap before doubling both applies the cap
		// and prevents time.Duration overflow for very large intervals.
		if backoff >= interval/2 {
			return interval
		}
		backoff *= 2
	}
	if backoff > interval {
		return interval
	}
	return backoff
}
