package verifier

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
)

// DefaultAddTransitionReadyWait bounds how long a replay job whose table-set
// transition adds tables waits for this verifier's reconciler to create them.
// The arbiter re-sends an unattested job to every connected verifier of the
// block every dispatch.retry_interval (default 5s) until quorum, so a refusal
// only delays this verifier's vote to a later redelivery.
const DefaultAddTransitionReadyWait = 10 * time.Second

// ErrAddedTableNotReady refuses to attest a transition that adds a table
// whose hg_* tables this verifier has not created and verified.
var ErrAddedTableNotReady = errors.New("verifier: added table is not ready")

// requireAddedTablesReady is the attestation gate: a transition that adds
// tables is attested only once every added table is Ready here, so an add
// cannot reach quorum before verifiers hold its tables.
func (r *Role) requireAddedTablesReady(ctx context.Context, job replay.ReplayJob) error {
	if job.TableSetTransition == nil || len(job.TableSetTransition.Adds) == 0 {
		return nil
	}
	ids := make([]string, 0, len(job.TableSetTransition.Adds))
	missing := false
	for _, add := range job.TableSetTransition.Adds {
		ids = append(ids, add.TableID)
		if !r.tables.Ready(add.TableID) {
			missing = true
		}
	}
	if !missing {
		return nil
	}
	// Without an enabled registry no chain table is ever reconciled, so
	// waiting could only block the subscription (and overflow its stream
	// buffer) for the whole bound on every redelivery: refuse at once.
	if !r.followsEnabledRegistry() {
		return fmt.Errorf("%w: block %d adds %v, and this verifier follows no enabled table registry", ErrAddedTableNotReady, job.BlockSeq, ids)
	}
	if r.tables.WaitReady(ctx, ids, r.cfg.AddTransitionReadyWait) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: block %d adds %v", ErrAddedTableNotReady, job.BlockSeq, ids)
}

// followsEnabledRegistry reports whether this verifier reconciles the table
// registry's chain tables: it follows a registry and that registry is enabled.
func (r *Role) followsEnabledRegistry() bool {
	if r.tables == nil || r.d.Registry == nil {
		return false
	}
	_, enabled := r.d.Registry.View()
	return enabled
}

// requireGenesisReadSet keeps the snapshot-query lane on the static genesis
// table set: its dynamic appender is not built, so a read of any other table
// is refused before any historical read.
func (r *Role) requireGenesisReadSet(job replay.SnapshotQueryJob) error {
	for _, table := range job.Statement.Envelope.Input.ReadSet.Tables {
		if _, ok := genesisSchema(r.cfg.Tables, table.TableID); !ok {
			return fmt.Errorf("snapshot query reads %s, which is outside the genesis table set; the snapshot-query lane keeps a static table set", table.TableID)
		}
	}
	return nil
}

// TableReady reports whether this verifier's hg_* tables for tableID's
// current incarnation exist and verify.
func (r *Role) TableReady(tableID string) bool { return r.tables.Ready(tableID) }

// TableSetStats returns the reconciler's metrics for the host to export.
func (r *Role) TableSetStats() tableset.Stats {
	if r.tables == nil {
		return tableset.Stats{}
	}
	return r.tables.Stats()
}

func genesisSchema(tables []payloadexec.TableSchema, tableID string) (payloadexec.TableSchema, bool) {
	for _, t := range tables {
		if t.TableID == tableID {
			return t, true
		}
	}
	return payloadexec.TableSchema{}, false
}

// checkScannerRegistry refuses a *CHScanner whose registry view is not the
// role's: a scanner built by NewScanner under a followed registry would scan
// a recreated genesis key with its stale genesis schema, and a scanner that
// follows a registry the role does not follow would resolve tables the role
// never reconciles. Other scanner implementations are the host's
// responsibility.
func checkScannerRegistry(s scanner, registry dataplane.RegistryView) error {
	ch, ok := s.(*CHScanner)
	if !ok || ch == nil {
		return nil
	}
	switch {
	case registry != nil && ch.registry == nil:
		return errors.New("verifier: Deps.Registry is set but Deps.Scanner follows no table registry; build it with NewRegistryScanner(cfg, conn, registry)")
	case registry == nil && ch.registry != nil:
		return errors.New("verifier: Deps.Scanner follows a table registry but Deps.Registry is nil")
	case registry != nil && !sameRegistryView(ch.registry, registry):
		return errors.New("verifier: Deps.Scanner follows a different table registry than Deps.Registry")
	}
	return nil
}

// sameRegistryView reports whether a and b are the same view. A dynamic type
// that is not comparable is never the same.
func sameRegistryView(a, b dataplane.RegistryView) (same bool) {
	defer func() {
		if recover() != nil {
			same = false
		}
	}()
	return a == b
}
