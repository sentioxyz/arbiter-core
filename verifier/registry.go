package verifier

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
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
	if r.tables.WaitReady(ctx, ids, r.cfg.AddTransitionReadyWait) {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fmt.Errorf("%w: block %d adds %v", ErrAddedTableNotReady, job.BlockSeq, ids)
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

// registrySchema resolves tableID from an enabled registry by the SNode's rule
// (snode.Role.schemaFor): the schema of the key's live incarnation when that
// is Active, Retiring or Purging (so a block admitted before a retirement is
// still scanned) — the configured schema for a genesis-origin incarnation,
// otherwise the decoded schema_json, which must hash to the incarnation's
// schema_hash under networkID. Anything else is refused; an enabled registry
// never falls back to the configured tables.
func registrySchema(networkID string, genesis []payloadexec.TableSchema, snap wire.TableRegistrySnapshot, tableID string) (payloadexec.TableSchema, error) {
	live := snap.Live(tableID)
	if live == nil {
		return payloadexec.TableSchema{}, fmt.Errorf("table %s is not in the table registry", tableID)
	}
	switch live.Status {
	case wire.TableStatusActive, wire.TableStatusRetiring, wire.TableStatusPurging:
	default:
		return payloadexec.TableSchema{}, fmt.Errorf("table %s is %s in the table registry", tableID, live.Status)
	}
	return incarnationSchema(networkID, genesis, *live)
}

// incarnationSchema mirrors snode.Role.incarnationSchema.
func incarnationSchema(networkID string, genesis []payloadexec.TableSchema, inc wire.TableIncarnation) (payloadexec.TableSchema, error) {
	if inc.Origin == wire.TableOriginGenesis {
		if t, ok := genesisSchema(genesis, inc.Key()); ok {
			return t, nil
		}
		return payloadexec.TableSchema{}, fmt.Errorf("genesis table %s is not configured", inc.Key())
	}
	schema, err := inc.Schema()
	if err != nil {
		return payloadexec.TableSchema{}, err
	}
	if got := payloadexec.TableSchemaHash(networkID, schema); got != inc.SchemaHash {
		return payloadexec.TableSchema{}, fmt.Errorf("table %s schema_json hashes to %s, registry records %s", inc.Key(), got, inc.SchemaHash)
	}
	return schema, nil
}
