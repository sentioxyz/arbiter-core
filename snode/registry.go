package snode

import (
	"bytes"
	"errors"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// ErrTableNotReady refuses a fresh intake whose target table is not yet
// admissible on this source: the registry has not made it Active, or the
// reconciler has not created and verified its local tables. It is raised
// before any journal record or ClickHouse write, and a retry can succeed.
var ErrTableNotReady = errors.New("snode: target table is not ready on this source")

// registryView returns the followed registry snapshot; enabled is false when
// the role follows no registry or the registry is disabled.
func (r *Role) registryView() (wire.TableRegistrySnapshot, bool) {
	if r.d.Registry == nil {
		return wire.TableRegistrySnapshot{}, false
	}
	return r.d.Registry.View()
}

func (r *Role) genesisSchema(tableID string) (payloadexec.TableSchema, bool) {
	for _, t := range r.cfg.Tables {
		if t.TableID == tableID {
			return t, true
		}
	}
	return payloadexec.TableSchema{}, false
}

// schemaFor resolves tableID for promotion, cleanup and recorded intake.
// Without an enabled registry it is the configured genesis table. With one it
// is the schema of the key's live storage-integrity incarnation when that is
// Active, Retiring or Purging (so a statement admitted before a retirement
// still converges and promotes): the configured schema for a genesis-origin
// incarnation, the decoded schema_json otherwise.
func (r *Role) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	snap, enabled := r.registryView()
	if !enabled {
		if t, ok := r.genesisSchema(tableID); ok {
			return t, nil
		}
		return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
	}
	live := snap.Live(tableID)
	if live == nil {
		return payloadexec.TableSchema{}, fmt.Errorf("table %s is not in the table registry", tableID)
	}
	switch live.Status {
	case wire.TableStatusActive, wire.TableStatusRetiring, wire.TableStatusPurging:
	default:
		return payloadexec.TableSchema{}, fmt.Errorf("table %s is %s in the table registry", tableID, live.Status)
	}
	return r.incarnationSchema(*live)
}

func (r *Role) incarnationSchema(inc wire.TableIncarnation) (payloadexec.TableSchema, error) {
	if inc.Origin == wire.TableOriginGenesis {
		if t, ok := r.genesisSchema(inc.Key()); ok {
			return t, nil
		}
		return payloadexec.TableSchema{}, fmt.Errorf("genesis table %s is not configured", inc.Key())
	}
	schema, err := inc.Schema()
	if err != nil {
		return payloadexec.TableSchema{}, err
	}
	if got := payloadexec.TableSchemaHash(r.cfg.NetworkID, schema); got != inc.SchemaHash {
		return payloadexec.TableSchema{}, fmt.Errorf("table %s schema_json hashes to %s, registry records %s", inc.Key(), got, inc.SchemaHash)
	}
	return schema, nil
}

// requireAdmissible is the fresh-intake gate (defence in depth: HouseGate
// refuses first). With an enabled registry the target must be Active, the
// envelope must sign the registry's schema hash, and the reconciler must
// report the table Ready.
func (r *Role) requireAdmissible(tableID, schemaHash string) error {
	snap, enabled := r.registryView()
	if !enabled {
		return nil
	}
	live := snap.Live(tableID)
	switch {
	case live == nil:
		return fmt.Errorf("table %s is not in the table registry: %w", tableID, ErrSchemaUnknown)
	case live.Status == wire.TableStatusPending:
		return fmt.Errorf("table %s is pending in the table registry: %w", tableID, ErrTableNotReady)
	case live.Status != wire.TableStatusActive:
		return fmt.Errorf("table %s is %s in the table registry: %w", tableID, live.Status, ErrSchemaUnknown)
	case live.SchemaHash != schemaHash:
		return fmt.Errorf("table %s schema_hash %q, registry has %q: %w", tableID, schemaHash, live.SchemaHash, ErrSchemaHashMismatch)
	case !r.tables.Ready(tableID):
		return fmt.Errorf("table %s protocol tables are not ready: %w", tableID, ErrTableNotReady)
	}
	return nil
}

// TableReady reports whether this SNode's hg_* tables for tableID's current
// incarnation exist and verify (tableset.Reconciler.Ready). An embedding host
// reports a table Active only when the registry says Active and this is true.
func (r *Role) TableReady(tableID string) bool { return r.tables.Ready(tableID) }

// TableSetStats returns the reconciler's metrics for the host to export.
func (r *Role) TableSetStats() tableset.Stats {
	if r.tables == nil {
		return tableset.Stats{}
	}
	return r.tables.Stats()
}

// tableQuiescent is the purge gate: no promotion intent, no promoted part
// awaiting cleanup, no unpromoted rows and no unfinished intake still
// reference tableID.
func (r *Role) tableQuiescent(tableID string) (bool, error) {
	quiet, err := r.state.quiescent(tableID)
	if err != nil || !quiet {
		return false, err
	}
	records, err := r.journal.list()
	if err != nil {
		return false, err
	}
	for _, rec := range records {
		if rec.Envelope.TargetTableID != tableID {
			continue
		}
		switch rec.Lifecycle {
		case LifecyclePreparing, LifecycleAbortPending, LifecycleUnsafeWritten:
			return false, nil
		}
	}
	return true, nil
}

func (st *stateStore) quiescent(table string) (bool, error) {
	st.mu.Lock()
	defer st.mu.Unlock()
	prefix := table + "\x00"
	zero := lthash.New().Bytes()
	for ks := range st.s.PromotionIntents {
		if strings.HasPrefix(ks, prefix) {
			return false, nil
		}
	}
	for ks, parts := range st.s.PromotedUnsafeParts {
		if strings.HasPrefix(ks, prefix) && len(parts) > 0 {
			return false, nil
		}
	}
	for ks, sum := range st.s.UnpromotedSums {
		if !strings.HasPrefix(ks, prefix) {
			continue
		}
		acc, err := parseAccumulatorHex(sum)
		if err != nil {
			return false, err
		}
		if !bytes.Equal(acc.Bytes(), zero) {
			return false, nil
		}
	}
	return true, nil
}
