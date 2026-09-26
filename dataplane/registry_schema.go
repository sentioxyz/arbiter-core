package dataplane

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/wire"
)

// RegistrySchema resolves tableID from an enabled table registry: the schema
// of the key's live incarnation when that is Active, Retiring or Purging (so
// work admitted before a retirement still converges, promotes and scans). A
// genesis-origin incarnation uses the configured genesis schema of its key; a
// chain-origin one its decoded schema_json, which must hash to the
// incarnation's schema_hash under networkID. Anything else is refused: an
// enabled registry never falls back to the configured tables. The SNode and
// the verifier share this rule; each keeps its own registry-disabled branch.
func RegistrySchema(networkID string, genesis []payloadexec.TableSchema, snap wire.TableRegistrySnapshot, tableID string) (payloadexec.TableSchema, error) {
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

func incarnationSchema(networkID string, genesis []payloadexec.TableSchema, inc wire.TableIncarnation) (payloadexec.TableSchema, error) {
	if inc.Origin == wire.TableOriginGenesis {
		for _, t := range genesis {
			if t.TableID == inc.Key() {
				return t, nil
			}
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
