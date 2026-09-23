package arbiter

import (
	"fmt"
	"regexp"
)

// Confirmation levels an L2 event must reach before the leader proposes it
// to the table registry (spec D6).
const (
	TableRegistryConfirmationFinalized = "finalized"
	TableRegistryConfirmationSafe      = "safe"
)

var databasesContractPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

// TableRegistryParams enables the dynamic SI table registry (spec D11). It is
// set once through a signed consensus update and never changed or removed.
type TableRegistryParams struct {
	ChainID           uint64 `json:"chain_id"`
	DatabasesContract string `json:"databases_contract"`
	SIIndexerID       uint64 `json:"si_indexer_id"`
	ActivationBlock   uint64 `json:"activation_block"`
	Confirmation      string `json:"confirmation"`
}

// Validate checks a normalized (lowercase contract) parameter set.
func (p TableRegistryParams) Validate() error {
	switch {
	case p.ChainID == 0:
		return fmt.Errorf("table registry: chain_id must be positive")
	case !databasesContractPattern.MatchString(p.DatabasesContract):
		return fmt.Errorf("table registry: databases_contract must be a lowercase 0x-prefixed 20-byte address")
	case p.ActivationBlock == 0:
		return fmt.Errorf("table registry: activation_block must be at least 1")
	case p.Confirmation != TableRegistryConfirmationFinalized && p.Confirmation != TableRegistryConfirmationSafe:
		return fmt.Errorf("table registry: confirmation must be %q or %q", TableRegistryConfirmationFinalized, TableRegistryConfirmationSafe)
	}
	return nil
}

// L2BlockRef names one L2 block.
type L2BlockRef struct {
	Number uint64 `json:"number"`
	Hash   string `json:"hash"`
}

// L2EventRef locates one contract log; (BlockNumber, LogIndex) orders events.
type L2EventRef struct {
	BlockNumber uint64 `json:"block_number"`
	BlockHash   string `json:"block_hash"`
	LogIndex    uint64 `json:"log_index"`
	TxHash      string `json:"tx_hash"`
}

// LegacyTable is a table active on the SI indexer before activation_block.
type LegacyTable struct {
	DatabaseID string     `json:"database_id"`
	TableID    string     `json:"table_id"`
	Created    L2EventRef `json:"created"`
}

// TableKey is the logical table id shared with StatementEnvelope.TargetTableID
// and replay.TableManifest.TableID.
func TableKey(databaseID, tableID string) string {
	return databaseID + "." + tableID
}
