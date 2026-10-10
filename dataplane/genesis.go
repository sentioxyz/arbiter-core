package dataplane

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// GenesisSnapshotID derives the network's genesis snapshot id the way the
// arbiter does (arbiter cmd/arbiter/genesis.go loadGenesisManifest): the
// SnapshotID of the sealed empty block-0 snapshot over the genesis table set.
// Signed data-plane messages bind it (housegate spec 2026-10-10 §6.5). Only a
// node holding the network's whole genesis set (verifiers, the founding
// indexer's SNode) can derive it; any other node must be configured with it.
func GenesisSnapshotID(networkID, schemaSnapshotID, executorProfileID string, genesis []payloadexec.TableSchema) (string, error) {
	m, err := payloadexec.New(networkID, genesis...).GenesisSnapshot(0, schemaSnapshotID, executorProfileID)
	if err != nil {
		return "", fmt.Errorf("derive genesis snapshot id: %w", err)
	}
	return m.SnapshotID, nil
}
