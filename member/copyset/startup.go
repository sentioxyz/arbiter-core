package copyset

import (
	"errors"
	"fmt"
	"slices"

	"github.com/sentioxyz/arbiter-core/wire"
)

// ErrNodeIDInUse reports a member node id that an SNode or a registered node
// already uses.
var ErrNodeIDInUse = errors.New("copyset: node id already in use")

// sentinelError is an error with its own text that still matches a sentinel
// through errors.Is.
type sentinelError struct {
	msg      string
	sentinel error
}

func (e sentinelError) Error() string { return e.msg }
func (e sentinelError) Unwrap() error { return e.sentinel }

// CheckNodeIDUnused is the member's startup refusal of a node id in use (plan
// S3-C2 K29; housegate spec 2026-10-14 §6.1): nodeID must be no enrolled SI
// indexer's snode_node_id and not in purgeNodeSet, the arbiter's purge node
// set of every registered, non-evicted SNode and verifier. A member's id
// prefixes its scratch tables and becomes its VERIFIER id and Keeper replica
// name at P6, so it is never shared.
func CheckNodeIDUnused(snap wire.TableRegistrySnapshot, purgeNodeSet []string, nodeID string) error {
	if nodeID == "" {
		return errors.New("copyset: a node id is required")
	}
	for _, e := range snap.SIIndexers {
		if e.SNodeNodeID == nodeID {
			return inUse(nodeID, fmt.Sprintf("si_indexers entry %d (its SNode)", e.IndexerID))
		}
	}
	if slices.Contains(purgeNodeSet, nodeID) {
		return inUse(nodeID, "the purge node set (a registered SNode or verifier)")
	}
	return nil
}

func inUse(nodeID, by string) error {
	return sentinelError{msg: fmt.Sprintf("copyset: node id %q is already used by %s", nodeID, by), sentinel: ErrNodeIDInUse}
}
