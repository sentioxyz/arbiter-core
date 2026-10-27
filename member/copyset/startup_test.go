package copyset

import (
	"errors"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestCheckNodeIDUnusedRefusesAnSNodeOrVerifierID: a member id is never an
// enrolled indexer's SNode id nor in the purge node set (every registered,
// non-evicted SNode and verifier), because it becomes the member's VERIFIER id
// and Keeper replica name at P6 (plan S3-C2 K29; housegate spec 2026-10-14
// §6.1).
func TestCheckNodeIDUnusedRefusesAnSNodeOrVerifierID(t *testing.T) {
	snap := snapshot()
	snap.SIIndexers = append(snap.SIIndexers, arbiter.SIIndexerEntry{IndexerID: 7, SNodeNodeID: "snode-9"})
	purge := []string{"snode-1", "snode-2", "snode-3", "verifier-1", "verifier-2", "verifier-3"}
	for _, id := range []string{"member-a", "member-b", "member-c"} {
		if err := CheckNodeIDUnused(snap, purge, id); err != nil {
			t.Fatalf("CheckNodeIDUnused(%s) = %v", id, err)
		}
	}
	for _, tc := range []struct{ id, want string }{
		{"snode-2", `copyset: node id "snode-2" is already used by si_indexers entry 1 (its SNode)`},
		{"snode-9", `copyset: node id "snode-9" is already used by si_indexers entry 7 (its SNode)`},
		{"verifier-2", `copyset: node id "verifier-2" is already used by the purge node set (a registered SNode or verifier)`},
	} {
		err := CheckNodeIDUnused(snap, purge, tc.id)
		if err == nil || err.Error() != tc.want || !errors.Is(err, ErrNodeIDInUse) {
			t.Fatalf("CheckNodeIDUnused(%s) = %v, want %q wrapping ErrNodeIDInUse", tc.id, err, tc.want)
		}
	}
	if err := CheckNodeIDUnused(snap, purge, ""); err == nil || err.Error() != "copyset: a node id is required" || errors.Is(err, ErrNodeIDInUse) {
		t.Fatalf("CheckNodeIDUnused(\"\") = %v", err)
	}
}
