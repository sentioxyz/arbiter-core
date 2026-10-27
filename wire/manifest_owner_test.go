package wire

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// manifestOwnerSnapshot is an enabled registry founded by indexer 1 with
// indexers 0, 1 and 2 enrolled.
func manifestOwnerSnapshot(incs ...TableIncarnation) TableRegistrySnapshot {
	for i := range incs {
		incs[i].Seq = uint64(i) + 1
	}
	return TableRegistrySnapshot{
		Params:       arbiter.TableRegistryParams{ChainID: 7, SIIndexerID: 1, ActivationBlock: 100, Confirmation: arbiter.TableRegistryConfirmationSafe},
		Version:      3,
		Seeded:       true,
		SIIndexers:   []arbiter.SIIndexerEntry{{IndexerID: 0}, {IndexerID: 1}, {IndexerID: 2}},
		Incarnations: incs,
	}
}

func ownedBy(id uint64) *uint64 { return &id }

func requireManifestOwner(t *testing.T, s TableRegistrySnapshot, key string, wantOwner uint64, wantOK bool) {
	t.Helper()
	got, ok := s.ManifestOwner(key)
	if got != wantOwner || ok != wantOK {
		t.Fatalf("ManifestOwner(%q) = (%d, %v), want (%d, %v)", key, got, ok, wantOwner, wantOK)
	}
}

// TestManifestOwnerIsTheNewestNonRefusedIncarnationThatEnteredATableSet: a
// re-created table is attributed to the incarnation whose add transition was
// sealed last; a Retiring incarnation, still in the manifests until its retire
// transition, keeps its owner; a key the registry never had has none.
func TestManifestOwnerIsTheNewestNonRefusedIncarnationThatEnteredATableSet(t *testing.T) {
	s := manifestOwnerSnapshot(
		TableIncarnation{DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusPurged, AddBlockSeq: 4, RetireBlockSeq: 6, OwnerIndexerID: ownedBy(0)},
		TableIncarnation{DatabaseID: "db", TableID: "r", Origin: TableOriginChain, Status: TableStatusRetiring, AddBlockSeq: 5, OwnerIndexerID: ownedBy(2)},
		TableIncarnation{DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusActive, AddBlockSeq: 9, OwnerIndexerID: ownedBy(2)},
	)
	requireManifestOwner(t, s, "db.t", 2, true)
	requireManifestOwner(t, s, "db.r", 2, true)
	requireManifestOwner(t, s, "db.unknown", 0, false)
}

// TestManifestOwnerOfAPreSignedClaimsIncarnationIsTheFoundingIndexer: an
// incarnation recorded before the signed-claims activation names no owner and
// belongs to Params.SIIndexerID, a genesis one included; a recorded owner 0 is
// indexer 0, not the founding indexer.
func TestManifestOwnerOfAPreSignedClaimsIncarnationIsTheFoundingIndexer(t *testing.T) {
	s := manifestOwnerSnapshot(
		TableIncarnation{DatabaseID: "db", TableID: "g", Origin: TableOriginGenesis, Status: TableStatusActive},
		TableIncarnation{DatabaseID: "db", TableID: "old", Origin: TableOriginChain, Status: TableStatusActive, AddBlockSeq: 3},
		TableIncarnation{DatabaseID: "db", TableID: "zero", Origin: TableOriginChain, Status: TableStatusActive, AddBlockSeq: 8, OwnerIndexerID: ownedBy(0)},
	)
	requireManifestOwner(t, s, "db.g", 1, true)
	requireManifestOwner(t, s, "db.old", 1, true)
	requireManifestOwner(t, s, "db.zero", 0, true)
}

// TestManifestOwnerIgnoresAPendingIncarnationNeverAdded: a Pending
// re-creation by another indexer does not take the key over before its add
// transition is sealed, and a key whose only incarnation is Pending is in no
// manifest.
func TestManifestOwnerIgnoresAPendingIncarnationNeverAdded(t *testing.T) {
	s := manifestOwnerSnapshot(
		TableIncarnation{DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusRetiring, AddBlockSeq: 4, OwnerIndexerID: ownedBy(0)},
		TableIncarnation{DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusPending, OwnerIndexerID: ownedBy(2)},
		TableIncarnation{DatabaseID: "db", TableID: "new", Origin: TableOriginChain, Status: TableStatusPending, OwnerIndexerID: ownedBy(2)},
	)
	requireManifestOwner(t, s, "db.t", 0, true)
	requireManifestOwner(t, s, "db.new", 0, false)
}

// TestManifestOwnerSkipsARefusedRecreation: a Refused re-creation never
// enters a table set; the rule skips Refused before it looks at the origin or
// the add block, as the FSM's does, so even a Refused incarnation of genesis
// origin or with an add block is not the owner.
func TestManifestOwnerSkipsARefusedRecreation(t *testing.T) {
	s := manifestOwnerSnapshot(
		TableIncarnation{DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusActive, AddBlockSeq: 4, OwnerIndexerID: ownedBy(0)},
		TableIncarnation{DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusRefused, RefusedCode: "column_type", OwnerIndexerID: ownedBy(2)},
		TableIncarnation{DatabaseID: "db", TableID: "t", Origin: TableOriginChain, Status: TableStatusRefused, AddBlockSeq: 12, OwnerIndexerID: ownedBy(2)},
		TableIncarnation{DatabaseID: "db", TableID: "t", Origin: TableOriginGenesis, Status: TableStatusRefused},
	)
	requireManifestOwner(t, s, "db.t", 0, true)
}
