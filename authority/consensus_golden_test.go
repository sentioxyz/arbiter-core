package authority

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestConsensusParamsUpdateHashIsFrozen pins the canonical hash of an update
// signed before client lanes existed. A previously signed update must keep its
// digest forever: the FSM re-verifies every historical transition on restore.
// Never regenerate this value.
func TestConsensusParamsUpdateHashIsFrozen(t *testing.T) {
	update := arbiter.ConsensusParamsUpdate{
		NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", ExpectedEpoch: 1, PreviousParamsDigest: "0xdigest",
		AuthorityAddresses: []string{"0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705"}, MaxWriters: 1, ExpectedPromotionSeq: 3,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000D1",
			SIIndexerID: 0, ActivationBlock: 5508931, Confirmation: arbiter.TableRegistryConfirmationSafe},
	}
	got, err := ConsensusParamsUpdateHash(update)
	if err != nil {
		t.Fatal(err)
	}
	const want = "0x9a2297ef87ddfb00103d913d4488dc37dda8a535c772dac496820df4820d8bc6"
	if got != want {
		t.Fatalf("ConsensusParamsUpdateHash = %s, want %s", got, want)
	}
}

func TestNormalizeConsensusParamsUpdateClientLanes(t *testing.T) {
	base := arbiter.ConsensusParamsUpdate{NetworkID: "n", GenesisSnapshotID: "g", PreviousParamsDigest: "d",
		AuthorityAddresses: []string{"0x9ef3a259d1d87c864431cab5ed5f6578ad5ad705"}, MaxWriters: 1}
	withLanes := base
	withLanes.ClientLanes = &arbiter.ClientLaneParams{}
	if _, err := NormalizeConsensusParamsUpdate(withLanes); err == nil {
		t.Fatal("max_lanes_per_account 0 must be refused")
	}
	params := &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	withLanes.ClientLanes = params
	got, err := NormalizeConsensusParamsUpdate(withLanes)
	if err != nil || got.ClientLanes == params || *got.ClientLanes != *params {
		t.Fatalf("normalised lanes %+v (%v): want an equal, independent copy", got.ClientLanes, err)
	}
	a, err := ConsensusParamsUpdateHash(base)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ConsensusParamsUpdateHash(withLanes)
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatal("client_lanes must be bound by the update hash")
	}
}

// TestConsensusParamsUpdateWithLanesHashIsFrozen pins the digest of the update
// shape a network commits after client lanes (artifact disposition and lanes
// set, no signed-claims field). si_indexers and verifiers are omitted while
// absent, so adding them must leave it unchanged. Never regenerate this value.
func TestConsensusParamsUpdateWithLanesHashIsFrozen(t *testing.T) {
	update := arbiter.ConsensusParamsUpdate{
		NetworkID: "devnet2", GenesisSnapshotID: "0xgenesis", ExpectedEpoch: 2, PreviousParamsDigest: "0xdigest",
		AuthorityAddresses: []string{"0x9Ef3A259D1D87C864431CAb5Ed5F6578Ad5Ad705"}, MaxWriters: 1, ExpectedPromotionSeq: 3,
		ArtifactDispositionCapability: 1,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000D1",
			SIIndexerID: 0, ActivationBlock: 5508931, Confirmation: arbiter.TableRegistryConfirmationSafe},
		ClientLanes: &arbiter.ClientLaneParams{MaxLanesPerAccount: 256},
	}
	got, err := ConsensusParamsUpdateHash(update)
	if err != nil {
		t.Fatal(err)
	}
	const want = "0xebfe1e7715581d067ce52f54e561d7b401a7250084695d4b96432a94f70fdb42"
	if got != want {
		t.Fatalf("ConsensusParamsUpdateHash = %s, want %s", got, want)
	}
}
