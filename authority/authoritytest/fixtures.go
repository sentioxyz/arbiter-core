// Package authoritytest holds deterministic signed-claims fixtures for the
// multi-source storage-integrity network (housegate spec 2026-10-10 §6.2,
// §6.5): throwaway keys (never provision them), one message context, one
// issue time, one body per message kind, and the exact hashes, tokens and
// signatures arbiter-core produces for them. arbiter's FSM and server tests
// import it so both repositories sign and verify the same bytes. The vector
// constants are consensus constants: never regenerate them.
package authoritytest

import (
	"crypto/ed25519"
	"fmt"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
)

const (
	// NetworkID and GenesisSnapshotID form the fixture MessageContext.
	NetworkID         = "devnet2"
	GenesisSnapshotID = "0xgenesis"
	// Iat is every fixture token's issue time (2025-10-10T00:00:00Z).
	Iat int64 = 1760054400
	// RegistrationSeq is Iat in milliseconds: the clock floor a data-plane
	// node reserves when its clock reads Iat.
	RegistrationSeq uint64 = 1760054400000
	// ActivationBlock0 is the founding indexer's activation block.
	ActivationBlock0 uint64 = 5508931

	AuthorityKeyHex = "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"
	AuthorityAddr   = "0x970e8128ab834e8eac17ab8e3812f010678cf791"
	IndexerKeyHex0  = "0000000000000000000000000000000000000000000000000000000000000a11"
	IndexerAddr0    = "0x563bd9e11d18b6ea60c2f159f8d3062d30e8039e"
	IndexerKeyHex1  = "0000000000000000000000000000000000000000000000000000000000000b22"
	IndexerAddr1    = "0x89fd7c610ac4aa2e17c3b15a2c386a4b215f96d9"
	StrangerKeyHex  = "0000000000000000000000000000000000000000000000000000000000000c33"
	StrangerAddr    = "0x009cfebab1cc20d08e23eb7ea89dcefab3349e45"

	// SNodeNodeID0 and SNodeNodeID1 are the SNodes of indexers 0 and 1.
	SNodeNodeID0 = "snode-1"
	SNodeNodeID1 = "snode-2"
)

// MustSigner loads a fixture key.
func MustSigner(t testing.TB, keyHex string) *authority.Signer {
	t.Helper()
	s, err := authority.NewSignerFromHex(keyHex)
	if err != nil {
		t.Fatalf("authoritytest: load key: %v", err)
	}
	return s
}

// Context is the fixture message context.
func Context() authority.MessageContext {
	return authority.MessageContext{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID}
}

// ConsensusContext is the fixture authority context at epoch.
func ConsensusContext(epoch uint64) authority.ConsensusContext {
	return authority.ConsensusContext{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, AuthorityEpoch: epoch}
}

// VerifierNodeID names fixture verifier i (1-based).
func VerifierNodeID(i int) string { return fmt.Sprintf("verifier-%d", i) }

// VerifierKey is fixture verifier i's ed25519 key: seed byte 0 is 6+i.
func VerifierKey(i int) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = byte(6 + i)
	return ed25519.NewKeyFromSeed(seed)
}

// VerifierEntries lists verifiers 1-3, sorted by node id.
func VerifierEntries() []arbiter.VerifierEntry {
	out := make([]arbiter.VerifierEntry, 0, 3)
	for i := 1; i <= 3; i++ {
		out = append(out, arbiter.VerifierEntry{NodeID: VerifierNodeID(i), Ed25519Pubkey: VerifierKey(i).Public().(ed25519.PublicKey)})
	}
	return out
}

// SIIndexerEntry0 is the founding indexer restated as si_indexers entry 0.
func SIIndexerEntry0() arbiter.SIIndexerEntry {
	return arbiter.SIIndexerEntry{IndexerID: 0, ActivationBlock: ActivationBlock0, Signer: IndexerAddr0,
		SNodeNodeID: SNodeNodeID0, EnrollmentJWS: SNodeEnrollmentVectors[0].JWS}
}

// SIIndexerEntry1 enrols indexer 1 at activationBlock (its enrolment
// statement does not cover the block).
func SIIndexerEntry1(activationBlock uint64) arbiter.SIIndexerEntry {
	return arbiter.SIIndexerEntry{IndexerID: 1, ActivationBlock: activationBlock, Signer: IndexerAddr1,
		SNodeNodeID: SNodeNodeID1, EnrollmentJWS: SNodeEnrollmentVectors[1].JWS}
}

// ActivationUpdate is a signed-claims activation update in normalized form.
func ActivationUpdate() arbiter.ConsensusParamsUpdate {
	return arbiter.ConsensusParamsUpdate{
		NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, ExpectedEpoch: 2, PreviousParamsDigest: "0xdigest",
		AuthorityAddresses: []string{AuthorityAddr}, MaxWriters: 1, ExpectedPromotionSeq: 3,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1",
			SIIndexerID: 0, ActivationBlock: ActivationBlock0, Confirmation: arbiter.TableRegistryConfirmationSafe},
		ClientLanes: &arbiter.ClientLaneParams{MaxLanesPerAccount: 256},
		SIIndexers:  []arbiter.SIIndexerEntry{SIIndexerEntry0()},
		Verifiers:   VerifierEntries(),
	}
}

// SNodeRegistration is indexer 1's SNode registration at RegistrationSeq.
func SNodeRegistration() arbiter.NodeRegistration {
	return arbiter.NodeRegistration{NodeID: SNodeNodeID1, Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: RegistrationSeq}
}

// SNodeMarkActive is the activation following SNodeRegistration.
func SNodeMarkActive() authority.MarkActiveBody {
	return authority.MarkActiveBody{NodeID: SNodeNodeID1, RegistrationSeq: RegistrationSeq}
}

// ResultClaim is a one-part RC of indexer 1's SNode.
func ResultClaim() arbiter.RCRecord {
	return arbiter.RCRecord{
		StatementID: arbiter.StatementID{ClientAccount: "0x00000000000000000000000000000000000000a1", ClientSeq: 42, ClientNonce: "9f1c"},
		SourceNode:  SNodeNodeID1,
		CandidateParts: []arbiter.CandidatePart{{TableID: "db1.t", PartitionID: "all", PartName: "all_1_1_0",
			PartRowLtHash: "0xaa", PartPhysHash: "0xbb", RowCount: 3, Bytes: 512}},
		SourceClaimRoot:      "0xroot",
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: "db1.t", PartitionID: "all", NewPartsLtHashSum: "0xaa"}},
	}
}

// PromotionAck is an applied acknowledgement of indexer 1's SNode.
func PromotionAck() arbiter.PromotionAck {
	return arbiter.PromotionAck{NodeID: SNodeNodeID1, PromotionSeq: 7, TableID: "db1.t", PartitionID: "all",
		PostPartitionCommitment: "0xpost", Applied: true,
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xaa", SafePartName: "all_7_7_0", PartPhysHash: "0xbb"}}}
}

// CleanupAck is the cleanup acknowledgement of the same promotion.
func CleanupAck() arbiter.CleanupAck {
	return arbiter.CleanupAck{NodeID: SNodeNodeID1, PromotionSeq: 7, TableID: "db1.t", PartitionID: "all"}
}

// SNodeTablePurged is indexer 1's SNode reporting incarnation 5 purged.
func SNodeTablePurged() authority.TablePurgedBody {
	return authority.TablePurgedBody{NodeID: SNodeNodeID1, IncarnationSeq: 5}
}

// VerifierRegistration is verifier 1's registration at RegistrationSeq.
func VerifierRegistration() arbiter.NodeRegistration {
	return arbiter.NodeRegistration{NodeID: VerifierNodeID(1), Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier},
		Ed25519Pubkey: VerifierKey(1).Public().(ed25519.PublicKey), RegistrationSeq: RegistrationSeq}
}

// VerifierMarkActive is the activation following VerifierRegistration.
func VerifierMarkActive() authority.MarkActiveBody {
	return authority.MarkActiveBody{NodeID: VerifierNodeID(1), RegistrationSeq: RegistrationSeq}
}

// VerifierTablePurged is verifier 1 reporting incarnation 5 purged.
func VerifierTablePurged() authority.TablePurgedBody {
	return authority.TablePurgedBody{NodeID: VerifierNodeID(1), IncarnationSeq: 5}
}

// EvictCommand evicts indexer 1's SNode registered at RegistrationSeq.
func EvictCommand() arbiter.EvictNodeCommand {
	return arbiter.EvictNodeCommand{NodeID: SNodeNodeID1, ExpectedRegistrationSeq: RegistrationSeq, Reason: "key compromised"}
}
