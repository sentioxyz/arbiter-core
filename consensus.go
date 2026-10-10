package arbiter

import "fmt"

// ConsensusAdminProtocolVersion advertises support for the ConsensusAdmin
// capability/read/update RPCs and the replicated parameter-update command.
// Every voter must support this version before operators enable updates.
const ConsensusAdminProtocolVersion uint32 = 1

// ClientLanesFeature is the capability string a lane-aware binary advertises
// in ProtocolInfo.features and NodeRegistration.features (housegate spec
// 2026-10-09 §5.6).
const ClientLanesFeature = "client_lanes_v1"

// LocalNodeFeatures returns this binary's capability strings in a fresh slice.
func LocalNodeFeatures() []string { return []string{ClientLanesFeature} }

// ClientLaneParams enables client_seq lanes (housegate spec 2026-10-09 D13).
// Absent from every update until an authority sets it; afterwards every
// update carries it, MaxLanesPerAccount only rises, and it is never removed.
type ClientLaneParams struct {
	MaxLanesPerAccount uint32 `json:"max_lanes_per_account"`
}

// Validate refuses a budget that would admit no client lane at all.
func (p ClientLaneParams) Validate() error {
	if p.MaxLanesPerAccount == 0 {
		return fmt.Errorf("client lanes: max_lanes_per_account must be at least 1")
	}
	return nil
}

// SIIndexerEntry enrols one indexer into the network's storage-integrity layer
// (housegate spec 2026-10-10 D3, §6.1-§6.2). The list is append-only: an
// entry's IndexerID, ActivationBlock and SNodeNodeID never change, and its
// Signer changes only together with a new EnrollmentJWS by the new signer.
type SIIndexerEntry struct {
	IndexerID       uint64 `json:"indexer_id"`
	ActivationBlock uint64 `json:"activation_block"`
	// Signer is the indexer's IndexerRegistry signer, lowercase 0x + 40 hex;
	// it signs every message of the entry's SNode (spec D6).
	Signer string `json:"signer"`
	// SNodeNodeID names the indexer's one SNode, the source of every
	// statement on the indexer's tables (spec D5).
	SNodeNodeID string `json:"snode_node_id"`
	// EnrollmentJWS is Signer's ES256K enrolment statement (purpose
	// arbiter-snode-enrollment-v1) over {network_id, genesis_snapshot_id,
	// indexer_id, snode_node_id}; the FSM verifies it.
	EnrollmentJWS string `json:"enrollment_jws"`
}

// VerifierEntry lists one verifier that may register (spec D7): its node id
// and the ed25519 key that signs its registration, activation, purge reports
// and evidence.
type VerifierEntry struct {
	NodeID        string `json:"node_id"`
	Ed25519Pubkey []byte `json:"ed25519_pubkey"`
}

// ConsensusParamsUpdate is the canonical signing form of a complete mutable
// consensus-parameter transition. Identity and compare-and-swap preconditions
// are signed along with the target authority set and writer limit.
//
// authority.NormalizeConsensusParamsUpdate canonicalizes the address set
// before hashing. Protobuf serialization is transport only, never the signed
// form. The FSM additionally checks identity, epoch, digest, promotion sequence,
// membership capacity and drained authority work against committed state.
type ConsensusParamsUpdate struct {
	NetworkID            string   `json:"network_id"`
	GenesisSnapshotID    string   `json:"genesis_snapshot_id"`
	ExpectedEpoch        uint64   `json:"expected_epoch"`
	PreviousParamsDigest string   `json:"previous_params_digest"`
	AuthorityAddresses   []string `json:"authority_addresses"`
	MaxWriters           uint64   `json:"max_writers"`
	ExpectedPromotionSeq uint64   `json:"expected_promotion_seq"`
	// ArtifactDispositionCapability is the C1 artifact-disposition kill switch
	// as a governed consensus parameter: 0 keeps every tag-28 command refused,
	// 1 enables the lane. It is absent from the canonical form when zero, so
	// every previously signed update keeps its digest; the FSM refuses 1 -> 0.
	ArtifactDispositionCapability uint32 `json:"artifact_disposition_capability,omitempty"`
	// TableRegistry enables the dynamic SI table registry. Absent (nil) keeps
	// it disabled and is omitted from the canonical form, so every previously
	// signed update keeps its digest. Once committed it must be resent
	// unchanged by every later update; the FSM refuses any change or removal.
	TableRegistry *TableRegistryParams `json:"table_registry,omitempty"`
	// ClientLanes enables client_seq lanes. Absent (nil) keeps them disabled
	// and is omitted from the canonical form, so every previously signed
	// update keeps its digest. Once committed it must be resent by every later
	// update; the FSM refuses removal and lowering and requires the table
	// registry to be enabled first.
	ClientLanes *ClientLaneParams `json:"client_lanes,omitempty"`
	// SIIndexers is the enrolled SI indexer list (housegate spec 2026-10-10
	// §6.1), sorted by IndexerID. Absent (nil) until the signed-claims
	// activation and omitted from the canonical form, so every earlier digest
	// is unchanged; once set every update carries the complete list.
	SIIndexers []SIIndexerEntry `json:"si_indexers,omitempty"`
	// Verifiers is the governed verifier set (spec D7), sorted by NodeID.
	// Absent until set with SIIndexers; then carried by every update.
	Verifiers []VerifierEntry `json:"verifiers,omitempty"`
}
