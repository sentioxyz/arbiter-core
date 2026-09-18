package arbiter

// ConsensusAdminProtocolVersion advertises support for the ConsensusAdmin
// capability/read/update RPCs and the replicated parameter-update command.
// Every voter must support this version before operators enable updates.
const ConsensusAdminProtocolVersion uint32 = 1

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
}
