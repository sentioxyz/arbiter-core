package arbiter

import (
	"crypto/ed25519"
	"fmt"
	"regexp"
	"strings"
	"unicode"
)

// ConsensusAdminProtocolVersion advertises support for the ConsensusAdmin
// capability/read/update RPCs and the replicated parameter-update command.
// Every voter must support this version before operators enable updates.
const ConsensusAdminProtocolVersion uint32 = 1

// ClientLanesFeature is the capability string a lane-aware binary advertises
// in ProtocolInfo.features and NodeRegistration.features (housegate spec
// 2026-10-09 §5.6).
const ClientLanesFeature = "client_lanes_v1"

// SignedClaimsFeature is the capability string of a binary that signs and
// verifies SNode and verifier messages (housegate spec 2026-10-10 §6.5,
// §6.7). The signed-claims activation gate requires it from every voter and
// every registered data-plane node.
const SignedClaimsFeature = "signed_claims_v1"

// TrackManifestsFeature is the capability string of a binary that decodes,
// validates and replays against track-aware safe-snapshot manifests and
// decodes the tracks-stage commands (housegate spec 2026-10-10 §7.3, §7.6).
// The tracks activation gate requires it from every voter and every
// registered data-plane node; after the activation the leader refuses a
// data-plane registration without it.
const TrackManifestsFeature = "track_manifests_v1"

// Stage3Feature is the capability string of a binary that decodes the stage-3
// v21 wire contract: the stage3 parameter, the member VerifierEntry fields,
// Raft tags 38-42 and the registry view of groups (housegate spec 2026-10-14
// §10.3; plan S3-E1 K65). The v21 activation requires it from every voter and
// from every registered, non-evicted data-plane node not on a quarantined
// track; a leader that predates it keeps the string without acting on it.
const Stage3Feature = "stage3_v1"

// MembershipLeaseFeature is the capability string of a data-plane binary that
// renews its membership lease (Membership.RenewNodeLease, Raft tag 44) while
// it is Active, and of a voter that decodes tag 44 and applies the lease. The
// lease activation requires it from every voter and every registered,
// non-evicted data-plane node: a node that does not renew would expire one
// lease after the activation. It is not in LocalNodeFeatures: a data-plane
// role appends it only while it renews, which needs a signer.
const MembershipLeaseFeature = "membership_lease_v1"

// MembershipLeaseOffReason is the refusal of a lease renewal before
// membership_lease_l2_blocks is committed: the leader proposes no Raft tag 44
// until then. A renewing role treats it as "nothing to do yet".
const MembershipLeaseOffReason = "membership lease: refused until membership_lease_l2_blocks is committed"

// MembershipLeaseNotActiveReason is the refusal of a lease renewal for a node
// whose committed status is not Active (its lease expired, it re-registered
// or it was evicted). A running role that registered and was not deactivated
// answers it with MarkActive at its current registration_seq.
const MembershipLeaseNotActiveReason = "membership lease: node is not active; re-activate with MarkActive at the current registration_seq"

// MinMembershipLeaseL2Blocks is the smallest non-zero membership lease
// authority.NormalizeConsensusParamsUpdate accepts. It is a floor against a
// typo, not a recommendation: the lease must be far longer than the nodes'
// renewal interval expressed in L2 blocks of the registry chain (at least
// ten intervals is a sane choice), and longer than several registry cursor
// advances, because expiry is evaluated only when the committed cursor moves.
const MinMembershipLeaseL2Blocks uint64 = 300

// LocalNodeFeatures returns this binary's capability strings in a fresh slice.
func LocalNodeFeatures() []string {
	return []string{ClientLanesFeature, SignedClaimsFeature, TrackManifestsFeature, Stage3Feature}
}

// signerAddressPattern is a normalized (lowercase) 20-byte address.
var signerAddressPattern = regexp.MustCompile(`^0x[0-9a-f]{40}$`)

const zeroAddress = "0x0000000000000000000000000000000000000000"

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
// and evidence. An entry without HostIndexerID is a network-pool verifier and
// sets nothing else, so its canonical form is the one every earlier digest
// hashed. An entry with HostIndexerID is a stage-3 member (housegate spec
// 2026-10-14 §6.1) hosted by that SI indexer (a pointer: indexer 0 is a valid
// host) and states every other field explicitly, with no absent-means-default
// (plan S3-E1 K58): ReplicaSigner (lowercase 0x + 40 hex) signs its
// checkpoints, ConsentJWS is the host signer's ES256K consent (purpose
// arbiter-group-member-v1, verified by the FSM), Weight is 1-64, and
// FailureDomain equals the stage3 parameter's domain of the host. Every
// stage-3 field is omitempty, so a pool entry hashes as it always did.
type VerifierEntry struct {
	NodeID        string  `json:"node_id"`
	Ed25519Pubkey []byte  `json:"ed25519_pubkey"`
	HostIndexerID *uint64 `json:"host_indexer_id,omitempty"`
	ReplicaSigner string  `json:"replica_signer,omitempty"`
	ConsentJWS    string  `json:"consent_jws,omitempty"`
	Weight        uint32  `json:"weight,omitempty"`
	FailureDomain string  `json:"failure_domain,omitempty"`
	Verify        bool    `json:"verify,omitempty"`
	Replicate     bool    `json:"replicate,omitempty"`
	Counts        bool    `json:"counts,omitempty"`
	SharedRMT     bool    `json:"shared_rmt,omitempty"`
}

// IsMember reports whether the entry is a stage-3 member: it names a host.
func (e VerifierEntry) IsMember() bool { return e.HostIndexerID != nil }

// Validate checks one normalized entry (lowercase Signer). Whether its
// enrolment statement verifies, and every rule over committed state, is the
// FSM's.
func (e SIIndexerEntry) Validate() error {
	switch {
	case e.ActivationBlock == 0:
		return fmt.Errorf("si_indexers: indexer %d: activation_block must be at least 1", e.IndexerID)
	case !signerAddressPattern.MatchString(e.Signer) || e.Signer == zeroAddress:
		return fmt.Errorf("si_indexers: indexer %d: signer must be a non-zero lowercase 0x-prefixed 20-byte address", e.IndexerID)
	case !validNodeID(e.SNodeNodeID):
		return fmt.Errorf("si_indexers: indexer %d: snode_node_id must be non-empty and free of whitespace and control characters", e.IndexerID)
	case strings.TrimSpace(e.EnrollmentJWS) == "":
		return fmt.Errorf("si_indexers: indexer %d: enrollment_jws is required", e.IndexerID)
	}
	return nil
}

// Validate checks one verifier entry.
func (e VerifierEntry) Validate() error {
	switch {
	case strings.TrimSpace(e.NodeID) == "":
		return fmt.Errorf("verifiers: node_id is required")
	case len(e.Ed25519Pubkey) != ed25519.PublicKeySize:
		return fmt.Errorf("verifiers: %s: ed25519_pubkey must be %d bytes, got %d", e.NodeID, ed25519.PublicKeySize, len(e.Ed25519Pubkey))
	}
	return nil
}

func validNodeID(id string) bool {
	return id != "" && !strings.ContainsFunc(id, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) })
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
	// TracksEnabled is the tracks activation (housegate spec 2026-10-10 §7.6).
	// False, and omitted from the canonical form so every earlier digest is
	// unchanged, until one authority-signed update after the signed-claims
	// activation sets it; afterwards every update carries it and it is never
	// lowered. The raise-only and carry rules are the FSM's.
	TracksEnabled bool `json:"tracks_enabled,omitempty"`
	// Stage3 is the stage-3 parameter (housegate spec 2026-10-14 §6.1,
	// §10.3-§10.4). Absent (nil), and omitted from the canonical form so every
	// earlier digest is unchanged, until the v21 activation sets it; afterwards
	// every update carries it complete. authority.NormalizeConsensusParamsUpdate
	// checks its static rules; the activation, carry and one-way rules are the
	// FSM's.
	Stage3 *Stage3Params `json:"stage3,omitempty"`
	// PromotionFinalityGate is the promotion finality gate: once committed,
	// Apply refuses a promotion that covers a statement which is not
	// Promotable (quorum-verified with its block's anchor final and
	// last-mergeable). False, and omitted from the canonical form so every
	// earlier digest is unchanged, until one authority-signed update sets it;
	// it needs TracksEnabled in the same update, and afterwards every update
	// carries it and it is never lowered. The raise-only and carry rules are
	// the FSM's.
	PromotionFinalityGate bool `json:"promotion_finality_gate,omitempty"`
	// ManifestDeltaSnapshot is the manifest delta snapshot switch: once
	// committed, the arbiter writes snapshot v22, which stores every published
	// manifest other than the genesis and the safe watermark's as a delta
	// against its parent. It changes no Apply rule. False, and omitted from
	// the canonical form so every earlier digest is unchanged, until one
	// authority-signed update sets it; it needs Stage3 in the same update, and
	// afterwards every update carries it and it is never lowered. The
	// raise-only and carry rules are the FSM's.
	ManifestDeltaSnapshot bool `json:"manifest_delta_snapshot,omitempty"`
	// TableAdmissionRulesVersion is the table admission rules version every
	// later AddTable is judged by (tableadmission.AdmitDeclaredSchema): 0
	// means rules v1, the only rules before the field existed; 2 selects
	// rules v2. Zero, and omitted from the canonical form so every earlier
	// digest is unchanged, until one authority-signed update raises it; it
	// needs TableRegistry in the same update, and afterwards every update
	// carries it and it is never lowered. authority.NormalizeConsensusParamsUpdate
	// checks the value (tableadmission.ValidRulesVersionParam); the raise-only
	// and carry rules are the FSM's.
	TableAdmissionRulesVersion uint32 `json:"table_admission_rules_version,omitempty"`
	// PartClaimUniqueness is the part-claim uniqueness gate: once committed,
	// Apply refuses a result claim that names a (table, partition, part hash)
	// twice or one another statement's bound or parked claim names, and the
	// acknowledged cleanup of a rejected partition drops the Rejected
	// statements' parts in it from promotion coverage. False, and omitted from
	// the canonical form so every earlier digest is unchanged, until one
	// authority-signed update sets it; it needs TracksEnabled in the same
	// update, and afterwards every update carries it and it is never lowered.
	// The raise-only and carry rules are the FSM's.
	PartClaimUniqueness bool `json:"part_claim_uniqueness,omitempty"`
	// Voters are the enrolled Raft voters, sorted by voter id, ids and keys
	// unique, each consent signature verifying. Absent, and omitted from the
	// canonical form, until an update sets it. The set may change between
	// updates; once AnchorFinality is committed every update carries a
	// non-empty set (the FSM's rule).
	Voters []VoterEntry `json:"voters,omitempty"`
	// AnchorFinality is the anchor finality quorum: once committed, a block's
	// finality flags latch only from signed attestations of Quorum distinct
	// enrolled voters. Absent, and omitted from the canonical form, until one
	// update sets it with Voters and PromotionFinalityGate in the same update;
	// afterwards every update carries it, Quorum stays a majority of Voters,
	// and every other field is immutable (the FSM's rules).
	AnchorFinality *AnchorFinalityParams `json:"anchor_finality,omitempty"`
	// AuthorityThreshold is the governance authority threshold k (k-of-n):
	// once committed non-zero, every governance command needs authority
	// tokens from at least k distinct members of the current authority set
	// (authority.Validator.Threshold); operational commands stay
	// single-signature. Zero means never set (effective k = 1) and is omitted
	// from the canonical form, so every earlier digest is unchanged.
	// authority.NormalizeConsensusParamsUpdate refuses a value above the
	// number of AuthorityAddresses; the old-k authorization, carry and
	// shrink rules are the FSM's (and the SNode follower's).
	AuthorityThreshold uint32 `json:"authority_threshold,omitempty"`
	// MembershipLeaseL2Blocks is the membership lease, in L2 blocks of the
	// committed table-registry cursor. Zero, and omitted from the canonical
	// form so every earlier digest is unchanged, means no lease: an Active node
	// stays Active until it re-registers or is evicted. Once committed, every
	// Active node renews its lease (Raft tag 44), and an Active node whose last
	// renewal or MarkActive is at least this many L2 blocks behind the cursor
	// becomes Syncing in the Apply that advances the cursor (the transition a
	// re-registration without MarkActive makes, not eviction). Expiry moves
	// only with the cursor, so it stops while the registry watcher stops: a
	// stalled watcher expires nothing. It needs TableRegistry in the same
	// update and at least MinMembershipLeaseL2Blocks
	// (authority.NormalizeConsensusParamsUpdate); the carry and one-way rules
	// are the FSM's.
	MembershipLeaseL2Blocks uint64 `json:"membership_lease_l2_blocks,omitempty"`
	// PromotionAckDeltaSnapshot is the promotion acknowledgement delta
	// snapshot switch: once committed, the arbiter writes snapshot v23, which
	// stores the complete safe-partition inventory of every acknowledged
	// promotion (PromotionAck.SafePartitionParts) as a delta against the
	// previous acknowledgement of its partition. It changes no Apply rule.
	// False, and omitted from the canonical form so every earlier digest is
	// unchanged, until one authority-signed update sets it; it needs
	// ManifestDeltaSnapshot in the same update, and afterwards every update
	// carries it and it is never lowered. The raise-only and carry rules are
	// the FSM's.
	PromotionAckDeltaSnapshot bool `json:"promotion_ack_delta_snapshot,omitempty"`
	// SnapshotQueryTracks opens the snapshot-query lane on a network with
	// tracks: the query belongs to the owner track of its target table, only
	// that track's admission is fenced, and the control tokens are purpose
	// housegate-snapshot-query-control-v2, which signs target_table_id. Once
	// committed, the arbiter writes snapshot v24. False, and omitted from the
	// canonical form so every earlier digest is unchanged, until one
	// authority-signed update sets it; it needs TracksEnabled and
	// PromotionAckDeltaSnapshot in the same update, and afterwards every
	// update carries it and it is never lowered. The raise-only, carry and
	// "after the tracks activation" rules are the FSM's.
	SnapshotQueryTracks bool `json:"snapshot_query_tracks,omitempty"`
	// CustodyFrontier turns on the committed custody frontier: the leader
	// records its payload custody progress (Raft tag 45,
	// wire.RecordCustodyProgress) and a new leader resumes custody from it
	// instead of re-asserting every block's holds. Once committed, the
	// arbiter writes snapshot v25. False, and omitted from the canonical form
	// so every earlier digest is unchanged, until one authority-signed update
	// sets it; it needs SnapshotQueryTracks in the same update, and afterwards
	// every update carries it and it is never lowered. The raise-only and
	// carry rules are the FSM's.
	CustodyFrontier bool `json:"custody_frontier,omitempty"`
	// OperationsAuthorityAddresses is the operations authority set
	// (arbiter#173 Slice 2): the addresses that sign operational commands,
	// which the leader signs automatically with its one online key
	// (promotion, cleanup, member lagging, verifier reselection, payload-store
	// ReleasePins). Absent (nil), and omitted from the canonical form so every
	// earlier digest is unchanged, until one governance-signed update sets it;
	// operational commands verify against AuthorityAddresses until then. Once
	// committed, operational commands verify only against it and governance
	// commands only against AuthorityAddresses. authority.NormalizeConsensusParamsUpdate
	// normalizes it like AuthorityAddresses and refuses an address that is
	// also in the update's AuthorityAddresses, so an online key never counts
	// toward the governance threshold; the one-way carry rule (once set, every
	// update carries a non-empty set) and the never-at-genesis rule are the
	// FSM's (and the SNode follower's). To stop all operational signing,
	// governance sets an address whose key nobody holds.
	OperationsAuthorityAddresses []string `json:"operations_authority_addresses,omitempty"`
	// HistoryRetainBlocks turns on history pruning and archival with a
	// retention window of this many safe blocks: the leader archives sealed
	// history below the prune horizon to the payload store
	// (PIN_PURPOSE_ARCHIVE) and proposes wire.PruneHistory (Raft tag 46).
	// Once committed, the arbiter writes snapshot v26. 0, and omitted from
	// the canonical form so every earlier digest is unchanged, until one
	// authority-signed update sets it; it needs CustodyFrontier in the same
	// update, and afterwards every update carries a value at least as large
	// (raise-only). The raise-only and carry rules are the FSM's.
	HistoryRetainBlocks uint64 `json:"history_retain_blocks,omitempty"`
	// AuthorityChangeDelayL2Blocks is the authority change delay D
	// (arbiter#173 Slice 3), in L2 blocks of the committed table-registry
	// cursor, not seconds: scale it to the L2's block time
	// (RecommendedAuthorityChangeDelayL2Blocks is about a day at 2 s). 0, and
	// omitted from the canonical form so every earlier digest is unchanged,
	// means never set. Once committed, AuthorityAddresses, AuthorityThreshold
	// and this field change only by executing a PendingAuthorityChange whose
	// NotBeforeL2Block the committed cursor has reached;
	// OperationsAuthorityAddresses and every other field change at once.
	// authority.NormalizeConsensusParamsUpdate refuses a value below
	// MinAuthorityChangeDelayL2Blocks and one without an AuthorityThreshold
	// of at least 2 and a TableRegistry in the same update; the activation,
	// carry, propose / execute / cancel and genesis rules are the FSM's.
	AuthorityChangeDelayL2Blocks uint64 `json:"authority_change_delay_l2_blocks,omitempty"`
	// PendingAuthorityChange is the proposed change of the delayed authority
	// fields, absent (and omitted from the canonical form) when none is
	// pending. It needs AuthorityChangeDelayL2Blocks in the same update.
	PendingAuthorityChange *PendingAuthorityChange `json:"pending_authority_change,omitempty"`
	// AccountBudget is the write budget of signed statements (arbiter#143
	// layer C): a leaky bucket per signer and one for the whole network,
	// measured on the committed table-registry cursor. Absent (nil), and
	// omitted from the canonical form so every earlier digest is unchanged,
	// means never set. authority.NormalizeConsensusParamsUpdate refuses
	// values outside AccountBudgetParams.Validate and one without
	// TableRegistry and SIIndexers in the same update; the carry (once set,
	// every update carries it; its values may change) and genesis rules are
	// the FSM's.
	AccountBudget *AccountBudgetParams `json:"account_budget,omitempty"`
	// PendingVoterChange is the proposed addition to Voters and/or lowering
	// of AnchorFinality.Quorum (arbiter#223), absent (and omitted from the
	// canonical form, so every earlier digest is unchanged) when none is
	// pending. Once AuthorityChangeDelayL2Blocks and AnchorFinality are both
	// committed, a voter is added (a new voter_id, or a new key for an
	// enrolled id) and the quorum lowered only by executing a pending change
	// whose NotBeforeL2Block the committed cursor has reached; removals and
	// quorum raises take effect at once. authority.NormalizeConsensusParamsUpdate
	// refuses it without AuthorityChangeDelayL2Blocks and AnchorFinality in
	// the same update, normalizes its additions like Voters, and refuses one
	// that adds nothing and lowers nothing or has no NotBeforeL2Block; the
	// propose / execute / cancel, carry and genesis rules are the FSM's.
	PendingVoterChange *PendingVoterChange `json:"pending_voter_change,omitempty"`
}

// AccountBudgetParams bounds the signed statements admitted to the shared
// log (arbiter#143 layer C), measured in L2 blocks of the committed
// table-registry cursor, never a wall clock. Each bucket is a leaky bucket
// that holds at most its Max* value and refills Max* / WindowL2Blocks per L2
// block the cursor advances: after a full refill at most Max* is admitted
// over any WindowL2Blocks span. A statement costs one statement and its
// payload_length bytes. The signer's bucket (client_account, every client
// lane together) is checked first, then the network-wide total, so one
// account cannot take more than its own share of the shared total. An empty
// bucket always admits one statement, whatever its size; the overdraft is
// repaid before the next. With Enforce false (observe mode) every admission
// still charges the buckets and nothing is refused. The buckets refill only
// when the cursor moves: a stalled table-registry watcher refills nothing,
// so a signer or the network already at its budget stays refused until the
// cursor moves again (fail-closed); everyone below it is unaffected.
type AccountBudgetParams struct {
	WindowL2Blocks     uint64 `json:"window_l2_blocks"`
	MaxStatements      uint64 `json:"max_statements"`
	MaxBytes           uint64 `json:"max_bytes"`
	MaxStatementsTotal uint64 `json:"max_statements_total"`
	MaxBytesTotal      uint64 `json:"max_bytes_total"`
	Enforce            bool   `json:"enforce,omitempty"`
}

// MaxAccountBudgetWindowL2Blocks is the largest account budget window.
const MaxAccountBudgetWindowL2Blocks uint64 = 1_000_000

// MaxAccountBudgetValue is the largest Max* value of an account budget. With
// MaxAccountBudgetWindowL2Blocks it keeps every scaled bucket level
// (value x window) below 2^60.
const MaxAccountBudgetValue uint64 = 1 << 40

// Validate checks the static bounds of an account budget.
func (p AccountBudgetParams) Validate() error {
	switch {
	case p.WindowL2Blocks == 0 || p.WindowL2Blocks > MaxAccountBudgetWindowL2Blocks:
		return fmt.Errorf("account budget: window_l2_blocks %d must be in [1, %d]", p.WindowL2Blocks, MaxAccountBudgetWindowL2Blocks)
	case p.MaxStatements == 0 || p.MaxBytes == 0:
		return fmt.Errorf("account budget: max_statements and max_bytes must be positive")
	case p.MaxStatementsTotal < p.MaxStatements:
		return fmt.Errorf("account budget: max_statements_total %d is below max_statements %d", p.MaxStatementsTotal, p.MaxStatements)
	case p.MaxBytesTotal < p.MaxBytes:
		return fmt.Errorf("account budget: max_bytes_total %d is below max_bytes %d", p.MaxBytesTotal, p.MaxBytes)
	case p.MaxStatementsTotal > MaxAccountBudgetValue || p.MaxBytesTotal > MaxAccountBudgetValue:
		return fmt.Errorf("account budget: every max_* value must be at most %d", MaxAccountBudgetValue)
	}
	return nil
}

// PendingAuthorityChange is a proposed change of the delayed authority fields
// (ConsensusParamsUpdate.PendingAuthorityChange): the complete target
// authority set, threshold and delay, and the earliest committed
// table-registry cursor block at which an update may execute it.
type PendingAuthorityChange struct {
	AuthorityAddresses           []string `json:"authority_addresses"`
	AuthorityThreshold           uint32   `json:"authority_threshold"`
	AuthorityChangeDelayL2Blocks uint64   `json:"authority_change_delay_l2_blocks"`
	NotBeforeL2Block             uint64   `json:"not_before_l2_block"`
}

// PendingVoterChange is a proposed change of the delayed voter fields
// (ConsensusParamsUpdate.PendingVoterChange): enrolments to add (a voter_id
// not enrolled in force, or an enrolled voter_id with a different key), an
// optional lowered anchor finality quorum (0 = none; the execution may then
// keep or raise the quorum), and the earliest committed table-registry
// cursor block at which an update may execute it.
type PendingVoterChange struct {
	Additions            []VoterEntry `json:"additions,omitempty"`
	AnchorFinalityQuorum uint32       `json:"anchor_finality_quorum,omitempty"`
	NotBeforeL2Block     uint64       `json:"not_before_l2_block"`
}

// MinAuthorityChangeDelayL2Blocks is the smallest authority change delay
// authority.NormalizeConsensusParamsUpdate accepts (about two hours on a 2 s
// L2), low enough to test on a devnet. It is a floor, not a recommendation.
const MinAuthorityChangeDelayL2Blocks uint64 = 3600

// RecommendedAuthorityChangeDelayL2Blocks is the documented default delay:
// about one day on a 2 s L2. Scale it to the L2's block time.
const RecommendedAuthorityChangeDelayL2Blocks uint64 = 43200

// EvictNodeCommand is the canonical signing form of an authority-signed
// eviction (housegate spec 2026-10-10 §6.5); its wire form is
// pb.EvictNodeRequest minus authority_jws. ExpectedRegistrationSeq is a
// compare-and-swap on the node's last applied registration_seq, so a token
// can never evict a later registration of the same node.
type EvictNodeCommand struct {
	NodeID                  string `json:"node_id"`
	ExpectedRegistrationSeq uint64 `json:"expected_registration_seq"`
	Reason                  string `json:"reason"`
}

// QuarantineIndexerCommand is the canonical signing form of an
// authority-signed quarantine (housegate spec 2026-10-10 §7.4); its wire form
// is pb.QuarantineIndexerCmd minus authority_jws. ExpectedGeneration is a
// compare-and-swap on the indexer's quarantine_generation, which every applied
// quarantine, release and automatic quarantine increments, so a token can
// never be replayed against a later state of the same indexer. Indexer 0 and
// generation 0 are signed like any other value (no omitempty).
type QuarantineIndexerCommand struct {
	IndexerID          uint64 `json:"indexer_id"`
	ExpectedGeneration uint64 `json:"expected_generation"`
	Reason             string `json:"reason"`
}

// ReleaseIndexerCommand is the canonical signing form of an authority-signed
// release (spec §7.4, §7.5); its wire form is pb.ReleaseIndexerCmd minus
// authority_jws. AcknowledgedRejectedBlocks lists, sorted ascending without
// duplicates, exactly the track's rejected blocks the release acknowledges;
// it is nil, and omitted from the canonical form, when there are none.
type ReleaseIndexerCommand struct {
	IndexerID                  uint64   `json:"indexer_id"`
	ExpectedGeneration         uint64   `json:"expected_generation"`
	AcknowledgedRejectedBlocks []uint64 `json:"acknowledged_rejected_blocks,omitempty"`
}

// Validate refuses an acknowledged list that is not sorted ascending without
// duplicates, so no one signs, and the FSM never compares, an ambiguous list.
func (c ReleaseIndexerCommand) Validate() error {
	for i := 1; i < len(c.AcknowledgedRejectedBlocks); i++ {
		if c.AcknowledgedRejectedBlocks[i-1] >= c.AcknowledgedRejectedBlocks[i] {
			return fmt.Errorf("release indexer %d: acknowledged_rejected_blocks must be sorted ascending without duplicates", c.IndexerID)
		}
	}
	return nil
}

// SettleTrackCommand is the canonical signing form of an authority-signed
// settle of a quarantined track (housegate spec 2026-10-14 §14 "Exits from the
// release refusals", P4; plan S3-D clarification K53); its wire form is
// pb.SettleTrackRequest minus authority_jws and timeout_ms. It is never a Raft
// command: only the leader checks it, before it streams anything, and no
// Apply reads it. PromotionSeqs lists, strictly ascending, exactly the track's
// issued, unacknowledged promotions at signing time; ExpectedGeneration is the
// track's quarantine_generation. Indexer 0 and generation 0 are signed like
// any other value (no omitempty).
type SettleTrackCommand struct {
	IndexerID          uint64   `json:"indexer_id"`
	ExpectedGeneration uint64   `json:"expected_generation"`
	PromotionSeqs      []uint64 `json:"promotion_seqs"`
}

// Validate refuses an empty list and one that is not strictly ascending. It
// never sorts, so no one signs, and no leader compares, an ambiguous list.
func (c SettleTrackCommand) Validate() error {
	if len(c.PromotionSeqs) == 0 {
		return fmt.Errorf("settle track: promotion_seqs must be non-empty and strictly ascending")
	}
	for i := 1; i < len(c.PromotionSeqs); i++ {
		if c.PromotionSeqs[i-1] >= c.PromotionSeqs[i] {
			return fmt.Errorf("settle track: promotion_seqs must be non-empty and strictly ascending")
		}
	}
	return nil
}
