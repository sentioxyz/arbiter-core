package authoritytest

import (
	"crypto/ed25519"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
)

// Stage-3 fixture identities (housegate spec 2026-10-14 §6.1; plan S3-E1
// contract §41): member-b, hosted by indexer 1 (snode-2, signed by
// IndexerKeyHex1), with its ed25519 node key and its replica_signer key.
// Throwaway keys: never provision them.
const (
	MemberNodeIDB               = "member-b"
	MemberHostIndexerIDB uint64 = 1
	ReplicaKeyHexB              = "0000000000000000000000000000000000000000000000000000000000000d44"
	ReplicaAddrB                = "0xeab0e192c789a8342f51891d8467de2acd7e416a"
	MemberFailureDomainB        = "node:test-b"
)

// MemberKeyB is member-b's ed25519 node key: a seed of 32 bytes 0x0b.
func MemberKeyB() ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = 0x0b
	}
	return ed25519.NewKeyFromSeed(seed)
}

// RebalanceGroupCommand moves group (0, dex) at generation 1 from member-b to
// member-c with network fill on.
func RebalanceGroupCommand() arbiter.RebalanceGroupCommand {
	return arbiter.RebalanceGroupCommand{OwnerIndexerID: 0, DatabaseID: "dex", ExpectedGeneration: 1,
		Add: []string{"member-c"}, Remove: []string{MemberNodeIDB}, NetworkFill: true}
}

// ReselectVerifiersCommand names block 100's current verifier set.
func ReselectVerifiersCommand() arbiter.ReselectVerifiersCommand {
	return arbiter.ReselectVerifiersCommand{L3BlockSeq: 100, ExpectedSet: []string{MemberNodeIDB, VerifierNodeID(1), VerifierNodeID(2)}}
}

// MarkMemberLaggingCommand marks member-b lagging in group (0, dex), behind
// indexer 0's entry (90, 40).
func MarkMemberLaggingCommand() arbiter.MarkMemberLaggingCommand {
	return arbiter.MarkMemberLaggingCommand{NodeID: MemberNodeIDB, OwnerIndexerID: 0, DatabaseID: "dex",
		ObservedEntry: replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 90, AppliedPromotionSeq: 40}}
}

// GroupMemberConsent is indexer 1's consent to member-b in the fixture
// network.
func GroupMemberConsent() authority.GroupMemberConsentStatement {
	return authority.GroupMemberConsentStatement{NetworkID: NetworkID, GenesisSnapshotID: GenesisSnapshotID, HostIndexerID: MemberHostIndexerIDB,
		NodeID: MemberNodeIDB, Ed25519Pubkey: MemberKeyB().Public().(ed25519.PublicKey), ReplicaSigner: ReplicaAddrB}
}

// MemberReadyBody is member-b reporting its replicas of group (0, dex) at
// generation 1.
func MemberReadyBody() arbiter.MemberReadyBody {
	return arbiter.MemberReadyBody{NodeID: MemberNodeIDB, OwnerIndexerID: 0, DatabaseID: "dex", Generation: 1}
}

// MemberCheckpoint is member-b's first checkpoint for owner 0: group dex at
// generation 1 without dex.pools, at snapshot 0xsnap whose entry for
// indexer 0 is (90, 40).
func MemberCheckpoint() arbiter.MemberCheckpoint {
	return arbiter.MemberCheckpoint{NodeID: MemberNodeIDB, OwnerIndexerID: 0, CheckpointSeq: 1,
		Groups:     []arbiter.CheckpointGroup{{DatabaseID: "dex", GroupGeneration: 1, Excluded: []arbiter.ExcludedTable{{TableID: "dex.pools", Reason: arbiter.ExclusionNotInstalled}}}},
		SnapshotID: "0xsnap", Entry: replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 90, AppliedPromotionSeq: 40}}
}

// MemberVerifierEntryB is member-b's verifiers entry: every field explicit
// (plan S3-E1 K58), weight 1, every flag on, consent GroupMemberConsentJWS.
func MemberVerifierEntryB() arbiter.VerifierEntry {
	host := MemberHostIndexerIDB
	return arbiter.VerifierEntry{NodeID: MemberNodeIDB, Ed25519Pubkey: MemberKeyB().Public().(ed25519.PublicKey), HostIndexerID: &host,
		ReplicaSigner: ReplicaAddrB, ConsentJWS: GroupMemberConsentJWS, Weight: 1, FailureDomain: MemberFailureDomainB,
		Verify: true, Replicate: true, Counts: true, SharedRMT: true}
}

// Stage3Update is ActivationUpdate with indexer 1 enrolled (max_writers 2),
// the tracks activation and the stage-3 parameter of the v21 activation:
// every switch off, failure domains for indexers 0 and 1. It is a signing and
// encoding vector in normalized form, not an FSM fixture.
func Stage3Update() arbiter.ConsensusParamsUpdate {
	u := ActivationUpdate()
	u.MaxWriters = 2
	u.SIIndexers = []arbiter.SIIndexerEntry{SIIndexerEntry0(), SIIndexerEntry1(6_000_000)}
	u.TracksEnabled = true
	u.Stage3 = &arbiter.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []arbiter.IndexerFailureDomain{{IndexerID: 0, FailureDomain: "node:test-a"}, {IndexerID: MemberHostIndexerIDB, FailureDomain: MemberFailureDomainB}},
		MaxLagPublications:    4, LagLow: 2, EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2400, MaxUncleanedPartsPerTable: 20000}
	return u
}

// Pinned stage-3 vectors (plan S3-E1 contract §41), computed outside Go:
// hashes as sha256("housegate-replay-mvp-v0:" + domain + "\x00" + json)
// through shasum -a 256; ES256K tokens as cast keccak of the signing input,
// then cast wallet sign --no-hash with the fixture key; the ed25519 signature
// with openssl pkeyutl -rawin over the hash string and the fixture seed. The
// authority tokens are AuthorityKeyHex's in ConsensusContext(2), the consent
// IndexerKeyHex1's, the checkpoint ReplicaKeyHexB's, all at Iat; the
// MemberReady signature is MemberKeyB's in Context(). Preimages:
//
//	arbiter-rebalance-group-command-v1:
//	{"owner_indexer_id":0,"database_id":"dex","expected_generation":1,"add":["member-c"],"remove":["member-b"],"network_fill":true}
//	arbiter-reselect-verifiers-command-v1:
//	{"l3_block_seq":100,"expected_set":["member-b","verifier-1","verifier-2"]}
//	arbiter-mark-member-lagging-command-v1:
//	{"node_id":"member-b","owner_indexer_id":0,"database_id":"dex","observed_entry":{"indexer_id":0,"safe_block_seq":90,"applied_promotion_seq":40}}
//	arbiter-group-member-statement-v1:
//	{"network_id":"devnet2","genesis_snapshot_id":"0xgenesis","host_indexer_id":1,"node_id":"member-b","ed25519_pubkey":"Zr5+Myx6RTMyvZ0Kf32wVfXF7xoGraZtmLOftoEMRzo=","replica_signer":"0xeab0e192c789a8342f51891d8467de2acd7e416a"}
//	arbiter-member-ready-v1:
//	{"context":{"network_id":"devnet2","genesis_snapshot_id":"0xgenesis"},"body":{"node_id":"member-b","owner_indexer_id":0,"database_id":"dex","generation":1}}
//	arbiter-member-checkpoint-body-v1:
//	{"context":{"network_id":"devnet2","genesis_snapshot_id":"0xgenesis"},"body":{"node_id":"member-b","owner_indexer_id":0,"checkpoint_seq":1,"groups":[{"database_id":"dex","group_generation":1,"excluded":[{"table_id":"dex.pools","reason":"not_installed"}]}],"snapshot_id":"0xsnap","entry":{"indexer_id":0,"safe_block_seq":90,"applied_promotion_seq":40}}}
//	arbiter-consensus-params-update-command-v1 (Stage3UpdateHash): the
//	canonical JSON of ActivationUpdate with "max_writers":2, indexer 1's
//	si_indexers entry appended, and after "verifiers" the members
//	"tracks_enabled":true,"stage3":{"group_size":3,"network_fill_default":true,"min_safe_copies":1,"strict_copies":false,"indexer_failure_domains":[{"indexer_id":0,"failure_domain":"node:test-a"},{"indexer_id":1,"failure_domain":"node:test-b"}],"max_lag_publications":4,"lag_low":2,"evidence_timeout_l2_blocks":30,"max_uncleaned_parts_per_partition":2400,"max_uncleaned_parts_per_table":20000,"exclude_quarantined_hosts":false,"purge":false,"eviction_voiding":false,"owner_aware_selection":false,"placement":false,"reselection":false,"checkpoints":false,"narrowed_drain":false}
//	arbiter-consensus-params-update-command-v1 (Stage3MemberUpdateHash): the
//	same with MemberVerifierEntryB first in "verifiers", every member field
//	in proto order:
//	{"node_id":"member-b","ed25519_pubkey":"Zr5+Myx6RTMyvZ0Kf32wVfXF7xoGraZtmLOftoEMRzo=","host_indexer_id":1,"replica_signer":"0xeab0e192c789a8342f51891d8467de2acd7e416a","consent_jws":<GroupMemberConsentJWS>,"weight":1,"failure_domain":"node:test-b","verify":true,"replicate":true,"counts":true,"shared_rmt":true}
//
// Consensus constants once committed: never regenerate them.
const (
	RebalanceGroupHash     = "0x70ce7cf82e46fe4e0573c8c7aaf5fe76fc4ad77a4f99d4a1ff7f08cfd369bcbf"
	RebalanceGroupJWS      = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXJlYmFsYW5jZS1ncm91cC12MSIsImNtZF9oYXNoIjoiMHg3MGNlN2NmODJlNDZmZTRlMDU3M2M4YzdhYWY1ZmU3NmZjNGFkNzdhNGY5OWQ0YTFmZjdmMDhjZmQzNjliY2JmIiwibmV0d29ya19pZCI6ImRldm5ldDIiLCJnZW5lc2lzX3NuYXBzaG90X2lkIjoiMHhnZW5lc2lzIiwiYXV0aG9yaXR5X2Vwb2NoIjoyfQ._RMF6atTnWiFfOdabPmQOSb0kprbAgi5sCwVJ-8pwVBy3l8Z17U1FndA_Wb_zaPNh9nq7FyToeNbB18N245WUxs"
	ReselectVerifiersHash  = "0x0ceea0ed3ec80c5a943db3c22d7fe335ecad1f8c343a47b6e56daf624600a3c7"
	ReselectVerifiersJWS   = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLXJlc2VsZWN0LXZlcmlmaWVycy12MSIsImNtZF9oYXNoIjoiMHgwY2VlYTBlZDNlYzgwYzVhOTQzZGIzYzIyZDdmZTMzNWVjYWQxZjhjMzQzYTQ3YjZlNTZkYWY2MjQ2MDBhM2M3IiwibmV0d29ya19pZCI6ImRldm5ldDIiLCJnZW5lc2lzX3NuYXBzaG90X2lkIjoiMHhnZW5lc2lzIiwiYXV0aG9yaXR5X2Vwb2NoIjoyfQ.JDyl5OLSnTxA71YU4oyWiTuqZvUDpYWFVtQDfNV-w3UJBTI9RjP1YzR1k9uaNBvG6IzaKbJy-cH35l8uGKhV6hw"
	MarkMemberLaggingHash  = "0xc47f716f47821908ee666595b1b9de42a742fecb7b170cff734e43c83aeeba20"
	MarkMemberLaggingJWS   = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLW1hcmstbWVtYmVyLWxhZ2dpbmctdjEiLCJjbWRfaGFzaCI6IjB4YzQ3ZjcxNmY0NzgyMTkwOGVlNjY2NTk1YjFiOWRlNDJhNzQyZmVjYjdiMTcwY2ZmNzM0ZTQzYzgzYWVlYmEyMCIsIm5ldHdvcmtfaWQiOiJkZXZuZXQyIiwiZ2VuZXNpc19zbmFwc2hvdF9pZCI6IjB4Z2VuZXNpcyIsImF1dGhvcml0eV9lcG9jaCI6Mn0.OzohgpzF85x_72Yv4hxy3e0WOWcMxYUAWZ6TldjW1M4OAoIMxG8o-LSGRQorQbfXXkQ20hc5wu4Y95BkLKTyYRw"
	GroupMemberConsentHash = "0x27b76cffd70c885fc63dfa6a3fdb10114939ff9e4a422f8fcb202903921a7546"
	GroupMemberConsentJWS  = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLWdyb3VwLW1lbWJlci12MSIsImNtZF9oYXNoIjoiMHgyN2I3NmNmZmQ3MGM4ODVmYzYzZGZhNmEzZmRiMTAxMTQ5MzlmZjllNGE0MjJmOGZjYjIwMjkwMzkyMWE3NTQ2In0.5Y29pepLEREzgIJEk3MUMYIZgAqLgkbjWJ8-xuntPtty6Ds0em54nn9XQlQvVxpHV6-TTURCtyJfMgrQQwvcsRs"
	MemberReadyHash        = "0xfbf286eda5230fea48ea2c4fc55a2693703fcf5f55cd64c21925593eb362c932"
	MemberReadySignature   = "f0b6385f6e9a11dcf9d0c3a7a36d55fd8caf6c3c3a6f251237fef1d6db7ea44ee19945dcb67ab415d48695693642266186d5fb4d5fc0a189d4fe44a8f7c49801"
	MemberCheckpointHash   = "0x7fb91939a22e8549258248ff7ef3f27d59e335a5e5cc0564cbba45f8ee2fd3e1"
	MemberCheckpointJWS    = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJpYXQiOjE3NjAwNTQ0MDAsInB1cnBvc2UiOiJhcmJpdGVyLW1lbWJlci1jaGVja3BvaW50LXYxIiwiY21kX2hhc2giOiIweDdmYjkxOTM5YTIyZTg1NDkyNTgyNDhmZjdlZjNmMjdkNTllMzM1YTVlNWNjMDU2NGNiYmE0NWY4ZWUyZmQzZTEifQ.pmxIjPdo1BVRlJ-L4vCjUXEhYkXwSbn4-R2Ij16mokp6PVjnlMwpV3CjkLD_w1ADbXxE1RboG8lHS5NXR0WQPRw"
	Stage3UpdateHash       = "0xc2d661ecae63231e279537752ce89c17ae10b436a945e50d868fe4c54724efe9"
	Stage3MemberUpdateHash = "0x0f2fec8d9ab1ede1b5ba6cf312d12295e1358682262c297f26e4663a53b2432c"
)
