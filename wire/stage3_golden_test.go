package wire

import (
	"bytes"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// stage3ActivationGoldenUpdate is the shape of the v21 activation (housegate
// spec 2026-10-14 §10.3): legacyGoldenUpdate with two enrolled indexers, the
// tracks activation, a pool verifier and the stage3 parameter of
// authoritytest.Stage3Update (plan S3-E1 contract §41), every switch off.
func stage3ActivationGoldenUpdate() arbiter.ConsensusParamsUpdate {
	u := legacyGoldenUpdate()
	u.ExpectedEpoch = 5
	u.MaxWriters = 2
	u.ArtifactDispositionCapability = 1
	u.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	u.SIIndexers = []arbiter.SIIndexerEntry{
		{IndexerID: 0, ActivationBlock: 100, Signer: "0x0000000000000000000000000000000000000001", SNodeNodeID: "snode-1", EnrollmentJWS: "h.p.s"},
		{IndexerID: 1, ActivationBlock: 200, Signer: "0x0000000000000000000000000000000000000002", SNodeNodeID: "snode-2", EnrollmentJWS: "h.p.s"},
	}
	u.Verifiers = []arbiter.VerifierEntry{{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{7}, 32)}}
	u.TracksEnabled = true
	u.Stage3 = &arbiter.Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []arbiter.IndexerFailureDomain{{IndexerID: 0, FailureDomain: "node:test-a"}, {IndexerID: 1, FailureDomain: "node:test-b"}},
		MaxLagPublications:    4, LagLow: 2, EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2400, MaxUncleanedPartsPerTable: 20000}
	return u
}

// stage3MemberGoldenUpdate is the update of spec §10.4 step 4: the activation
// shape after switches 1-3 (purge, eviction_voiding, owner_aware_selection),
// listing the first member entry, hosted by indexer 1, with every member
// field explicit (plan S3-E1 K58; counts false between true flags), before
// the pool verifier.
func stage3MemberGoldenUpdate() arbiter.ConsensusParamsUpdate {
	host := uint64(1)
	u := stage3ActivationGoldenUpdate()
	u.Verifiers = []arbiter.VerifierEntry{
		{NodeID: "member-b", Ed25519Pubkey: bytes.Repeat([]byte{0x0b}, 32), HostIndexerID: &host,
			ReplicaSigner: "0x00000000000000000000000000000000000000eb", ConsentJWS: "h.p.s", Weight: 2, FailureDomain: "node:test-b",
			Verify: true, Replicate: true, Counts: false, SharedRMT: true},
		{NodeID: "verifier-1", Ed25519Pubkey: bytes.Repeat([]byte{7}, 32)},
	}
	u.Stage3.Purge, u.Stage3.EvictionVoiding, u.Stage3.OwnerAwareSelection = true, true, true
	return u
}

// stage3Golden lists one command per stage-3 Raft tag (38-42), with the
// bodies of authoritytest's stage-3 fixtures (plan S3-E1 contract §41) and
// placeholder tokens, plus the two update shapes above. The bytes were
// computed outside Go with protoc --encode=arbiter.RaftCommand over the
// arbiter-proto release PROTO_TAG_P5 (the plan S2-A pipeline; the same command
// reproduces record_cleanup_ack of preSignedClaimsGolden byte for byte).
// authoritytest imports authority, which imports this package, so the bodies
// are spelled out here instead of imported.
func stage3Golden() []signedClaimsGolden {
	entry := replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 90, AppliedPromotionSeq: 40}
	return []signedClaimsGolden{
		{"rebalance_group", Command{RebalanceGroup: &RebalanceGroup{OwnerIndexerID: 0, DatabaseID: "dex", ExpectedGeneration: 1,
			Add: []string{"member-c"}, Remove: []string{"member-b"}, NetworkFill: true, AuthorityJWS: "h.p.s"}},
			"b202241203646578180122086d656d6265722d632a086d656d6265722d6230013a05682e702e73"},
		{"reselect_verifiers", Command{ReselectVerifiers: &ReselectVerifiers{L3BlockSeq: 100,
			ExpectedSet: []string{"member-b", "verifier-1", "verifier-2"}, AuthorityJWS: "h.p.s"}},
			"ba022b086412086d656d6265722d62120a76657269666965722d31120a76657269666965722d321a05682e702e73"},
		{"member_ready", Command{MemberReady: &MemberReady{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex", Generation: 1, Ed25519Signature: "ab"}},
			"c202150a086d656d6265722d621a0364657820012a026162"},
		{"member_checkpoint", Command{MemberCheckpoint: &MemberCheckpoint{Checkpoint: arbiter.MemberCheckpoint{NodeID: "member-b", OwnerIndexerID: 0, CheckpointSeq: 1,
			Groups: []arbiter.CheckpointGroup{{DatabaseID: "dex", GroupGeneration: 1,
				Excluded: []arbiter.ExcludedTable{{TableID: "dex.pools", Reason: arbiter.ExclusionNotInstalled}}}},
			SnapshotID: "0xsnap", Entry: entry}, ReplicaJWS: "h.p.s"}},
			"ca023b0a320a086d656d6265722d62180122160a0364657810011a0d0a096465782e706f6f6c7310012a063078736e61703204105a18281205682e702e73"},
		{"mark_member_lagging", Command{MarkMemberLagging: &MarkMemberLagging{NodeID: "member-b", OwnerIndexerID: 0, DatabaseID: "dex",
			ObservedEntry: entry, AuthorityJWS: "h.p.s"}},
			"d2021c0a086d656d6265722d621a036465782204105a18282a05682e702e73"},
		{"update_consensus_params_with_stage3", Command{UpdateConsensusParams: &UpdateConsensusParams{Update: stage3ActivationGoldenUpdate(), AuthorityJWS: "h.p.s"}},
			"920182030af8020a036e65741209307867656e657369731805220830786469676573742a2a3078303030303030303030303030303030303030303030303030303030303030303030303030303062323002380740014a3908cddae103122a30783030303030303030303030303030303030303030303030303030303030303030303030303030643120642a047361666552030880025a3e10641a2a3078303030303030303030303030303030303030303030303030303030303030303030303030303030312207736e6f64652d312a05682e702e735a41080110c8011a2a3078303030303030303030303030303030303030303030303030303030303030303030303030303030322207736e6f64652d322a05682e702e73622e0a0a76657269666965722d3112200707070707070707070707070707070707070707070707070707070707070707680172330803100118012a0d120b6e6f64653a746573742d612a0f0801120b6e6f64653a746573742d6230043802401e48e01250a09c011205682e702e73"},
		{"update_consensus_params_with_a_member", Command{UpdateConsensusParams: &UpdateConsensusParams{Update: stage3MemberGoldenUpdate(), AuthorityJWS: "h.p.s"}},
			"920180040af6030a036e65741209307867656e657369731805220830786469676573742a2a3078303030303030303030303030303030303030303030303030303030303030303030303030303062323002380740014a3908cddae103122a30783030303030303030303030303030303030303030303030303030303030303030303030303030643120642a047361666552030880025a3e10641a2a3078303030303030303030303030303030303030303030303030303030303030303030303030303030312207736e6f64652d312a05682e702e735a41080110c8011a2a3078303030303030303030303030303030303030303030303030303030303030303030303030303030322207736e6f64652d322a05682e702e7362760a086d656d6265722d6212200b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b0b1801222a3078303030303030303030303030303030303030303030303030303030303030303030303030303065622a05682e702e7330023a0b6e6f64653a746573742d62400148015801622e0a0a76657269666965722d3112200707070707070707070707070707070707070707070707070707070707070707680172390803100118012a0d120b6e6f64653a746573742d612a0f0801120b6e6f64653a746573742d6230043802401e48e01250a09c016001680170011205682e702e73"},
	}
}

// TestStage3CommandBytesAreFrozen pins the RaftCommand bytes of every stage-3
// command, of the v21 activation update and of an update listing a member
// entry (plan S3-E1 contract §38, §41). From the v21 activation every voter
// decodes these bytes, so tags 38-42 and the field numbers of contract §39 are
// consensus constants: a diff here is a consensus break. Never regenerate
// these values.
func TestStage3CommandBytesAreFrozen(t *testing.T) {
	for _, tc := range stage3Golden() {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Encode(tc.cmd)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if hex.EncodeToString(got) != tc.want {
				t.Fatalf("encoded bytes changed:\n got %s\nwant %s", hex.EncodeToString(got), tc.want)
			}
			back, err := Decode(got)
			if err != nil {
				t.Fatalf("Decode(own bytes): %v", err)
			}
			if !reflect.DeepEqual(back, tc.cmd) {
				t.Fatalf("Decode(own bytes) = %+v, want %+v", back, tc.cmd)
			}
		})
	}
}
