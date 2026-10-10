package wire

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

type signedClaimsGolden struct {
	name string
	cmd  Command
	want string
}

func goldenClaim() arbiter.RCRecord {
	return arbiter.RCRecord{
		StatementID: legacyGoldenEnvelope().StatementID, SourceNode: "snode-1",
		CandidateParts: []arbiter.CandidatePart{{TableID: "db1.t", PartitionID: "all", PartName: "all_1_1_0",
			PartRowLtHash: "0xaa", PartPhysHash: "0xbb", RowCount: 3, Bytes: 512}},
		SourceClaimRoot:      "0xroot",
		PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: "db1.t", PartitionID: "all", NewPartsLtHashSum: "0xaa"}},
	}
}

func goldenPromotionAck() arbiter.PromotionAck {
	return arbiter.PromotionAck{NodeID: "snode-1", PromotionSeq: 7, TableID: "db1.t", PartitionID: "all",
		PostPartitionCommitment: "0xpost", Applied: true,
		Parts: []arbiter.SafePartMapping{{PartRowLtHash: "0xaa", SafePartName: "all_7_7_0", PartPhysHash: "0xbb"}}}
}

// preSignedClaimsGolden lists, for every Raft command the signed-claims stage
// extends, the shape a pre-activation leader proposes and its exact bytes,
// computed from arbiter-proto 2eb3917 (protoc --encode arbiter.RaftCommand).
func preSignedClaimsGolden() []signedClaimsGolden {
	lanes := legacyGoldenUpdate()
	lanes.ExpectedEpoch = 2
	lanes.ArtifactDispositionCapability = 1
	lanes.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	ev := func(block, logIndex uint64, hash, tx string) arbiter.L2EventRef {
		return arbiter.L2EventRef{BlockNumber: block, BlockHash: hash, LogIndex: logIndex, TxHash: tx}
	}
	return []signedClaimsGolden{
		{"register_rc_with_parts", Command{RegisterRC: &RegisterRC{RC: goldenClaim()}},
			"2288010a85010a340a2a307830303030303030303030303030303030303030303030303030303030303030303030303030306131102a1a04396631631207736e6f64652d311a280a056462312e741203616c6c1a09616c6c5f315f315f302204307861612a0430786262300338800422063078726f6f742a120a056462312e741203616c6c1a0430786161"},
		{"record_promotion_ack", Command{RecordPromotionAck: &RecordPromotionAck{Ack: goldenPromotionAck()}},
			"4a3c0a3a0a07736e6f64652d3110071a056462312e742203616c6c2a063078706f737432170a04307861611209616c6c5f375f375f301a04307862623801"},
		{"record_cleanup_ack", Command{RecordCleanupAck: &RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "snode-1", PromotionSeq: 7, TableID: "db1.t", PartitionID: "all"}}},
			"62190a170a07736e6f64652d3110071a056462312e742203616c6c"},
		{"register_snode", Command{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}},
			"7a090a070a027331120102"},
		{"register_verifier", Command{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier},
			Ed25519Pubkey: bytes.Repeat([]byte{7}, 32), DialAddr: "v1:7080"}}},
			"7a340a320a0276311201011a200707070707070707070707070707070707070707070707070707070707070707220776313a37303830"},
		{"mark_active", Command{MarkActive: &MarkActive{NodeID: "snode-1"}}, "8201090a07736e6f64652d31"},
		{"evict_node", Command{EvictNode: &EvictNode{NodeID: "snode-1", Reason: "operator"}}, "8a01130a07736e6f64652d3112086f70657261746f72"},
		{"record_table_purged", Command{RecordTablePurged: &RecordTablePurged{NodeID: "verifier-1", IncarnationSeq: 5}}, "9a020e0a0a76657269666965722d311005"},
		{"add_table", Command{AddTable: &AddTable{DatabaseID: "db1", TableID: "t",
			Created: ev(5508940, 2, "0xb1", "0xt1"), Schema: ev(5508941, 0, "0xb2", "0xt2"),
			SchemaVersion: 1, SchemaHash: "0xh", SchemaJSON: `{"table_id":"db1.t"}`}},
			"82024d0a036462311201741a1308cc9ed0021204307862311802220430787431221108cd9ed002120430786232220430787432280132033078683a147b227461626c655f6964223a226462312e74227d"},
		{"seed_legacy_tables", Command{SeedLegacyTables: &SeedLegacyTables{AtBlock: arbiter.L2BlockRef{Number: 5508930, Hash: "0xb0"},
			Tables: []arbiter.LegacyTable{{DatabaseID: "db1", TableID: "old", Created: ev(100, 1, "0xc", "0xd")}}}},
			"fa01290a0b08c29ed002120430786230121a0a0364623112036f6c641a0e0864120330786318012203307864"},
		{"update_consensus_params_with_lanes", Command{UpdateConsensusParams: &UpdateConsensusParams{Update: lanes, AuthorityJWS: "h.p.s"}},
			"920198010a8e010a036e65741209307867656e657369731802220830786469676573742a2a3078303030303030303030303030303030303030303030303030303030303030303030303030303062323001380740014a3908cddae103122a30783030303030303030303030303030303030303030303030303030303030303030303030303030643120642a047361666552030880021205682e702e73"},
	}
}

// TestPreSignedClaimsCommandBytesAreFrozen pins the RaftCommand bytes of every
// command kind the signed-claims stage extends, in its pre-activation shape
// (housegate spec 2026-10-10 §16; plan S1-A CONTRACT §0). Until the
// activation every voter, upgraded or not, must decode exactly these bytes: a
// new field leaking into one of them forks an un-upgraded voter.
// Never regenerate these values: a diff here is a consensus break.
func TestPreSignedClaimsCommandBytesAreFrozen(t *testing.T) {
	for _, tc := range preSignedClaimsGolden() {
		t.Run(tc.name, func(t *testing.T) {
			got, err := Encode(tc.cmd)
			if err != nil {
				t.Fatalf("Encode: %v", err)
			}
			if hex.EncodeToString(got) != tc.want {
				t.Fatalf("encoded bytes changed:\n got %s\nwant %s", hex.EncodeToString(got), tc.want)
			}
			if _, err := Decode(got); err != nil {
				t.Fatalf("Decode(own bytes): %v", err)
			}
		})
	}
}
