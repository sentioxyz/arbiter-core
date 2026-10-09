package wire

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

func legacyGoldenEnvelope() arbiter.StatementEnvelope {
	return arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: "0x00000000000000000000000000000000000000a1", ClientSeq: 42, ClientNonce: "9f1c"},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             "INSERT INTO db.t FORMAT Native",
		SQLHash:         "0x" + string(bytes.Repeat([]byte("11"), 32)),
		SettingsHash:    "0x" + string(bytes.Repeat([]byte("22"), 32)),
		PayloadRef:      "ref-1",
		PayloadHash:     "0x" + string(bytes.Repeat([]byte("44"), 32)),
		PayloadLength:   3,
		TargetTableID:   "db.t",
		UserJWS:         "h.p.s",
		EnvelopeVersion: 2,
		NetworkID:       "net",
		PayloadFormat:   "clickhouse-native-data-v1",
		ClientRevision:  54460,
		SchemaHash:      "0x" + string(bytes.Repeat([]byte("33"), 32)),
		RowIDProfileID:  "housegate-row-id-v1",
	}
}

func legacyGoldenUpdate() arbiter.ConsensusParamsUpdate {
	return arbiter.ConsensusParamsUpdate{
		NetworkID: "net", GenesisSnapshotID: "0xgenesis", ExpectedEpoch: 1, PreviousParamsDigest: "0xdigest",
		AuthorityAddresses: []string{"0x00000000000000000000000000000000000000b2"}, MaxWriters: 1, ExpectedPromotionSeq: 7,
		TableRegistry: &arbiter.TableRegistryParams{ChainID: 7892301, DatabasesContract: "0x00000000000000000000000000000000000000d1",
			SIIndexerID: 0, ActivationBlock: 100, Confirmation: arbiter.TableRegistryConfirmationSafe},
	}
}

// TestLegacyCommandBytesAreFrozen pins the exact RaftCommand bytes this module
// encodes for the command kinds that exist before client lanes are activated
// (housegate spec 2026-10-09 §9.3 "strict decoder"). Every hex value was
// captured from the release before client lanes. The previous release's
// wire.Decode accepts exactly these bytes; an encoder that emitted even an
// empty new field would make every not-yet-upgraded voter reject the entry.
// Never regenerate these values: a diff here is a consensus break.
func TestLegacyCommandBytesAreFrozen(t *testing.T) {
	for _, tc := range []struct {
		name string
		cmd  Command
		want string
	}{
		{"submit_statement_legacy_id", Command{SubmitStatement: &SubmitStatement{Envelope: legacyGoldenEnvelope()}}, "0abe030abb030a340a2a307830303030303030303030303030303030303030303030303030303030303030303030303030306131102a1a043966316310011a1e494e5345525420494e544f2064622e7420464f524d4154204e617469766522423078313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131313131312a4230783232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232323232057265662d313a4230783434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343434343440034a0464622e745205682e702e73580262036e65747219636c69636b686f7573652d6e61746976652d646174612d763178bca9038201423078333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333333338a0113686f757365676174652d726f772d69642d7631"},
		{"register_rc_legacy_id", Command{RegisterRC: &RegisterRC{RC: arbiter.RCRecord{StatementID: legacyGoldenEnvelope().StatementID, SourceNode: "s1", SourceClaimRoot: "0xroot"}}}, "22440a420a340a2a307830303030303030303030303030303030303030303030303030303030303030303030303030306131102a1a04396631631202733122063078726f6f74"},
		{"register_verifier", Command{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "v1", Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: bytes.Repeat([]byte{7}, 32), DialAddr: "v1:7080"}}}, "7a340a320a0276311201011a200707070707070707070707070707070707070707070707070707070707070707220776313a37303830"},
		{"register_snode", Command{RegisterNode: &RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}}, "7a090a070a027331120102"},
		{"update_consensus_params_with_registry", Command{UpdateConsensusParams: &UpdateConsensusParams{Update: legacyGoldenUpdate(), AuthorityJWS: "h.p.s"}}, "920191010a87010a036e65741209307867656e657369731801220830786469676573742a2a307830303030303030303030303030303030303030303030303030303030303030303030303030306232300138074a3908cddae103122a30783030303030303030303030303030303030303030303030303030303030303030303030303030643120642a04736166651205682e702e73"},
	} {
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
