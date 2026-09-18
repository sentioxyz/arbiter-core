package wire

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"google.golang.org/protobuf/encoding/protowire"
)

func unionMessage(tag protowire.Number, body []byte) []byte {
	return protowire.AppendBytes(protowire.AppendTag(nil, tag, protowire.BytesType), body)
}

func TestConsensusSnapshotUnionKeys(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command Command
		key     []byte
	}{
		{"consensus18", Command{UpdateConsensusParams: &UpdateConsensusParams{}}, []byte{0x92, 0x01}},
		{"begin30", Command{BeginSnapshotQuery: &BeginSnapshotQuery{}}, []byte{0xf2, 0x01}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw, err := Encode(tc.command)
			if err != nil || !bytes.HasPrefix(raw, tc.key) {
				t.Fatalf("key: %x, %v", raw, err)
			}
			// Empty wire messages prove variant selection, not runtime admission.
			got, err := Decode(append(append([]byte(nil), tc.key...), 0))
			if err != nil || (got.UpdateConsensusParams != nil) != (tc.command.UpdateConsensusParams != nil) || (got.BeginSnapshotQuery != nil) != (tc.command.BeginSnapshotQuery != nil) {
				t.Fatalf("variant: %+v, %v", got, err)
			}
		})
	}
	if _, err := Encode(Command{UpdateConsensusParams: &UpdateConsensusParams{}, BeginSnapshotQuery: &BeginSnapshotQuery{}}); err == nil {
		t.Fatal("encoded consensus and Begin together")
	}
}

func TestConsensusSnapshotUnionRejectsMalformedBytes(t *testing.T) {
	consensus, begin := unionMessage(18, nil), unionMessage(30, nil)
	for name, raw := range map[string][]byte{
		"mixed18_then30":                append(append([]byte(nil), consensus...), begin...),
		"mixed30_then18":                append(append([]byte(nil), begin...), consensus...),
		"duplicate18":                   append(append([]byte(nil), consensus...), consensus...),
		"duplicate30":                   append(append([]byte(nil), begin...), begin...),
		"wrongwire18":                   {0x90, 0x01, 0},
		"wrongwire30":                   {0xf0, 0x01, 0},
		"unallocated28":                 unionMessage(28, nil),
		"unallocated29":                 unionMessage(29, nil),
		"unknown_with_consensus":        append(append([]byte(nil), consensus...), unionMessage(31, nil)...),
		"unknown_with_begin":            append(append([]byte(nil), begin...), unionMessage(31, nil)...),
		"consensus_unknown_nested":      unionMessage(18, unionMessage(99, nil)),
		"begin_unknown_nested":          unionMessage(30, unionMessage(99, nil)),
		"consensus_duplicate_update":    unionMessage(18, []byte{0x0a, 0, 0x0a, 0}),
		"begin_duplicate_request":       unionMessage(30, []byte{0x0a, 0, 0x0a, 0}),
		"consensus_duplicate_scalar":    unionMessage(18, unionMessage(1, []byte{0x18, 0, 0x18, 0})),
		"begin_duplicate_scalar":        unionMessage(30, unionMessage(1, []byte{0x0a, 0, 0x0a, 0})),
		"consensus_wrongwire_signature": unionMessage(18, []byte{0x10, 0}),
		"begin_wrongwire_request":       unionMessage(30, []byte{0x08, 0}),
		"consensus_unknown_update":      unionMessage(18, unionMessage(1, unionMessage(99, nil))),
		"begin_unknown_request":         unionMessage(30, unionMessage(1, unionMessage(99, nil))),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := Decode(raw); err == nil {
				t.Fatalf("accepted malformed command %x", raw)
			}
		})
	}
}

func TestConsensusSnapshotUnionRepeatedScalars(t *testing.T) {
	// Repeated strings retain duplicates, order, and explicitly empty values;
	// canonical address-set validation remains the authority layer's job.
	addresses := []string{"second", "", "first", "second"}
	want := Command{UpdateConsensusParams: &UpdateConsensusParams{
		Update: arbiter.ConsensusParamsUpdate{AuthorityAddresses: addresses}, AuthorityJWS: "header.payload.signature",
	}}
	mustRoundTrip(t, want)

	// NodeRegistration.roles is a repeated enum. Both protobuf representations
	// must survive the same descriptor precheck used by consensus and Begin.
	for name, roles := range map[string][]byte{
		"unpacked": {0x10, 1, 0x10, 2},
		"packed":   {0x12, 2, 1, 2},
	} {
		t.Run(name, func(t *testing.T) {
			got, err := Decode(unionMessage(15, unionMessage(1, roles)))
			if err != nil || got.RegisterNode == nil || !reflect.DeepEqual(got.RegisterNode.Registration.Roles, []arbiter.NodeRole{1, 2}) {
				t.Fatalf("repeated enum: %+v, %v", got, err)
			}
		})
	}
}

func TestConsensusSnapshotUnionNonNilJSON(t *testing.T) {
	command := Command{UpdateConsensusParams: &UpdateConsensusParams{
		Update: arbiter.ConsensusParamsUpdate{NetworkID: "net", GenesisSnapshotID: "genesis", ExpectedEpoch: 1, PreviousParamsDigest: "prior", AuthorityAddresses: []string{"a", "b"}, MaxWriters: 2, ExpectedPromotionSeq: 3}, AuthorityJWS: "signed",
	}}
	// Exact main rendering for a nonnil consensus command. The separately
	// retained legacy fixture freezes the deliberately chosen nil omission.
	const want = `{"SubmitStatement":null,"SealL3Block":null,"MarkReplaying":null,"RegisterRC":null,"RecordAttestation":null,"RecordByteSideScan":null,"RecordAnchorFinality":null,"RecordPromotionIssued":null,"RecordPromotionAck":null,"PublishSafeSnapshot":null,"ScheduleUnsafeCleanup":null,"RecordCleanupAck":null,"OpenChallenge":null,"ResolveChallenge":null,"RegisterNode":null,"MarkActive":null,"EvictNode":null,"UpdateConsensusParams":{"Update":{"network_id":"net","genesis_snapshot_id":"genesis","expected_epoch":1,"previous_params_digest":"prior","authority_addresses":["a","b"],"max_writers":2,"expected_promotion_seq":3},"AuthorityJWS":"signed"}}`
	raw, err := json.Marshal(command)
	if err != nil || string(raw) != want {
		t.Fatalf("nonnil consensus JSON changed: %s, %v", raw, err)
	}
}
