package conformance

import (
	"bytes"
	"encoding/json"
	"os"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// goldenStatementsRoot freezes CanonicalDigest(DomainL3Statements, envelopes)
// for the fixture below. CanonicalDigest is SHA-256 over json.Marshal, and
// encoding/json emits struct fields in DECLARATION order, so this constant is
// a function of arbiter.StatementEnvelope's field order as well as its field
// set. Every historical statements_root — hence every historical L3 ChainHash,
// hence every anchored value — depends on that order, so it is a consensus
// parameter and changing it is a versioned migration, never a refactor.
//
// If this test fails: do NOT paste the new digest in. Revert the struct change.
const goldenStatementsRoot = "0x72683a2f8d7c288579d12dc232dda2571d6f62313b534b8fd7ab7531a5e82921"

type statementEnvelopeGolden struct {
	HashProfile string          `json:"hash_profile"`
	Domain      string          `json:"domain"`
	Digest      string          `json:"digest"`
	Value       json.RawMessage `json:"value"`
}

// goldenEnvelopes is the fixture as Go values. It must stay in lockstep with
// testdata/statement_envelope_golden.json; the test compares the marshalled
// bytes so a diff of that file shows WHAT changed, not only that a hash moved.
func goldenEnvelopes() []arbiter.StatementEnvelope {
	return []arbiter.StatementEnvelope{
		{
			StatementID: arbiter.StatementID{
				ClientAccount: "0x00000000000000000000000000000000000000a1",
				ClientSeq:     11,
				ClientNonce:   "00112233445566778899aabbccddeeff",
			},
			StatementKind:   arbiter.StatementKindInsert,
			SQL:             "INSERT INTO db1.events FORMAT Native",
			SQLHash:         "0x1111111111111111111111111111111111111111111111111111111111111111",
			SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
			PayloadRef:      "payload/golden-1",
			PayloadHash:     "0x2222222222222222222222222222222222222222222222222222222222222222",
			PayloadLength:   4096,
			TargetTableID:   "db1.events",
			UserJWS:         "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjF9.Z29sZGVuLXNpZ25hdHVyZS0x",
			EnvelopeVersion: 2,
			NetworkID:       "arbiter-golden-net",
			KeeperShardID:   0,
			PayloadFormat:   "clickhouse-native-data-v1",
			ClientRevision:  54460,
			SchemaHash:      "0x3333333333333333333333333333333333333333333333333333333333333333",
			RowIDProfileID:  "housegate-row-id-v1",
		},
		{
			StatementID: arbiter.StatementID{
				ClientAccount: "0x00000000000000000000000000000000000000b2",
				ClientSeq:     12,
				ClientNonce:   "ffeeddccbbaa99887766554433221100",
			},
			StatementKind:   arbiter.StatementKindInsert,
			SQL:             "INSERT INTO db1.metrics FORMAT Native",
			SQLHash:         "0x4444444444444444444444444444444444444444444444444444444444444444",
			SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
			PayloadRef:      "payload/golden-2",
			PayloadHash:     "0x5555555555555555555555555555555555555555555555555555555555555555",
			PayloadLength:   8192,
			TargetTableID:   "db1.metrics",
			UserJWS:         "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ.eyJnb2xkZW4iOjJ9.Z29sZGVuLXNpZ25hdHVyZS0y",
			EnvelopeVersion: 2,
			NetworkID:       "arbiter-golden-net",
			KeeperShardID:   0,
			PayloadFormat:   "clickhouse-native-data-v1",
			ClientRevision:  54460,
			SchemaHash:      "0x6666666666666666666666666666666666666666666666666666666666666666",
			RowIDProfileID:  "housegate-row-id-v1",
		},
	}
}

func loadStatementEnvelopeGolden(t *testing.T) statementEnvelopeGolden {
	t.Helper()
	raw, err := os.ReadFile("testdata/statement_envelope_golden.json")
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var g statementEnvelopeGolden
	if err := json.Unmarshal(raw, &g); err != nil {
		t.Fatalf("decode golden: %v", err)
	}
	return g
}

func TestStatementEnvelopeCanonicalEncodingIsFrozen(t *testing.T) {
	g := loadStatementEnvelopeGolden(t)
	if g.HashProfile != "housegate-replay-mvp-v0" || g.Domain != arbiter.DomainL3Statements {
		t.Fatalf("golden profile/domain = %s/%s", g.HashProfile, g.Domain)
	}
	if g.Digest != goldenStatementsRoot {
		t.Fatalf("testdata digest %s disagrees with the source constant %s; both must move together in a deliberate versioned change", g.Digest, goldenStatementsRoot)
	}

	got, err := json.Marshal(goldenEnvelopes())
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var want bytes.Buffer
	if err := json.Compact(&want, g.Value); err != nil {
		t.Fatalf("compact golden value: %v", err)
	}
	if !bytes.Equal(got, want.Bytes()) {
		t.Fatalf("StatementEnvelope canonical JSON changed.\n got: %s\nwant: %s\n"+
			"encoding/json emits fields in declaration order, so a reorder, rename, retag or new field breaks every historical statements_root. Revert the struct change.", got, want.Bytes())
	}
}

func TestStatementsRootGoldenDigest(t *testing.T) {
	got, err := replay.CanonicalDigest(arbiter.DomainL3Statements, goldenEnvelopes())
	if err != nil {
		t.Fatalf("CanonicalDigest: %v", err)
	}
	if got != goldenStatementsRoot {
		t.Fatalf("statements_root golden drift: got %s want %s\n"+
			"this digest is anchored on L2 through L3BlockHeader.ChainHash. Do not re-bless it: revert whatever changed the encoding.", got, goldenStatementsRoot)
	}
}
