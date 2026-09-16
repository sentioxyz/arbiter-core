package conformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/crypto"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/wire"
	"google.golang.org/protobuf/proto"
)

type queryVector struct {
	Name, Domain  string
	Value         json.RawMessage
	CanonicalJSON string `json:"canonical_json"`
	Hash          string
}
type queryFixture struct {
	Name                string
	Input               replay.SnapshotQueryInput
	CanonicalJSON       string `json:"canonical_input_json"`
	InputRoot           string `json:"input_root"`
	ReadSetRoot         string `json:"read_set_root"`
	ReadSetJSON         string `json:"canonical_read_set_json"`
	Statement           replay.SnapshotQueryStatement
	StatementRoot       string `json:"statement_root"`
	StatementJSON       string `json:"canonical_statement_root_json"`
	Receipt             replay.SnapshotQueryReceipt
	ReceiptHash         string `json:"receipt_hash"`
	ReceiptJSON         string `json:"canonical_receipt_json"`
	Manifest            replay.SafeSnapshotManifest
	Genesis             replay.SafeSnapshotManifest
	ErrorContains       string `json:"error_contains"`
	Contracts           []queryVector
	ReservationStatuses []replay.SnapshotQueryReservationStatus `json:"reservation_statuses"`
	Statuses            []replay.SnapshotQueryStatus
}

func snapshotFixtures(t *testing.T) []queryFixture {
	t.Helper()
	b, err := os.ReadFile("testdata/snapshot_query_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(b)) != "3558d62035a23f4e572d09a59f82bc0ebac4f9cae175ccee127b0034600ed137" {
		t.Fatal("authoritative HG fixture changed")
	}
	var out []queryFixture
	if err = json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	return out
}
func throughPB[I any, P proto.Message](t *testing.T, in I, to func(I) P, from func(P) I) I {
	t.Helper()
	m := to(in)
	b, err := proto.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	out := m.ProtoReflect().Type().New().Interface().(P)
	if err = proto.Unmarshal(b, out); err != nil {
		t.Fatal(err)
	}
	return from(out)
}
func equalJSON(t *testing.T, want string, v any) {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil || string(b) != want {
		t.Fatalf("canonical bytes differ: %s, want %s, %v", b, want, err)
	}
}
func rootEqual(t *testing.T, want, got string, err error) {
	t.Helper()
	if err != nil || got != want {
		t.Fatalf("root=%s want=%s err=%v", got, want, err)
	}
}
func literalDigest(domain, literal string) string {
	return fmt.Sprintf("0x%x", sha256.Sum256([]byte("housegate-replay-mvp-v0:"+domain+"\x00"+literal)))
}
func TestSnapshotQueryPairedGolden(t *testing.T) {
	for _, f := range snapshotFixtures(t) {
		t.Run(f.Name, func(t *testing.T) {
			in := throughPB(t, f.Input, wire.SnapshotQueryInputToPB, wire.SnapshotQueryInputFromPB)
			canonical, err := replay.CanonicalSnapshotQueryInput(in)
			if f.ErrorContains != "" {
				if err == nil || !strings.Contains(err.Error(), f.ErrorContains) {
					t.Fatalf("got %v want %s", err, f.ErrorContains)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			equalJSON(t, f.CanonicalJSON, canonical)
			equalJSON(t, f.ReadSetJSON, canonical.ReadSet)
			root, err := replay.SnapshotQueryInputRoot(in)
			rootEqual(t, f.InputRoot, root, err)
			root, err = replay.SnapshotQueryReadSetRoot(in.ReadSet)
			rootEqual(t, f.ReadSetRoot, root, err)
			st := throughPB(t, f.Statement, wire.SnapshotQueryStatementToPB, wire.SnapshotQueryStatementFromPB)
			root, err = replay.SnapshotQueryStatementRoot(st)
			rootEqual(t, f.StatementRoot, root, err)
			equalJSON(t, f.StatementJSON, struct {
				StatementSeq uint64 `json:"statement_seq"`
				InputRoot    string `json:"input_root"`
				UserJWS      string `json:"user_jws"`
			}{st.StatementSeq, st.Envelope.InputRoot, st.Envelope.UserJWS})
			receipt := throughPB(t, f.Receipt, wire.SnapshotQueryReceiptToPB, wire.SnapshotQueryReceiptFromPB)
			root, err = receipt.Hash()
			rootEqual(t, f.ReceiptHash, root, err)
			// An independent receipt projection verifies literal bytes. Hints must survive
			// transport and disappear only from the new commitment projection.
			if !reflect.DeepEqual(receipt.AffectedParts, f.Receipt.AffectedParts) {
				t.Fatal("transport hints lost")
			}
			for i := range receipt.AffectedParts {
				receipt.AffectedParts[i].StorageRefs = nil
			}
			sort.Slice(receipt.AffectedParts, func(i, j int) bool {
				a, b := receipt.AffectedParts[i], receipt.AffectedParts[j]
				if a.TableID != b.TableID {
					return a.TableID < b.TableID
				}
				if a.PartitionID != b.PartitionID {
					return a.PartitionID < b.PartitionID
				}
				return a.PartName < b.PartName
			})
			sort.Slice(receipt.PartitionCommitmentsAfter, func(i, j int) bool {
				a, b := receipt.PartitionCommitmentsAfter[i], receipt.PartitionCommitmentsAfter[j]
				if a.TableID != b.TableID {
					return a.TableID < b.TableID
				}
				return a.PartitionID < b.PartitionID
			})
			equalJSON(t, f.ReceiptJSON, receipt)
			for _, v := range []struct{ d, b, h string }{{"snapshot-query-input-v1", f.CanonicalJSON, f.InputRoot}, {"snapshot-query-read-set-v1", f.ReadSetJSON, f.ReadSetRoot}, {"snapshot-query-statement-root-v1", f.StatementJSON, f.StatementRoot}, {"snapshot-query-receipt-v1", f.ReceiptJSON, f.ReceiptHash}} {
				if literalDigest(v.d, v.b) != v.h {
					t.Fatal("independent literal digest")
				}
			}
			manifest := throughPB(t, f.Manifest, wire.ManifestToPB, wire.ManifestFromPB)
			if err = replay.ValidateSnapshotQueryManifest(canonical, manifest); err != nil {
				t.Fatal(err)
			}
			if err = manifest.Validate(); err != nil {
				t.Fatal(err)
			}
			if err = throughPB(t, f.Genesis, wire.ManifestToPB, wire.ManifestFromPB).Validate(); err != nil {
				t.Fatal(err)
			}
			for _, status := range f.ReservationStatuses {
				got := throughPB(t, status, wire.SnapshotQueryReservationStatusToPB, wire.SnapshotQueryReservationStatusFromPB)
				if len(status.TerminalProof) == 0 {
					status.TerminalProof = nil
				}
				a, _ := json.Marshal(status)
				equalJSON(t, string(a), got)
			}
			for _, status := range f.Statuses {
				got := throughPB(t, status, wire.SnapshotQueryStatusToPB, wire.SnapshotQueryStatusFromPB)
				if len(status.TerminalProof) == 0 {
					status.TerminalProof = nil
				}
				a, _ := json.Marshal(status)
				equalJSON(t, string(a), got)
			}
		})
	}
}
func TestSnapshotQueryPairedAncillaryGolden(t *testing.T) {
	for _, v := range snapshotFixtures(t)[0].Contracts {
		t.Run(v.Name, func(t *testing.T) {
			if literalDigest(v.Domain, v.CanonicalJSON) != v.Hash {
				t.Fatal("literal digest")
			}
			var got interface{ Hash() (string, error) }
			switch v.Domain {
			case "snapshot-query-output-v1":
				var x replay.SnapshotQueryOutputCommitment
				if err := json.Unmarshal(v.Value, &x); err != nil {
					t.Fatal(err)
				}
				got = x // Go-only projection: only the root travels.
			case "snapshot-query-profile-v1":
				var x replay.QueryProfileRecord
				if err := json.Unmarshal(v.Value, &x); err != nil {
					t.Fatal(err)
				}
				got = throughPB(t, x, wire.QueryProfileRecordToPB, wire.QueryProfileRecordFromPB)
			case "snapshot-query-artifact-set-v1":
				var x replay.SnapshotArtifactSet
				if err := json.Unmarshal(v.Value, &x); err != nil {
					t.Fatal(err)
				}
				got = throughPB(t, x, wire.SnapshotArtifactSetToPB, wire.SnapshotArtifactSetFromPB)
			case "snapshot-query-artifact-ready-v1":
				var x replay.SnapshotArtifactReady
				if err := json.Unmarshal(v.Value, &x); err != nil {
					t.Fatal(err)
				}
				got = throughPB(t, x, wire.SnapshotArtifactReadyToPB, wire.SnapshotArtifactReadyFromPB)
			case "snapshot-query-abort-v1":
				var x replay.SnapshotQueryAbortRecord
				if err := json.Unmarshal(v.Value, &x); err != nil {
					t.Fatal(err)
				}
				got = throughPB(t, x, wire.SnapshotQueryAbortRecordToPB, wire.SnapshotQueryAbortRecordFromPB)
			case "snapshot-query-claim-v1":
				var x replay.SnapshotQueryClaim
				if err := json.Unmarshal(v.Value, &x); err != nil {
					t.Fatal(err)
				}
				got = throughPB(t, x, wire.SnapshotQueryClaimToPB, wire.SnapshotQueryClaimFromPB)
			case "executor-profile-transition-v1":
				var x replay.ExecutorProfileTransition
				if err := json.Unmarshal(v.Value, &x); err != nil {
					t.Fatal(err)
				}
				got = throughPB(t, x, wire.ExecutorProfileTransitionToPB, wire.ExecutorProfileTransitionFromPB)
			case "snapshot-query-receipt-v1":
				var x replay.SnapshotQueryReceipt
				if err := json.Unmarshal(v.Value, &x); err != nil {
					t.Fatal(err)
				}
				got = throughPB(t, x, wire.SnapshotQueryReceiptToPB, wire.SnapshotQueryReceiptFromPB)
			default:
				t.Fatal(v.Domain)
			}
			equalJSON(t, v.CanonicalJSON, got)
			h, err := got.Hash()
			rootEqual(t, v.Hash, h, err)
			if claim, ok := got.(replay.SnapshotQueryClaim); ok && h == claim.ComputedStateRoot {
				t.Fatal("compound claim digest conflated with state root")
			}
		})
	}
}
func TestSnapshotQuerySignedInputPresence(t *testing.T) {
	good := snapshotFixtures(t)[0].CanonicalJSON
	for _, field := range []string{`"payload_length":0,`, `"payload_hash":"",`, `"statements":[],`, `"user_jws":"",`, `"unknown":null,`} {
		raw := []byte("{" + field + good[1:])
		var in replay.SnapshotQueryInput
		if json.Unmarshal(raw, &in) == nil {
			t.Fatalf("accepted present foreign %s", field)
		}
		if _, err := replay.DecodeCanonicalSnapshotQueryInput(raw); err == nil {
			t.Fatal("signed transport accepted")
		}
	}
	if _, err := replay.DecodeCanonicalSnapshotQueryInput([]byte(" " + good)); err == nil {
		t.Fatal("noncanonical transport")
	}
	in, err := replay.DecodeCanonicalSnapshotQueryInput([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	got := throughPB(t, in, wire.SnapshotQueryInputToPB, wire.SnapshotQueryInputFromPB)
	b, _ := json.Marshal(got)
	if !bytes.Equal(b, []byte(good)) {
		t.Fatal("canonical empty-array transport changed")
	}
}

// These fixtures prove cryptographic identity and wire preservation only. They
// intentionally do not implement the A2 verifier or C3/C4 admission decisions.
func TestSnapshotQueryValidSignatureIdentityFixture(t *testing.T) {
	raw, err := os.ReadFile("testdata/snapshot_query_identity_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Account    string
		Input      replay.SnapshotQueryInput
		InputRoot  string `json:"input_root"`
		Identities []struct {
			Iat         int64
			UserJWS     string `json:"user_jws"`
			UserJWSHash string `json:"user_jws_hash"`
		}
	}
	if err = json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	if len(f.Identities) != 2 {
		t.Fatal("two identities required")
	}
	root, err := replay.SnapshotQueryInputRoot(f.Input)
	rootEqual(t, f.InputRoot, root, err)
	if f.Account != f.Input.Binding.ClientAccount {
		t.Fatal("bound account")
	}
	var statementRoots []string
	for _, id := range f.Identities {
		pieces := strings.Split(id.UserJWS, ".")
		if len(pieces) != 3 {
			t.Fatal("compact JWS")
		}
		header, err := base64.RawURLEncoding.DecodeString(pieces[0])
		if err != nil || string(header) != `{"alg":"ES256K","typ":"JWT"}` {
			t.Fatal("header")
		}
		payload, err := base64.RawURLEncoding.DecodeString(pieces[1])
		if err != nil {
			t.Fatal(err)
		}
		var claims struct {
			Purpose string                      `json:"purpose"`
			Iat     int64                       `json:"iat"`
			Binding replay.SnapshotQueryBinding `json:"binding"`
		}
		if err = json.Unmarshal(payload, &claims); err != nil {
			t.Fatal(err)
		}
		if claims.Purpose != "housegate-statement-v3" || claims.Iat != id.Iat || claims.Binding != f.Input.Binding {
			t.Fatal("bound payload")
		}
		sig, err := base64.RawURLEncoding.DecodeString(pieces[2])
		if err != nil || len(sig) != 65 || sig[64] < 27 || sig[64] > 28 {
			t.Fatal("signature profile")
		}
		sig[64] -= 27
		hash := crypto.Keccak256([]byte(pieces[0] + "." + pieces[1]))
		pub, err := crypto.SigToPub(hash, sig)
		if err != nil {
			t.Fatal(err)
		}
		if strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()) != f.Account || !crypto.VerifySignature(crypto.FromECDSAPub(pub), hash, sig[:64]) {
			t.Fatal("cryptographic signer mismatch")
		}
		if replay.DigestString(id.UserJWS) != id.UserJWSHash {
			t.Fatal("exact original hash")
		}
		envelope := replay.SnapshotQueryEnvelope{Input: f.Input, InputRoot: f.InputRoot, UserJWS: id.UserJWS}
		got := throughPB(t, envelope, wire.SnapshotQueryEnvelopeToPB, wire.SnapshotQueryEnvelopeFromPB)
		if got.UserJWS != id.UserJWS {
			t.Fatal("original JWS changed")
		}
		statementRoot, err := replay.SnapshotQueryStatementRoot(replay.SnapshotQueryStatement{StatementSeq: 101, Envelope: got})
		if err != nil {
			t.Fatal(err)
		}
		statementRoots = append(statementRoots, statementRoot)
	}
	if statementRoots[0] == statementRoots[1] {
		t.Fatal("original JWS identity missing from statement root")
	}
	if f.Identities[0].Iat == f.Identities[1].Iat || f.Identities[0].UserJWSHash == f.Identities[1].UserJWSHash {
		t.Fatal("distinct signature identity collapsed")
	}
	for _, tc := range []struct{ name, hash string }{
		{"omitted hash", ""}, {"wrong hash", replay.DigestString("wrong")},
		{"lost rejection then lookup", f.Identities[1].UserJWSHash},
		{"exact original JWS retry", f.Identities[0].UserJWSHash},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := wire.GetSnapshotQueryStatusRequest{NetworkID: f.Input.Binding.NetworkID, KeeperShardID: f.Input.Binding.KeeperShardID, ClientAccount: f.Account, StatementID: f.Input.Binding.StatementID, ExpectedInputRoot: f.InputRoot, ExpectedUserJWSHash: tc.hash}
			got := throughPB(t, request, wire.GetSnapshotQueryStatusRequestToPB, wire.GetSnapshotQueryStatusRequestFromPB)
			if got != request {
				t.Fatal("lookup conflict/retry identity lost")
			}
		})
	}
}
