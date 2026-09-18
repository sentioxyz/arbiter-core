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
	"time"

	"github.com/housegate/housegate/pkg/auth"
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

type snapshotIdentityFixture struct {
	Account    string
	Input      replay.SnapshotQueryInput
	InputRoot  string `json:"input_root"`
	Identities []struct {
		Iat         int64
		UserJWS     string `json:"user_jws"`
		UserJWSHash string `json:"user_jws_hash"`
	}
}

func loadSnapshotIdentityFixture(t *testing.T) snapshotIdentityFixture {
	t.Helper()
	raw, err := os.ReadFile("testdata/snapshot_query_identity_v1.json")
	if err != nil {
		t.Fatal(err)
	}
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != "fce47e90a772cbe648c657844bb10d04567ba6f9196718d596ffd6f557c7a23f" {
		t.Fatal("immutable A1 identity fixture changed")
	}
	var f snapshotIdentityFixture
	if err = json.Unmarshal(raw, &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func verifySnapshotQueryEnvelopeProduction(envelope replay.SnapshotQueryEnvelope) (string, error) {
	if err := replay.ValidateSnapshotQueryInput(envelope.Input); err != nil {
		return "", fmt.Errorf("validate complete input: %w", err)
	}
	root, err := replay.SnapshotQueryInputRoot(envelope.Input)
	if err != nil {
		return "", fmt.Errorf("recompute input root: %w", err)
	}
	if root != envelope.InputRoot {
		return "", fmt.Errorf("input_root mismatch: got %s want %s", envelope.InputRoot, root)
	}
	want := auth.JWSStatementPayloadV3{
		Purpose:   auth.StatementPurposeV3,
		Binding:   envelope.Input.Binding,
		InputRoot: root,
	}
	account, err := auth.VerifyStatementV3Signature(envelope.UserJWS, want)
	if err != nil {
		return "", err
	}
	if account != envelope.Input.Binding.ClientAccount {
		return "", fmt.Errorf("client_account does not match signature")
	}
	return account, nil
}

// The independent payload projection and key recovery below remain fixture
// provenance checks. The production API is then exercised after protobuf
// transport, complete input validation and input-root recomputation.
func TestSnapshotQueryValidSignatureIdentityFixture(t *testing.T) {
	f := loadSnapshotIdentityFixture(t)
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
		expected := snapshotIdentityPayload{"housegate-statement-v3", id.Iat, f.Input.Binding, root}
		if err = checkSnapshotIdentityPayload(payload, expected); err != nil {
			t.Fatal(err)
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
		account, err := verifySnapshotQueryEnvelopeProduction(got)
		if err != nil || account != got.Input.Binding.ClientAccount || account != f.Account {
			t.Fatalf("production verifier account=%q err=%v", account, err)
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

func TestSnapshotQueryProductionVerifierRejectsTransportTampering(t *testing.T) {
	f := loadSnapshotIdentityFixture(t)
	base := replay.SnapshotQueryEnvelope{Input: f.Input, InputRoot: f.InputRoot, UserJWS: f.Identities[0].UserJWS}
	otherAccount := "0x1111111111111111111111111111111111111111"
	tests := []struct {
		name      string
		wantError string
		recompute bool
		mutate    func(*replay.SnapshotQueryEnvelope)
	}{
		{"binding read root", "read_set_root", true, func(v *replay.SnapshotQueryEnvelope) {
			v.Input.ReadSet.Tables = append(v.Input.ReadSet.Tables, replay.SnapshotReadTable{
				Database:       "tampered",
				Table:          "read_set",
				TableID:        "tampered-read-set",
				SchemaHash:     replay.DigestString("tampered read schema"),
				PartitionRoots: []replay.PartitionCommitment{},
				ActiveParts:    []replay.SnapshotReadPart{},
			})
			v.Input.Binding.ReadSetRoot, _ = replay.SnapshotQueryReadSetRoot(v.Input.ReadSet)
		}},
		{"pin snapshot", "read_snapshot.snapshot_id", true, func(v *replay.SnapshotQueryEnvelope) {
			v.Input.Binding.ReadSnapshot.SnapshotID += "-tampered"
			v.Input.ReadSet.ReadSnapshot.SnapshotID = v.Input.Binding.ReadSnapshot.SnapshotID
			v.Input.Binding.ReadSetRoot, _ = replay.SnapshotQueryReadSetRoot(v.Input.ReadSet)
		}},
		{"schema", "read_snapshot.schema_root", true, func(v *replay.SnapshotQueryEnvelope) {
			v.Input.Binding.ReadSnapshot.SchemaRoot = replay.DigestString("tampered schema")
			v.Input.Binding.SchemaRoot = v.Input.Binding.ReadSnapshot.SchemaRoot
			v.Input.ReadSet.ReadSnapshot.SchemaRoot = v.Input.Binding.ReadSnapshot.SchemaRoot
			v.Input.Binding.ReadSetRoot, _ = replay.SnapshotQueryReadSetRoot(v.Input.ReadSet)
		}},
		{"account", "client_account", true, func(v *replay.SnapshotQueryEnvelope) {
			v.Input.Binding.ClientAccount = otherAccount
			v.Input.Binding.StatementID = otherAccount + ":1:fixture"
		}},
		{"query profile", "query_profile_id", true, func(v *replay.SnapshotQueryEnvelope) {
			v.Input.Binding.QueryProfileID += "-active"
		}},
		{"executor profile", "executor_profile_id", true, func(v *replay.SnapshotQueryEnvelope) {
			v.Input.Binding.ExecutorProfileID += "-active"
		}},
		{"history", "read_snapshot.safe_block_seq", true, func(v *replay.SnapshotQueryEnvelope) {
			v.Input.Binding.ReadSnapshot.SafeBlockSeq++
			v.Input.ReadSet.ReadSnapshot.SafeBlockSeq = v.Input.Binding.ReadSnapshot.SafeBlockSeq
			v.Input.Binding.ReadSetRoot, _ = replay.SnapshotQueryReadSetRoot(v.Input.ReadSet)
		}},
		{"generation", "fencing_generation", true, func(v *replay.SnapshotQueryEnvelope) {
			v.Input.Binding.FencingGeneration++
		}},
		{"input root", "input_root mismatch", false, func(v *replay.SnapshotQueryEnvelope) {
			v.InputRoot = replay.DigestString("tampered input root")
		}},
		{"token", "statement v3", false, func(v *replay.SnapshotQueryEnvelope) {
			v.UserJWS += "x"
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutated := throughPB(t, base, wire.SnapshotQueryEnvelopeToPB, wire.SnapshotQueryEnvelopeFromPB)
			tc.mutate(&mutated)
			if tc.recompute {
				var err error
				mutated.InputRoot, err = replay.SnapshotQueryInputRoot(mutated.Input)
				if err != nil {
					t.Fatalf("tamper must remain a structurally valid input: %v", err)
				}
			}
			got := throughPB(t, mutated, wire.SnapshotQueryEnvelopeToPB, wire.SnapshotQueryEnvelopeFromPB)
			if _, err := verifySnapshotQueryEnvelopeProduction(got); err == nil || !strings.Contains(err.Error(), tc.wantError) {
				t.Fatalf("expected %q refusal, got %v", tc.wantError, err)
			}
		})
	}
}

func TestSnapshotQueryProductionVerifierSeparatesTokenDomains(t *testing.T) {
	f := loadSnapshotIdentityFixture(t)
	want := auth.JWSStatementPayloadV3{
		Purpose:   auth.StatementPurposeV3,
		Binding:   f.Input.Binding,
		InputRoot: f.InputRoot,
	}
	signer, err := auth.NewRelaySigner(strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	v2, err := signer.SignStatementV2(auth.JWSStatementPayloadV2{Iat: f.Identities[0].Iat})
	if err != nil {
		t.Fatal(err)
	}
	query, err := signer.SignToken("SELECT 1")
	if err != nil {
		t.Fatal(err)
	}
	peer, err := signer.SignPeerLogin("fixture-peer", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for name, token := range map[string]string{"v2": v2, "ordinary query": query, "peer": peer} {
		t.Run(name, func(t *testing.T) {
			if _, err := auth.VerifyStatementV3Signature(token, want); err == nil {
				t.Fatal("accepted token from another domain")
			}
		})
	}
}

func TestSnapshotQueryHistoricalProfileSignatureIsNotCurrentAuthorization(t *testing.T) {
	f := loadSnapshotIdentityFixture(t)
	wantHistorical := auth.JWSStatementPayloadV3{
		Purpose:   auth.StatementPurposeV3,
		Binding:   f.Input.Binding,
		InputRoot: f.InputRoot,
	}
	account, err := auth.VerifyStatementV3Signature(f.Identities[0].UserJWS, wantHistorical)
	if err != nil || account != f.Account {
		t.Fatalf("historical identity must verify independently of current time: %q, %v", account, err)
	}
	wantActive := wantHistorical
	wantActive.Binding.QueryProfileID += "-active"
	if _, err := auth.VerifyStatementV3Signature(f.Identities[0].UserJWS, wantActive); err == nil || !strings.Contains(err.Error(), "query_profile_id") {
		t.Fatalf("historical signature granted different active profile: %v", err)
	}
}

// The full ordered A2 payload is frozen independently of signature validity.
type snapshotIdentityPayload struct {
	Purpose   string                      `json:"purpose"`
	Iat       int64                       `json:"iat"`
	Binding   replay.SnapshotQueryBinding `json:"binding"`
	InputRoot string                      `json:"input_root"`
}

func checkSnapshotIdentityPayload(payload []byte, expected snapshotIdentityPayload) error {
	canonical, err := json.Marshal(expected)
	if err != nil {
		return err
	}
	if !bytes.Equal(payload, canonical) {
		return fmt.Errorf("complete four-field canonical payload differs (purpose, iat, binding, input_root)")
	}
	return nil
}

func TestSnapshotQueryIdentityPayloadRejectsIncompleteOrNoncanonical(t *testing.T) {
	fixture := loadSnapshotIdentityFixture(t)
	if err := replay.ValidateSnapshotQueryInput(fixture.Input); err != nil {
		t.Fatal(err)
	}
	root, err := replay.SnapshotQueryInputRoot(fixture.Input)
	rootEqual(t, fixture.InputRoot, root, err)
	expected := snapshotIdentityPayload{"housegate-statement-v3", 1789550000, fixture.Input.Binding, root}
	want := auth.JWSStatementPayloadV3{Purpose: auth.StatementPurposeV3, Binding: fixture.Input.Binding, InputRoot: root}
	canonical, err := json.Marshal(expected)
	if err != nil {
		t.Fatal(err)
	}
	missing, _ := json.Marshal(struct {
		Purpose string                      `json:"purpose"`
		Iat     int64                       `json:"iat"`
		Binding replay.SnapshotQueryBinding `json:"binding"`
	}{expected.Purpose, expected.Iat, expected.Binding})
	wrong := expected
	wrong.InputRoot = "0xwrong"
	wrongBytes, _ := json.Marshal(wrong)
	reordered, _ := json.Marshal(struct {
		Iat       int64                       `json:"iat"`
		Purpose   string                      `json:"purpose"`
		Binding   replay.SnapshotQueryBinding `json:"binding"`
		InputRoot string                      `json:"input_root"`
	}{expected.Iat, expected.Purpose, expected.Binding, expected.InputRoot})
	extra := append(append([]byte{}, canonical[:len(canonical)-1]...), []byte(`,"extra":0}`)...)
	key, err := crypto.HexToECDSA(strings.Repeat("a", 64))
	if err != nil {
		t.Fatal(err)
	}
	for name, payload := range map[string][]byte{"missing input_root": missing, "wrong input_root": wrongBytes, "reordered fields": reordered, "extra field": extra} {
		t.Run(name, func(t *testing.T) {
			signing := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"ES256K","typ":"JWT"}`)) + "." + base64.RawURLEncoding.EncodeToString(payload)
			digest := crypto.Keccak256([]byte(signing))
			sig, err := crypto.Sign(digest, key)
			if err != nil {
				t.Fatal(err)
			}
			pub, err := crypto.SigToPub(digest, sig)
			if err != nil {
				t.Fatal(err)
			}
			if strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()) != fixture.Input.Binding.ClientAccount || !crypto.VerifySignature(crypto.FromECDSAPub(pub), digest, sig[:64]) {
				t.Fatal("negative signature must remain cryptographically valid")
			}
			// Match the compact transport profile, then inspect its actual decoded bytes.
			sig[64] += 27
			token := signing + "." + base64.RawURLEncoding.EncodeToString(sig)
			decoded, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[1])
			if err != nil {
				t.Fatal(err)
			}
			if err = checkSnapshotIdentityPayload(decoded, expected); err == nil {
				t.Fatal("valid signature admitted malformed complete payload")
			}
			if _, err = auth.VerifyStatementV3Signature(token, want); err == nil {
				t.Fatal("production verifier admitted validly signed malformed payload")
			}
		})
	}
}
