package wire

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/housegate/housegate/pkg/replay"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"reflect"
	"strings"
	"testing"
)

// Every scalar gets its own value, including every nested field and list item.
// Compare each Go leaf to the corresponding protobuf descriptor field before
// round-trip: symmetric conversion mistakes cannot cancel one another.
func seedSnapshot(v reflect.Value, path string, serial *uint64) {
	*serial++
	switch v.Kind() {
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			seedSnapshot(v.Field(i), path+"."+v.Type().Field(i).Name, serial)
		}
	case reflect.Pointer:
		v.Set(reflect.New(v.Type().Elem()))
		seedSnapshot(v.Elem(), path, serial)
	case reflect.Slice:
		v.Set(reflect.MakeSlice(v.Type(), 2, 2))
		for i := 0; i < 2; i++ {
			seedSnapshot(v.Index(i), fmt.Sprintf("%s[%d]", path, i), serial)
		}
	case reflect.String:
		v.SetString(path)
	case reflect.Uint8:
		v.SetUint(*serial%250 + 1)
	case reflect.Uint32, reflect.Uint64:
		v.SetUint(*serial + 100)
	case reflect.Bool:
		v.SetBool(true)
	default:
		panic(v.Type())
	}
}
func assertSnapshotLeaves(t *testing.T, v reflect.Value, m protoreflect.Message) {
	t.Helper()
	fs := m.Descriptor().Fields()
	if v.NumField() != fs.Len() {
		t.Fatalf("%s field count", v.Type())
	}
	for i := 0; i < v.NumField(); i++ {
		field := v.Type().Field(i)
		tag := strings.Split(field.Tag.Get("json"), ",")[0]
		fd := fs.ByName(protoreflect.Name(tag))
		g := v.Field(i)
		if fd == nil || fd.Number() != protoreflect.FieldNumber(i+1) {
			t.Fatalf("%s.%s order/tag", v.Type(), field.Name)
		}
		p := m.Get(fd)
		var check func(reflect.Value, protoreflect.Value)
		check = func(g reflect.Value, p protoreflect.Value) {
			switch g.Kind() {
			case reflect.Pointer:
				if g.IsNil() {
					if m.Has(fd) {
						t.Fatalf("%s presence", field.Name)
					}
					return
				}
				check(g.Elem(), p)
			case reflect.Struct:
				assertSnapshotLeaves(t, g, p.Message())
			case reflect.String:
				if g.String() != p.String() {
					t.Fatalf("%s = %q, want %q", tag, p.String(), g.String())
				}
			case reflect.Uint32, reflect.Uint64:
				if g.Uint() != p.Uint() {
					t.Fatalf("%s scalar lost", tag)
				}
			case reflect.Bool:
				if g.Bool() != p.Bool() {
					t.Fatalf("%s bool lost", tag)
				}
			case reflect.Slice:
				if g.Type().Elem().Kind() == reflect.Uint8 {
					if !bytes.Equal(g.Bytes(), p.Bytes()) {
						t.Fatalf("%s bytes lost", tag)
					}
					return
				}
				list := p.List()
				if g.Len() != list.Len() {
					t.Fatalf("%s length", tag)
				}
				for j := 0; j < g.Len(); j++ {
					check(g.Index(j), list.Get(j))
				}
			}
		}
		check(g, p)
	}
}
func TestSnapshotQuerySemanticConversions(t *testing.T) {
	cases := []struct {
		name     string
		to, from any
	}{
		{"SnapshotPin", SnapshotPinToPB, SnapshotPinFromPB},
		{"SnapshotReadPart", SnapshotReadPartToPB, SnapshotReadPartFromPB},
		{"SnapshotReadTable", SnapshotReadTableToPB, SnapshotReadTableFromPB},
		{"SnapshotReadSet", SnapshotReadSetToPB, SnapshotReadSetFromPB},
		{"SnapshotQueryBinding", SnapshotQueryBindingToPB, SnapshotQueryBindingFromPB},
		{"SnapshotQueryInput", SnapshotQueryInputToPB, SnapshotQueryInputFromPB},
		{"SnapshotQueryEnvelope", SnapshotQueryEnvelopeToPB, SnapshotQueryEnvelopeFromPB},
		{"SnapshotQueryReservation", SnapshotQueryReservationToPB, SnapshotQueryReservationFromPB},
		{"SnapshotQueryReservationStatus", SnapshotQueryReservationStatusToPB, SnapshotQueryReservationStatusFromPB},
		{"SnapshotQueryStatement", SnapshotQueryStatementToPB, SnapshotQueryStatementFromPB},
		{"SnapshotQueryJob", SnapshotQueryJobToPB, SnapshotQueryJobFromPB},
		{"SnapshotQueryEvidence", SnapshotQueryEvidenceToPB, SnapshotQueryEvidenceFromPB},
		{"SnapshotQueryReceipt", SnapshotQueryReceiptToPB, SnapshotQueryReceiptFromPB},
		{"SnapshotQueryAttestation", SnapshotQueryAttestationToPB, SnapshotQueryAttestationFromPB},
		{"SnapshotQuerySubmitResult", SnapshotQuerySubmitResultToPB, SnapshotQuerySubmitResultFromPB},
		{"SnapshotQueryStatus", SnapshotQueryStatusToPB, SnapshotQueryStatusFromPB},
		{"ActiveQueryPolicy", ActiveQueryPolicyToPB, ActiveQueryPolicyFromPB},
		{"ExecutorProfileTransition", ExecutorProfileTransitionToPB, ExecutorProfileTransitionFromPB},
		{"ExecutorProfileTransitionReceipt", ExecutorProfileTransitionReceiptToPB, ExecutorProfileTransitionReceiptFromPB},
		{"SnapshotArtifactReady", SnapshotArtifactReadyToPB, SnapshotArtifactReadyFromPB},
		{"SnapshotQueryAbortRecord", SnapshotQueryAbortRecordToPB, SnapshotQueryAbortRecordFromPB},
		{"SnapshotQueryClaim", SnapshotQueryClaimToPB, SnapshotQueryClaimFromPB},
		{"ProfileSetting", ProfileSettingToPB, ProfileSettingFromPB},
		{"QueryLimits", QueryLimitsToPB, QueryLimitsFromPB},
		{"QueryProfileRecord", QueryProfileRecordToPB, QueryProfileRecordFromPB},
		{"SnapshotArtifactEntry", SnapshotArtifactEntryToPB, SnapshotArtifactEntryFromPB},
		{"SnapshotArtifactSet", SnapshotArtifactSetToPB, SnapshotArtifactSetFromPB},
		{"SnapshotArtifactReadySubmission", SnapshotArtifactReadySubmissionToPB, SnapshotArtifactReadySubmissionFromPB},
		{"AcquireSnapshotQueryRequest", AcquireSnapshotQueryRequestToPB, AcquireSnapshotQueryRequestFromPB},
		{"GetSnapshotQueryReservationRequest", GetSnapshotQueryReservationRequestToPB, GetSnapshotQueryReservationRequestFromPB},
		{"ReleaseSnapshotQueryRequest", ReleaseSnapshotQueryRequestToPB, ReleaseSnapshotQueryRequestFromPB},
		{"GetSnapshotQueryStatusRequest", GetSnapshotQueryStatusRequestToPB, GetSnapshotQueryStatusRequestFromPB},
		{"SnapshotBarrier", SnapshotBarrierToPB, SnapshotBarrierFromPB},
		{"GetPublishedSnapshotRequest", GetPublishedSnapshotRequestToPB, GetPublishedSnapshotRequestFromPB},
		{"PublishedSnapshot", PublishedSnapshotToPB, PublishedSnapshotFromPB},
		{"GetQueryPolicyRequest", GetQueryPolicyRequestToPB, GetQueryPolicyRequestFromPB},
		{"QueryPolicyStatus", QueryPolicyStatusToPB, QueryPolicyStatusFromPB},
		{"SnapshotQueryControlBinding", SnapshotQueryControlBindingToPB, SnapshotQueryControlBindingFromPB},
		{"BeginSnapshotQuery", BeginSnapshotQueryToPB, BeginSnapshotQueryFromPB},
		{"GrantSnapshotQuery", GrantSnapshotQueryToPB, GrantSnapshotQueryFromPB},
		{"ReleaseSnapshotQuery", ReleaseSnapshotQueryToPB, ReleaseSnapshotQueryFromPB},
		{"SubmitSnapshotQuery", SubmitSnapshotQueryToPB, SubmitSnapshotQueryFromPB},
		{"AbortSnapshotQuery", AbortSnapshotQueryToPB, AbortSnapshotQueryFromPB},
		{"ActivateQueryProfile", ActivateQueryProfileToPB, ActivateQueryProfileFromPB},
		{"RecordSnapshotQueryClaim", RecordSnapshotQueryClaimToPB, RecordSnapshotQueryClaimFromPB},
		{"RecordSnapshotQueryAttestation", RecordSnapshotQueryAttestationToPB, RecordSnapshotQueryAttestationFromPB},
		{"PublishExecutorProfileTransition", PublishExecutorProfileTransitionToPB, PublishExecutorProfileTransitionFromPB},
		{"RecordSnapshotArtifactReady", RecordSnapshotArtifactReadyToPB, RecordSnapshotArtifactReadyFromPB},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			to, from := reflect.ValueOf(c.to), reflect.ValueOf(c.from)
			original := reflect.New(to.Type().In(0)).Elem()
			var serial uint64
			seedSnapshot(original, c.name, &serial)
			message := to.Call([]reflect.Value{original})[0].Interface().(proto.Message)
			assertSnapshotLeaves(t, original, message.ProtoReflect())
			raw, err := proto.Marshal(message)
			if err != nil {
				t.Fatal(err)
			}
			decoded := message.ProtoReflect().Type().New().Interface()
			if err = proto.Unmarshal(raw, decoded); err != nil {
				t.Fatal(err)
			}
			got := from.Call([]reflect.Value{reflect.ValueOf(decoded)})[0]
			if !reflect.DeepEqual(original.Interface(), got.Interface()) {
				t.Fatalf("round trip:\n%#v\n%#v", original.Interface(), got.Interface())
			}
		})
	}
}
func TestSnapshotQueryOptionalAndEmpty(t *testing.T) {
	if SnapshotQueryJobFromPB(SnapshotQueryJobToPB(replay.SnapshotQueryJob{})).SourceClaim != nil {
		t.Fatal("invented source claim")
	}
	if SnapshotQueryReservationStatusFromPB(SnapshotQueryReservationStatusToPB(replay.SnapshotQueryReservationStatus{})).Reservation != nil {
		t.Fatal("invented grant")
	}
	v := SnapshotReadSetFromPB(&pb.SnapshotReadSet{})
	if v.Tables == nil {
		t.Fatal("new arrays must be []")
	}
	if mapSlice([]int{}, func(i int) int { return i }) != nil {
		t.Fatal("legacy nil changed")
	}
	for _, state := range []string{"", "draining", "granted", "consumed", "released"} {
		for _, block := range []uint64{0, 71} {
			want := replay.SnapshotQueryReservationStatus{Version: 1, Found: state != "", State: state, RequestID: "request/lost-response", ClientAccount: "account", StatementID: "statement", BlockSeq: block}
			if state != "" {
				want.FencingGeneration = 29
			}
			if state == "granted" || state == "consumed" {
				want.Reservation = &replay.SnapshotQueryReservation{ReservationID: "grant", FencingGeneration: 29}
			}
			if state == "released" {
				want.TerminalProof = []byte("durable tombstone after lost release")
			}
			got := SnapshotQueryReservationStatusFromPB(SnapshotQueryReservationStatusToPB(want))
			a, _ := json.Marshal(want)
			b, _ := json.Marshal(got)
			if !bytes.Equal(a, b) {
				t.Fatalf("status %s lost: %s/%s", state, a, b)
			}
		}
	}
}
func TestSnapshotQueryCommands(t *testing.T) {
	commands := []Command{
		{BeginSnapshotQuery: &BeginSnapshotQuery{}},
		{GrantSnapshotQuery: &GrantSnapshotQuery{}},
		{ReleaseSnapshotQuery: &ReleaseSnapshotQuery{}},
		{SubmitSnapshotQuery: &SubmitSnapshotQuery{}},
		{AbortSnapshotQuery: &AbortSnapshotQuery{}},
		{ActivateQueryProfile: &ActivateQueryProfile{}},
		{RecordSnapshotQueryClaim: &RecordSnapshotQueryClaim{}},
		{RecordSnapshotQueryAttestation: &RecordSnapshotQueryAttestation{}},
		{PublishExecutorProfileTransition: &PublishExecutorProfileTransition{}},
		{RecordSnapshotArtifactReady: &RecordSnapshotArtifactReady{}},
	}
	allocations := []protowire.Number{30, 19, 20, 21, 22, 23, 24, 25, 26, 27}
	for i, c := range commands {
		// Populate every command field and nested leaf with distinct values.
		value := reflect.ValueOf(c)
		for j := 0; j < value.NumField(); j++ {
			if !value.Field(j).IsNil() {
				var serial uint64
				seedSnapshot(value.Field(j).Elem(), "command", &serial)
			}
		}
		b, err := Encode(c)
		if err != nil {
			t.Fatal(err)
		}
		tag, _, _ := protowire.ConsumeTag(b)
		if tag != allocations[i] {
			t.Fatalf("tag %d", tag)
		}
		got, err := Decode(b)
		if err != nil || !reflect.DeepEqual(c, got) {
			t.Fatalf("command %d: %#v %v", i, got, err)
		}
	}
	mixed := Command{SealL3Block: &SealL3Block{}, SubmitSnapshotQuery: &SubmitSnapshotQuery{}}
	if _, err := Encode(mixed); err == nil {
		t.Fatal("mixed Go command")
	}
	// Presence matters: both messages are zero length and still count.
	for _, b := range [][]byte{{0x12, 0}, {0x12, 0, 0xaa, 1, 0}, {0xaa, 1, 0, 0x12, 0}, {0xaa, 1, 0, 0xaa, 1, 0}, {0xfa, 7, 0}, {0xaa, 1, 2, 0x7a, 0}} {
		_, err := Decode(b)
		if len(b) == 2 {
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err == nil {
			t.Fatalf("accepted unknown/mixed %x", b)
		}
	}

}
func TestSnapshotQueryStatusLookupIdentity(t *testing.T) {
	tokens := []string{"header.payload.signature-A", "header.payload.signature-B"}
	// The transport preserves exact identities and conflict candidates; C3/C4
	// authenticate signatures and compare stored identities at runtime.
	for _, hash := range []string{"", "wrong", replay.DigestString(tokens[0]), replay.DigestString(tokens[1])} {
		want := GetSnapshotQueryStatusRequest{NetworkID: "net", KeeperShardID: 9, ClientAccount: "a", StatementID: "s", ExpectedInputRoot: "same-input", ExpectedUserJWSHash: hash}
		msg := GetSnapshotQueryStatusRequestToPB(want)
		raw, _ := proto.Marshal(msg)
		var decoded pb.GetSnapshotQueryStatusRequest
		if err := proto.Unmarshal(raw, &decoded); err != nil {
			t.Fatal(err)
		}
		if got := GetSnapshotQueryStatusRequestFromPB(&decoded); got != want {
			t.Fatal("lost rejection/retry lookup identity")
		}
	}
	if replay.DigestString(tokens[0]) == replay.DigestString(tokens[1]) {
		t.Fatal("signature identity conflated")
	}
	job := replay.SnapshotQueryJob{Statement: replay.SnapshotQueryStatement{StatementSeq: 401, Envelope: replay.SnapshotQueryEnvelope{UserJWS: tokens[0]}}}
	got := SnapshotQueryJobFromPB(SnapshotQueryJobDispatch(job).GetSnapshotQueryJob())
	if got.Statement.StatementSeq != 401 || got.Statement.Envelope.UserJWS != tokens[0] {
		t.Fatal("dispatch identity lost")
	}
}

func TestSnapshotQueryPreservesLegacyCommandJSONAndBytes(t *testing.T) {
	const oldJSON = `{"SubmitStatement":null,"SealL3Block":{},"MarkReplaying":null,"RegisterRC":null,"RecordAttestation":null,"RecordByteSideScan":null,"RecordAnchorFinality":null,"RecordPromotionIssued":null,"RecordPromotionAck":null,"PublishSafeSnapshot":null,"ScheduleUnsafeCleanup":null,"RecordCleanupAck":null,"OpenChallenge":null,"ResolveChallenge":null,"RegisterNode":null,"MarkActive":null,"EvictNode":null}`
	command := Command{SealL3Block: &SealL3Block{}}
	raw, err := json.Marshal(command)
	if err != nil || string(raw) != oldJSON {
		t.Fatalf("legacy JSON changed: %s %v", raw, err)
	}
	b, err := Encode(command)
	if err != nil || !bytes.Equal(b, []byte{0x12, 0}) {
		t.Fatalf("legacy protobuf changed %x %v", b, err)
	}
	// The canonical legacy repeated-field rule remains null, even inside JSON.
	raw, err = json.Marshal(RCFromPB(&pb.RCRecord{}))
	if err != nil || !bytes.Contains(raw, []byte(`"candidate_parts":null`)) {
		t.Fatalf("legacy nil changed %s %v", raw, err)
	}
}

func TestSnapshotQueryRejectWrongWireTypeBeforeConversion(t *testing.T) {
	// SubmitSnapshotQuery.non_membership_proof is bytes, not a zero varint.
	// Protobuf otherwise stores the mismatch as unknown and getters erase it.
	if _, err := Decode([]byte{0xaa, 0x01, 0x02, 0x10, 0x00}); err == nil {
		t.Fatal("accepted present zero field with wrong wire type")
	}
}
