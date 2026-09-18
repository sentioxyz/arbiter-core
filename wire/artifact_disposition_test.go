package wire

import (
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
)

func TestArtifactDispositionBindPolicyRoundTrip(t *testing.T) {
	in := Command{ArtifactDisposition: &ArtifactDispositionCmd{
		Command: ArtifactDispositionCommandV1{
			Version: 1, NetworkID: "net-1", KeeperShardID: 2, ActorID: "0xabc", RequestID: "aa", ExpectedRevision: 0,
			Action: ArtifactDispositionActionV1{BindPolicy: &ArtifactDispositionBindPolicyV1{Policy: ArtifactDispositionPolicyV1{
				PolicyID: "p1", Kind: "explicit_historical_window_v1", AdministratorAddresses: []string{"0x1", "0x2"},
			}}},
		},
		AdministratorJWS: "jws",
		Validation: ArtifactDispositionValidationV1{
			Version: 1, CommandRoot: "0xcmd", ActorID: "0xabc", ActorRole: "governance_admin",
			CapacityAllowanceOrdinal: 3, RegistryObservations: []ArtifactDispositionRegistryObservationV1{},
			SourceObservations: []ArtifactDispositionSourceObservationV1{},
		},
	}}
	raw, err := Encode(in)
	if err != nil {
		t.Fatal(err)
	}
	var envelope pb.RaftCommand
	if err := proto.Unmarshal(raw, &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.GetArtifactDisposition() == nil {
		t.Fatal("tag 28 missing")
	}
	out, err := Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip changed command:\n%#v\n%#v", in, out)
	}
}

func TestArtifactDispositionRejectsMixedActions(t *testing.T) {
	_, err := Encode(Command{ArtifactDisposition: &ArtifactDispositionCmd{Command: ArtifactDispositionCommandV1{
		Action: ArtifactDispositionActionV1{
			BindPolicy:        &ArtifactDispositionBindPolicyV1{},
			ResolveObligation: &ArtifactDispositionResolveObligationV1{ObligationSeq: 1},
		},
	}}})
	if err == nil {
		t.Fatal("mixed actions encoded")
	}
}
