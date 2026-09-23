package wire

import (
	"math"
	"reflect"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

func TestConsensusParamsUpdateRaftRoundTrip(t *testing.T) {
	update := arbiter.ConsensusParamsUpdate{
		NetworkID: "testnet", GenesisSnapshotID: "snapshot:genesis", ExpectedEpoch: math.MaxUint64,
		PreviousParamsDigest: "0xprevious", MaxWriters: math.MaxUint64, ExpectedPromotionSeq: math.MaxUint64,
		AuthorityAddresses:            []string{"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"},
		ArtifactDispositionCapability: 1,
	}
	command := Command{UpdateConsensusParams: &UpdateConsensusParams{Update: update, AuthorityJWS: "header.payload.signature"}}
	mustRoundTrip(t, command)
	encoded, err := Encode(command)
	if err != nil {
		t.Fatal(err)
	}
	var envelope pb.RaftCommand
	if err := proto.Unmarshal(encoded, &envelope); err != nil {
		t.Fatal(err)
	}
	field := envelope.ProtoReflect().WhichOneof(envelope.ProtoReflect().Descriptor().Oneofs().ByName("cmd"))
	if field == nil || field.Number() != 18 || envelope.GetUpdateConsensusParams().GetAuthorityJws() != "header.payload.signature" {
		t.Fatalf("unexpected Raft command encoding: %v", &envelope)
	}
	if _, err := Encode(Command{UpdateConsensusParams: command.UpdateConsensusParams, SealL3Block: &SealL3Block{}}); err == nil {
		t.Fatal("update and another command encoded together")
	}
}

func TestConsensusParamsUpdateConvertersCopySlices(t *testing.T) {
	command := arbiter.ConsensusParamsUpdate{AuthorityAddresses: []string{"first", "second"}}
	message := ConsensusParamsUpdateToPB(command)
	message.AuthorityAddresses[0] = "changed PB"
	if command.AuthorityAddresses[0] != "first" {
		t.Fatal("ToPB shares the caller's address slice")
	}
	decoded := ConsensusParamsUpdateFromPB(message)
	decoded.AuthorityAddresses[1] = "changed Go"
	if message.AuthorityAddresses[1] != "second" {
		t.Fatal("FromPB shares the message's address slice")
	}
	if got := ConsensusParamsUpdateFromPB(nil); !reflect.DeepEqual(got, arbiter.ConsensusParamsUpdate{}) {
		t.Fatalf("absent update decoded as %+v", got)
	}
	if got := ConsensusParamsUpdateFromPB(&pb.ConsensusParamsUpdate{AuthorityAddresses: []string{}}); got.AuthorityAddresses != nil {
		t.Fatal("empty repeated field did not decode to nil")
	}
}

func TestConsensusParamsUpdateCarriesTableRegistry(t *testing.T) {
	in := arbiter.ConsensusParamsUpdate{NetworkID: "n", MaxWriters: 1, TableRegistry: &arbiter.TableRegistryParams{
		ChainID: 1, DatabasesContract: "0x0000000000000000000000000000000000000001", SIIndexerID: 2, ActivationBlock: 3, Confirmation: "finalized"}}
	out := ConsensusParamsUpdateFromPB(ConsensusParamsUpdateToPB(in))
	if out.TableRegistry == nil || *out.TableRegistry != *in.TableRegistry {
		t.Fatalf("round trip = %+v", out.TableRegistry)
	}
	if ConsensusParamsUpdateFromPB(ConsensusParamsUpdateToPB(arbiter.ConsensusParamsUpdate{})).TableRegistry != nil {
		t.Fatal("absent registry must decode as nil")
	}
}
