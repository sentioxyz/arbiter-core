package wire

import (
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"
)

// TestRenewNodeLeaseCommand (arbiter-core#66): tag 44 and its request
// round-trip through the strict codec with every field.
func TestRenewNodeLeaseCommand(t *testing.T) {
	c := RenewNodeLease{NodeID: "snode-1", RegistrationSeq: 7, LeaseSeq: 3, SignerJWS: "h.p.s", Ed25519Signature: "ab"}
	if got := RenewNodeLeaseFromRequest(RenewNodeLeaseToRequest(c)); got != c {
		t.Fatalf("request round trip = %+v", got)
	}
	mustRoundTrip(t, Command{RenewNodeLease: &c})
	b, err := Encode(Command{RenewNodeLease: &c})
	if err != nil {
		t.Fatal(err)
	}
	var raw pb.RaftCommand
	if err := proto.Unmarshal(b, &raw); err != nil {
		t.Fatal(err)
	}
	if r := raw.GetRenewNodeLease().GetRenewal(); r.GetNodeId() != "snode-1" || r.GetLeaseSeq() != 3 {
		t.Fatalf("encoded command = %v", &raw)
	}
	if got := RenewNodeLeaseFromRequest(nil); got != (RenewNodeLease{}) {
		t.Fatalf("a missing renewal decoded to %+v", got)
	}
}

// TestConsensusParamsUpdateCarriesMembershipLease: the converters copy the
// lease both ways; zero stays off the wire.
func TestConsensusParamsUpdateCarriesMembershipLease(t *testing.T) {
	u := legacyGoldenUpdate()
	if ConsensusParamsUpdateToPB(u).GetMembershipLeaseL2Blocks() != 0 {
		t.Fatal("an update without a lease set one")
	}
	u.MembershipLeaseL2Blocks = 600
	m := ConsensusParamsUpdateToPB(u)
	if m.GetMembershipLeaseL2Blocks() != 600 || ConsensusParamsUpdateFromPB(m).MembershipLeaseL2Blocks != 600 {
		t.Fatalf("lease not carried: %v", m)
	}
	mustRoundTrip(t, Command{UpdateConsensusParams: &UpdateConsensusParams{Update: u, AuthorityJWS: "h.p.s"}})
}
