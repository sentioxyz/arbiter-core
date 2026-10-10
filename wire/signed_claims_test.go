package wire

import (
	"encoding/hex"
	"reflect"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/sentioxyz/arbiter-core"
)

// withEverySignedClaimsField sets every field the signed-claims stage adds to
// the one command c carries (UpdateConsensusParams is left alone).
func withEverySignedClaimsField(c Command) Command {
	const seq, jws, sig = uint64(1760054400000), "h.p.s", "ab"
	owner := uint64(1)
	out := c
	switch {
	case c.RegisterRC != nil:
		v := *c.RegisterRC
		v.SourceJWS = jws
		out.RegisterRC = &v
	case c.RecordPromotionAck != nil:
		v := *c.RecordPromotionAck
		v.SourceJWS = jws
		out.RecordPromotionAck = &v
	case c.RecordCleanupAck != nil:
		v := *c.RecordCleanupAck
		v.SourceJWS = jws
		out.RecordCleanupAck = &v
	case c.RegisterNode != nil:
		v := *c.RegisterNode
		v.Registration.RegistrationSeq, v.SignerJWS, v.Ed25519Signature = seq, jws, sig
		out.RegisterNode = &v
	case c.MarkActive != nil:
		v := *c.MarkActive
		v.RegistrationSeq, v.SignerJWS, v.Ed25519Signature = seq, jws, sig
		out.MarkActive = &v
	case c.EvictNode != nil:
		v := *c.EvictNode
		v.ExpectedRegistrationSeq, v.AuthorityJWS = seq, jws
		out.EvictNode = &v
	case c.RecordTablePurged != nil:
		v := *c.RecordTablePurged
		v.SignerJWS, v.Ed25519Signature = jws, sig
		out.RecordTablePurged = &v
	case c.AddTable != nil:
		v := *c.AddTable
		v.OwnerIndexerID = &owner
		out.AddTable = &v
	case c.SeedLegacyTables != nil:
		v := *c.SeedLegacyTables
		v.IndexerID = &owner
		out.SeedLegacyTables = &v
	}
	return out
}

func TestSignedClaimsCommandsRoundTrip(t *testing.T) {
	for _, tc := range preSignedClaimsGolden() {
		t.Run(tc.name, func(t *testing.T) { mustRoundTrip(t, withEverySignedClaimsField(tc.cmd)) })
	}
}

// TestStripSignedClaimsRestoresThePreActivationBytes: whatever an upgraded
// SNode, verifier or watcher supplies, a pre-activation proposal encodes to
// the previous release's bytes (plan S1-A CONTRACT §0).
func TestStripSignedClaimsRestoresThePreActivationBytes(t *testing.T) {
	for _, tc := range preSignedClaimsGolden() {
		t.Run(tc.name, func(t *testing.T) {
			full := withEverySignedClaimsField(tc.cmd)
			decorated, err := Encode(full)
			if err != nil {
				t.Fatal(err)
			}
			if tc.cmd.UpdateConsensusParams == nil && hex.EncodeToString(decorated) == tc.want {
				t.Fatal("the decorated command must differ from the pre-activation bytes")
			}
			stripped, err := Encode(StripSignedClaims(full))
			if err != nil {
				t.Fatal(err)
			}
			if hex.EncodeToString(stripped) != tc.want {
				t.Fatalf("stripped bytes:\n got %s\nwant %s", hex.EncodeToString(stripped), tc.want)
			}
			if !reflect.DeepEqual(full, withEverySignedClaimsField(tc.cmd)) {
				t.Fatal("StripSignedClaims modified its argument")
			}
		})
	}
}

// TestDecodeRefusesSignaturesInRequestOnlyCopies makes a buggy encoder fail on
// every voter alike: the Raft command carries each signature in its own field.
func TestDecodeRefusesSignaturesInRequestOnlyCopies(t *testing.T) {
	for name, cmd := range map[string]*pb.RaftCommand{
		"RCRecord.source_jws": {Cmd: &pb.RaftCommand_RegisterRc{RegisterRc: &pb.RegisterRCCmd{
			Rc: &pb.RCRecord{SourceNode: "s1", SourceJws: "h.p.s"}}}},
		"PromotionAck.source_jws": {Cmd: &pb.RaftCommand_RecordPromotionAck{RecordPromotionAck: &pb.RecordPromotionAckCmd{
			Ack: &pb.PromotionAck{NodeId: "s1", SourceJws: "h.p.s"}}}},
		"CleanupAck.source_jws": {Cmd: &pb.RaftCommand_RecordCleanupAck{RecordCleanupAck: &pb.RecordCleanupAckCmd{
			Ack: &pb.CleanupAck{NodeId: "s1", SourceJws: "h.p.s"}}}},
		"NodeRegistration.signer_jws": {Cmd: &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{
			Registration: &pb.NodeRegistration{NodeId: "s1", SignerJws: "h.p.s"}}}},
		"NodeRegistration.ed25519_signature": {Cmd: &pb.RaftCommand_RegisterNode{RegisterNode: &pb.RegisterNodeCmd{
			Registration: &pb.NodeRegistration{NodeId: "v1", Ed25519Signature: "ab"}}}},
	} {
		t.Run(name, func(t *testing.T) {
			b, err := proto.Marshal(cmd)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := Decode(b); err == nil || !strings.Contains(err.Error(), "request-only") {
				t.Fatalf("Decode = %v, want a request-only refusal", err)
			}
		})
	}
}

func TestRequestConvertersCarrySignatures(t *testing.T) {
	rc := RegisterRC{RC: goldenClaim(), SourceJWS: "h.p.s"}
	if got := RegisterRCFromRequest(RegisterRCToRequest(rc)); !reflect.DeepEqual(got, rc) {
		t.Fatalf("RC request round trip = %+v", got)
	}
	if RCToPB(rc.RC).GetSourceJws() != "" {
		t.Fatal("the canonical RC converter must never set the request-only signature")
	}
	pack := RecordPromotionAck{Ack: goldenPromotionAck(), SourceJWS: "h.p.s"}
	if got := RecordPromotionAckFromRequest(RecordPromotionAckToRequest(pack)); !reflect.DeepEqual(got, pack) {
		t.Fatalf("promotion ack request round trip = %+v", got)
	}
	cack := RecordCleanupAck{Ack: arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 7, TableID: "db1.t", PartitionID: "all"}, SourceJWS: "h.p.s"}
	if got := RecordCleanupAckFromRequest(RecordCleanupAckToRequest(cack)); !reflect.DeepEqual(got, cack) {
		t.Fatalf("cleanup ack request round trip = %+v", got)
	}
	reg := RegisterNode{Registration: arbiter.NodeRegistration{NodeID: "s1", Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}, RegistrationSeq: 7}, SignerJWS: "h.p.s"}
	req := RegisterNodeToRequest(reg, []string{arbiter.ClientLanesFeature, arbiter.SignedClaimsFeature})
	if req.GetRegistrationSeq() != 7 || req.GetSignerJws() != "h.p.s" || len(req.GetFeatures()) != 2 {
		t.Fatalf("registration request = %v", req)
	}
	if got := RegisterNodeFromRequest(req); !reflect.DeepEqual(got, reg) {
		t.Fatalf("registration request round trip = %+v", got)
	}
	b, err := Encode(Command{RegisterNode: &reg})
	if err != nil {
		t.Fatal(err)
	}
	var m pb.RaftCommand
	if err := proto.Unmarshal(b, &m); err != nil {
		t.Fatal(err)
	}
	if r := m.GetRegisterNode().GetRegistration(); r.GetSignerJws() != "" || len(r.GetFeatures()) != 0 || r.GetRegistrationSeq() != 7 || m.GetRegisterNode().GetSignerJws() != "h.p.s" {
		t.Fatalf("RegisterNodeCmd = %v: the signature rides beside the registration, the seq inside it", &m)
	}
	mark := MarkActive{NodeID: "s1", RegistrationSeq: 7, SignerJWS: "h.p.s"}
	if got := MarkActiveFromRequest(MarkActiveToRequest(mark)); got != mark {
		t.Fatalf("mark-active request round trip = %+v", got)
	}
	purged := RecordTablePurged{NodeID: "v1", IncarnationSeq: 5, Ed25519Signature: "ab"}
	if got := RecordTablePurgedFromRequest(RecordTablePurgedToRequest(purged)); got != purged {
		t.Fatalf("purge report round trip = %+v", got)
	}
	evict := EvictNode{NodeID: "snode-2", Reason: "key compromised", ExpectedRegistrationSeq: 9, AuthorityJWS: "h.p.s"}
	if got := EvictNodeFromRequest(EvictNodeToRequest(evict)); got != evict {
		t.Fatalf("eviction request round trip = %+v", got)
	}
	if evict.Canonical() != (arbiter.EvictNodeCommand{NodeID: "snode-2", ExpectedRegistrationSeq: 9, Reason: "key compromised"}) {
		t.Fatalf("Canonical = %+v", evict.Canonical())
	}
}
