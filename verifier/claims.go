package verifier

import (
	"crypto/ed25519"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// Signed verifier messages (housegate spec 2026-10-10 D7, §6.5): the verifier
// signs its registration, activation and purge reports with the ed25519 key
// that already signs its evidence. The arbiter drops the signatures before
// the signed-claims activation and requires them after it.

func (r *Role) now() time.Time {
	if r.d.Now != nil {
		return r.d.Now()
	}
	return time.Now()
}

func (r *Role) nowMillis() uint64 {
	if ms := r.now().UnixMilli(); ms > 0 {
		return uint64(ms)
	}
	return 0
}

func (r *Role) messageContext() authority.MessageContext {
	return authority.MessageContext{NetworkID: r.cfg.NetworkID, GenesisSnapshotID: r.genesisID}
}

// registrationRequests builds the signed RegisterNode and MarkActive requests
// of one Register call; both carry one fresh registration_seq.
func (r *Role) registrationRequests() (*pb.NodeRegistration, *pb.NodeRef, error) {
	seq, err := r.nextRegistrationSeq()
	if err != nil {
		return nil, nil, err
	}
	ctx := r.messageContext()
	reg := wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: r.cfg.ReplicaID,
		Roles: []arbiter.NodeRole{arbiter.NodeRoleVerifier}, Ed25519Pubkey: r.priv.Public().(ed25519.PublicKey), RegistrationSeq: seq}}
	if reg.Ed25519Signature, err = authority.SignVerifierMessage(r.priv, authority.VerifierMessageRegistration, ctx, reg.Registration); err != nil {
		return nil, nil, err
	}
	mark := wire.MarkActive{NodeID: r.cfg.ReplicaID, RegistrationSeq: seq}
	if mark.Ed25519Signature, err = authority.SignVerifierMessage(r.priv, authority.VerifierMessageMarkActive, ctx,
		authority.MarkActiveBody{NodeID: mark.NodeID, RegistrationSeq: seq}); err != nil {
		return nil, nil, err
	}
	return wire.RegisterNodeToRequest(reg, arbiter.LocalNodeFeatures()), wire.MarkActiveToRequest(mark), nil
}

// signTablePurged is the reconciler's purge-report signer.
func (r *Role) signTablePurged(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error) {
	sig, err := authority.SignVerifierMessage(r.priv, authority.VerifierMessageTablePurged, r.messageContext(),
		authority.TablePurgedBody{NodeID: nodeID, IncarnationSeq: incarnationSeq})
	if err != nil {
		return wire.RecordTablePurged{}, err
	}
	return wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq, Ed25519Signature: sig}, nil
}
