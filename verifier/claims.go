package verifier

import (
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane/evidence"
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
	return evidence.VerifierRegistrationRequests(r.priv, r.cfg.ReplicaID, seq, r.messageContext())
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
