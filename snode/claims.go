package snode

import (
	"errors"
	"fmt"
	"slices"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// Signed claims and owner scope (housegate spec 2026-10-10 D6, D8, §6.5, §8).
// With Deps.ClaimSigner the SNode signs every message the arbiter accepts
// from it and serves only Config.IndexerID's tables; without one it is the
// legacy SNode, byte for byte.

func (r *Role) now() time.Time {
	if r.d.Now != nil {
		return r.d.Now()
	}
	return time.Now()
}

// nowMillis is the registration_seq clock floor: Unix milliseconds (0 before 1970).
func (r *Role) nowMillis() uint64 {
	if ms := r.now().UnixMilli(); ms > 0 {
		return uint64(ms)
	}
	return 0
}

func (r *Role) messageContext() authority.MessageContext {
	return authority.MessageContext{NetworkID: r.cfg.NetworkID, GenesisSnapshotID: r.genesisID}
}

// sign signs one SNode message with the indexer key at the role's clock.
func (r *Role) sign(kind authority.SNodeMessageKind, body any) (string, error) {
	jws, err := r.d.ClaimSigner.SignSNodeMessageAt(kind, r.messageContext(), body, r.now().Unix())
	if err != nil {
		return "", fmt.Errorf("sign %s: %w", kind, err)
	}
	return jws, nil
}

// nodeFeatures is what RegisterNode advertises: signed_claims_v1 only with a
// claim signer, so the activation gate never counts an SNode that cannot sign.
func (r *Role) nodeFeatures() []string {
	features := arbiter.LocalNodeFeatures()
	if r.d.ClaimSigner != nil {
		return features
	}
	return slices.DeleteFunc(features, func(f string) bool { return f == arbiter.SignedClaimsFeature })
}

// registrationRequests builds the RegisterNode and MarkActive requests of one
// Register call. With a claim signer both carry one fresh registration_seq,
// durable before either is sent: max(persisted + 1, now in Unix ms).
func (r *Role) registrationRequests() (*pb.NodeRegistration, *pb.NodeRef, error) {
	reg := wire.RegisterNode{Registration: arbiter.NodeRegistration{NodeID: r.cfg.NodeID, Roles: []arbiter.NodeRole{arbiter.NodeRoleSNode}}}
	mark := wire.MarkActive{NodeID: r.cfg.NodeID}
	if r.d.ClaimSigner != nil {
		seq, err := r.state.NextRegistrationSeq(r.nowMillis())
		if err != nil {
			return nil, nil, err
		}
		reg.Registration.RegistrationSeq, mark.RegistrationSeq = seq, seq
		if reg.SignerJWS, err = r.sign(authority.SNodeMessageRegistration, reg.Registration); err != nil {
			return nil, nil, err
		}
		if mark.SignerJWS, err = r.sign(authority.SNodeMessageMarkActive, authority.MarkActiveBody{NodeID: mark.NodeID, RegistrationSeq: seq}); err != nil {
			return nil, nil, err
		}
	}
	return wire.RegisterNodeToRequest(reg, r.nodeFeatures()), wire.MarkActiveToRequest(mark), nil
}

// resultClaimRequest is the RegisterResultClaim request for rc.
func (r *Role) resultClaimRequest(rc arbiter.RCRecord) (*pb.RCRecord, error) {
	c := wire.RegisterRC{RC: rc}
	if r.d.ClaimSigner != nil {
		jws, err := r.sign(authority.SNodeMessageResultClaim, rc)
		if err != nil {
			return nil, err
		}
		c.SourceJWS = jws
	}
	return wire.RegisterRCToRequest(c), nil
}

// promotionAckRequest is the AckPromotion request for ack.
func (r *Role) promotionAckRequest(ack arbiter.PromotionAck) (*pb.PromotionAck, error) {
	c := wire.RecordPromotionAck{Ack: ack}
	if r.d.ClaimSigner != nil {
		jws, err := r.sign(authority.SNodeMessagePromotionAck, ack)
		if err != nil {
			return nil, err
		}
		c.SourceJWS = jws
	}
	return wire.RecordPromotionAckToRequest(c), nil
}

// cleanupAckRequest is the AckCleanup request for ack.
func (r *Role) cleanupAckRequest(ack arbiter.CleanupAck) (*pb.CleanupAck, error) {
	c := wire.RecordCleanupAck{Ack: ack}
	if r.d.ClaimSigner != nil {
		jws, err := r.sign(authority.SNodeMessageCleanupAck, ack)
		if err != nil {
			return nil, err
		}
		c.SourceJWS = jws
	}
	return wire.RecordCleanupAckToRequest(c), nil
}

// signTablePurged is the reconciler's purge-report signer.
func (r *Role) signTablePurged(nodeID string, incarnationSeq uint64) (wire.RecordTablePurged, error) {
	jws, err := r.sign(authority.SNodeMessageTablePurged, authority.TablePurgedBody{NodeID: nodeID, IncarnationSeq: incarnationSeq})
	if err != nil {
		return wire.RecordTablePurged{}, err
	}
	return wire.RecordTablePurged{NodeID: nodeID, IncarnationSeq: incarnationSeq, SignerJWS: jws}, nil
}

// purgeArbiter is the reconciler's arbiter: signing with a claim signer.
func (r *Role) purgeArbiter() tableset.Arbiter {
	if r.d.ClaimSigner == nil {
		return r.d.Client
	}
	return dataplane.PurgeReporter{Client: r.d.Client, Sign: r.signTablePurged}
}

// ownerFilter is the reconciler's owner filter: this SNode's indexer when it
// signs its claims, none for a legacy SNode.
func (r *Role) ownerFilter() *uint64 {
	if r.d.ClaimSigner == nil {
		return nil
	}
	id := r.cfg.IndexerID
	return &id
}

// owns reports whether this SNode serves inc (spec D4, D8).
func (r *Role) owns(snap wire.TableRegistrySnapshot, inc wire.TableIncarnation) bool {
	return r.d.ClaimSigner == nil || snap.Owner(inc) == r.cfg.IndexerID
}

// requireOwned refuses tableID when the followed registry assigns its live
// incarnation to another SI indexer. Without an enabled registry, or for a
// key the registry does not know, ownership is not decided here.
func (r *Role) requireOwned(tableID string) error {
	snap, enabled := r.registryView()
	if !enabled {
		return nil
	}
	live := snap.Live(tableID)
	if live == nil || r.owns(snap, *live) {
		return nil
	}
	return fmt.Errorf("table %s is owned by SI indexer %d, not by this source's indexer %d: %w",
		tableID, snap.Owner(*live), r.cfg.IndexerID, ErrTableNotOwned)
}

// claimGenesisSnapshotID is the genesis snapshot id signed messages bind: the
// configured one, else the arbiter's derivation over the genesis tables.
func claimGenesisSnapshotID(cfg Config, signs bool) (string, error) {
	switch {
	case !signs:
		return "", nil
	case cfg.GenesisSnapshotID != "":
		return cfg.GenesisSnapshotID, nil
	case len(cfg.Tables) == 0:
		return "", errors.New("genesis snapshot id is required for a signing SNode that holds no genesis table")
	}
	return dataplane.GenesisSnapshotID(cfg.NetworkID, cfg.SchemaSnapshotID, cfg.ExecutorProfileID, cfg.Tables)
}
