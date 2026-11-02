package member

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/member/consensus"
	"github.com/sentioxyz/arbiter-core/member/copyset"
)

// consensusMode is what the role uses of *consensus.Mode (plan S3-E1 K66).
// Run's bootNonce is the boot nonce of startup step (7).
type consensusMode interface {
	Run(ctx context.Context, manifests replay.SnapshotStore, inst consensus.InstallView, bootNonce string) error
	Status() consensus.Status
	Metrics() *consensus.Metrics
}

// newConsensusMode builds the member consensus mode of cfg.Consensus; without
// it, nil and no error, and the role is the P2/P3 member byte for byte. The
// mode's identity must be the member's own: its node id, network, genesis
// snapshot, state directory and pinned storage policy.
func newConsensusMode(cfg Config, d Deps, logger *slog.Logger, now func() time.Time, peers []Peer) (consensusMode, error) {
	if cfg.Consensus == nil {
		return nil, nil
	}
	c := *cfg.Consensus
	for _, f := range []struct{ name, got, want string }{
		{"NodeID", c.NodeID, cfg.NodeID},
		{"NetworkID", c.NetworkID, cfg.NetworkID},
		{"GenesisSnapshotID", c.GenesisSnapshotID, cfg.GenesisSnapshotID},
		{"StateDir", c.StateDir, cfg.StateDir},
		{"StoragePolicy", c.StoragePolicy, cfg.Pinned.StoragePolicy},
	} {
		if f.got != f.want {
			return nil, fmt.Errorf("member: Config.Consensus.%s %q differs from the member's %q", f.name, f.got, f.want)
		}
	}
	if d.Payloads == nil {
		return nil, errors.New("member: the consensus mode's verify role needs Deps.Payloads")
	}
	owners := make([]uint64, len(peers))
	for i, p := range peers {
		owners[i] = p.IndexerID
	}
	m, err := consensus.NewMode(c, consensus.ModeDeps{
		Client: d.Client, Registry: d.Registry, Conn: d.Conn, Payloads: d.Payloads,
		MemberUnsafeDB: cfg.Pinned.MemberUnsafeDB, Owners: owners, Logger: logger, Now: now,
	})
	if err != nil {
		return nil, fmt.Errorf("member: %w", err)
	}
	return m, nil
}

// ConsensusMetrics are the member consensus series (plan S3-E1 contract
// §42.3), nil without Config.Consensus; sentio-node registers them on the
// si-member process's registry (Plan S3-E4 Task 79).
func (r *Role) ConsensusMetrics() *consensus.Metrics {
	if r.mode == nil {
		return nil
	}
	return r.mode.Metrics()
}

// consensusStatus is the serving view's consensus status: the mode's, or
// consensus.OffStatus without Config.Consensus.
func consensusStatus(mode consensusMode) func() consensus.Status {
	if mode == nil {
		return consensus.OffStatus
	}
	return mode.Status
}

// nodeIDCheckSet is the purge node set startup step (6) checks the node id
// against (plan S3-C2 K29): all of it without Config.Consensus. In the
// consensus mode the member registers as a VERIFIER under its own id, so
// from its first registration its id is in the purge node set; that id is
// the member's own registration (a VERIFIER registration is signed with the
// key of the id's verifiers entry, which only this member holds, and the
// arbiter refuses a member entry whose id any node record holds), so it is
// left out, and copyset.CheckNodeIDUnused still refuses an SNode's id.
func (r *Role) nodeIDCheckSet(purge []string) []string {
	if r.mode == nil || !slices.Contains(purge, r.cfg.NodeID) {
		return purge
	}
	r.log.Info("member: the node id is in the purge node set as this member's own verifier registration")
	return slices.DeleteFunc(slices.Clone(purge), func(id string) bool { return id == r.cfg.NodeID })
}

// checkGovernedHost is the consensus mode's governed-identity check of
// startup step (6): the registry serves each group member with the host of
// its verifiers entry (plan S3-E1 D27), and a group member with this
// member's node id served with another host is another host's identity, so
// the start is refused. A member served without a host (delisted) and every
// check without Config.Consensus pass.
func (r *Role) checkGovernedHost(owner uint64, database, nodeID string, host *uint64) error {
	if r.mode == nil || nodeID != r.cfg.NodeID || host == nil || *host == r.cfg.HostIndexerID {
		return nil
	}
	return fmt.Errorf("member: node id %q is served as a member of group %d/%s hosted by indexer %d, not by this host %d: %w",
		nodeID, owner, database, *host, r.cfg.HostIndexerID, copyset.ErrNodeIDInUse)
}
