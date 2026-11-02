package consensus

import (
	"cmp"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/wire"
)

// OwnerInstallState is one owner's install state as a checkpoint states it
// (plan S3-E1 contract §42.3): the manifest and track entry the member's
// install journal records for the owner, and every table the member holds a
// copy of, with the reason it is excluded ("" when it is installed at
// SnapshotID).
type OwnerInstallState struct {
	SnapshotID string
	Entry      replay.TrackFrontier
	Tables     []TableInstallState
}

// TableInstallState is one copied table; Excluded is "" for a table installed
// at the owner's journaled manifest.
type TableInstallState struct {
	TableID  string
	Excluded arbiter.CheckpointExclusionReason
}

// InstallStateOf adapts owner's install journal of Plan S3-C2 (the
// installer's Entry, Tables and Stats) to an OwnerInstallState. A copy counts
// only when it is a copy of the live incarnation of its table owned by owner
// in snap (a copy of an earlier incarnation of the same name installs nothing
// of the live one). Its reason, in this order: foreign or diverged as the
// journal marks it; not_installed while its install pass is changing it
// before the journal write (its bytes may already be a newer manifest's);
// audit_pending while the installer's audit is not clean
// (an audit completed since the start or the last ClickHouse restart and no
// copy awaits a repair, spec §8.7), so a checkpoint never claims a copy the
// last audit did not see clean; then, for a copy whose journal is not at the
// owner's manifest, disk_threshold while installs are paused by the member
// disk budget (Plan S3-C2 K35), else not_installed.
func InstallStateOf(snap wire.TableRegistrySnapshot, owner uint64, j install.OwnerJournal, tables []install.TableState, stats install.Stats) OwnerInstallState {
	out := OwnerInstallState{SnapshotID: j.SnapshotID, Entry: j.Entry, Tables: make([]TableInstallState, 0, len(tables))}
	for _, t := range tables {
		live := snap.Live(t.TableKey)
		if live == nil || live.Seq != t.IncarnationSeq || snap.Owner(*live) != owner {
			continue
		}
		var reason arbiter.CheckpointExclusionReason
		switch {
		case t.Foreign:
			reason = arbiter.ExclusionForeign
		case t.Diverged:
			reason = arbiter.ExclusionDiverged
		case t.Installing:
			reason = arbiter.ExclusionNotInstalled
		case !stats.AuditClean:
			reason = arbiter.ExclusionAuditPending
		case t.SnapshotID != j.SnapshotID || t.Entry != j.Entry:
			reason = arbiter.ExclusionNotInstalled
			if stats.InstallsPaused {
				reason = arbiter.ExclusionDiskThreshold
			}
		}
		out.Tables = append(out.Tables, TableInstallState{TableID: t.TableKey, Excluded: reason})
	}
	slices.SortFunc(out.Tables, func(a, b TableInstallState) int { return strings.Compare(a.TableID, b.TableID) })
	return out
}

// PlanCheckpoint plans nodeID's checkpoint for owner (spec §9.1): one group
// per database group of owner that lists the node as a current member (a
// Leaving record is no membership), sorted by database, at the group's
// generation. Each group excludes, sorted by table id, every table of its
// database whose live incarnation is physical and owned by owner and that
// the member has not installed at st.SnapshotID: a Pending or Active one with
// its install reason, or not_installed when st lists no copy of it; a
// Retiring or Purging one as retiring, because the member drops its copy from
// Retiring on (spec §8.8). CheckpointSeq is left 0 for CheckpointSigner.Sign.
// It answers false when the node is a current member of no group of owner,
// the journal records no manifest for owner, or the journaled entry is not
// owner's.
func PlanCheckpoint(snap wire.TableRegistrySnapshot, nodeID string, owner uint64, st OwnerInstallState) (arbiter.MemberCheckpoint, bool) {
	if st.SnapshotID == "" || st.Entry.IndexerID != owner {
		return arbiter.MemberCheckpoint{}, false
	}
	installed := make(map[string]arbiter.CheckpointExclusionReason, len(st.Tables))
	for _, t := range st.Tables {
		installed[t.TableID] = t.Excluded
	}
	cp := arbiter.MemberCheckpoint{NodeID: nodeID, OwnerIndexerID: owner, SnapshotID: st.SnapshotID, Entry: st.Entry}
	for _, g := range snap.Groups {
		if g.OwnerIndexerID != owner || !slices.ContainsFunc(g.Members, func(m wire.GroupMember) bool { return m.NodeID == nodeID }) {
			continue
		}
		cg := arbiter.CheckpointGroup{DatabaseID: g.DatabaseID, GroupGeneration: g.Generation}
		for _, live := range liveTablesOf(snap, owner, g.DatabaseID) {
			var reason arbiter.CheckpointExclusionReason
			switch live.Status {
			case wire.TableStatusPending, wire.TableStatusActive:
				r, ok := installed[live.Key()]
				if !ok {
					r = arbiter.ExclusionNotInstalled
				}
				reason = r
			case wire.TableStatusRetiring, wire.TableStatusPurging:
				reason = arbiter.ExclusionRetiring
			}
			if reason != "" {
				cg.Excluded = append(cg.Excluded, arbiter.ExcludedTable{TableID: live.Key(), Reason: reason})
			}
		}
		cp.Groups = append(cp.Groups, cg)
	}
	if len(cp.Groups) == 0 {
		return arbiter.MemberCheckpoint{}, false
	}
	slices.SortFunc(cp.Groups, func(a, b arbiter.CheckpointGroup) int { return cmp.Compare(a.DatabaseID, b.DatabaseID) })
	return cp, true
}

// liveTablesOf lists, sorted by table key, the live incarnation of every
// table of database that is physical and owned by owner.
func liveTablesOf(snap wire.TableRegistrySnapshot, owner uint64, database string) []wire.TableIncarnation {
	seen := map[string]bool{}
	var out []wire.TableIncarnation
	for _, inc := range snap.Incarnations {
		key := inc.Key()
		if inc.DatabaseID != database || seen[key] {
			continue
		}
		seen[key] = true
		if live := snap.Live(key); live != nil && live.HasPhysicalTables() && snap.Owner(*live) == owner {
			out = append(out, *live)
		}
	}
	slices.SortFunc(out, func(a, b wire.TableIncarnation) int { return strings.Compare(a.Key(), b.Key()) })
	return out
}

// CheckpointSigner signs a member's checkpoints with its replica_signer key
// (purpose arbiter-member-checkpoint-v1, plan S3-E1 K63), each under a fresh
// checkpoint_seq from the store.
type CheckpointSigner struct {
	signer *authority.Signer
	ctx    authority.MessageContext
	store  *CheckpointSeqStore
}

// NewCheckpointSigner loads replicaSignerKeyHex, a secp256k1 private key in
// hex with or without 0x. The key never appears in an error or a log.
func NewCheckpointSigner(replicaSignerKeyHex string, ctx authority.MessageContext, store *CheckpointSeqStore) (*CheckpointSigner, error) {
	if store == nil {
		return nil, errors.New("member consensus: a checkpoint signer needs a checkpoint_seq store")
	}
	if strings.TrimSpace(ctx.NetworkID) == "" || strings.TrimSpace(ctx.GenesisSnapshotID) == "" {
		return nil, errors.New("member consensus: a checkpoint signer needs the network id and the genesis snapshot id")
	}
	signer, err := authority.NewSignerFromHex(replicaSignerKeyHex)
	if err != nil {
		return nil, errReplicaSignerKey
	}
	return &CheckpointSigner{signer: signer, ctx: ctx, store: store}, nil
}

// errReplicaSignerKey is the refusal of a key that does not load; it carries
// no part of the key.
var errReplicaSignerKey = errors.New("member consensus: the replica_signer key is not a secp256k1 private key in hex")

// Address is the replica_signer address this signer signs as.
func (s *CheckpointSigner) Address() string { return s.signer.Address() }

// Sign gives cp the next checkpoint_seq of its owner, durable before Sign
// returns (CheckpointSeqStore.Next; published is the registry's value under
// this signer, else 0), and signs it. A checkpoint the signing hash refuses
// is refused before a seq is used.
func (s *CheckpointSigner) Sign(cp arbiter.MemberCheckpoint, published uint64) (wire.MemberCheckpoint, error) {
	probe := cp
	probe.CheckpointSeq = 1
	if _, err := authority.MemberCheckpointHash(s.ctx, probe); err != nil {
		return wire.MemberCheckpoint{}, err
	}
	seq, err := s.store.Next(cp.OwnerIndexerID, s.signer.Address(), published)
	if err != nil {
		return wire.MemberCheckpoint{}, err
	}
	cp.CheckpointSeq = seq
	jws, err := s.signer.SignMemberCheckpoint(s.ctx, cp)
	if err != nil {
		return wire.MemberCheckpoint{}, fmt.Errorf("member consensus: sign checkpoint %d of owner %d: %w", seq, cp.OwnerIndexerID, err)
	}
	return wire.MemberCheckpoint{Checkpoint: cp, ReplicaJWS: jws}, nil
}
