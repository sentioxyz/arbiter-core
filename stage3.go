package arbiter

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
)

// IndexerFailureDomain names one enrolled SI indexer's failure domain
// (housegate spec 2026-10-14 §6.1, §9.3); equal strings are one domain.
type IndexerFailureDomain struct {
	IndexerID     uint64 `json:"indexer_id"`
	FailureDomain string `json:"failure_domain"`
}

// Stage3Params is the canonical stage-3 parameter (spec §6.1, §10.4), the
// mirror of pb.Stage3Params. Every field is explicit: there is no
// absent-means-default (plan S3-E1 K58), and only IndexerFailureDomains is
// omitted when empty. The switches (Purge through NarrowedDrain, in the order
// of Stage3Switches) and StrictCopies are one-way, and MinSafeCopies and
// GroupSize never decrease; those rules compare two parameters and are the
// FSM's. Validate checks the rules one parameter carries by itself.
type Stage3Params struct {
	GroupSize                     uint32                 `json:"group_size"`
	NetworkFillDefault            bool                   `json:"network_fill_default"`
	MinSafeCopies                 uint32                 `json:"min_safe_copies"`
	StrictCopies                  bool                   `json:"strict_copies"`
	IndexerFailureDomains         []IndexerFailureDomain `json:"indexer_failure_domains,omitempty"`
	MaxLagPublications            uint32                 `json:"max_lag_publications"`
	LagLow                        uint32                 `json:"lag_low"`
	EvidenceTimeoutL2Blocks       uint64                 `json:"evidence_timeout_l2_blocks"`
	MaxUncleanedPartsPerPartition uint32                 `json:"max_uncleaned_parts_per_partition"`
	MaxUncleanedPartsPerTable     uint32                 `json:"max_uncleaned_parts_per_table"`
	ExcludeQuarantinedHosts       bool                   `json:"exclude_quarantined_hosts"`
	Purge                         bool                   `json:"purge"`
	EvictionVoiding               bool                   `json:"eviction_voiding"`
	OwnerAwareSelection           bool                   `json:"owner_aware_selection"`
	Placement                     bool                   `json:"placement"`
	Reselection                   bool                   `json:"reselection"`
	Checkpoints                   bool                   `json:"checkpoints"`
	NarrowedDrain                 bool                   `json:"narrowed_drain"`
}

// Stage3Switches lists the switch names in §10.4 order: "purge", "eviction_voiding",
// "owner_aware_selection", "placement", "reselection", "checkpoints", "narrowed_drain".
// Each is the JSON name of a Stage3Params field; each switch requires every
// earlier one. Read-only.
var Stage3Switches = []string{"purge", "eviction_voiding", "owner_aware_selection", "placement", "reselection", "checkpoints", "narrowed_drain"}

// SwitchOn reports whether the named switch is on; a name that is not in
// Stage3Switches is never on.
func (p Stage3Params) SwitchOn(name string) bool {
	switch name {
	case "purge":
		return p.Purge
	case "eviction_voiding":
		return p.EvictionVoiding
	case "owner_aware_selection":
		return p.OwnerAwareSelection
	case "placement":
		return p.Placement
	case "reselection":
		return p.Reselection
	case "checkpoints":
		return p.Checkpoints
	case "narrowed_drain":
		return p.NarrowedDrain
	}
	return false
}

// FailureDomain returns the failure domain IndexerFailureDomains names for
// indexerID, and whether it names one.
func (p Stage3Params) FailureDomain(indexerID uint64) (string, bool) {
	for _, d := range p.IndexerFailureDomains {
		if d.IndexerID == indexerID {
			return d.FailureDomain, true
		}
	}
	return "", false
}

// Validate checks the rules a single stage-3 parameter carries (plan S3-E1
// contract §41): the bounds, the domain list (sorted by indexer id without
// duplicates; authority.NormalizeConsensusParamsUpdate sorts it first), the
// switch prefix rule, and the static form of the copy-floor rule of plan S3-E2
// K69. Whether the domains name exactly the enrolled indexers is checked
// against the update's si_indexers by the normalizer.
func (p Stage3Params) Validate() error {
	switch {
	case p.GroupSize < 1:
		return fmt.Errorf("stage3: group_size must be at least 1")
	case p.MinSafeCopies < 1:
		return fmt.Errorf("stage3: min_safe_copies must be at least 1")
	case p.LagLow < 1 || p.LagLow >= p.MaxLagPublications:
		return fmt.Errorf("stage3: lag_low must satisfy 1 <= lag_low < max_lag_publications, got lag_low %d and max_lag_publications %d", p.LagLow, p.MaxLagPublications)
	case p.EvidenceTimeoutL2Blocks < 1:
		return fmt.Errorf("stage3: evidence_timeout_l2_blocks must be at least 1")
	case p.MaxUncleanedPartsPerPartition < 1 || p.MaxUncleanedPartsPerTable < 1:
		return fmt.Errorf("stage3: max_uncleaned_parts_per_partition and max_uncleaned_parts_per_table must be at least 1")
	}
	for i, d := range p.IndexerFailureDomains {
		if !validNodeID(d.FailureDomain) {
			return fmt.Errorf("stage3: indexer_failure_domains: indexer %d: failure_domain must be non-empty and free of whitespace and control characters", d.IndexerID)
		}
		if i > 0 && p.IndexerFailureDomains[i-1].IndexerID >= d.IndexerID {
			return fmt.Errorf("stage3: indexer_failure_domains must be sorted by indexer_id without duplicates")
		}
	}
	for i, name := range Stage3Switches {
		if !p.SwitchOn(name) {
			continue
		}
		for _, earlier := range Stage3Switches[:i] {
			if !p.SwitchOn(earlier) {
				return fmt.Errorf("stage3: switch %s requires %s", name, earlier)
			}
		}
	}
	if (p.MinSafeCopies > 1 || p.StrictCopies) && !(p.Checkpoints && p.NarrowedDrain) {
		return fmt.Errorf("stage3: min_safe_copies above 1 and strict_copies require the checkpoints and narrowed_drain switches")
	}
	return nil
}

// RebalanceGroupCommand is the canonical signing form of an authority-signed
// RebalanceGroup (spec §6.4); its wire form is pb.RebalanceGroupCmd minus
// authority_jws. ExpectedGeneration is a compare-and-swap on the group's
// generation; Add and Remove are sorted, unique and disjoint, nil (omitted)
// when empty; NetworkFill is the group's fill after the command.
type RebalanceGroupCommand struct {
	OwnerIndexerID     uint64   `json:"owner_indexer_id"`
	DatabaseID         string   `json:"database_id"`
	ExpectedGeneration uint64   `json:"expected_generation"`
	Add                []string `json:"add,omitempty"`
	Remove             []string `json:"remove,omitempty"`
	NetworkFill        bool     `json:"network_fill"`
}

// ReselectVerifiersCommand is the canonical signing form of the
// orchestrator's ReselectVerifiers (spec §7.4); its wire form is
// pb.ReselectVerifiersCmd minus authority_jws. ExpectedSet is the block's
// current verifier set: three sorted, unique node ids.
type ReselectVerifiersCommand struct {
	L3BlockSeq  uint64   `json:"l3_block_seq"`
	ExpectedSet []string `json:"expected_set"`
}

// MarkMemberLaggingCommand is the canonical signing form of an
// authority-signed MarkMemberLagging (spec §9.2); its wire form is
// pb.MarkMemberLaggingCmd minus authority_jws. ObservedEntry.IndexerID equals
// OwnerIndexerID.
type MarkMemberLaggingCommand struct {
	NodeID         string               `json:"node_id"`
	OwnerIndexerID uint64               `json:"owner_indexer_id"`
	DatabaseID     string               `json:"database_id"`
	ObservedEntry  replay.TrackFrontier `json:"observed_entry"`
}

// MemberReadyBody is what a member's ed25519 MemberReady signature covers
// (spec §6.4), with the network context; its wire form is pb.MemberReadyCmd
// minus ed25519_signature.
type MemberReadyBody struct {
	NodeID         string `json:"node_id"`
	OwnerIndexerID uint64 `json:"owner_indexer_id"`
	DatabaseID     string `json:"database_id"`
	Generation     uint64 `json:"generation"`
}

// CheckpointExclusionReason is why a checkpoint excludes a table (plan S3-E1
// K63): the closed set below, the lowercase names of
// pb.CheckpointExclusionReason without its prefix. Any other value, the empty
// one included, is refused.
type CheckpointExclusionReason string

const (
	ExclusionNotInstalled  CheckpointExclusionReason = "not_installed"
	ExclusionDiverged      CheckpointExclusionReason = "diverged"
	ExclusionForeign       CheckpointExclusionReason = "foreign"
	ExclusionDiskThreshold CheckpointExclusionReason = "disk_threshold"
	ExclusionAuditPending  CheckpointExclusionReason = "audit_pending"
	ExclusionRetiring      CheckpointExclusionReason = "retiring"
)

// ExcludedTable is one table a checkpoint group does not speak for.
type ExcludedTable struct {
	TableID string                    `json:"table_id"`
	Reason  CheckpointExclusionReason `json:"reason"`
}

// CheckpointGroup is one group a checkpoint speaks for; Excluded is sorted by
// table id without duplicates, nil (omitted) when empty.
type CheckpointGroup struct {
	DatabaseID      string          `json:"database_id"`
	GroupGeneration uint64          `json:"group_generation"`
	Excluded        []ExcludedTable `json:"excluded,omitempty"`
}

// MemberCheckpoint is the canonical signing form of a replicate member's
// checkpoint (spec §9.1): its copies of the listed groups' tables equal the
// manifest SnapshotID at Entry = track_frontiers[OwnerIndexerID].
// CheckpointSeq strictly increases per (member, owner); Groups is sorted by
// database id without duplicates. The member's replica_signer signs it with
// the network context; its wire form is pb.MemberCheckpoint.
type MemberCheckpoint struct {
	NodeID         string               `json:"node_id"`
	OwnerIndexerID uint64               `json:"owner_indexer_id"`
	CheckpointSeq  uint64               `json:"checkpoint_seq"`
	Groups         []CheckpointGroup    `json:"groups"`
	SnapshotID     string               `json:"snapshot_id"`
	Entry          replay.TrackFrontier `json:"entry"`
}
