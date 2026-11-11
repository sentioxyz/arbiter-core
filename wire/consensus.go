package wire

import (
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

// ConsensusParamsUpdateFromPB copies the transport fields. Address-set
// normalization and structural validation belong to authority hashing; a
// missing update decodes to a zero value that that validation rejects.
func ConsensusParamsUpdateFromPB(m *pb.ConsensusParamsUpdate) arbiter.ConsensusParamsUpdate {
	return arbiter.ConsensusParamsUpdate{
		NetworkID:                     m.GetNetworkId(),
		GenesisSnapshotID:             m.GetGenesisSnapshotId(),
		ExpectedEpoch:                 m.GetExpectedEpoch(),
		PreviousParamsDigest:          m.GetPreviousParamsDigest(),
		AuthorityAddresses:            mapSlice(m.GetAuthorityAddresses(), func(address string) string { return address }),
		MaxWriters:                    m.GetMaxWriters(),
		ExpectedPromotionSeq:          m.GetExpectedPromotionSeq(),
		ArtifactDispositionCapability: m.GetArtifactDispositionCapability(),
		TableRegistry:                 TableRegistryParamsFromPB(m.GetTableRegistry()),
		ClientLanes:                   ClientLaneParamsFromPB(m.GetClientLanes()),
		SIIndexers:                    SIIndexerEntriesFromPB(m.GetSiIndexers()),
		Verifiers:                     VerifierEntriesFromPB(m.GetVerifiers()),
		TracksEnabled:                 m.GetTracksEnabled(),
		Stage3:                        Stage3ParamsFromPB(m.GetStage3()),
		PromotionFinalityGate:         m.GetPromotionFinalityGate(),
		ManifestDeltaSnapshot:         m.GetManifestDeltaSnapshot(),
		TableAdmissionRulesVersion:    m.GetTableAdmissionRulesVersion(),
		PartClaimUniqueness:           m.GetPartClaimUniqueness(),
		Voters:                        VoterEntriesFromPB(m.GetVoters()),
		AnchorFinality:                AnchorFinalityParamsFromPB(m.GetAnchorFinality()),
		AuthorityThreshold:            m.GetAuthorityThreshold(),
		MembershipLeaseL2Blocks:       m.GetMembershipLeaseL2Blocks(),
		PromotionAckDeltaSnapshot:     m.GetPromotionAckDeltaSnapshot(),
		SnapshotQueryTracks:           m.GetSnapshotQueryTracks(),
	}
}

// ConsensusParamsUpdateToPB returns an independent transport message without
// changing the command's address ordering or other signed fields.
func ConsensusParamsUpdateToPB(v arbiter.ConsensusParamsUpdate) *pb.ConsensusParamsUpdate {
	return &pb.ConsensusParamsUpdate{
		NetworkId:                     v.NetworkID,
		GenesisSnapshotId:             v.GenesisSnapshotID,
		ExpectedEpoch:                 v.ExpectedEpoch,
		PreviousParamsDigest:          v.PreviousParamsDigest,
		AuthorityAddresses:            mapSlice(v.AuthorityAddresses, func(address string) string { return address }),
		MaxWriters:                    v.MaxWriters,
		ExpectedPromotionSeq:          v.ExpectedPromotionSeq,
		ArtifactDispositionCapability: v.ArtifactDispositionCapability,
		TableRegistry:                 TableRegistryParamsToPB(v.TableRegistry),
		ClientLanes:                   ClientLaneParamsToPB(v.ClientLanes),
		SiIndexers:                    SIIndexerEntriesToPB(v.SIIndexers),
		Verifiers:                     VerifierEntriesToPB(v.Verifiers),
		TracksEnabled:                 v.TracksEnabled,
		Stage3:                        Stage3ParamsToPB(v.Stage3),
		PromotionFinalityGate:         v.PromotionFinalityGate,
		ManifestDeltaSnapshot:         v.ManifestDeltaSnapshot,
		TableAdmissionRulesVersion:    v.TableAdmissionRulesVersion,
		PartClaimUniqueness:           v.PartClaimUniqueness,
		Voters:                        VoterEntriesToPB(v.Voters),
		AnchorFinality:                AnchorFinalityParamsToPB(v.AnchorFinality),
		AuthorityThreshold:            v.AuthorityThreshold,
		MembershipLeaseL2Blocks:       v.MembershipLeaseL2Blocks,
		PromotionAckDeltaSnapshot:     v.PromotionAckDeltaSnapshot,
		SnapshotQueryTracks:           v.SnapshotQueryTracks,
	}
}

// TableRegistryParamsFromPB copies the transport fields. Address-set
// normalization and structural validation belong to authority hashing.
func TableRegistryParamsFromPB(m *pb.TableRegistryParams) *arbiter.TableRegistryParams {
	if m == nil {
		return nil
	}
	return &arbiter.TableRegistryParams{ChainID: m.GetChainId(), DatabasesContract: m.GetDatabasesContract(),
		SIIndexerID: m.GetSiIndexerId(), ActivationBlock: m.GetActivationBlock(), Confirmation: m.GetConfirmation()}
}

// TableRegistryParamsToPB returns an independent transport message.
func TableRegistryParamsToPB(v *arbiter.TableRegistryParams) *pb.TableRegistryParams {
	if v == nil {
		return nil
	}
	return &pb.TableRegistryParams{ChainId: v.ChainID, DatabasesContract: v.DatabasesContract,
		SiIndexerId: v.SIIndexerID, ActivationBlock: v.ActivationBlock, Confirmation: v.Confirmation}
}

// ClientLaneParamsFromPB copies the transport fields; validation belongs to
// authority normalisation. A missing message decodes to nil.
func ClientLaneParamsFromPB(m *pb.ClientLaneParams) *arbiter.ClientLaneParams {
	if m == nil {
		return nil
	}
	return &arbiter.ClientLaneParams{MaxLanesPerAccount: m.GetMaxLanesPerAccount()}
}

// ClientLaneParamsToPB returns an independent transport message.
func ClientLaneParamsToPB(v *arbiter.ClientLaneParams) *pb.ClientLaneParams {
	if v == nil {
		return nil
	}
	return &pb.ClientLaneParams{MaxLanesPerAccount: v.MaxLanesPerAccount}
}

// L2BlockRefFromPB copies the transport fields; a missing ref decodes to nil.
func L2BlockRefFromPB(m *pb.L2BlockRef) *arbiter.L2BlockRef {
	if m == nil {
		return nil
	}
	return &arbiter.L2BlockRef{Number: m.GetNumber(), Hash: m.GetHash()}
}

// L2BlockRefToPB returns an independent transport message.
func L2BlockRefToPB(v *arbiter.L2BlockRef) *pb.L2BlockRef {
	if v == nil {
		return nil
	}
	return &pb.L2BlockRef{Number: v.Number, Hash: v.Hash}
}

// L2EventRefFromPB copies the transport fields; a missing ref decodes to nil.
func L2EventRefFromPB(m *pb.L2EventRef) *arbiter.L2EventRef {
	if m == nil {
		return nil
	}
	return &arbiter.L2EventRef{BlockNumber: m.GetBlockNumber(), BlockHash: m.GetBlockHash(),
		LogIndex: m.GetLogIndex(), TxHash: m.GetTxHash()}
}

// L2EventRefToPB returns an independent transport message.
func L2EventRefToPB(v *arbiter.L2EventRef) *pb.L2EventRef {
	if v == nil {
		return nil
	}
	return &pb.L2EventRef{BlockNumber: v.BlockNumber, BlockHash: v.BlockHash,
		LogIndex: v.LogIndex, TxHash: v.TxHash}
}

// SIIndexerEntriesFromPB copies a repeated SIIndexerEntry; validation belongs to
// authority normalisation. An empty list decodes to nil.
func SIIndexerEntriesFromPB(ms []*pb.SIIndexerEntry) []arbiter.SIIndexerEntry {
	return mapSlice(ms, func(m *pb.SIIndexerEntry) arbiter.SIIndexerEntry {
		return arbiter.SIIndexerEntry{IndexerID: m.GetIndexerId(), ActivationBlock: m.GetActivationBlock(),
			Signer: m.GetSigner(), SNodeNodeID: m.GetSnodeNodeId(), EnrollmentJWS: m.GetEnrollmentJws()}
	})
}

// SIIndexerEntriesToPB returns independent transport messages (nil for an empty list).
func SIIndexerEntriesToPB(v []arbiter.SIIndexerEntry) []*pb.SIIndexerEntry {
	return mapSlice(v, func(e arbiter.SIIndexerEntry) *pb.SIIndexerEntry {
		return &pb.SIIndexerEntry{IndexerId: e.IndexerID, ActivationBlock: e.ActivationBlock,
			Signer: e.Signer, SnodeNodeId: e.SNodeNodeID, EnrollmentJws: e.EnrollmentJWS}
	})
}

// VerifierEntriesFromPB copies a repeated VerifierEntry with all eleven
// fields (plan S3-E1 contract §41): each key's bytes, host_indexer_id as a
// fresh pointer (absent stays nil, indexer 0 stays 0) and every member field.
// An empty list and an empty key decode to nil; validation belongs to
// authority normalisation, which refuses member fields on a pool entry.
func VerifierEntriesFromPB(ms []*pb.VerifierEntry) []arbiter.VerifierEntry {
	return mapSlice(ms, func(m *pb.VerifierEntry) arbiter.VerifierEntry {
		if m == nil {
			return arbiter.VerifierEntry{}
		}
		return arbiter.VerifierEntry{NodeID: m.GetNodeId(), Ed25519Pubkey: cloneBytes(m.GetEd25519Pubkey()),
			HostIndexerID: cloneUint64(m.HostIndexerId), ReplicaSigner: m.GetReplicaSigner(), ConsentJWS: m.GetConsentJws(),
			Weight: m.GetWeight(), FailureDomain: m.GetFailureDomain(),
			Verify: m.GetVerify(), Replicate: m.GetReplicate(), Counts: m.GetCounts(), SharedRMT: m.GetSharedRmt()}
	})
}

// VerifierEntriesToPB returns independent transport messages. A pool entry
// (no HostIndexerID, every member field zero) encodes to exactly the bytes of
// arbiter-proto v0.11.0's two-field VerifierEntry.
func VerifierEntriesToPB(v []arbiter.VerifierEntry) []*pb.VerifierEntry {
	return mapSlice(v, func(e arbiter.VerifierEntry) *pb.VerifierEntry {
		return &pb.VerifierEntry{NodeId: e.NodeID, Ed25519Pubkey: cloneBytes(e.Ed25519Pubkey),
			HostIndexerId: cloneUint64(e.HostIndexerID), ReplicaSigner: e.ReplicaSigner, ConsentJws: e.ConsentJWS,
			Weight: e.Weight, FailureDomain: e.FailureDomain,
			Verify: e.Verify, Replicate: e.Replicate, Counts: e.Counts, SharedRmt: e.SharedRMT}
	})
}

// Stage3ParamsFromPB copies the stage-3 parameter (housegate spec 2026-10-14
// §6.1, §10.4; plan S3-E1 contract §41): nil for an absent message, an empty
// indexer_failure_domains list as nil. Validation belongs to
// arbiter.Stage3Params.Validate and authority normalisation.
func Stage3ParamsFromPB(m *pb.Stage3Params) *arbiter.Stage3Params {
	if m == nil {
		return nil
	}
	return &arbiter.Stage3Params{
		GroupSize:          m.GetGroupSize(),
		NetworkFillDefault: m.GetNetworkFillDefault(),
		MinSafeCopies:      m.GetMinSafeCopies(),
		StrictCopies:       m.GetStrictCopies(),
		IndexerFailureDomains: mapSlice(m.GetIndexerFailureDomains(), func(d *pb.IndexerFailureDomain) arbiter.IndexerFailureDomain {
			return arbiter.IndexerFailureDomain{IndexerID: d.GetIndexerId(), FailureDomain: d.GetFailureDomain()}
		}),
		MaxLagPublications:            m.GetMaxLagPublications(),
		LagLow:                        m.GetLagLow(),
		EvidenceTimeoutL2Blocks:       m.GetEvidenceTimeoutL2Blocks(),
		MaxUncleanedPartsPerPartition: m.GetMaxUncleanedPartsPerPartition(),
		MaxUncleanedPartsPerTable:     m.GetMaxUncleanedPartsPerTable(),
		ExcludeQuarantinedHosts:       m.GetExcludeQuarantinedHosts(),
		Purge:                         m.GetPurge(),
		EvictionVoiding:               m.GetEvictionVoiding(),
		OwnerAwareSelection:           m.GetOwnerAwareSelection(),
		Placement:                     m.GetPlacement(),
		Reselection:                   m.GetReselection(),
		Checkpoints:                   m.GetCheckpoints(),
		NarrowedDrain:                 m.GetNarrowedDrain(),
	}
}

// Stage3ParamsToPB returns an independent transport message, nil for nil.
func Stage3ParamsToPB(v *arbiter.Stage3Params) *pb.Stage3Params {
	if v == nil {
		return nil
	}
	return &pb.Stage3Params{
		GroupSize:          v.GroupSize,
		NetworkFillDefault: v.NetworkFillDefault,
		MinSafeCopies:      v.MinSafeCopies,
		StrictCopies:       v.StrictCopies,
		IndexerFailureDomains: mapSlice(v.IndexerFailureDomains, func(d arbiter.IndexerFailureDomain) *pb.IndexerFailureDomain {
			return &pb.IndexerFailureDomain{IndexerId: d.IndexerID, FailureDomain: d.FailureDomain}
		}),
		MaxLagPublications:            v.MaxLagPublications,
		LagLow:                        v.LagLow,
		EvidenceTimeoutL2Blocks:       v.EvidenceTimeoutL2Blocks,
		MaxUncleanedPartsPerPartition: v.MaxUncleanedPartsPerPartition,
		MaxUncleanedPartsPerTable:     v.MaxUncleanedPartsPerTable,
		ExcludeQuarantinedHosts:       v.ExcludeQuarantinedHosts,
		Purge:                         v.Purge,
		EvictionVoiding:               v.EvictionVoiding,
		OwnerAwareSelection:           v.OwnerAwareSelection,
		Placement:                     v.Placement,
		Reselection:                   v.Reselection,
		Checkpoints:                   v.Checkpoints,
		NarrowedDrain:                 v.NarrowedDrain,
	}
}

// cloneBytes copies b; an empty slice becomes nil, the canonical empty form.
func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
