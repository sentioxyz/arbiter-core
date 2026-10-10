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

// VerifierEntriesFromPB copies a repeated VerifierEntry including each key's bytes;
// an empty list and an empty key decode to nil.
func VerifierEntriesFromPB(ms []*pb.VerifierEntry) []arbiter.VerifierEntry {
	return mapSlice(ms, func(m *pb.VerifierEntry) arbiter.VerifierEntry {
		return arbiter.VerifierEntry{NodeID: m.GetNodeId(), Ed25519Pubkey: cloneBytes(m.GetEd25519Pubkey())}
	})
}

// VerifierEntriesToPB returns independent transport messages.
func VerifierEntriesToPB(v []arbiter.VerifierEntry) []*pb.VerifierEntry {
	return mapSlice(v, func(e arbiter.VerifierEntry) *pb.VerifierEntry {
		return &pb.VerifierEntry{NodeId: e.NodeID, Ed25519Pubkey: cloneBytes(e.Ed25519Pubkey)}
	})
}

// cloneBytes copies b; an empty slice becomes nil, the canonical empty form.
func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	return append([]byte(nil), b...)
}
