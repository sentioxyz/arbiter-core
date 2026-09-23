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
