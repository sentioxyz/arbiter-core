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
	}
}
