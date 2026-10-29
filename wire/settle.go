package wire

import (
	"slices"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core"
)

// SettleTrack mirrors pb.SettleTrackRequest, the ConsensusAdmin.SettleTrack
// request of a leader-local settle of a quarantined track (housegate spec
// 2026-10-14 §14, P4; plan S3-D clarification K53). It is never a Raft command
// and has no Command field. PromotionSeqs is kept exactly as sent: Canonical
// does not sort it, and the leader refuses a list that is not strictly
// ascending (arbiter.SettleTrackCommand.Validate).
type SettleTrack struct {
	IndexerID          uint64
	ExpectedGeneration uint64
	PromotionSeqs      []uint64
	AuthorityJWS       string
	TimeoutMS          uint64
}

// Canonical is the settle's signing form (authority.SettleTrackHash) with an
// independent copy of the list.
func (s SettleTrack) Canonical() arbiter.SettleTrackCommand {
	return arbiter.SettleTrackCommand{IndexerID: s.IndexerID, ExpectedGeneration: s.ExpectedGeneration, PromotionSeqs: slices.Clone(s.PromotionSeqs)}
}

// SettleTrackFromRequest decodes a SettleTrack request; an empty list is nil.
func SettleTrackFromRequest(m *pb.SettleTrackRequest) SettleTrack {
	return SettleTrack{IndexerID: m.GetIndexerId(), ExpectedGeneration: m.GetExpectedGeneration(),
		PromotionSeqs: mapSlice(m.GetPromotionSeqs(), func(s uint64) uint64 { return s }),
		AuthorityJWS:  m.GetAuthorityJws(), TimeoutMS: m.GetTimeoutMs()}
}

// SettleTrackToRequest builds the SettleTrack request with an independent
// copy of the list.
func SettleTrackToRequest(s SettleTrack) *pb.SettleTrackRequest {
	return &pb.SettleTrackRequest{IndexerId: s.IndexerID, ExpectedGeneration: s.ExpectedGeneration,
		PromotionSeqs: mapSlice(s.PromotionSeqs, func(s uint64) uint64 { return s }),
		AuthorityJws:  s.AuthorityJWS, TimeoutMs: s.TimeoutMS}
}
