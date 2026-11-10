package wire

import (
	"errors"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// BlockHeaderView is the part of a sealed block's header that a block
// verification read is used for (plan S3-C2 contract §21.2): which block, which
// statements, which base manifest, whether it is a table-set transition and
// whose track it is. The other header fields (hashes, roots, the L2 anchor)
// stay with GetL3Block, which serves the whole header for auditing.
type BlockHeaderView struct {
	L3BlockSeq         uint64
	StatementSeqStart  uint64
	StatementCount     uint32
	PrevSafeSnapshotID string
	PrevStateRoot      string
	// IsTableSetTransition reports that header.table_set_transition is present.
	IsTableSetTransition bool
	// TrackIndexerID is header.track; nil on a block sealed before the tracks
	// activation. Indexer 0 is a value, distinct from absent.
	TrackIndexerID *uint64
	// GroupKey is header.track.group_key (plan S3-E1 contract §39): set on
	// every block sealed after the placement switch's cutover block, empty on
	// every earlier block and on a block without a track.
	GroupKey string
}

// StatementCandidates mirrors pb.StatementCandidates: one statement of the
// block with the candidate parts and partition sums its source claimed.
type StatementCandidates struct {
	StatementSeq              uint64
	StatementID               string
	Status                    string
	SourceNode                string
	RCBound                   bool
	SourceClaimRoot           string
	CandidateParts            []arbiter.CandidatePart
	PartitionNewPartSums      []arbiter.PartitionLtHashSum
	UnpromotedPartRowLtHashes []string
}

// VerifierEvidence mirrors pb.VerifierEvidence: what one verifier of the
// block's verifier set recorded, and the FSM's checks of it. Evaluated is
// false for evidence recorded after the quorum latched (plan S3-A contract
// §4); its checks are then not part of the verdict.
type VerifierEvidence struct {
	ReplicaID            string
	Attestation          *replay.ReplayAttestation // nil when not recorded
	ByteSideScan         *arbiter.ByteSideScanMsg  // nil when not recorded
	Evaluated            bool
	Check1RootMatch      bool
	Check2PartitionMatch bool
	Check3ByteSideMatch  bool
	Pass                 bool
}

// BlockVerification is the canonical Go form of SafeState.GetBlockVerification
// (plan S3-A contract §1; plan S3-C2 contract §21.2). It is a read reply, not
// a signing form, so it carries no JSON tags.
type BlockVerification struct {
	Header           BlockHeaderView
	ChainHash        string
	ReplayJob        replay.ReplayJob
	Statements       []StatementCandidates
	ScanParts        []arbiter.PartRef
	SourceNodes      []string
	VerifierSet      []string
	Evidence         []VerifierEvidence
	VerdictEvaluated bool
	Quorum           bool
	Anchor           *arbiter.AnchorRef // nil when the block has no anchor
	Finality         bool
	LastMergeable    bool
	// The stage-3 selection record (housegate spec 2026-10-14 §7.1, §7.3,
	// §7.4; plan S3-E1 contract §39), absent on every block marked before the
	// owner_aware_selection switch: the eligible ring MarkReplaying drew from
	// (sorted), the last ring position used and the registry cursor block at
	// the marking; the group key, ready verify members and fill recorded at
	// seal with every (database, group_generation) that carried the key (K62);
	// every applied ReselectVerifiers and the count at the quorum latch.
	EligibleRing        []string
	RingPosition        uint32
	MarkedCursorBlock   uint64
	GroupKey            string
	GroupMembers        []string
	GroupFill           bool
	GroupDatabases      []BlockGroupRef
	Reselections        []BlockReselection
	ReselectionsAtLatch uint32
	// FinalityAttestations are the latest anchor finality attestation each
	// voter recorded, sorted by voter id; nil before the anchor finality
	// quorum is active.
	FinalityAttestations []arbiter.AnchorFinalityAttestation
}

// BlockGroupRef mirrors pb.BlockGroupRef: one database whose group carried the
// block's group key at seal, at that group's generation.
type BlockGroupRef struct {
	DatabaseID      string
	GroupGeneration uint64
}

// BlockReselection mirrors pb.BlockReselection: one applied ReselectVerifiers
// with the replaced ids (sorted) and corresponding replacements (in stored-ring
// walk order), the ring position after it, the registry cursor block it recorded
// and its authority proof.
type BlockReselection struct {
	Replaced, Replacements []string
	RingPosition           uint32
	CursorBlock            uint64
	AuthorityJWS           string
	AuthorityEpoch         uint64
}

// BlockVerificationFromPB converts a GetBlockVerification answer. A message
// without a header names no block and is refused; repeated fields decode empty
// to nil and absent messages to nil pointers. The selection record must be as
// the arbiter keeps it: a sorted eligible ring and member list, and a ring
// position inside the ring (0 when there is none).
func BlockVerificationFromPB(m *pb.BlockVerification) (BlockVerification, error) {
	h := m.GetHeader()
	if h == nil {
		return BlockVerification{}, errors.New("wire: block verification has no header")
	}
	out := BlockVerification{
		Header:           blockHeaderViewFromPB(h),
		ChainHash:        m.GetChainHash(),
		ReplayJob:        ReplayJobFromPB(m.GetReplayJob()),
		Statements:       mapSlice(m.GetStatements(), statementCandidatesFromPB),
		ScanParts:        PartRefsFromPB(m.GetScanParts()),
		SourceNodes:      mapSlice(m.GetSourceNodes(), func(s string) string { return s }),
		VerifierSet:      mapSlice(m.GetVerifierSet(), func(s string) string { return s }),
		Evidence:         mapSlice(m.GetEvidence(), verifierEvidenceFromPB),
		VerdictEvaluated: m.GetVerdictEvaluated(),
		Quorum:           m.GetQuorum(),
		Finality:         m.GetFinality(),
		LastMergeable:    m.GetLastMergeable(),

		EligibleRing:      mapSlice(m.GetEligibleRing(), func(s string) string { return s }),
		RingPosition:      m.GetRingPosition(),
		MarkedCursorBlock: m.GetMarkedCursorBlock(),
		GroupKey:          m.GetGroupKey(),
		GroupMembers:      mapSlice(m.GetGroupMembers(), func(s string) string { return s }),
		GroupFill:         m.GetGroupFill(),
		GroupDatabases: mapSlice(m.GetGroupDatabases(), func(g *pb.BlockGroupRef) BlockGroupRef {
			return BlockGroupRef{DatabaseID: g.GetDatabaseId(), GroupGeneration: g.GetGroupGeneration()}
		}),
		Reselections:         mapSlice(m.GetReselections(), blockReselectionFromPB),
		ReselectionsAtLatch:  m.GetReselectionsAtLatch(),
		FinalityAttestations: mapSlice(m.GetFinalityAttestations(), AnchorFinalityAttestationFromPB),
	}
	if a := m.GetAnchor(); a != nil {
		anchor := AnchorRefFromPB(a)
		out.Anchor = &anchor
	}
	if !ascendingUnique(out.EligibleRing) || !ascendingUnique(out.GroupMembers) {
		return BlockVerification{}, errors.New("block verification: eligible_ring and group_members must be sorted ascending without duplicates")
	}
	if n := len(out.EligibleRing); (n == 0 && out.RingPosition != 0) || (n > 0 && int(out.RingPosition) >= n) {
		return BlockVerification{}, fmt.Errorf("block verification: ring_position %d is outside an eligible ring of %d", out.RingPosition, n)
	}
	return out, nil
}

// BlockVerificationToPB is BlockVerificationFromPB's inverse, for fake arbiter
// servers and tests. The header fields outside BlockHeaderView stay empty, and
// a transition block's table_set_transition is an empty, present message.
func BlockVerificationToPB(v BlockVerification) *pb.BlockVerification {
	h := &pb.L3BlockHeader{
		L3BlockSeq:         v.Header.L3BlockSeq,
		StatementSeqStart:  v.Header.StatementSeqStart,
		StatementCount:     v.Header.StatementCount,
		PrevSafeSnapshotId: v.Header.PrevSafeSnapshotID,
		PrevStateRoot:      v.Header.PrevStateRoot,
	}
	if v.Header.IsTableSetTransition {
		h.TableSetTransition = &pb.TableSetTransition{}
	}
	if v.Header.TrackIndexerID != nil {
		h.Track = &pb.L3BlockTrack{IndexerId: *v.Header.TrackIndexerID, GroupKey: v.Header.GroupKey}
	}
	out := &pb.BlockVerification{
		Header:           h,
		ChainHash:        v.ChainHash,
		ReplayJob:        ReplayJobToPB(v.ReplayJob),
		Statements:       mapSlice(v.Statements, statementCandidatesToPB),
		ScanParts:        mapSlice(v.ScanParts, partRefToPB),
		SourceNodes:      mapSlice(v.SourceNodes, func(s string) string { return s }),
		VerifierSet:      mapSlice(v.VerifierSet, func(s string) string { return s }),
		Evidence:         mapSlice(v.Evidence, verifierEvidenceToPB),
		VerdictEvaluated: v.VerdictEvaluated,
		Quorum:           v.Quorum,
		Finality:         v.Finality,
		LastMergeable:    v.LastMergeable,

		EligibleRing:      mapSlice(v.EligibleRing, func(s string) string { return s }),
		RingPosition:      v.RingPosition,
		MarkedCursorBlock: v.MarkedCursorBlock,
		GroupKey:          v.GroupKey,
		GroupMembers:      mapSlice(v.GroupMembers, func(s string) string { return s }),
		GroupFill:         v.GroupFill,
		GroupDatabases: mapSlice(v.GroupDatabases, func(g BlockGroupRef) *pb.BlockGroupRef {
			return &pb.BlockGroupRef{DatabaseId: g.DatabaseID, GroupGeneration: g.GroupGeneration}
		}),
		Reselections:         mapSlice(v.Reselections, blockReselectionToPB),
		ReselectionsAtLatch:  v.ReselectionsAtLatch,
		FinalityAttestations: mapSlice(v.FinalityAttestations, AnchorFinalityAttestationToPB),
	}
	if v.Anchor != nil {
		out.Anchor = AnchorRefToPB(*v.Anchor)
	}
	return out
}

func blockHeaderViewFromPB(h *pb.L3BlockHeader) BlockHeaderView {
	v := BlockHeaderView{
		L3BlockSeq:           h.GetL3BlockSeq(),
		StatementSeqStart:    h.GetStatementSeqStart(),
		StatementCount:       h.GetStatementCount(),
		PrevSafeSnapshotID:   h.GetPrevSafeSnapshotId(),
		PrevStateRoot:        h.GetPrevStateRoot(),
		IsTableSetTransition: h.GetTableSetTransition() != nil,
	}
	if track := h.GetTrack(); track != nil {
		id := track.GetIndexerId()
		v.TrackIndexerID = &id
		v.GroupKey = track.GetGroupKey()
	}
	return v
}

func blockReselectionFromPB(m *pb.BlockReselection) BlockReselection {
	return BlockReselection{
		Replaced:       mapSlice(m.GetReplaced(), func(s string) string { return s }),
		Replacements:   mapSlice(m.GetReplacements(), func(s string) string { return s }),
		RingPosition:   m.GetRingPosition(),
		CursorBlock:    m.GetCursorBlock(),
		AuthorityJWS:   m.GetAuthorityJws(),
		AuthorityEpoch: m.GetAuthorityEpoch(),
	}
}

func blockReselectionToPB(v BlockReselection) *pb.BlockReselection {
	return &pb.BlockReselection{
		Replaced:       mapSlice(v.Replaced, func(s string) string { return s }),
		Replacements:   mapSlice(v.Replacements, func(s string) string { return s }),
		RingPosition:   v.RingPosition,
		CursorBlock:    v.CursorBlock,
		AuthorityJws:   v.AuthorityJWS,
		AuthorityEpoch: v.AuthorityEpoch,
	}
}

// ascendingUnique reports whether ids is sorted ascending without duplicates.
func ascendingUnique(ids []string) bool {
	for i := 1; i < len(ids); i++ {
		if ids[i-1] >= ids[i] {
			return false
		}
	}
	return true
}

func statementCandidatesFromPB(m *pb.StatementCandidates) StatementCandidates {
	return StatementCandidates{
		StatementSeq:              m.GetStatementSeq(),
		StatementID:               m.GetStatementId(),
		Status:                    m.GetStatus(),
		SourceNode:                m.GetSourceNode(),
		RCBound:                   m.GetRcBound(),
		SourceClaimRoot:           m.GetSourceClaimRoot(),
		CandidateParts:            mapSlice(m.GetCandidateParts(), candidatePartFromPB),
		PartitionNewPartSums:      mapSlice(m.GetPartitionNewPartSums(), partitionSumFromPB),
		UnpromotedPartRowLtHashes: mapSlice(m.GetUnpromotedPartRowLthashes(), func(s string) string { return s }),
	}
}

func statementCandidatesToPB(v StatementCandidates) *pb.StatementCandidates {
	return &pb.StatementCandidates{
		StatementSeq:              v.StatementSeq,
		StatementId:               v.StatementID,
		Status:                    v.Status,
		SourceNode:                v.SourceNode,
		RcBound:                   v.RCBound,
		SourceClaimRoot:           v.SourceClaimRoot,
		CandidateParts:            mapSlice(v.CandidateParts, CandidatePartToPB),
		PartitionNewPartSums:      mapSlice(v.PartitionNewPartSums, PartitionLtHashSumToPB),
		UnpromotedPartRowLthashes: mapSlice(v.UnpromotedPartRowLtHashes, func(s string) string { return s }),
	}
}

func verifierEvidenceFromPB(m *pb.VerifierEvidence) VerifierEvidence {
	out := VerifierEvidence{
		ReplicaID:            m.GetReplicaId(),
		Evaluated:            m.GetEvaluated(),
		Check1RootMatch:      m.GetCheck1RootMatch(),
		Check2PartitionMatch: m.GetCheck2PartitionMatch(),
		Check3ByteSideMatch:  m.GetCheck3ByteSideMatch(),
		Pass:                 m.GetPass(),
	}
	if a := m.GetAttestation(); a != nil {
		att := AttestationFromPB(a)
		out.Attestation = &att
	}
	if s := m.GetByteSideScan(); s != nil {
		scan := ScanFromPB(s)
		out.ByteSideScan = &scan
	}
	return out
}

func verifierEvidenceToPB(v VerifierEvidence) *pb.VerifierEvidence {
	out := &pb.VerifierEvidence{
		ReplicaId:            v.ReplicaID,
		Evaluated:            v.Evaluated,
		Check1RootMatch:      v.Check1RootMatch,
		Check2PartitionMatch: v.Check2PartitionMatch,
		Check3ByteSideMatch:  v.Check3ByteSideMatch,
		Pass:                 v.Pass,
	}
	if v.Attestation != nil {
		out.Attestation = AttestationToPB(*v.Attestation)
	}
	if v.ByteSideScan != nil {
		out.ByteSideScan = ScanToPB(*v.ByteSideScan)
	}
	return out
}
