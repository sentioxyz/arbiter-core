package shadow

import (
	"strings"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// ownScan is the shadow's scan of one candidate part: the sum of the row
// LtHashes of every part its scratch holds (one part after a no-split pull).
type ownScan struct {
	Part    arbiter.CandidatePart
	Scanned string
}

// mismatch is one recorded body that disagrees with the shadow.
type mismatch struct {
	Kind                           MismatchKind
	ReplicaID                      string // "" for a claim mismatch
	TableID, PartitionID, PartName string
	Own, Recorded                  string
}

// compareBlock is clarification K34 over one block. It compares bodies only:
//   - receipt: every recorded attestation's receipt_hash against the shadow's
//     own receipt hash, one mismatch per disagreeing attestation;
//   - scan: for every candidate part the shadow scanned, the scanned row
//     LtHash every recorded byte-side scan reports for it (keyed, as the
//     FSM's check 3 keys it, by the claimed row LtHash), one mismatch per
//     disagreeing or missing report;
//   - claim: only when the block's quorum passed, every scanned candidate
//     whose row LtHash differs from the claimed one.
//
// Evidence evaluated by the verdict and evidence recorded after the quorum
// latched are compared alike. Hex comparisons ignore case, as check 3 does.
func compareBlock(ownReceipt string, scans []ownScan, bv wire.BlockVerification) []mismatch {
	var out []mismatch
	for _, ev := range bv.Evidence {
		if ev.Attestation != nil && ev.Attestation.ReceiptHash != ownReceipt {
			out = append(out, mismatch{Kind: MismatchReceipt, ReplicaID: ev.ReplicaID, Own: ownReceipt, Recorded: ev.Attestation.ReceiptHash})
		}
	}
	for _, ev := range bv.Evidence {
		if ev.ByteSideScan == nil {
			continue
		}
		reported := make(map[string]string, len(ev.ByteSideScan.Parts))
		for _, p := range ev.ByteSideScan.Parts {
			reported[strings.ToLower(p.ClaimedPartRowLtHash)] = p.ScannedPartRowLtHash
		}
		for _, s := range scans {
			recorded, ok := reported[strings.ToLower(s.Part.PartRowLtHash)]
			if !ok || !strings.EqualFold(recorded, s.Scanned) {
				out = append(out, mismatch{Kind: MismatchScan, ReplicaID: ev.ReplicaID, TableID: s.Part.TableID,
					PartitionID: s.Part.PartitionID, PartName: s.Part.PartName, Own: s.Scanned, Recorded: recorded})
			}
		}
	}
	if bv.VerdictEvaluated && bv.Quorum {
		for _, s := range scans {
			if !strings.EqualFold(s.Scanned, s.Part.PartRowLtHash) {
				out = append(out, mismatch{Kind: MismatchClaim, TableID: s.Part.TableID, PartitionID: s.Part.PartitionID,
					PartName: s.Part.PartName, Own: s.Scanned, Recorded: s.Part.PartRowLtHash})
			}
		}
	}
	return out
}
