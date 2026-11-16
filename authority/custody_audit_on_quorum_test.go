package authority

import "testing"

// TestNormalizeConsensusParamsUpdateCustodyAuditOnQuorum: the parameter needs
// custody_frontier in the same update (every update carries it once it is
// set), and it survives normalization.
func TestNormalizeConsensusParamsUpdateCustodyAuditOnQuorum(t *testing.T) {
	u := stage3TestUpdate()
	u.ManifestDeltaSnapshot, u.PromotionAckDeltaSnapshot, u.CustodyFrontier, u.CustodyAuditOnQuorum = true, true, true, true
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || !got.CustodyAuditOnQuorum {
		t.Fatalf("parameter with its prerequisite: %v, parameter %v", err, got.CustodyAuditOnQuorum)
	}
	const want = "consensus params update: custody_audit_on_quorum requires custody_frontier in the same update"
	noFrontier := u
	noFrontier.CustodyFrontier = false
	if _, err := NormalizeConsensusParamsUpdate(noFrontier); err == nil || err.Error() != want {
		t.Fatalf("parameter without custody_frontier: err = %v, want %q", err, want)
	}
	u.CustodyAuditOnQuorum = false
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.CustodyAuditOnQuorum {
		t.Fatalf("no parameter: %v, parameter %v", err, got.CustodyAuditOnQuorum)
	}
}
