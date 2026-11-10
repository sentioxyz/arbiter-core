package authority

import (
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestNormalizeConsensusParamsUpdateTableAdmissionRulesVersion: the version
// is 0 (rules v1) or a known version above 1, needs table_registry in the
// same update, survives normalization, and is a signed field.
func TestNormalizeConsensusParamsUpdateTableAdmissionRulesVersion(t *testing.T) {
	u := testConsensusUpdate()
	u.TableRegistry = validRegistryParams()
	u.TableAdmissionRulesVersion = 2
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || got.TableAdmissionRulesVersion != 2 {
		t.Fatalf("v2 with the registry: %v, version %d", err, got.TableAdmissionRulesVersion)
	}
	for _, bad := range []uint32{1, 3, 0xffffffff} {
		u.TableAdmissionRulesVersion = bad
		if _, err := NormalizeConsensusParamsUpdate(u); err == nil || !strings.Contains(err.Error(), "table_admission_rules_version") {
			t.Fatalf("version %d: err = %v", bad, err)
		}
	}
	u = testConsensusUpdate()
	u.TableAdmissionRulesVersion = 2
	const want = "consensus params update: table_admission_rules_version requires table_registry in the same update"
	if _, err := NormalizeConsensusParamsUpdate(u); err == nil || err.Error() != want {
		t.Fatalf("v2 without the registry: err = %v, want %q", err, want)
	}
	u.TableAdmissionRulesVersion = 0
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.TableAdmissionRulesVersion != 0 {
		t.Fatalf("unset: %v, version %d", err, got.TableAdmissionRulesVersion)
	}

	// Signed: a token over the unset update does not verify the raise, and
	// the unset update hashes exactly as before the field existed (the field
	// is omitted from the canonical form while zero).
	s, v := newTestPair(t)
	base := testConsensusUpdate()
	base.TableRegistry = validRegistryParams()
	token, err := s.SignConsensusParamsUpdateAt(base, 1)
	if err != nil {
		t.Fatal(err)
	}
	raised := base
	raised.TableAdmissionRulesVersion = 2
	if _, err := v.VerifyConsensusParamsUpdate(raised, token); err == nil {
		t.Fatal("the rules version is not covered by the authority signature")
	}
	h0, err := ConsensusParamsUpdateHash(base)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := ConsensusParamsUpdateHash(raised)
	if err != nil || h0 == h2 {
		t.Fatalf("raised update hash = %s (%v), unset %s", h2, err, h0)
	}
	var _ arbiter.ConsensusParamsUpdate = raised
}
