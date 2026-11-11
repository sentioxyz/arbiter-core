package authority

import (
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// TestNormalizeConsensusParamsUpdateAccountBudget (arbiter#143 layer C): the
// budget needs table_registry and si_indexers in the same update, its static
// bounds hold, and it survives normalization as an independent copy.
func TestNormalizeConsensusParamsUpdateAccountBudget(t *testing.T) {
	valid := arbiter.AccountBudgetParams{WindowL2Blocks: 1800, MaxStatements: 100, MaxBytes: 1 << 30, MaxStatementsTotal: 1000, MaxBytesTotal: 1 << 33, Enforce: true}
	u := signedClaimsTestUpdate()
	u.TableRegistry = validRegistryParams()
	budget := valid
	u.AccountBudget = &budget
	got, err := NormalizeConsensusParamsUpdate(u)
	if err != nil || got.AccountBudget == nil || *got.AccountBudget != valid {
		t.Fatalf("budget with its prerequisites: %v, %+v", err, got.AccountBudget)
	}
	if got.AccountBudget == u.AccountBudget {
		t.Fatal("normalization must copy the budget")
	}

	const prereq = "consensus params update: account_budget requires table_registry and si_indexers in the same update: the budget refills on committed L2 cursor blocks"
	noRegistry := u
	noRegistry.TableRegistry = nil
	if _, err := NormalizeConsensusParamsUpdate(noRegistry); err == nil || !strings.Contains(err.Error(), "account_budget requires") {
		t.Fatalf("budget without table_registry: err = %v", err)
	}
	noIndexers := u
	noIndexers.SIIndexers = nil
	noIndexers.Verifiers = nil
	if _, err := NormalizeConsensusParamsUpdate(noIndexers); err == nil || err.Error() != prereq {
		t.Fatalf("budget without si_indexers: err = %v, want %q", err, prereq)
	}

	for name, mutate := range map[string]func(*arbiter.AccountBudgetParams){
		"zero window":            func(p *arbiter.AccountBudgetParams) { p.WindowL2Blocks = 0 },
		"window above the cap":   func(p *arbiter.AccountBudgetParams) { p.WindowL2Blocks = arbiter.MaxAccountBudgetWindowL2Blocks + 1 },
		"zero max_statements":    func(p *arbiter.AccountBudgetParams) { p.MaxStatements = 0 },
		"zero max_bytes":         func(p *arbiter.AccountBudgetParams) { p.MaxBytes = 0 },
		"total statements below": func(p *arbiter.AccountBudgetParams) { p.MaxStatementsTotal = p.MaxStatements - 1 },
		"total bytes below":      func(p *arbiter.AccountBudgetParams) { p.MaxBytesTotal = p.MaxBytes - 1 },
		"total above the cap":    func(p *arbiter.AccountBudgetParams) { p.MaxBytesTotal = arbiter.MaxAccountBudgetValue + 1 },
	} {
		bad := valid
		mutate(&bad)
		v := u
		v.AccountBudget = &bad
		if _, err := NormalizeConsensusParamsUpdate(v); err == nil || !strings.HasPrefix(err.Error(), "consensus params update: account budget: ") {
			t.Fatalf("%s: err = %v", name, err)
		}
	}

	u.AccountBudget = nil
	if got, err := NormalizeConsensusParamsUpdate(u); err != nil || got.AccountBudget != nil {
		t.Fatalf("no budget: %v, %+v", err, got.AccountBudget)
	}
}
