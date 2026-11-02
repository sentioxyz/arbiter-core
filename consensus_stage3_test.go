package arbiter

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
)

// validStage3Params is a complete parameter with every switch off, the shape
// of the v21 activation (plan S3-E1 K58: every field explicit).
func validStage3Params() Stage3Params {
	return Stage3Params{GroupSize: 3, NetworkFillDefault: true, MinSafeCopies: 1,
		IndexerFailureDomains: []IndexerFailureDomain{{IndexerID: 0, FailureDomain: "node:a"}, {IndexerID: 1, FailureDomain: "node:b"}},
		MaxLagPublications:    4, LagLow: 2, EvidenceTimeoutL2Blocks: 30, MaxUncleanedPartsPerPartition: 2400, MaxUncleanedPartsPerTable: 20000}
}

// withSwitches returns p with exactly the named switches on.
func withSwitches(p Stage3Params, names ...string) Stage3Params {
	v := reflect.ValueOf(&p).Elem()
	for _, name := range names {
		for i := 0; i < v.NumField(); i++ {
			if strings.Split(v.Type().Field(i).Tag.Get("json"), ",")[0] == name {
				v.Field(i).SetBool(true)
			}
		}
	}
	return p
}

// TestStage3ParamsValidate: every static refusal of contract §41, verbatim,
// and a complete parameter with every switch off passes.
func TestStage3ParamsValidate(t *testing.T) {
	if err := validStage3Params().Validate(); err != nil {
		t.Fatalf("the activation shape: %v", err)
	}
	all := withSwitches(validStage3Params(), Stage3Switches...)
	all.MinSafeCopies, all.StrictCopies = 2, true
	if err := all.Validate(); err != nil {
		t.Fatalf("every switch on with k=2 and strict copies: %v", err)
	}
	for name, tc := range map[string]struct {
		mutate func(*Stage3Params)
		want   string
	}{
		"group_size 0":       {func(p *Stage3Params) { p.GroupSize = 0 }, "stage3: group_size must be at least 1"},
		"min_safe_copies 0":  {func(p *Stage3Params) { p.MinSafeCopies = 0 }, "stage3: min_safe_copies must be at least 1"},
		"lag_low 0":          {func(p *Stage3Params) { p.LagLow = 0 }, "stage3: lag_low must satisfy 1 <= lag_low < max_lag_publications, got lag_low 0 and max_lag_publications 4"},
		"lag_low equal":      {func(p *Stage3Params) { p.LagLow = 4 }, "stage3: lag_low must satisfy 1 <= lag_low < max_lag_publications, got lag_low 4 and max_lag_publications 4"},
		"no max lag":         {func(p *Stage3Params) { p.MaxLagPublications, p.LagLow = 0, 0 }, "stage3: lag_low must satisfy 1 <= lag_low < max_lag_publications, got lag_low 0 and max_lag_publications 0"},
		"evidence timeout 0": {func(p *Stage3Params) { p.EvidenceTimeoutL2Blocks = 0 }, "stage3: evidence_timeout_l2_blocks must be at least 1"},
		"partition bound 0":  {func(p *Stage3Params) { p.MaxUncleanedPartsPerPartition = 0 }, "stage3: max_uncleaned_parts_per_partition and max_uncleaned_parts_per_table must be at least 1"},
		"table bound 0":      {func(p *Stage3Params) { p.MaxUncleanedPartsPerTable = 0 }, "stage3: max_uncleaned_parts_per_partition and max_uncleaned_parts_per_table must be at least 1"},
		"empty domain":       {func(p *Stage3Params) { p.IndexerFailureDomains[1].FailureDomain = "" }, "stage3: indexer_failure_domains: indexer 1: failure_domain must be non-empty and free of whitespace and control characters"},
		"domain with space":  {func(p *Stage3Params) { p.IndexerFailureDomains[0].FailureDomain = "node a" }, "stage3: indexer_failure_domains: indexer 0: failure_domain must be non-empty and free of whitespace and control characters"},
		"domain with a tab":  {func(p *Stage3Params) { p.IndexerFailureDomains[0].FailureDomain = "node\ta" }, "stage3: indexer_failure_domains: indexer 0: failure_domain must be non-empty and free of whitespace and control characters"},
		"unsorted domains": {func(p *Stage3Params) {
			p.IndexerFailureDomains[0], p.IndexerFailureDomains[1] = p.IndexerFailureDomains[1], p.IndexerFailureDomains[0]
		}, "stage3: indexer_failure_domains must be sorted by indexer_id without duplicates"},
		"duplicate domain":     {func(p *Stage3Params) { p.IndexerFailureDomains[1].IndexerID = 0 }, "stage3: indexer_failure_domains must be sorted by indexer_id without duplicates"},
		"k above 1 too early":  {func(p *Stage3Params) { *p = withSwitches(*p, Stage3Switches[:5]...); p.MinSafeCopies = 2 }, "stage3: min_safe_copies above 1 and strict_copies require the checkpoints and narrowed_drain switches"},
		"strict without drain": {func(p *Stage3Params) { *p = withSwitches(*p, Stage3Switches[:6]...); p.StrictCopies = true }, "stage3: min_safe_copies above 1 and strict_copies require the checkpoints and narrowed_drain switches"},
	} {
		t.Run(name, func(t *testing.T) {
			p := validStage3Params()
			tc.mutate(&p)
			if err := p.Validate(); err == nil || err.Error() != tc.want {
				t.Fatalf("Validate = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestStage3SwitchesArePrefixes: the switches are the last seven fields of
// Stage3Params in §10.4 order, SwitchOn reads exactly its own field, every
// prefix of the order validates, and a switch whose earlier switch is off is
// refused naming the first earlier one that is off.
func TestStage3SwitchesArePrefixes(t *testing.T) {
	want := []string{"purge", "eviction_voiding", "owner_aware_selection", "placement", "reselection", "checkpoints", "narrowed_drain"}
	if !slices.Equal(Stage3Switches, want) {
		t.Fatalf("Stage3Switches = %v, want %v", Stage3Switches, want)
	}
	rt := reflect.TypeOf(Stage3Params{})
	for i, name := range Stage3Switches {
		f := rt.Field(rt.NumField() - len(Stage3Switches) + i)
		if f.Type.Kind() != reflect.Bool || strings.Split(f.Tag.Get("json"), ",")[0] != name {
			t.Fatalf("field %s (%s) is not switch %s", f.Name, f.Tag.Get("json"), name)
		}
		one := withSwitches(Stage3Params{}, name)
		for _, other := range Stage3Switches {
			if one.SwitchOn(other) != (other == name) {
				t.Fatalf("with only %s on, SwitchOn(%s) = %v", name, other, one.SwitchOn(other))
			}
		}
	}
	if (withSwitches(Stage3Params{}, Stage3Switches...)).SwitchOn("strict_copies") {
		t.Fatal("SwitchOn answered true for a name that is not a switch")
	}
	for n := 0; n <= len(Stage3Switches); n++ {
		if err := withSwitches(validStage3Params(), Stage3Switches[:n]...).Validate(); err != nil {
			t.Fatalf("prefix %v: %v", Stage3Switches[:n], err)
		}
	}
	for i := range Stage3Switches {
		for j := 0; j < i; j++ {
			on := slices.Concat(Stage3Switches[:j], Stage3Switches[j+1:i+1])
			err := withSwitches(validStage3Params(), on...).Validate()
			if wantErr := "stage3: switch " + Stage3Switches[j+1] + " requires " + Stage3Switches[j]; err == nil || err.Error() != wantErr {
				t.Fatalf("%v on without %s: Validate = %v, want %q", on, Stage3Switches[j], err, wantErr)
			}
		}
	}
}

// TestStage3FailureDomainLookupAndIsMember: FailureDomain finds a listed
// indexer (indexer 0 included) and nothing else, and an entry is a member
// exactly when it names a host, indexer 0 included.
func TestStage3FailureDomainLookupAndIsMember(t *testing.T) {
	p := validStage3Params()
	if d, ok := p.FailureDomain(0); !ok || d != "node:a" {
		t.Fatalf("FailureDomain(0) = %q, %v", d, ok)
	}
	if d, ok := p.FailureDomain(1); !ok || d != "node:b" {
		t.Fatalf("FailureDomain(1) = %q, %v", d, ok)
	}
	if d, ok := p.FailureDomain(2); ok || d != "" {
		t.Fatalf("FailureDomain(2) = %q, %v; want none", d, ok)
	}
	zero := uint64(0)
	if (VerifierEntry{NodeID: "verifier-1"}).IsMember() || !(VerifierEntry{NodeID: "member-a", HostIndexerID: &zero}).IsMember() {
		t.Fatal("IsMember must be true exactly when host_indexer_id is set, indexer 0 included")
	}
}

// TestStage3FieldsAreOmittedWhenUnset: an update without stage3 and a pool
// entry encode exactly as before the stage-3 fields existed, so every earlier
// digest is unchanged; a member hosted by indexer 0 keeps its host.
func TestStage3FieldsAreOmittedWhenUnset(t *testing.T) {
	b, err := json.Marshal(ConsensusParamsUpdate{NetworkID: "n", Verifiers: []VerifierEntry{{NodeID: "verifier-1", Ed25519Pubkey: []byte{1}}}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"verifiers":[{"node_id":"verifier-1","ed25519_pubkey":"AQ=="}]`) || strings.Contains(string(b), "stage3") {
		t.Fatalf("pool-only update JSON = %s", b)
	}
	zero := uint64(0)
	b, err = json.Marshal(VerifierEntry{NodeID: "member-a", HostIndexerID: &zero})
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"node_id":"member-a","ed25519_pubkey":null,"host_indexer_id":0}` {
		t.Fatalf("member hosted by indexer 0 = %s", b)
	}
}
