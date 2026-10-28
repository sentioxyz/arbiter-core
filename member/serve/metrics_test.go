package serve

import (
	"maps"
	"slices"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

// TestServeMetricsExportEveryStateReasonAndOutcomeIncludingZeros: every series
// of contract §30.5 is exported from New on, zeros included.
func TestServeMetricsExportEveryStateReasonAndOutcomeIncludingZeros(t *testing.T) {
	m := NewMetrics()
	cfg := testConfig()
	cfg.OwnerHouseGate = map[uint64]string{0: "sentio-node-devnet2-indexer-a:32001", 2: "sentio-node-devnet2-indexer-c:34001"}
	if _, err := New(cfg, Deps{Registry: &fakeRegistry{}, Published: &fakePublished{}, Copies: &fakeCopies{}, Metrics: m, StateDir: t.TempDir()}); err != nil {
		t.Fatalf("New: %v", err)
	}
	want := map[string]float64{
		"sentio_node_si_member_serve_databases{state=serving}":            0,
		"sentio_node_si_member_serve_databases{state=revoking}":           0,
		"sentio_node_si_member_serve_databases{state=not_serving}":        0,
		"sentio_node_si_member_serve_owner_unreachable{owner=0}":          0,
		"sentio_node_si_member_serve_owner_unreachable{owner=2}":          0,
		"sentio_node_si_member_serve_lease_waits_total{outcome=released}": 0,
		"sentio_node_si_member_serve_lease_waits_total{outcome=expired}":  0,
		"sentio_node_si_member_serve_revoking_seconds":                    0,
	}
	for _, reason := range Reasons() {
		want["sentio_node_si_member_serve_not_serving{reason="+string(reason)+"}"] = 0
	}
	if len(Reasons()) != 11 {
		t.Fatalf("%d reasons, want the closed set of 11", len(Reasons()))
	}
	got := metricValues(t, m)
	if !maps.Equal(got, want) {
		t.Fatalf("series %v\nwant %v", slices.Sorted(maps.Keys(got)), slices.Sorted(maps.Keys(want)))
	}
	if err := m.Register(prometheus.NewRegistry()); err != nil {
		t.Fatalf("Register on a fresh registry: %v", err)
	}
	reg := prometheus.NewRegistry()
	if err := m.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := m.Register(reg); err == nil {
		t.Fatal("a second Register on the same registry succeeded")
	}
}
