package serve

import (
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Database states of sentio_node_si_member_serve_databases.
const (
	stateServing    = "serving"
	stateRevoking   = "revoking"
	stateNotServing = "not_serving"
)

// Metrics are the serving series. Every state, reason and outcome is
// exported from NewMetrics on, zeros included; New adds every owner of
// Config.OwnerHouseGate at 0.
type Metrics struct {
	databases        *prometheus.GaugeVec
	notServing       *prometheus.GaugeVec
	ownerUnreachable *prometheus.GaugeVec
	leaseWaits       *prometheus.CounterVec
	revokingSeconds  prometheus.Gauge
}

// NewMetrics returns an unregistered set.
func NewMetrics() *Metrics {
	m := &Metrics{
		databases: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sentio_node_si_member_serve_databases",
			Help: "Databases of peer owners by serving state: serving, revoking (not serving, its lease not yet released or expired) or not_serving.",
		}, []string{"state"}),
		notServing: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sentio_node_si_member_serve_not_serving",
			Help: "Databases of peer owners that are not serving, by the first serving condition that fails.",
		}, []string{"reason"}),
		ownerUnreachable: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sentio_node_si_member_serve_owner_unreachable",
			Help: "1 while every probe of the owner's HouseGate failed for at least owner_unreachable_after, else 0.",
		}, []string{"owner"}),
		leaseWaits: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentio_node_si_member_serve_lease_waits_total",
			Help: "Installs that waited for a revoked lease, by how the wait ended: released by the host or expired.",
		}, []string{"outcome"}),
		revokingSeconds: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentio_node_si_member_serve_revoking_seconds",
			Help: "Seconds since the oldest current lease revocation that the host has not yet released and that has not expired; 0 when none.",
		}),
	}
	for _, state := range []string{stateServing, stateRevoking, stateNotServing} {
		m.databases.WithLabelValues(state).Set(0)
	}
	for _, reason := range Reasons() {
		m.notServing.WithLabelValues(string(reason)).Set(0)
	}
	for _, outcome := range []string{waitReleased, waitExpired} {
		m.leaseWaits.WithLabelValues(outcome).Add(0)
	}
	return m
}

// Register registers every series on r.
func (m *Metrics) Register(r prometheus.Registerer) error {
	for _, c := range []prometheus.Collector{m.databases, m.notServing, m.ownerUnreachable, m.leaseWaits, m.revokingSeconds} {
		if err := r.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// updateMetricsLocked sets every gauge from the current state.
func (s *Server) updateMetricsLocked(now time.Time) {
	states := map[string]int{stateServing: 0, stateRevoking: 0, stateNotServing: 0}
	reasons := map[Reason]int{}
	for key, st := range s.dbs {
		switch l := s.leases[key.Database]; {
		case st.serving:
			states[stateServing]++
		case l != nil && l.owner == key.Owner && l.revoked:
			states[stateRevoking]++
		default:
			states[stateNotServing]++
		}
		if !st.serving {
			reasons[st.reason]++
		}
	}
	for state, n := range states {
		s.metrics.databases.WithLabelValues(state).Set(float64(n))
	}
	for _, reason := range Reasons() {
		s.metrics.notServing.WithLabelValues(string(reason)).Set(float64(reasons[reason]))
	}
	var longest time.Duration
	for _, l := range s.byID {
		if l.revoked {
			longest = max(longest, now.Sub(l.revokedAt))
		}
	}
	s.metrics.revokingSeconds.Set(longest.Seconds())
	for owner, o := range s.owners {
		if o.addr != "" {
			s.metrics.ownerUnreachable.WithLabelValues(strconv.FormatUint(owner, 10)).Set(boolGauge(s.unreachableLocked(owner, now)))
		}
	}
}
