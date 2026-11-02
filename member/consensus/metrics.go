package consensus

import (
	"context"
	"errors"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

// Label values of the member consensus series (plan S3-E1 contract §42.3).
// Every value is exported from the start, zeros included.
const (
	// EvidenceReplay and EvidenceScan are the kinds of evidence_total.
	EvidenceReplay = "replay"
	EvidenceScan   = "scan"
	// EvidenceSubmitted and EvidenceRefused are its results: the arbiter
	// accepted the dispatch's evidence, or the member refused or failed it.
	EvidenceSubmitted = "submitted"
	EvidenceRefused   = "refused"
	// CheckpointAccepted, CheckpointSwitchOff and CheckpointRefused are the
	// results of checkpoints_total: acknowledged, refused because the
	// checkpoints switch is off, refused for any other reason.
	CheckpointAccepted  = "accepted"
	CheckpointSwitchOff = "switch_off"
	CheckpointRefused   = "refused"
)

// Metrics are the series of a member's main process. sentio-node registers
// them on the si-member process's own registry (member.Role.ConsensusMetrics;
// Plan S3-E4 Task 79).
type Metrics struct {
	registered    prometheus.Gauge
	readyGroups   prometheus.Gauge
	checkpointSeq *prometheus.GaugeVec
	checkpoints   *prometheus.CounterVec
	evidence      *prometheus.CounterVec
	purgeReports  prometheus.Counter
}

// NewMetrics returns an unregistered set with every evidence kind and result
// at zero; NewMode adds the configured owners' series.
func NewMetrics() *Metrics {
	m := &Metrics{
		registered: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentio_node_si_member_consensus_registered",
			Help: "1 once the member registered and marked itself active as a verifier in this process.",
		}),
		readyGroups: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "sentio_node_si_member_ready_groups",
			Help: "Database groups that list this member as a ready current member.",
		}),
		checkpointSeq: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "sentio_node_si_member_checkpoint_seq",
			Help: "checkpoint_seq of the member's last accepted checkpoint, per owner.",
		}, []string{"owner"}),
		checkpoints: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentio_node_si_member_checkpoints_total",
			Help: "Checkpoints the member submitted, per owner and result.",
		}, []string{"owner", "result"}),
		evidence: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "sentio_node_si_member_evidence_total",
			Help: "Verifier dispatches the member handled, per kind and result.",
		}, []string{"kind", "result"}),
		purgeReports: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "sentio_node_si_member_purge_reports_total",
			Help: "Purge reports the arbiter accepted from this member.",
		}),
	}
	for _, kind := range []string{EvidenceReplay, EvidenceScan} {
		for _, result := range []string{EvidenceSubmitted, EvidenceRefused} {
			m.evidence.WithLabelValues(kind, result)
		}
	}
	return m
}

// Register registers every series on r.
func (m *Metrics) Register(r prometheus.Registerer) error {
	for _, c := range []prometheus.Collector{m.registered, m.readyGroups, m.checkpointSeq, m.checkpoints, m.evidence, m.purgeReports} {
		if err := r.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// declareOwners exports every owner's checkpoint series at zero.
func (m *Metrics) declareOwners(owners []uint64) {
	for _, owner := range owners {
		label := strconv.FormatUint(owner, 10)
		m.checkpointSeq.WithLabelValues(label)
		for _, result := range []string{CheckpointAccepted, CheckpointSwitchOff, CheckpointRefused} {
			m.checkpoints.WithLabelValues(label, result)
		}
	}
}

func (m *Metrics) countCheckpoint(owner uint64, result string) {
	m.checkpoints.WithLabelValues(strconv.FormatUint(owner, 10), result).Inc()
}

// countEvidence counts one verifier dispatch of the verify role and returns
// err unchanged: nil is submitted, an error refused; a dispatch cut short by
// the end of its context is not counted, and a role without metrics counts
// nothing.
func (v *VerifyRole) countEvidence(kind string, err error) error {
	if v.metrics == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	result := EvidenceSubmitted
	if err != nil {
		result = EvidenceRefused
	}
	v.metrics.evidence.WithLabelValues(kind, result).Inc()
	return err
}

// ReconcilerMetrics are the series of the member's reconciler process
// (UnsafeReconciler, run as si_member_ddl): sentio_node_si_member_unsafe_replicas
// and sentio_node_si_member_replica_already_exists_total, read from stats at
// every scrape.
type ReconcilerMetrics struct {
	stats    func() ReconcilerStats
	replicas *prometheus.Desc
	exists   *prometheus.Desc
}

// NewReconcilerMetrics reads stats, normally (*UnsafeReconciler).Stats.
func NewReconcilerMetrics(stats func() ReconcilerStats) *ReconcilerMetrics {
	return &ReconcilerMetrics{
		stats:    stats,
		replicas: prometheus.NewDesc("sentio_node_si_member_unsafe_replicas", "hg_member_unsafe replicas the member's reconciler holds.", nil, nil),
		exists: prometheus.NewDesc("sentio_node_si_member_replica_already_exists_total",
			"Tables the reconciler stopped on because a replica of this member already exists at the Keeper path.", nil, nil),
	}
}

// Register registers both series on r.
func (m *ReconcilerMetrics) Register(r prometheus.Registerer) error { return r.Register(m) }

// Describe implements prometheus.Collector.
func (m *ReconcilerMetrics) Describe(ch chan<- *prometheus.Desc) {
	ch <- m.replicas
	ch <- m.exists
}

// Collect implements prometheus.Collector.
func (m *ReconcilerMetrics) Collect(ch chan<- prometheus.Metric) {
	s := m.stats()
	ch <- prometheus.MustNewConstMetric(m.replicas, prometheus.GaugeValue, float64(s.Replicas))
	ch <- prometheus.MustNewConstMetric(m.exists, prometheus.CounterValue, float64(s.ReplicaAlreadyExists))
}
