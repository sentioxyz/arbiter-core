package member

import (
	"slices"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/manifestfeed"
	"github.com/sentioxyz/arbiter-core/member/shadow"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

// The label values Stats always carries, so that every series sentio-node
// exports from it exists with zeros (contract §22.1, §22.3, §24.4).
var (
	installOutcomes = []install.Outcome{
		install.OutcomeKept, install.OutcomeRenameRace, install.OutcomeSplit, install.OutcomeDoubled, install.OutcomeForeign,
		install.OutcomeMissed, install.OutcomeRefused, install.OutcomeUnreachable,
	}
	installSources = []string{sources.SourceOwnerPull, sources.SourcePeerUnsafePull, sources.SourceRestore}
	auditTriggers  = []string{install.TriggerStartup, install.TriggerClickHouseRestart, install.TriggerSelfAudit}
)

// OwnerStats is one peer's view (contract §23.1).
type OwnerStats struct {
	// Copies counts the peer's ready copies.
	Copies int
	// Entry is the peer's journaled entry; PublishedEntry the newest one a
	// manifest the feed saw publishes.
	Entry, PublishedEntry replay.TrackFrontier
	// BehindSince is zero while Entry equals PublishedEntry, else the first
	// time the role saw them differ.
	BehindSince                     time.Time
	DivergedPartitions              uint64
	DivergedTables, ForeignTables   int
	ShadowBlocks, ShadowMissedScans uint64
	ShadowUnverdicted               uint64
	ShadowMismatches                map[shadow.MismatchKind]uint64
}

// Stats is the role's view for metrics and /readyz (contract §23.1). Every
// configured peer, install outcome, source, audit trigger and mismatch kind
// is present, with zeros when nothing happened.
type Stats struct {
	NodeID                                     string
	HostIndexerID                              uint64
	Ready                                      bool
	Owners                                     map[uint64]OwnerStats
	InstallOutcomes                            map[install.Outcome]uint64
	Installs                                   map[string]uint64
	FeedAccepted, FeedRefused, FeedFullFetches uint64
	AuditRuns                                  map[string]uint64
	AuditDroppedTwins                          uint64
	AuditClean, InstallsPaused                 bool
	GrantsUnreadable                           bool
	DiskUsedBytes, DiskBudgetBytes             uint64
	// UnconfiguredOwners lists, ascending, the enrolled SI indexers that are
	// neither a peer nor the host.
	UnconfiguredOwners []uint64
	ShadowNextBlockSeq uint64
}

// Stats returns the current view. It takes no install lock, so a scrape
// never waits for an install pass.
func (r *Role) Stats() Stats {
	r.mu.Lock()
	p := r.parts
	s := Stats{
		NodeID: r.cfg.NodeID, HostIndexerID: r.cfg.HostIndexerID,
		Owners:           make(map[uint64]OwnerStats, len(r.peers)),
		InstallOutcomes:  make(map[install.Outcome]uint64, len(installOutcomes)),
		Installs:         make(map[string]uint64, len(installSources)),
		AuditRuns:        make(map[string]uint64, len(auditTriggers)),
		GrantsUnreadable: r.grantsUnreadable, DiskBudgetBytes: r.cfg.DiskBudgetBytes,
	}
	for _, peer := range r.peers {
		o := OwnerStats{PublishedEntry: r.published[peer.IndexerID], BehindSince: r.behindSince[peer.IndexerID],
			ShadowMismatches: make(map[shadow.MismatchKind]uint64, len(shadow.MismatchKinds()))}
		for _, k := range shadow.MismatchKinds() {
			o.ShadowMismatches[k] = 0
		}
		s.Owners[peer.IndexerID] = o
	}
	var follower *manifestfeed.Follower
	if p != nil {
		follower = p.follower
	}
	r.mu.Unlock()

	for _, v := range installOutcomes {
		s.InstallOutcomes[v] = 0
	}
	for _, v := range installSources {
		s.Installs[v] = 0
	}
	for _, v := range auditTriggers {
		s.AuditRuns[v] = 0
	}
	s.UnconfiguredOwners = r.unconfiguredOwners()
	if p == nil {
		return s
	}
	s.Ready = r.Ready()
	if follower != nil {
		fs := follower.Stats()
		s.FeedAccepted, s.FeedRefused, s.FeedFullFetches = fs.Accepted, fs.Refused, fs.FullFetches
	}

	is := p.installer.Stats()
	for k, v := range is.Outcomes {
		s.InstallOutcomes[k] = v
	}
	for k, v := range is.Installs {
		s.Installs[k] = v
	}
	for k, v := range is.AuditRuns {
		s.AuditRuns[k] = v
	}
	s.AuditDroppedTwins, s.AuditClean, s.InstallsPaused, s.DiskUsedBytes = is.AuditDroppedTwins, is.AuditClean, is.InstallsPaused, is.DiskUsedBytes
	if is.DiskBudgetBytes != 0 {
		s.DiskBudgetBytes = is.DiskBudgetBytes
	}
	copies := p.copies.Copies()
	var ss shadow.Stats
	if p.shadow != nil {
		ss = p.shadow.Stats()
		s.ShadowNextBlockSeq = ss.NextBlockSeq
	}
	for _, peer := range r.peers {
		id := peer.IndexerID
		o := s.Owners[id]
		for _, c := range copies {
			if c.Ready && c.Owner == id {
				o.Copies++
			}
		}
		if j, ok := p.installer.Entry(id); ok {
			o.Entry = j.Entry
		}
		o.DivergedPartitions = is.DivergedPartitions[id]
		for _, t := range p.installer.Tables(id) {
			if t.Diverged {
				o.DivergedTables++
			}
			if t.Foreign {
				o.ForeignTables++
			}
		}
		o.ShadowBlocks, o.ShadowMissedScans, o.ShadowUnverdicted = ss.Blocks[id], ss.MissedScans[id], ss.Unverdicted[id]
		for k, v := range ss.Mismatches[id] {
			o.ShadowMismatches[k] = v
		}
		s.Owners[id] = o
	}
	return s
}

// unconfiguredOwners lists the enrolled SI indexers that are neither a peer
// nor the host, from the registry's current view.
func (r *Role) unconfiguredOwners() []uint64 {
	snap, _ := r.d.Registry.View()
	var out []uint64
	for _, e := range snap.SIIndexers {
		if e.IndexerID == r.cfg.HostIndexerID || slices.ContainsFunc(r.peers, func(p Peer) bool { return p.IndexerID == e.IndexerID }) {
			continue
		}
		out = append(out, e.IndexerID)
	}
	slices.Sort(out)
	return out
}
