package serve

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"time"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/sitable"

	"github.com/sentioxyz/arbiter-core/wire"
)

// Reason says why a database is not serving. The set is closed.
type Reason string

const (
	// ReasonOwnerAvailable: the owner is neither quarantined nor unreachable.
	ReasonOwnerAvailable Reason = "owner_available"
	// ReasonEntryUnconfirmed: the owner's published entry has not been the
	// same over two polls since it last changed or since the last install.
	ReasonEntryUnconfirmed Reason = "entry_unconfirmed"
	// ReasonCopyBehind: the copy's entry is not the owner's published entry.
	ReasonCopyBehind Reason = "copy_behind"
	// ReasonAuditUnclean: the startup audit or the self-audit is not clean.
	ReasonAuditUnclean Reason = "audit_unclean"
	// ReasonTableForeign: the copy of an Active table of the database is
	// Foreign.
	ReasonTableForeign Reason = "table_foreign"
	// ReasonTableDiverged: the copy of an Active table of the database is
	// Diverged.
	ReasonTableDiverged Reason = "table_diverged"
	// ReasonTableExcluded: the copy of an Active table of the database is
	// excluded.
	ReasonTableExcluded Reason = "table_excluded"
	// ReasonTableMissing: some Active incarnation of the database has no
	// installed copy with its seq, schema hash and copy name, the database
	// has no Active incarnation or one of another owner, the member holds no
	// copy of the database, or two owners list the database.
	ReasonTableMissing Reason = "table_missing"
	// ReasonServingDisabled: Config.Enabled is off.
	ReasonServingDisabled Reason = "serving_disabled"
	// ReasonFeedUnavailable: the latest published manifest could not be read
	// in this poll, or it carries no entry of the owner.
	ReasonFeedUnavailable Reason = "feed_unavailable"
	// ReasonInstalling: an install of the database is pending or running.
	ReasonInstalling Reason = "installing"
)

// Reasons lists the closed set of Reason values; the metrics export one
// series for each.
func Reasons() []Reason {
	return []Reason{
		ReasonOwnerAvailable, ReasonEntryUnconfirmed, ReasonCopyBehind, ReasonAuditUnclean,
		ReasonTableForeign, ReasonTableDiverged, ReasonTableExcluded, ReasonTableMissing,
		ReasonServingDisabled, ReasonFeedUnavailable, ReasonInstalling,
	}
}

// ServingView is the answer of GET /v1/serving.
type ServingView struct {
	NodeID    string         `json:"node_id"`
	Databases []DatabaseView `json:"databases"`
}

// DatabaseView is one database of one owner.
type DatabaseView struct {
	Database string `json:"database"`
	Owner    uint64 `json:"owner"`
	Serving  bool   `json:"serving"`
	// Reason is "" while serving.
	Reason Reason `json:"reason"`
	// SnapshotID and Entry are the copy's (the published manifest it equals
	// and the owner's entry there); a host acquires with this SnapshotID.
	SnapshotID string               `json:"snapshot_id"`
	Entry      replay.TrackFrontier `json:"entry"`
	// FrozenSinceUnixMs is the first poll that read the owner's current
	// published entry, unchanged since; 0 before any.
	FrozenSinceUnixMs int64 `json:"frozen_since_unix_ms"`
	OwnerQuarantined  bool  `json:"owner_quarantined"`
	OwnerUnreachable  bool  `json:"owner_unreachable"`
	// Tables lists the tables a host may answer Replica, sorted by id: the
	// owner's Active incarnations of the database. It is empty unless
	// Serving.
	Tables []ServedTable `json:"tables"`
	// Lease is the database's lease, nil when none.
	Lease *LeaseView `json:"lease"`
}

// ServedTable is one table of a serving database.
type ServedTable struct {
	TableID        string `json:"table_id"`
	IncarnationSeq uint64 `json:"incarnation_seq"`
	CopyTable      string `json:"copy_table"`
	SchemaHash     string `json:"schema_hash"`
}

// LeaseView is a lease as the view shows it.
type LeaseView struct {
	LeaseID string `json:"lease_id"`
	// State is "held" or "revoking".
	State       string `json:"state"`
	RemainingMs int64  `json:"remaining_ms"`
}

// dbState is the evaluation state of one (owner, database).
type dbState struct {
	key        OwnerDatabase
	serving    bool
	reason     Reason
	copy       DatabaseCopy
	haveCopy   bool
	served     []TableCopy // the matched Active tables while serving
	lastEntry  replay.TrackFrontier
	polls      int    // consecutive polls that read lastEntry, none installing
	installing int    // BeforeInstall calls not yet paired by AfterInstall
	installed  uint64 // epoch of the last BeforeInstall or AfterInstall
	quarantine bool
	unreach    bool
}

// ownerState is what the polls and probes know of one owner.
type ownerState struct {
	addr         string
	firstFailure time.Time // zero unless every probe since it failed
	entry        replay.TrackFrontier
	entrySeen    bool
	entrySince   time.Time
}

type copyRead struct {
	copy DatabaseCopy
	ok   bool
}

// poll evaluates every database once. It reads the registry, the latest
// published manifest and the journal without the lock, then applies the
// result under it; an install that began or ended in between makes its
// database count as installing for this poll.
func (s *Server) poll(ctx context.Context) {
	s.mu.Lock()
	startEpoch := s.epoch
	s.mu.Unlock()

	snap, registryOn := s.reg.View()
	// With serving off nothing is read from the arbiter.
	var manifest replay.SafeSnapshotManifest
	feedErr := errServingOff
	if s.cfg.Enabled {
		pctx, cancel := context.WithTimeout(ctx, s.cfg.PollInterval)
		manifest, feedErr = s.pub.Latest(pctx)
		cancel()
	}
	pairs := s.copies.Databases()
	reads := make(map[OwnerDatabase]copyRead, len(pairs))
	for _, p := range pairs {
		c, ok := s.copies.Database(p.Owner, p.Database)
		reads[p] = copyRead{copy: c, ok: ok}
	}
	if ctx.Err() != nil {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepLocked(now)
	s.noteFeedLocked(feedErr)
	owners := map[uint64]bool{}
	names := map[string]int{}
	for _, p := range pairs {
		owners[p.Owner] = true
		names[p.Database]++
	}
	for owner := range owners {
		if feedErr != nil {
			continue
		}
		e, ok := replay.OwnerEntry(manifest, owner)
		if !ok {
			continue
		}
		o := s.ownerLocked(owner)
		if !o.entrySeen || o.entry != e {
			o.entry, o.entrySeen, o.entrySince = e, true, now
		}
	}
	listed := map[OwnerDatabase]bool{}
	for _, p := range pairs {
		listed[p] = true
		st := s.stateLocked(p)
		r := reads[p]
		st.copy, st.haveCopy = r.copy, r.ok
		if !r.ok {
			st.copy = DatabaseCopy{}
		}
		e, entryOK := replay.TrackFrontier{}, false
		if feedErr == nil {
			e, entryOK = replay.OwnerEntry(manifest, p.Owner)
		}
		installing := st.installing > 0 || st.installed > startEpoch
		switch {
		case installing || !entryOK:
			st.polls = 0
		case st.polls > 0 && st.lastEntry == e:
			st.polls++
		default:
			st.lastEntry, st.polls = e, 1
		}
		st.quarantine = registryOn && slices.Contains(snap.QuarantinedIndexers, p.Owner)
		st.unreach = s.unreachableLocked(p.Owner, now)
		reason, served := s.evaluateLocked(st, installing, entryOK, e, names[p.Database] > 1, snap, registryOn)
		st.served = served
		s.setServingLocked(st, reason, now)
	}
	for key, st := range s.dbs {
		if listed[key] || st.installing > 0 {
			continue
		}
		if l := s.leases[key.Database]; l != nil && l.owner == key.Owner {
			s.revokeLocked(l, now)
		}
		delete(s.dbs, key)
	}
	s.updateMetricsLocked(now)
}

// evaluateLocked applies the serving conditions in a fixed order and returns
// the first that fails, or "" and the tables to serve.
func (s *Server) evaluateLocked(st *dbState, installing, entryOK bool, e replay.TrackFrontier, duplicate bool, snap wire.TableRegistrySnapshot, registryOn bool) (Reason, []TableCopy) {
	switch {
	case !s.cfg.Enabled:
		return ReasonServingDisabled, nil
	case installing:
		return ReasonInstalling, nil
	case !entryOK:
		return ReasonFeedUnavailable, nil
	case !st.quarantine && !st.unreach:
		return ReasonOwnerAvailable, nil
	case st.polls < 2:
		return ReasonEntryUnconfirmed, nil
	case duplicate || !st.haveCopy:
		return ReasonTableMissing, nil
	case !st.copy.AuditClean:
		return ReasonAuditUnclean, nil
	}
	served, reason := matchActive(st.copy.Tables, activeTables(snap, registryOn, st.key))
	switch {
	case reason != "" && reason != ReasonTableMissing:
		return reason, nil
	case st.copy.Entry != e:
		return ReasonCopyBehind, nil
	case reason != "":
		return reason, nil
	}
	return "", served
}

// activeTables are the Active incarnations of the database, sorted by table
// id, when ManifestOwner gives every one of them to the owner. A database with
// an Active table of another owner has none (it is served all or nothing, as
// the host answers it), and so does a disabled registry.
func activeTables(snap wire.TableRegistrySnapshot, registryOn bool, key OwnerDatabase) []wire.TableIncarnation {
	if !registryOn {
		return nil
	}
	var out []wire.TableIncarnation
	for _, inc := range snap.ActiveTables() {
		if inc.DatabaseID != key.Database {
			continue
		}
		if owner, ok := snap.ManifestOwner(inc.Key()); !ok || owner != key.Owner {
			return nil
		}
		out = append(out, inc)
	}
	slices.SortFunc(out, func(a, b wire.TableIncarnation) int { return cmp.Compare(a.Key(), b.Key()) })
	return out
}

// matchActive finds, for every Active incarnation, the copy of that table at
// that incarnation seq, and returns them when each is installed with the
// incarnation's schema hash and the copy name sitable.CopyTable(id, seq).
// Copies of other incarnations (Pending, or an older seq) are not served and
// do not count. Otherwise it returns the first failing reason in the order
// table_foreign, table_diverged, table_excluded, table_missing.
func matchActive(tables []TableCopy, active []wire.TableIncarnation) ([]TableCopy, Reason) {
	if len(active) == 0 {
		return nil, ReasonTableMissing
	}
	var foreign, diverged, excluded, missing bool
	served := make([]TableCopy, 0, len(active))
	for _, inc := range active {
		key := inc.Key()
		i := slices.IndexFunc(tables, func(t TableCopy) bool { return t.TableID == key && t.IncarnationSeq == inc.Seq })
		if i < 0 {
			missing = true
			continue
		}
		t := tables[i]
		switch t.Condition {
		case ConditionForeign:
			foreign = true
		case ConditionDiverged:
			diverged = true
		case ConditionExcluded:
			excluded = true
		case ConditionInstalled:
			if t.SchemaHash != inc.SchemaHash || t.CopyTable != sitable.CopyTable(key, inc.Seq) {
				missing = true
				continue
			}
			served = append(served, t)
		default:
			missing = true
		}
	}
	switch {
	case foreign:
		return nil, ReasonTableForeign
	case diverged:
		return nil, ReasonTableDiverged
	case excluded:
		return nil, ReasonTableExcluded
	case missing:
		return nil, ReasonTableMissing
	}
	return served, ""
}

// setServingLocked records the poll's verdict, revokes the database's lease
// when it stops serving and logs every transition.
func (s *Server) setServingLocked(st *dbState, reason Reason, now time.Time) {
	was := st.serving
	st.serving, st.reason = reason == "", reason
	if !st.serving {
		st.served = nil
		if l := s.leases[st.key.Database]; l != nil && l.owner == st.key.Owner {
			s.revokeLocked(l, now)
		}
	}
	switch {
	case st.serving && !was:
		s.logger.Info("member serving: database serving", "database", st.key.Database, "owner", st.key.Owner,
			"snapshot_id", st.copy.SnapshotID, "safe_block_seq", st.copy.Entry.SafeBlockSeq,
			"applied_promotion_seq", st.copy.Entry.AppliedPromotionSeq,
			"owner_quarantined", st.quarantine, "owner_unreachable", st.unreach)
	case !st.serving && was:
		s.logger.Info("member serving: database stopped serving", "database", st.key.Database, "owner", st.key.Owner, "reason", string(reason))
	}
}

// errServingOff stands for the manifest a poll does not read while serving
// is off.
var errServingOff = errors.New("serving is off")

func (s *Server) noteFeedLocked(err error) {
	if errors.Is(err, errServingOff) {
		return
	}
	switch {
	case err != nil && err.Error() != s.feedErr:
		s.feedErr = err.Error()
		s.logger.Warn("member serving: the latest published manifest is unavailable; no database is served", "err", err)
	case err == nil && s.feedErr != "":
		s.feedErr = ""
		s.logger.Info("member serving: the latest published manifest is readable again")
	}
}

func (s *Server) stateLocked(key OwnerDatabase) *dbState {
	st := s.dbs[key]
	if st == nil {
		st = &dbState{key: key, reason: ReasonEntryUnconfirmed}
		s.dbs[key] = st
	}
	return st
}

func (s *Server) ownerLocked(owner uint64) *ownerState {
	o := s.owners[owner]
	if o == nil {
		o = &ownerState{}
		s.owners[owner] = o
	}
	return o
}

func (s *Server) viewLocked(now time.Time) ServingView {
	keys := make([]OwnerDatabase, 0, len(s.dbs))
	for key := range s.dbs {
		keys = append(keys, key)
	}
	slices.SortFunc(keys, func(a, b OwnerDatabase) int {
		return cmp.Or(cmp.Compare(a.Database, b.Database), cmp.Compare(a.Owner, b.Owner))
	})
	view := ServingView{NodeID: s.nodeID, Databases: make([]DatabaseView, 0, len(keys))}
	for _, key := range keys {
		st := s.dbs[key]
		dv := DatabaseView{
			Database: key.Database, Owner: key.Owner, Serving: st.serving, Reason: st.reason,
			SnapshotID: st.copy.SnapshotID, Entry: st.copy.Entry,
			OwnerQuarantined: st.quarantine, OwnerUnreachable: st.unreach, Tables: []ServedTable{},
		}
		if o := s.owners[key.Owner]; o != nil && o.entrySeen {
			dv.FrozenSinceUnixMs = o.entrySince.UnixMilli()
		}
		if st.serving {
			for _, t := range st.served {
				dv.Tables = append(dv.Tables, ServedTable{TableID: t.TableID, IncarnationSeq: t.IncarnationSeq, CopyTable: t.CopyTable, SchemaHash: t.SchemaHash})
			}
		}
		if l := s.leases[key.Database]; l != nil && l.owner == key.Owner {
			dv.Lease = l.view(now)
		}
		view.Databases = append(view.Databases, dv)
	}
	return view
}
