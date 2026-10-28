package serve

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

// A lease lets the host answer one database Replica. A database has at most
// one; it expires its ttl after its grant or last renew, and once revoked it
// can no longer be renewed.
type lease struct {
	id        string
	database  string
	owner     uint64
	renewed   time.Time     // grant or last renew
	ttl       time.Duration // the LeaseTTL it was granted with
	revoked   bool
	revokedAt time.Time
	done      chan struct{} // closed when the lease ends
	ended     string        // waitReleased or waitExpired once done is closed
}

// LeaseFile is the file in Deps.StateDir where the server records every
// lease it holds, before it grants or renews one (clarification K44). A
// member that restarts reads it back, so it never installs a database under
// a lease its previous process granted and its host still holds.
const LeaseFile = "serve-leases.json"

// leaseFileVersion is the only LeaseFile version this package reads and
// writes.
const leaseFileVersion = 1

// maxLeaseTTLMs is the largest millisecond duration representable without
// wrapping; an invalid durable record must fail startup, not lose its hold.
const maxLeaseTTLMs = int64((1<<63 - 1) / time.Millisecond)

// leaseFileContent is LeaseFile's JSON.
type leaseFileContent struct {
	Version int           `json:"version"`
	Leases  []leaseRecord `json:"leases"`
}

// leaseRecord is one recorded lease: its expiry is RenewedUnixNano plus
// TTLMs, on the member's clock.
type leaseRecord struct {
	LeaseID         string `json:"lease_id"`
	Database        string `json:"database"`
	Owner           uint64 `json:"owner"`
	RenewedUnixNano int64  `json:"renewed_unix_nano"`
	TTLMs           int64  `json:"ttl_ms"`
}

// The outcomes of a lease wait, the metric label outcome.
const (
	waitReleased = "released"
	waitExpired  = "expired"
)

func (l *lease) expiresAt() time.Time { return l.renewed.Add(l.ttl) }

func (l *lease) view(now time.Time) *LeaseView {
	state := "held"
	if l.revoked {
		state = "revoking"
	}
	return &LeaseView{LeaseID: l.id, State: state, RemainingMs: max(l.expiresAt().Sub(now), 0).Milliseconds()}
}

func (l *lease) record(renewed time.Time) leaseRecord {
	return leaseRecord{LeaseID: l.id, Database: l.database, Owner: l.owner, RenewedUnixNano: renewed.UnixNano(), TTLMs: l.ttl.Milliseconds()}
}

// restoreLeases reads LeaseFile back when New runs after a restart. Every
// lease the previous process recorded that has not expired is restored
// revoked: its database serves again only after two polls of this process,
// so the lease is never renewed, and BeforeInstall waits for its release or
// its expiry exactly as the previous process would have. A missing file
// holds no lease. A file New cannot read fails New, since the member could
// not tell which leases its host still holds.
func (s *Server) restoreLeases() error {
	b, err := os.ReadFile(s.leaseFile)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("serve: lease file %s: %w", s.leaseFile, err)
	}
	var f leaseFileContent
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&f); err != nil {
		return fmt.Errorf("serve: lease file %s: %w", s.leaseFile, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return fmt.Errorf("serve: lease file %s: expected exactly one JSON value", s.leaseFile)
	}
	if f.Version != leaseFileVersion {
		return fmt.Errorf("serve: lease file %s has version %d, want %d", s.leaseFile, f.Version, leaseFileVersion)
	}
	now := s.now()
	for _, r := range f.Leases {
		if r.LeaseID == "" || r.Database == "" || r.TTLMs <= 0 {
			return fmt.Errorf("serve: lease file %s has a lease record without an id, a database or a ttl", s.leaseFile)
		}
		if r.TTLMs > maxLeaseTTLMs {
			return fmt.Errorf("serve: lease file %s has a ttl that overflows time.Duration", s.leaseFile)
		}
		if s.leases[r.Database] != nil || s.byID[r.LeaseID] != nil {
			return fmt.Errorf("serve: lease file %s lists database %s or lease %s twice", s.leaseFile, r.Database, r.LeaseID)
		}
		l := &lease{
			id: r.LeaseID, database: r.Database, owner: r.Owner,
			renewed: time.Unix(0, r.RenewedUnixNano), ttl: time.Duration(r.TTLMs) * time.Millisecond,
			revoked: true, revokedAt: now, done: make(chan struct{}),
		}
		if !now.Before(l.expiresAt()) {
			continue
		}
		s.leases[l.database], s.byID[l.id] = l, l
		s.logger.Info("member serving: lease restored after a restart; revoked until the host releases it or it expires",
			"database", l.database, "owner", l.owner, "remaining", l.expiresAt().Sub(now))
	}
	return nil
}

// recordLeasesLocked replaces LeaseFile with the leases of byID as they are
// once the change about to be applied is: without drop, with add, and with
// renewed as the last renew of renewing. It runs under mu, before the change
// is applied, so the file always lists every lease a host may hold; the file
// is small and local, the one blocking call mu is held across.
func (s *Server) recordLeasesLocked(drop, add, renewing *lease, renewed time.Time) error {
	f := leaseFileContent{Version: leaseFileVersion, Leases: []leaseRecord{}}
	for _, l := range s.byID {
		switch l {
		case drop:
		case renewing:
			f.Leases = append(f.Leases, l.record(renewed))
		default:
			f.Leases = append(f.Leases, l.record(l.renewed))
		}
	}
	if add != nil {
		f.Leases = append(f.Leases, add.record(add.renewed))
	}
	slices.SortFunc(f.Leases, func(a, b leaseRecord) int { return strings.Compare(a.LeaseID, b.LeaseID) })
	if err := writeLeaseFile(s.leaseFile, f); err != nil {
		return fmt.Errorf("serve: record leases in %s: %w", s.leaseFile, err)
	}
	return nil
}

// writeLeaseFile replaces path atomically: a temp file in the same directory,
// Sync, rename, directory Sync (the pattern of the install journal), so a
// crash leaves either the old or the new record.
func writeLeaseFile(path string, f leaseFileContent) error {
	b, err := json.Marshal(f)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".serve-leases-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// newLeaseID is 128 random bits in hex.
func newLeaseID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("serve: lease id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

// acquireRefusal is a 409 refusal of acquire: the database is not served,
// or it moved to another snapshot.
type acquireRefusal struct{ msg string }

func (e *acquireRefusal) Error() string { return e.msg }

var (
	errUnknownLease = errors.New("unknown lease")
	errRevokedLease = errors.New("revoked lease")
)

// sweepLocked ends every lease whose expiry has passed.
func (s *Server) sweepLocked(now time.Time) {
	for _, l := range s.byID {
		if !now.Before(l.expiresAt()) {
			s.endLocked(l, waitExpired)
		}
	}
}

func (s *Server) endLocked(l *lease, outcome string) {
	if s.byID[l.id] != l {
		return
	}
	delete(s.byID, l.id)
	if s.leases[l.database] == l {
		delete(s.leases, l.database)
	}
	l.ended = outcome
	close(l.done)
}

// revokeLocked revokes l: renew answers 409 from now on, and l ends when the
// host releases it or it expires.
func (s *Server) revokeLocked(l *lease, now time.Time) {
	if l.revoked {
		return
	}
	l.revoked, l.revokedAt = true, now
	s.logger.Info("member serving: lease revoked; the host must release it before the database is installed", "database", l.database, "owner", l.owner)
}

// acquire grants a lease of database at snapshotID, replacing any earlier
// lease of the database, when the last poll found it serving.
func (s *Server) acquire(database, snapshotID string) (*lease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepLocked(now)
	var found *dbState
	count := 0
	for key, st := range s.dbs {
		if key.Database == database {
			found, count = st, count+1
		}
	}
	switch {
	case count != 1:
		return nil, &acquireRefusal{fmt.Sprintf("member serving: database %s is not served: %s", database, ReasonTableMissing)}
	case !found.serving:
		return nil, &acquireRefusal{fmt.Sprintf("member serving: database %s is not served: %s", database, found.reason)}
	case snapshotID != found.copy.SnapshotID:
		return nil, &acquireRefusal{fmt.Sprintf("member serving: database %s moved to snapshot %s; acquire again", database, found.copy.SnapshotID)}
	}
	id, err := newLeaseID()
	if err != nil {
		return nil, err
	}
	l := &lease{id: id, database: database, owner: found.key.Owner, renewed: now, ttl: s.cfg.LeaseTTL, done: make(chan struct{})}
	old := s.leases[database]
	if err := s.recordLeasesLocked(old, l, nil, time.Time{}); err != nil {
		return nil, err
	}
	if old != nil {
		s.endLocked(old, waitReleased)
	}
	s.leases[database], s.byID[id] = l, l
	s.updateMetricsLocked(now)
	return l, nil
}

func (s *Server) renew(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepLocked(now)
	l := s.byID[id]
	switch {
	case l == nil:
		return errUnknownLease
	case l.revoked:
		return errRevokedLease
	}
	if err := s.recordLeasesLocked(nil, nil, l, now); err != nil {
		return err
	}
	l.renewed = now
	return nil
}

func (s *Server) release(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	l := s.byID[id]
	if l != nil {
		s.endLocked(l, waitReleased)
	}
	s.sweepLocked(now)
	if l != nil {
		// A release the file misses only makes a restart wait for the
		// lease's expiry instead.
		if err := s.recordLeasesLocked(nil, nil, nil, time.Time{}); err != nil {
			s.logger.Warn("member serving: the released lease stays recorded until it expires", "database", l.database, "error", err)
		}
	}
	s.updateMetricsLocked(now)
}

// BeforeInstall marks database installing, so it stops serving and no lease
// of it is granted, and revokes its lease. It returns once the host released
// the lease or it expired, at once when there is none, or with ctx's error;
// on that error it clears installing again, so AfterInstall pairs only with
// a nil return.
func (s *Server) BeforeInstall(ctx context.Context, owner uint64, database string) error {
	s.mu.Lock()
	now := s.now()
	st := s.stateLocked(OwnerDatabase{Owner: owner, Database: database})
	st.installing++
	s.epoch++
	st.installed = s.epoch
	if st.serving {
		s.logger.Info("member serving: database stopped serving", "database", database, "owner", owner, "reason", string(ReasonInstalling))
	}
	st.serving, st.reason, st.served, st.polls = false, ReasonInstalling, nil, 0
	s.sweepLocked(now)
	l := s.leases[database]
	if l == nil {
		s.updateMetricsLocked(now)
		s.mu.Unlock()
		return nil
	}
	s.revokeLocked(l, now)
	s.updateMetricsLocked(now)
	s.mu.Unlock()

	for {
		s.mu.Lock()
		wait := l.expiresAt().Sub(s.now())
		s.mu.Unlock()
		timer := time.NewTimer(max(wait, 0))
		select {
		case <-l.done:
			timer.Stop()
		case <-timer.C:
			s.mu.Lock()
			s.sweepLocked(s.now())
			s.mu.Unlock()
		case <-ctx.Done():
			timer.Stop()
			s.mu.Lock()
			if st.installing > 0 {
				st.installing--
			}
			s.epoch++
			st.installed = s.epoch
			s.updateMetricsLocked(s.now())
			s.mu.Unlock()
			return ctx.Err()
		}
		select {
		case <-l.done:
		default:
			continue
		}
		s.mu.Lock()
		s.metrics.leaseWaits.WithLabelValues(l.ended).Inc()
		s.updateMetricsLocked(s.now())
		s.mu.Unlock()
		s.logger.Info("member serving: lease ended; installing", "database", database, "owner", owner, "outcome", l.ended)
		return nil
	}
}

// AfterInstall clears the installing mark BeforeInstall set. The database
// serves again only once the conditions hold over two full polls after it.
func (s *Server) AfterInstall(owner uint64, database string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.dbs[OwnerDatabase{Owner: owner, Database: database}]
	if st == nil {
		return
	}
	if st.installing > 0 {
		st.installing--
	}
	s.epoch++
	st.installed = s.epoch
	st.polls = 0
}
