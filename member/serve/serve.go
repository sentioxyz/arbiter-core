// Package serve is the serving side of a stage-3 si-member (housegate spec
// 2026-10-14 §11; Plan S3-C4 contract §30). It decides, per database of a
// peer owner, whether the member's copy may be served read-only by its host;
// it grants the host one lease per served database; and before any install
// of a database it revokes that lease and waits until the host released it
// or it expired, so the host never reads a copy while it changes. It changes
// no consensus state and calls nothing on the arbiter but the reads of its
// Published port.
package serve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/dataplane"
)

const (
	// DefaultOwnerUnreachableAfter is t_unreach when Config leaves it zero.
	DefaultOwnerUnreachableAfter = 60 * time.Second
	// MinOwnerUnreachableAfter is the smallest t_unreach New accepts.
	MinOwnerUnreachableAfter = 15 * time.Second
	// DefaultOwnerProbeInterval is the time between two probes of an owner.
	DefaultOwnerProbeInterval = 5 * time.Second
	// OwnerProbeTimeout bounds one probe (a TCP dial of the owner's HouseGate).
	OwnerProbeTimeout = 3 * time.Second
	// DefaultLeaseTTL is the lease lifetime after a grant or a renew.
	DefaultLeaseTTL = 30 * time.Second
	// MinLeaseTTL is the smallest lease lifetime New accepts.
	MinLeaseTTL = 10 * time.Second
	// DefaultPollInterval is the time between two evaluations of the
	// serving conditions.
	DefaultPollInterval = 5 * time.Second
	// HostDeadlineMargin is how long before its local lease deadline a host
	// stops answering a database Replica.
	HostDeadlineMargin = 3 * time.Second
	// DefaultListen is the serving endpoint's listen address (Plan S3-C3's
	// siMember.statusPort).
	DefaultListen = ":9096"
)

// Config configures serving. Zero durations take their defaults.
type Config struct {
	// Enabled turns serving on. Without it the endpoint still answers, every
	// database reports serving_disabled and no lease is granted.
	Enabled bool
	// Token is the endpoint's bearer token. The caller passes it from the
	// environment, never from a config file; String, GoString and LogValue
	// never print it.
	Token string
	// OwnerUnreachableAfter is t_unreach: an owner counts as unreachable once
	// every probe of its HouseGate failed since a first failure at least this
	// long ago.
	OwnerUnreachableAfter time.Duration
	// OwnerProbeInterval is the time between two probes of an owner.
	OwnerProbeInterval time.Duration
	// LeaseTTL is how long a lease lives after its grant or its last renew.
	LeaseTTL time.Duration
	// PollInterval is the time between two evaluations of the conditions.
	PollInterval time.Duration
	// OwnerHouseGate maps an owner's indexer id to its HouseGate host:port.
	// An owner without an address only ever triggers serving by quarantine.
	OwnerHouseGate map[uint64]string
}

// String describes cfg without its token.
func (c Config) String() string {
	return fmt.Sprintf("serve.Config{Enabled: %t, Token: %s, OwnerUnreachableAfter: %s, OwnerProbeInterval: %s, LeaseTTL: %s, PollInterval: %s, OwnerHouseGate: %v}",
		c.Enabled, redacted(c.Token), c.OwnerUnreachableAfter, c.OwnerProbeInterval, c.LeaseTTL, c.PollInterval, c.OwnerHouseGate)
}

// GoString is String, so %#v never prints the token either.
func (c Config) GoString() string { return c.String() }

// LogValue logs cfg without its token.
func (c Config) LogValue() slog.Value {
	return slog.GroupValue(
		slog.Bool("enabled", c.Enabled),
		slog.String("token", redacted(c.Token)),
		slog.Duration("owner_unreachable_after", c.OwnerUnreachableAfter),
		slog.Duration("owner_probe_interval", c.OwnerProbeInterval),
		slog.Duration("lease_ttl", c.LeaseTTL),
		slog.Duration("poll_interval", c.PollInterval),
		slog.Any("owner_housegate", c.OwnerHouseGate),
	)
}

func redacted(token string) string {
	if token == "" {
		return `""`
	}
	return "<redacted>"
}

func (c Config) withDefaults() Config {
	if c.OwnerUnreachableAfter == 0 {
		c.OwnerUnreachableAfter = DefaultOwnerUnreachableAfter
	}
	if c.OwnerProbeInterval == 0 {
		c.OwnerProbeInterval = DefaultOwnerProbeInterval
	}
	if c.LeaseTTL == 0 {
		c.LeaseTTL = DefaultLeaseTTL
	}
	if c.PollInterval <= 0 {
		c.PollInterval = DefaultPollInterval
	}
	return c
}

func (c Config) validate() error {
	if c.Token == "" {
		return errors.New("serve: a bearer token is required")
	}
	if c.OwnerUnreachableAfter < MinOwnerUnreachableAfter {
		return fmt.Errorf("serve: owner unreachable threshold %s is below the minimum %s", c.OwnerUnreachableAfter, MinOwnerUnreachableAfter)
	}
	if c.LeaseTTL < MinLeaseTTL {
		return fmt.Errorf("serve: lease ttl %s is below the minimum %s", c.LeaseTTL, MinLeaseTTL)
	}
	if c.OwnerProbeInterval < time.Second || c.OwnerProbeInterval >= c.OwnerUnreachableAfter {
		return fmt.Errorf("serve: owner probe interval %s must be at least 1s and below the unreachable threshold %s", c.OwnerProbeInterval, c.OwnerUnreachableAfter)
	}
	owners := make([]uint64, 0, len(c.OwnerHouseGate))
	for owner := range c.OwnerHouseGate {
		owners = append(owners, owner)
	}
	slices.Sort(owners)
	for _, owner := range owners {
		if !isHostPort(c.OwnerHouseGate[owner]) {
			return fmt.Errorf("serve: owner %d housegate address %q is not host:port", owner, c.OwnerHouseGate[owner])
		}
	}
	return nil
}

// isHostPort accepts host:port with a non-empty host and a numeric port
// other than 0.
func isHostPort(addr string) bool {
	host, port, err := net.SplitHostPort(addr)
	if err != nil || host == "" {
		return false
	}
	n, err := strconv.ParseUint(port, 10, 16)
	return err == nil && n != 0
}

// Deps are the ports of a Server; member.Role implements them.
type Deps struct {
	// Registry is the member's table-registry follower: quarantine and the
	// Active incarnations each database must list.
	Registry dataplane.RegistryView
	// Published reads the latest published manifest once per poll.
	Published Published
	// Copies is the member's install journal.
	Copies Copies
	// Dial probes an owner's HouseGate; nil dials TCP with a net.Dialer.
	Dial func(ctx context.Context, network, addr string) (net.Conn, error)
	// Now is the clock; nil is time.Now. Polls, probes and lease waits use
	// time.Ticker and time.Timer, so a test that replaces Now runs in a
	// testing/synctest bubble.
	Now func() time.Time
	// Metrics receives the serving series; nil keeps an unregistered set.
	Metrics *Metrics
	// Logger receives serving transitions; nil is slog.Default().
	Logger *slog.Logger
	// StateDir is the member's state directory (member.Config.StateDir). The
	// server records every lease it holds in its LeaseFile before it grants
	// or renews one, and New reads the leases back, so a restarted member
	// never installs under a lease its previous process granted (K44).
	StateDir string
	// NodeID is the member's node id, answered as ServingView.NodeID.
	NodeID string
}

// Published gives the latest published manifest, read from the arbiter in
// this call and validated by the member feed's acceptance rule.
type Published interface {
	Latest(ctx context.Context) (replay.SafeSnapshotManifest, error)
}

// Copies is the member's view of its install journal.
type Copies interface {
	// Databases lists every (owner, database) the member holds a copy of.
	Databases() []OwnerDatabase
	// Database is the copy of one database; false when the member holds none.
	Database(owner uint64, database string) (DatabaseCopy, bool)
}

// OwnerDatabase names one database of one owner.
type OwnerDatabase struct {
	Owner    uint64
	Database string
}

// DatabaseCopy is a copy of one database as the install journal records it.
type DatabaseCopy struct {
	// SnapshotID is the published manifest the owner's copies equal.
	SnapshotID string
	// Entry is the owner's track_frontiers entry of that manifest.
	Entry replay.TrackFrontier
	// Tables lists every ready copy of the database.
	Tables []TableCopy
	// AuditClean reports the startup audit and the self-audit clean.
	AuditClean bool
}

// TableCopy is one copied table.
type TableCopy struct {
	TableID        string
	IncarnationSeq uint64
	// CopyTable is the bare table name in hg_replica.
	CopyTable  string
	SchemaHash string
	Condition  TableCondition
}

// TableCondition is a copied table's install state.
type TableCondition string

const (
	// ConditionInstalled: the table is installed at DatabaseCopy.SnapshotID.
	ConditionInstalled TableCondition = "installed"
	// ConditionForeign: an install found a part no manifest lists; the table
	// is quarantined locally (Plan S3-C2 K36).
	ConditionForeign TableCondition = "foreign"
	// ConditionDiverged: a partition awaits its repair, or the copy holds a
	// part the journal does not list yet.
	ConditionDiverged TableCondition = "diverged"
	// ConditionExcluded: the table is not installed at the database's
	// snapshot (in P3: its journal is not at the owner's journal entry).
	ConditionExcluded TableCondition = "excluded"
)

// Server is the serving view, the owner probes, the leases and the endpoint.
type Server struct {
	cfg     Config
	reg     dataplane.RegistryView
	pub     Published
	copies  Copies
	dial    func(ctx context.Context, network, addr string) (net.Conn, error)
	now     func() time.Time
	metrics *Metrics
	logger  *slog.Logger
	nodeID  string
	// leaseFile is LeaseFile in Deps.StateDir.
	leaseFile string

	// mu guards everything below. It is never held across a call of a port
	// or a wait.
	mu      sync.Mutex
	epoch   uint64 // bumped by every BeforeInstall and AfterInstall
	dbs     map[OwnerDatabase]*dbState
	owners  map[uint64]*ownerState
	leases  map[string]*lease // by database
	byID    map[string]*lease
	feedErr string
}

// New validates cfg and returns a Server; Run starts its polls and probes.
func New(cfg Config, deps Deps) (*Server, error) {
	cfg = cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	if deps.Registry == nil || deps.Published == nil || deps.Copies == nil {
		return nil, errors.New("serve: Deps.Registry, Deps.Published and Deps.Copies are required")
	}
	if deps.StateDir == "" {
		return nil, errors.New("serve: Deps.StateDir is required: the server records its leases there before it grants or renews one")
	}
	s := &Server{
		cfg: cfg, reg: deps.Registry, pub: deps.Published, copies: deps.Copies,
		dial: deps.Dial, now: deps.Now, metrics: deps.Metrics, logger: deps.Logger, nodeID: deps.NodeID,
		leaseFile: filepath.Join(deps.StateDir, LeaseFile),
		dbs:       map[OwnerDatabase]*dbState{}, owners: map[uint64]*ownerState{},
		leases: map[string]*lease{}, byID: map[string]*lease{},
	}
	if s.dial == nil {
		s.dial = (&net.Dialer{}).DialContext
	}
	if s.now == nil {
		s.now = time.Now
	}
	if s.metrics == nil {
		s.metrics = NewMetrics()
	}
	if s.logger == nil {
		s.logger = slog.Default()
	}
	for owner, addr := range cfg.OwnerHouseGate {
		s.owners[owner] = &ownerState{addr: addr}
		s.metrics.ownerUnreachable.WithLabelValues(strconv.FormatUint(owner, 10)).Set(0)
	}
	if err := s.restoreLeases(); err != nil {
		return nil, err
	}
	return s, nil
}

// Run evaluates the serving conditions at once and then every PollInterval,
// and probes every owner of OwnerHouseGate every OwnerProbeInterval, until
// ctx ends; it returns ctx's error.
func (s *Server) Run(ctx context.Context) error {
	var probes sync.WaitGroup
	for owner, addr := range s.cfg.OwnerHouseGate {
		probes.Go(func() { s.probeLoop(ctx, owner, addr) })
	}
	defer probes.Wait()
	s.poll(ctx)
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			s.poll(ctx)
		}
	}
}

// Serving is the current serving view, databases sorted by name and owner.
func (s *Server) Serving() ServingView {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepLocked(now)
	return s.viewLocked(now)
}
