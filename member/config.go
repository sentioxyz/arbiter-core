package member

import (
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

const (
	// DefaultPollInterval is how often the role reads the safe watermark,
	// checks its copies and probes the ClickHouse start time when
	// Config.PollInterval is 0.
	DefaultPollInterval = 5 * time.Second
	// MinPollInterval is the shortest PollInterval New accepts: every
	// watermark read crosses one leader read barrier (contract §20.1).
	MinPollInterval = time.Second
	// LockFile is the file in StateDir on which a running role holds an
	// exclusive flock. It is install.LockFile, the lock install.ClearForeign
	// takes, so an operator never clears a quarantine under a running member
	// (K36).
	LockFile = install.LockFile
)

// The host SNode's protocol databases when Config.Protocol leaves them empty,
// and a peer's when Peer leaves them empty.
const (
	defaultUnsafeDB  = "hg_unsafe"
	defaultSafeDB    = "hg_safe"
	defaultPromoteDB = "hg_promote"
)

// nodeIDPattern is the member id form of clarification K29.
var nodeIDPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}[a-z0-9]$`)

// identifierPattern is one plain ClickHouse identifier: a named collection's
// name, interpolated unquoted into remote(<collection>, ...).
var identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Peer is one owner whose tables the member copies (P2's static group scope:
// every enrolled SI indexer but the host).
type Peer struct {
	IndexerID uint64
	// Collection is the NOT OVERRIDABLE named collection on the member's
	// ClickHouse that logs in to the owner's source ClickHouse as si_reader
	// (si_owner_<indexer_id>, contract §23.2).
	Collection string
	// SafeDatabase and UnsafeDatabase are the owner's protocol databases;
	// "" selects hg_safe and hg_unsafe.
	SafeDatabase, UnsafeDatabase string
}

// Config is one si-member (contract §23.1).
type Config struct {
	// NodeID is the member id (K29): never an SNode, verifier or voter id.
	NodeID string
	// HostIndexerID is the SI indexer whose host runs this member; its own
	// tables are never copied.
	HostIndexerID     uint64
	NetworkID         string
	GenesisSnapshotID string
	// Genesis is the network's genesis table set, checked at startup
	// against the genesis manifest.
	Genesis []payloadexec.TableSchema
	// Pinned names the member databases; the zero value selects
	// ddl.DefaultMemberPinned(NodeID, 0, "") for copies without shadow.
	// Shadow requires an explicit Pinned.StoragePolicy; New leaves it as
	// configured and Run fails closed when constructing the shadow if empty.
	Pinned ddl.MemberPinned
	// Protocol names the host SNode's protocol databases; "" fields select
	// hg_unsafe, hg_safe and hg_promote.
	Protocol ddl.Pinned
	StateDir string
	Peers    []Peer
	// PoolCollections name the network verifiers' ClickHouses
	// (si_peer_verifier_<n>), the peer unsafe pull's sources.
	PoolCollections []string
	// Restore is the object-store restore source; nil configures none.
	Restore *sources.RestoreConfig
	// Shadow runs the shadow verifier (spec §16 row P2).
	Shadow bool
	// PollInterval: 0 selects DefaultPollInterval.
	PollInterval time.Duration
	// DiskBudgetBytes and StopInstallsRatio bound the member disk (K35); a
	// ratio of 0 selects install.DefaultStopInstallsRatio.
	DiskBudgetBytes   uint64
	StopInstallsRatio float64
	// SelfAuditInterval: 0 selects install.DefaultSelfAuditInterval.
	SelfAuditInterval time.Duration
	// FetchTimeout bounds one source's fetch of one part: 0 selects
	// install.DefaultFetchTimeout.
	FetchTimeout time.Duration
	// FeedMaxWalk and FeedCacheEntries: 0 selects the manifestfeed defaults.
	FeedMaxWalk, FeedCacheEntries int
}

// Deps are the role's connections (contract §23.1). The role opens none of
// them and closes none of them.
type Deps struct {
	// Client reads the arbiter's data-plane listener.
	Client *dataplane.Client
	// Conn is the member's own ClickHouse, logged in as si_member.
	Conn clickhouse.Conn
	// Registry is the process's table-registry follower.
	Registry dataplane.RegistryView
	// Payloads reads DA payloads for the shadow's replays.
	Payloads replay.PayloadStore
	// AfterAttach is passed to install.Deps.AfterAttach: a fault-injection
	// seam for tests, nil in production.
	AfterAttach func(tableKey, partitionID string) error
	Logger      *slog.Logger
	Now         func() time.Time
}

// withDefaults fills every zero field that has a default.
func (c Config) withDefaults() Config {
	if c.PollInterval == 0 {
		c.PollInterval = DefaultPollInterval
	}
	if c.StopInstallsRatio == 0 {
		c.StopInstallsRatio = install.DefaultStopInstallsRatio
	}
	if c.SelfAuditInterval == 0 {
		c.SelfAuditInterval = install.DefaultSelfAuditInterval
	}
	if c.FetchTimeout == 0 {
		c.FetchTimeout = install.DefaultFetchTimeout
	}
	if c.Protocol.UnsafeDB == "" {
		c.Protocol.UnsafeDB = defaultUnsafeDB
	}
	if c.Protocol.SafeDB == "" {
		c.Protocol.SafeDB = defaultSafeDB
	}
	if c.Protocol.PromoteDB == "" {
		c.Protocol.PromoteDB = defaultPromoteDB
	}
	if c.Pinned == (ddl.MemberPinned{}) {
		c.Pinned = ddl.DefaultMemberPinned(c.NodeID, 0, "")
	}
	peers := make([]Peer, len(c.Peers))
	for i, p := range c.Peers {
		if p.SafeDatabase == "" {
			p.SafeDatabase = defaultSafeDB
		}
		if p.UnsafeDatabase == "" {
			p.UnsafeDatabase = defaultUnsafeDB
		}
		peers[i] = p
	}
	c.Peers = peers
	return c
}

// validate checks what needs no connection. The scope checks of startup
// step (2) run in Run, after the state directory lock.
func (c Config) validate() error {
	if !nodeIDPattern.MatchString(c.NodeID) {
		return fmt.Errorf("member: node id %q must match ^[a-z][a-z0-9-]{0,30}[a-z0-9]$", c.NodeID)
	}
	if c.Pinned.NodeID != c.NodeID {
		return fmt.Errorf("member: Pinned.NodeID %q differs from NodeID %q", c.Pinned.NodeID, c.NodeID)
	}
	for _, f := range []struct{ name, value string }{{"NetworkID", c.NetworkID}, {"GenesisSnapshotID", c.GenesisSnapshotID}, {"StateDir", c.StateDir}} {
		if f.value == "" {
			return fmt.Errorf("member: %s is required", f.name)
		}
	}
	if c.PollInterval < MinPollInterval {
		return fmt.Errorf("member: poll interval %s is below 1s", c.PollInterval)
	}
	if !(c.StopInstallsRatio > 0 && c.StopInstallsRatio < 1) {
		return fmt.Errorf("member: stop_installs_ratio must satisfy 0 < ratio < 1, got %v", c.StopInstallsRatio)
	}
	if c.DiskBudgetBytes == 0 {
		return errors.New("member: disk budget is required")
	}
	if c.SelfAuditInterval < install.MinSelfAuditInterval {
		return fmt.Errorf("member: self-audit interval %s is below %s", c.SelfAuditInterval, install.MinSelfAuditInterval)
	}
	if c.FetchTimeout < 0 {
		return fmt.Errorf("member: fetch timeout %s is not positive", c.FetchTimeout)
	}
	if len(c.Peers) == 0 {
		return errors.New("member: at least one peer is required")
	}
	for _, p := range c.Peers {
		if !identifierPattern.MatchString(p.Collection) {
			return fmt.Errorf("member: peer %d collection %q is not a ClickHouse identifier", p.IndexerID, p.Collection)
		}
	}
	for _, name := range c.PoolCollections {
		if !identifierPattern.MatchString(name) {
			return fmt.Errorf("member: pool collection %q is not a ClickHouse identifier", name)
		}
	}
	return nil
}

func (d Deps) validate(shadow bool) error {
	if d.Client == nil || d.Conn == nil || d.Registry == nil {
		return errors.New("member: Deps.Client, Deps.Conn and Deps.Registry are required")
	}
	if shadow && d.Payloads == nil {
		return errors.New("member: shadow verification needs Deps.Payloads")
	}
	return nil
}
