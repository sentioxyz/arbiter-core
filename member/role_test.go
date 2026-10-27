package member

import (
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/dataplane/dastore"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/copyset"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/manifestfeed"
	"github.com/sentioxyz/arbiter-core/member/shadow"
	"github.com/sentioxyz/arbiter-core/member/sources"
	"github.com/sentioxyz/arbiter-core/wire"
)

// steps records the role's calls on its ClickHouse and on its parts, in order.
type steps struct {
	mu  sync.Mutex
	got []string
}

func (s *steps) add(format string, args ...any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, fmt.Sprintf(format, args...))
}

func (s *steps) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.got)
}

func (s *steps) has(step string) bool { return slices.Contains(s.list(), step) }

// fakeCopies is the copyset of the unit tests; onReconcile, when set, runs
// at each Reconcile (a test sets it before Run).
type fakeCopies struct {
	steps       *steps
	mu          sync.Mutex
	copies      []copyset.Copy
	onReconcile func()
}

func (c *fakeCopies) set(copies ...copyset.Copy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.copies = copies
}

func (c *fakeCopies) Copies() []copyset.Copy {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.copies)
}

func (c *fakeCopies) Copy(tableKey string) (copyset.Copy, bool) {
	for _, x := range c.Copies() {
		if x.TableKey == tableKey {
			return x, true
		}
	}
	return copyset.Copy{}, false
}

func (c *fakeCopies) Reconcile(context.Context) error {
	c.steps.add("copyset reconcile")
	if c.onReconcile != nil {
		c.onReconcile()
	}
	return nil
}

func (c *fakeCopies) Run(ctx context.Context) error {
	c.steps.add("copyset run")
	<-ctx.Done()
	return ctx.Err()
}

// fakeInstaller journals, at each applied update, the owners of the ready
// copies and the owners it journaled before, as install.Installer does once
// every ready copy of an owner closes: an owner without a copy is never
// journaled. NeedsPass reports needsPass, which a successful Apply clears;
// auditSaw records how many ready copies each Audit saw. An Apply of partialOn
// journals partialOwner alone, sets needsPass and fails, as install.Installer
// is left by a pass that committed one owner and failed for another.
type fakeInstaller struct {
	steps        *steps
	copies       *fakeCopies
	mu           sync.Mutex
	entries      map[uint64]install.OwnerJournal
	applied      []string
	audits       []string
	auditSaw     []int
	failNext     int
	partialOn    string
	partialOwner uint64
	clean        bool
	needsPass    bool
	stats        install.Stats
	tables       map[uint64][]install.TableState
}

func (i *fakeInstaller) Apply(_ context.Context, u manifestfeed.Update) error {
	i.steps.add("apply %s", u.SnapshotID)
	i.mu.Lock()
	defer i.mu.Unlock()
	i.applied = append(i.applied, u.SnapshotID)
	if i.failNext > 0 {
		i.failNext--
		return errors.New("install: injected failure")
	}
	if i.partialOn != "" && u.SnapshotID == i.partialOn {
		i.entries[i.partialOwner] = install.OwnerJournal{SnapshotID: u.SnapshotID, Entry: u.Entries[i.partialOwner]}
		i.needsPass = true
		return errors.New("install: injected failure after one owner's pass committed")
	}
	owners := map[uint64]bool{}
	for _, c := range i.copies.Copies() {
		if c.Ready {
			owners[c.Owner] = true
		}
	}
	for owner := range i.entries {
		owners[owner] = true
	}
	for owner := range owners {
		i.entries[owner] = install.OwnerJournal{SnapshotID: u.SnapshotID, Entry: u.Entries[owner]}
	}
	i.needsPass = false
	return nil
}

func (i *fakeInstaller) Audit(_ context.Context, trigger string) error {
	i.steps.add("audit %s", trigger)
	i.mu.Lock()
	defer i.mu.Unlock()
	i.audits = append(i.audits, trigger)
	i.auditSaw = append(i.auditSaw, len(i.copies.Copies()))
	return nil
}

func (i *fakeInstaller) DropStaleScratch(context.Context) error {
	i.steps.add("drop stale scratch")
	return nil
}

func (i *fakeInstaller) Entry(owner uint64) (install.OwnerJournal, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	j, ok := i.entries[owner]
	return j, ok
}

func (i *fakeInstaller) Tables(owner uint64) []install.TableState {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.tables[owner]
}

func (i *fakeInstaller) Stats() install.Stats {
	i.mu.Lock()
	defer i.mu.Unlock()
	s := i.stats
	s.AuditClean = i.clean
	return s
}

func (i *fakeInstaller) appliedList() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return slices.Clone(i.applied)
}

func (i *fakeInstaller) auditList() []string {
	i.mu.Lock()
	defer i.mu.Unlock()
	return slices.Clone(i.audits)
}

func (i *fakeInstaller) setClean(clean bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.clean = clean
}

func (i *fakeInstaller) NeedsPass() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.needsPass
}

func (i *fakeInstaller) setNeedsPass(v bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.needsPass = v
}

func (i *fakeInstaller) setPartial(snapshotID string, owner uint64) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.partialOn, i.partialOwner = snapshotID, owner
}

func (i *fakeInstaller) auditSawList() []int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return slices.Clone(i.auditSaw)
}

// fakeShadow is the shadow of the unit tests; run, when set, replaces Run.
type fakeShadow struct {
	stats shadow.Stats
	run   func(ctx context.Context) error
}

func (s *fakeShadow) Run(ctx context.Context) error {
	if s.run != nil {
		return s.run(ctx)
	}
	<-ctx.Done()
	return ctx.Err()
}

func (s *fakeShadow) Stats() shadow.Stats { return s.stats }

// harness is one role on a fake arbiter, with fake ClickHouse calls and
// fake parts.
type harness struct {
	t          *testing.T
	arbiter    *fakeArbiter
	registry   dataplane.RegistryView
	payloads   replay.PayloadStore
	steps      *steps
	copies     *fakeCopies
	installer  *fakeInstaller
	shadow     *fakeShadow
	cfg        Config
	genesis    replay.SafeSnapshotManifest
	grantsErr  error
	fenceErr   error
	remoteErr  map[string]error
	restarts   chan struct{}
	shadowDeps shadow.Deps
}

func testConfig(t *testing.T) Config {
	return Config{
		NodeID: "member-b", HostIndexerID: 1, NetworkID: testNetworkID, Genesis: testGenesis(),
		StateDir:        t.TempDir(),
		Peers:           []Peer{{IndexerID: 2, Collection: "si_owner_2"}, {IndexerID: 0, Collection: "si_owner_0"}},
		PoolCollections: []string{"si_peer_verifier_1"}, PollInterval: time.Second, DiskBudgetBytes: 1 << 30,
	}
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	h := &harness{t: t, arbiter: newFakeArbiter(t), steps: &steps{}, remoteErr: map[string]error{}, restarts: make(chan struct{}, 1)}
	h.genesis = genesisManifest(t, testGenesis())
	h.arbiter.store(h.genesis)
	h.registry = newStaticRegistry(testRegistry(0, 1, 2))
	h.payloads = noPayloads{}
	h.copies = &fakeCopies{steps: h.steps}
	h.installer = &fakeInstaller{steps: h.steps, copies: h.copies, entries: map[uint64]install.OwnerJournal{}, clean: true, tables: map[uint64][]install.TableState{}}
	h.shadow = &fakeShadow{}
	h.cfg = testConfig(t)
	h.cfg.GenesisSnapshotID = h.genesis.SnapshotID
	return h
}

// role builds the role with every ClickHouse call and every part replaced.
func (h *harness) role() *Role {
	h.t.Helper()
	r, err := New(h.cfg, Deps{Client: h.arbiter.client(h.t), Conn: noConn{}, Registry: h.registry, Payloads: h.payloads})
	if err != nil {
		h.t.Fatalf("New: %v", err)
	}
	r.env = seams{
		ensureDatabases: func(_ context.Context, _ clickhouse.Conn, m ddl.MemberPinned) error {
			h.steps.add("ensure databases %s %s", m.ReplicaDB, m.StageDB)
			return nil
		},
		checkEngineFence: func(_ context.Context, _ clickhouse.Conn, stageDB, probe string) error {
			h.steps.add("engine fence %s.%s", stageDB, probe)
			return h.fenceErr
		},
		checkRemoteIsReadOnly: func(_ context.Context, _ clickhouse.Conn, collection string) error {
			h.steps.add("remote read-only %s", collection)
			return h.remoteErr[collection]
		},
		readOwnGrants: func(context.Context, clickhouse.Conn) ([]sources.Grant, error) {
			h.steps.add("read own grants")
			// The rows a confined si_member reads when system.grants is
			// readable: READ ON REMOTE and no WRITE (Task 31's
			// sources.CheckRemoteGrants refuses an empty list).
			return []sources.Grant{{AccessType: "READ", AccessObject: "REMOTE"}}, h.grantsErr
		},
		startedAt: func(_ context.Context, _ clickhouse.Conn, now time.Time) (time.Time, error) {
			h.steps.add("clickhouse started at")
			return now.Add(-time.Hour), nil
		},
		watchRestarts: func(ctx context.Context, _ clickhouse.Conn, w *chrestart.Watcher) {
			h.steps.add("watch restarts every %s", w.Interval)
			for {
				select {
				case <-ctx.Done():
					return
				case <-h.restarts:
					w.OnRestart(ctx, w.Baseline, w.Baseline.Add(time.Minute))
				}
			}
		},
		newCopySet: func(cfg copyset.Config, d copyset.Deps) (copySet, error) {
			h.steps.add("new copyset owners %v host %d drop hook %v", cfg.Owners, cfg.HostIndexerID, d.Dropped != nil)
			return h.copies, nil
		},
		newInstaller: func(cfg install.Config, d install.Deps) (installer, error) {
			names := make([][]string, 0, 2)
			for _, owner := range []uint64{0, 2} {
				var n []string
				for _, s := range d.Sources(owner) {
					n = append(n, s.Name())
				}
				names = append(names, n)
			}
			h.steps.add("new installer budget %d ratio %v sources %v", cfg.DiskBudgetBytes, cfg.StopInstallsRatio, names)
			return h.installer, nil
		},
		newShadow: func(cfg shadow.Config, d shadow.Deps) (shadowVerifier, error) {
			h.steps.add("new shadow owners %v", cfg.Owners)
			h.shadowDeps = d
			return h.shadow, nil
		},
	}
	return r
}

// start runs the role until the test ends and returns its result channel.
func (h *harness) start(r *Role) <-chan error {
	h.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	h.t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			h.t.Error("the role did not stop within 10s of its context ending")
		}
	})
	return done
}

// runToError runs the role and returns the startup error it must end with.
func (h *harness) runToError(r *Role) error {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := r.Run(ctx)
	if err == nil || errors.Is(err, context.DeadlineExceeded) {
		h.t.Fatalf("Run = %v, want a startup refusal", err)
	}
	return err
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// noConn is a ClickHouse connection the unit tests never use: every call the
// role makes on Conn goes through a seam.
type noConn struct{ clickhouse.Conn }

type noPayloads struct{}

func (noPayloads) GetPayload(context.Context, string) ([]byte, error) {
	return nil, errors.New("no payloads in this test")
}

func copyOf(owner uint64, key string, seq uint64) copyset.Copy {
	return copyset.Copy{Owner: owner, TableKey: key, IncarnationSeq: seq, Ready: true, Status: wire.TableStatusActive}
}

// hasPrefix reports whether any step starts with one of the prefixes.
func hasPrefix(got []string, prefixes ...string) bool {
	return slices.ContainsFunc(got, func(s string) bool {
		return slices.ContainsFunc(prefixes, func(p string) bool { return strings.HasPrefix(s, p) })
	})
}

func TestLocalNodeFeaturesAreUnchangedThroughP2(t *testing.T) {
	// Contract §20.1: P2 adds no feature string; a member has no consensus
	// identity and advertises nothing.
	if got := arbiter.LocalNodeFeatures(); !slices.Equal(got, []string{"client_lanes_v1", "signed_claims_v1", "track_manifests_v1"}) {
		t.Fatalf("LocalNodeFeatures = %v, want exactly [client_lanes_v1 signed_claims_v1 track_manifests_v1] through CORE_TAG_P2", got)
	}
}

func TestNewRefusesAnInvalidConfig(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*Config)
		want   string
	}{
		{"bad node id", func(c *Config) { c.NodeID = "Member_B" }, `member: node id "Member_B" must match ^[a-z][a-z0-9-]{0,30}[a-z0-9]$`},
		{"poll interval", func(c *Config) { c.PollInterval = 500 * time.Millisecond }, "member: poll interval 500ms is below 1s"},
		{"ratio one", func(c *Config) { c.StopInstallsRatio = 1 }, "member: stop_installs_ratio must satisfy 0 < ratio < 1, got 1"},
		{"ratio negative", func(c *Config) { c.StopInstallsRatio = -0.5 }, "member: stop_installs_ratio must satisfy 0 < ratio < 1, got -0.5"},
		{"no disk budget", func(c *Config) { c.DiskBudgetBytes = 0 }, "member: disk budget is required"},
		{"self-audit interval", func(c *Config) { c.SelfAuditInterval = time.Minute }, "member: self-audit interval 1m0s is below 10m0s"},
		{"no peers", func(c *Config) { c.Peers = nil }, "member: at least one peer is required"},
		{"collection", func(c *Config) { c.Peers[0].Collection = "si-owner-2" }, `member: peer 2 collection "si-owner-2" is not a ClickHouse identifier`},
		{"pool collection", func(c *Config) { c.PoolCollections = []string{"a b"} }, `member: pool collection "a b" is not a ClickHouse identifier`},
		{"pinned node id", func(c *Config) { c.Pinned = ddl.DefaultMemberPinned("member-a", 0, "") }, `member: Pinned.NodeID "member-a" differs from NodeID "member-b"`},
		{"state dir", func(c *Config) { c.StateDir = "" }, "member: StateDir is required"},
		{"genesis snapshot", func(c *Config) { c.GenesisSnapshotID = "" }, "member: GenesisSnapshotID is required"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.GenesisSnapshotID = "g"
			tc.mutate(&cfg)
			_, err := New(cfg, Deps{Client: newFakeArbiter(t).client(t), Conn: noConn{}, Registry: newStaticRegistry(testRegistry(0, 1, 2))})
			if err == nil || err.Error() != tc.want {
				t.Fatalf("New = %v, want %q", err, tc.want)
			}
		})
	}
	cfg := testConfig(t)
	cfg.GenesisSnapshotID, cfg.Shadow = "g", true
	if _, err := New(cfg, Deps{Client: newFakeArbiter(t).client(t), Conn: noConn{}, Registry: newStaticRegistry(testRegistry())}); err == nil ||
		err.Error() != "member: shadow verification needs Deps.Payloads" {
		t.Fatalf("New without payloads = %v", err)
	}
	if _, err := New(cfg, Deps{}); err == nil || err.Error() != "member: Deps.Client, Deps.Conn and Deps.Registry are required" {
		t.Fatalf("New without deps = %v", err)
	}
}

func TestRoleRunsTheStartupStepsInOrder(t *testing.T) {
	h := newHarness(t)
	h.cfg.Shadow = true
	h.copies.set(copyOf(0, "devuser1.t0", 7), copyOf(2, "devuser1.t2", 9))
	m := child(t, h.genesis, 3, 0, 1, 2)
	h.arbiter.publish(m)
	r := h.role()
	h.start(r)
	waitFor(t, "the first install pass", func() bool { return len(h.installer.appliedList()) == 1 })

	got := h.steps.list()
	want := []string{
		"ensure databases hg_replica hg_rstage",
		"engine fence hg_rstage.member-b_engine_probe",
		"remote read-only si_owner_0",
		"remote read-only si_owner_2",
		"remote read-only si_peer_verifier_1",
		"read own grants",
		"new copyset owners [0 2] host 1 drop hook false",
		"new installer budget 1073741824 ratio 0.8 sources [[owner_pull peer_unsafe_pull] [owner_pull peer_unsafe_pull]]",
		"new shadow owners [0 2]",
		"drop stale scratch",
		"clickhouse started at",
		"copyset reconcile",
		"audit startup",
	}
	if len(got) < len(want) || !slices.Equal(got[:len(want)], want) {
		t.Fatalf("startup steps:\n%s\nwant first:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	for _, s := range []string{"copyset run", "watch restarts every 1s", "apply " + m.SnapshotID} {
		waitFor(t, s, func() bool { return h.steps.has(s) })
	}
	for _, method := range []string{"/arbiter.TableRegistry/GetPurgeNodeSet", "/arbiter.SafeState/GetManifest", "/arbiter.SafeState/GetSafeWatermark"} {
		if h.arbiter.called(method) == 0 {
			t.Errorf("startup never called %s", method)
		}
	}
}

func TestRoleRefusesASecondProcessOnTheStateDir(t *testing.T) {
	h := newHarness(t)
	held, err := lockStateDir(h.cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Close()
	err = h.runToError(h.role())
	if want := fmt.Sprintf("member: state dir %s is locked by a running si-member", h.cfg.StateDir); err.Error() != want {
		t.Fatalf("Run = %v, want %q", err, want)
	}
	if got := h.steps.list(); len(got) != 0 {
		t.Fatalf("a refused start touched ClickHouse or built parts: %v", got)
	}
}

func TestClearForeignRefusesWhileTheRoleHoldsTheStateDir(t *testing.T) {
	dir := t.TempDir()
	held, err := lockStateDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	err = install.ClearForeign(dir, "devuser1.t0")
	if want := fmt.Sprintf("install: state dir %s is locked by a running si-member", dir); err == nil || err.Error() != want {
		t.Fatalf("ClearForeign under a running member = %v, want %q", err, want)
	}
	_ = held.Close()
	if _, err := lockStateDir(dir); err != nil {
		t.Fatalf("the lock must be free once the member exits: %v", err)
	}
}

func TestRoleRefusesTheHostAsAPeer(t *testing.T) {
	h := newHarness(t)
	h.cfg.Peers = append(h.cfg.Peers, Peer{IndexerID: 1, Collection: "si_owner_1"})
	err := h.runToError(h.role())
	if want := "member: 1 is the host's own indexer; a member never copies its host"; err.Error() != want {
		t.Fatalf("Run = %v, want %q", err, want)
	}
	if got := h.steps.list(); len(got) != 0 {
		t.Fatalf("a refused start touched ClickHouse: %v", got)
	}
}

func TestRoleRefusesANodeIDInUse(t *testing.T) {
	t.Run("an SNode", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.NodeID = "snode-3"
		err := h.runToError(h.role())
		if !errors.Is(err, copyset.ErrNodeIDInUse) || !strings.Contains(err.Error(), `node id "snode-3" is already used by si_indexers entry 2 (its SNode)`) {
			t.Fatalf("Run = %v, want copyset.ErrNodeIDInUse naming si_indexers entry 2", err)
		}
	})
	t.Run("the purge node set", func(t *testing.T) {
		h := newHarness(t)
		h.arbiter.setPurge("snode-1", "verifier-1", "member-b")
		err := h.runToError(h.role())
		if !errors.Is(err, copyset.ErrNodeIDInUse) || !strings.Contains(err.Error(), "the purge node set (a registered SNode or verifier)") {
			t.Fatalf("Run = %v, want copyset.ErrNodeIDInUse naming the purge node set", err)
		}
		if got := h.steps.list(); hasPrefix(got, "new ", "drop", "audit") {
			t.Fatalf("a refused start built parts, dropped tables or audited: %v", got)
		}
	})
}

func TestRoleRefusesAPeerThatIsNotEnrolled(t *testing.T) {
	h := newHarness(t)
	h.registry = newStaticRegistry(testRegistry(0, 1))
	err := h.runToError(h.role())
	if want := "member: peer indexer 2 is not enrolled in si_indexers"; err.Error() != want {
		t.Fatalf("Run = %v, want %q", err, want)
	}
	if got := h.steps.list(); hasPrefix(got, "new ", "drop", "audit") {
		t.Fatalf("a refused start built parts, dropped tables or audited: %v", got)
	}
}

func TestRoleRefusesGenesisTablesThatDifferFromTheGenesisManifest(t *testing.T) {
	t.Run("another table set", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Genesis = append(testGenesis(), payloadexec.TableSchema{TableID: "devuser1.g_c", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}})
		err := h.runToError(h.role())
		if want := "member: genesis tables [devuser1.g_a devuser1.g_b devuser1.g_c] differ from the genesis manifest's [devuser1.g_a devuser1.g_b]"; err.Error() != want {
			t.Fatalf("Run = %v, want %q", err, want)
		}
	})
	t.Run("another schema", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Genesis = testGenesis()
		h.cfg.Genesis[1].Columns = []lthash.Column{{Name: "v", Type: "Int64"}}
		err := h.runToError(h.role())
		want := fmt.Sprintf("member: genesis table devuser1.g_b schema hashes to %s, the genesis manifest records %s",
			payloadexec.TableSchemaHash(testNetworkID, h.cfg.Genesis[1]), payloadexec.TableSchemaHash(testNetworkID, testGenesis()[1]))
		if err.Error() != want {
			t.Fatalf("Run = %v, want %q", err, want)
		}
		if got := h.steps.list(); hasPrefix(got, "new ", "drop", "audit") {
			t.Fatalf("a refused start built parts, dropped tables or audited: %v", got)
		}
	})
}

func TestRoleRefusesToStartWithoutTheEngineFence(t *testing.T) {
	h := newHarness(t)
	h.fenceErr = fmt.Errorf("%w: sources: ClickHouse accepted ENGINE = Log for this user", sources.ErrEngineFenceOff)
	if err := h.runToError(h.role()); !errors.Is(err, sources.ErrEngineFenceOff) {
		t.Fatalf("Run = %v, want sources.ErrEngineFenceOff", err)
	}
	got := h.steps.list()
	if want := []string{"ensure databases hg_replica hg_rstage", "engine fence hg_rstage.member-b_engine_probe"}; !slices.Equal(got, want) {
		t.Fatalf("steps = %v, want exactly %v: nothing after a failed fence probe", got, want)
	}
}

func TestRoleRefusesAWritableRemote(t *testing.T) {
	for _, collection := range []string{"si_owner_0", "si_peer_verifier_1"} {
		t.Run(collection, func(t *testing.T) {
			h := newHarness(t)
			h.remoteErr[collection] = fmt.Errorf("%w: sources: INSERT INTO FUNCTION remote(%s) was not refused with ACCESS_DENIED", sources.ErrRemoteWritable, collection)
			if err := h.runToError(h.role()); !errors.Is(err, sources.ErrRemoteWritable) || !strings.Contains(err.Error(), collection) {
				t.Fatalf("Run = %v, want sources.ErrRemoteWritable naming %s", err, collection)
			}
			if got := h.steps.list(); hasPrefix(got, "read own grants", "new ", "drop", "audit") {
				t.Fatalf("steps after a writable remote(): %v", got)
			}
		})
	}
}

func TestRoleToleratesUnreadableGrants(t *testing.T) {
	h := newHarness(t)
	h.grantsErr = fmt.Errorf("%w: code 497", sources.ErrGrantsUnreadable)
	r := h.role()
	h.start(r)
	waitFor(t, "the startup audit", func() bool { return h.steps.has("audit startup") })
	if !r.Stats().GrantsUnreadable {
		t.Fatal("Stats.GrantsUnreadable = false after system.grants was unreadable")
	}
}

func TestRoleRefusesUnreadableGrantsForAnyOtherReason(t *testing.T) {
	h := newHarness(t)
	h.grantsErr = errors.New("connection reset")
	if err := h.runToError(h.role()); err.Error() != "member: read this user's grants: connection reset" {
		t.Fatalf("Run = %v", err)
	}
}

// readsOnlyShadow calls every read its Deps give it once, as the real shadow
// does in steady state, and records that it did.
type readsOnlyShadow struct {
	d     shadow.Deps
	snap  string
	done  chan error
	once  sync.Once
	stats shadow.Stats
}

func (s *readsOnlyShadow) Run(ctx context.Context) error {
	s.once.Do(func() {
		var errs []error
		if _, err := s.d.Blocks.GetBlockVerification(ctx, 1); !errors.Is(err, dataplane.ErrBlockNotSealed) {
			errs = append(errs, fmt.Errorf("GetBlockVerification(1) = %v, want ErrBlockNotSealed", err))
		}
		if _, err := s.d.Watermark.GetSafeWatermark(ctx); err != nil {
			errs = append(errs, err)
		}
		if _, err := s.d.Snapshots.GetSafeSnapshot(ctx, s.snap); err != nil {
			errs = append(errs, err)
		}
		if b, err := s.d.Payloads.GetPayload(ctx, "da://member-test"); err != nil || string(b) != testPayload {
			errs = append(errs, fmt.Errorf("GetPayload = %q, %v", b, err))
		}
		s.done <- errors.Join(errs...)
	})
	<-ctx.Done()
	return ctx.Err()
}

func (s *readsOnlyShadow) Stats() shadow.Stats { return s.stats }

func TestRoleCallsOnlyReadRPCs(t *testing.T) {
	// Every arbiter and DA call of a running role goes through the fake's
	// interceptors, which fail the test on any method outside contract
	// §20.1. The registry is a real RegistryFollower and the payloads a real
	// dastore client on the fake; the shadow uses each client it is given.
	h := newHarness(t)
	h.cfg.Shadow = true
	h.arbiter.setRegistry(testRegistry(0, 1, 2))
	m := child(t, h.genesis, 4, 0, 1, 2)
	h.arbiter.publish(m)
	client := h.arbiter.client(t)
	follower := dataplane.NewRegistryFollower(client, slog.Default())
	fctx, stop := context.WithCancel(context.Background())
	t.Cleanup(stop)
	go func() { _ = follower.Run(fctx) }()
	h.registry = follower
	payloads, err := dastore.New(dastore.Config{DataAddr: h.arbiter.addr})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = payloads.Close() })
	h.payloads = payloads
	h.copies.set(copyOf(0, "devuser1.t0", 7))
	r := h.role()
	reads := &readsOnlyShadow{snap: h.genesis.SnapshotID, done: make(chan error, 1)}
	r.env.newShadow = func(cfg shadow.Config, d shadow.Deps) (shadowVerifier, error) {
		reads.d = d
		return reads, nil
	}
	done := h.start(r)
	select {
	case err := <-done:
		t.Fatalf("the role stopped: %v; steps %v", err, h.steps.list())
	case err := <-reads.done:
		if err != nil {
			t.Fatalf("the shadow's reads: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the shadow never ran")
	}
	waitFor(t, "an install pass", func() bool { return len(h.installer.appliedList()) > 0 })
	for _, method := range readRPCs {
		if h.arbiter.called(method) == 0 {
			t.Errorf("the role never called %s; the test must exercise every read of contract §20.1", method)
		}
	}

	// The role itself holds no gRPC client: it reaches the arbiter only
	// through the read methods of dataplane.Client and the clients it hands
	// its parts.
	fset := token.NewFileSet()
	files, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, e := range files {
		name := e.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, parser.SkipObjectResolution)
		if err != nil {
			t.Fatal(err)
		}
		checked++
		for _, imp := range f.Imports {
			if p, _ := strconv.Unquote(imp.Path.Value); strings.HasPrefix(p, "google.golang.org/grpc") || strings.HasPrefix(p, "github.com/sentioxyz/arbiter-proto") {
				t.Errorf("%s imports %s: the member role holds no gRPC client of its own", name, p)
			}
		}
		ast.Inspect(f, func(n ast.Node) bool {
			if sel, ok := n.(*ast.SelectorExpr); ok {
				switch sel.Sel.Name {
				case "WithLeaderRetry", "LeaderConn", "RunVerifierSubscription", "RunPromotionSubscription", "SubmitTablePurged", "SubmitTablePurgedReport":
					t.Errorf("%s: %s calls %s, which is no member read", fset.Position(sel.Pos()), name, sel.Sel.Name)
				}
			}
			return true
		})
	}
	if checked < 4 {
		t.Fatalf("parsed %d non-test source files of the package, want config.go, lock.go, role.go and stats.go", checked)
	}
}

func TestRoleStatsExportEveryKeyIncludingZeros(t *testing.T) {
	h := newHarness(t)
	h.cfg.Shadow = true
	r := h.role()
	check := func(s Stats, when string) {
		t.Helper()
		if s.NodeID != "member-b" || s.HostIndexerID != 1 || s.DiskBudgetBytes != 1<<30 {
			t.Fatalf("%s: identity = %q %d %d", when, s.NodeID, s.HostIndexerID, s.DiskBudgetBytes)
		}
		if len(s.Owners) != 2 {
			t.Fatalf("%s: owners = %v, want exactly the peers 0 and 2", when, s.Owners)
		}
		for _, id := range []uint64{0, 2} {
			o, ok := s.Owners[id]
			if !ok {
				t.Fatalf("%s: owner %d missing from %v", when, id, s.Owners)
			}
			for _, k := range []shadow.MismatchKind{shadow.MismatchReceipt, shadow.MismatchScan, shadow.MismatchClaim} {
				if _, ok := o.ShadowMismatches[k]; !ok {
					t.Fatalf("%s: owner %d mismatch kind %s missing", when, id, k)
				}
			}
		}
		for _, o := range []install.Outcome{"kept", "rename_race", "split", "doubled", "foreign", "missed", "refused", "unreachable"} {
			if _, ok := s.InstallOutcomes[o]; !ok {
				t.Fatalf("%s: outcome %s missing from %v", when, o, s.InstallOutcomes)
			}
		}
		for _, src := range []string{"owner_pull", "peer_unsafe_pull", "restore"} {
			if _, ok := s.Installs[src]; !ok {
				t.Fatalf("%s: source %s missing from %v", when, src, s.Installs)
			}
		}
		for _, tr := range []string{"startup", "clickhouse_restart", "self_audit"} {
			if _, ok := s.AuditRuns[tr]; !ok {
				t.Fatalf("%s: trigger %s missing from %v", when, tr, s.AuditRuns)
			}
		}
	}
	check(r.Stats(), "before Run")

	h.installer.stats = install.Stats{Outcomes: map[install.Outcome]uint64{"kept": 3}, Installs: map[string]uint64{"owner_pull": 3},
		DivergedPartitions: map[uint64]uint64{2: 1}, AuditRuns: map[string]uint64{"startup": 1}, DiskUsedBytes: 42, InstallsPaused: true}
	h.installer.tables[2] = []install.TableState{{Owner: 2, TableKey: "devuser1.t2", Diverged: true}, {Owner: 2, TableKey: "devuser1.u2", Foreign: true}}
	h.shadow.stats = shadow.Stats{Blocks: map[uint64]uint64{0: 5}, MissedScans: map[uint64]uint64{0: 1}, Unverdicted: map[uint64]uint64{2: 4},
		Mismatches: map[uint64]map[shadow.MismatchKind]uint64{0: {shadow.MismatchScan: 2}}, NextBlockSeq: 9}
	h.copies.set(copyOf(0, "devuser1.t0", 7), copyOf(2, "devuser1.t2", 9), copyOf(2, "devuser1.u2", 10))
	h.arbiter.publish(child(t, h.genesis, 4, 0, 1, 2))
	h.start(r)
	waitFor(t, "the first install pass", func() bool { return len(h.installer.appliedList()) > 0 })
	waitFor(t, "the feed counters", func() bool { return r.Stats().FeedAccepted == 1 })
	s := r.Stats()
	check(s, "while running")
	if s.InstallOutcomes["kept"] != 3 || s.InstallOutcomes["split"] != 0 || s.Installs["owner_pull"] != 3 || s.Installs["restore"] != 0 ||
		s.AuditRuns["startup"] != 1 || s.DiskUsedBytes != 42 || !s.InstallsPaused {
		t.Fatalf("installer stats not carried: %+v", s)
	}
	o0, o2 := s.Owners[0], s.Owners[2]
	if o0.Copies != 1 || o2.Copies != 2 || o2.DivergedPartitions != 1 || o2.DivergedTables != 1 || o2.ForeignTables != 1 {
		t.Fatalf("owner stats: 0 = %+v, 2 = %+v", o0, o2)
	}
	if o0.ShadowBlocks != 5 || o0.ShadowMissedScans != 1 || o0.ShadowMismatches[shadow.MismatchScan] != 2 || o0.ShadowMismatches[shadow.MismatchClaim] != 0 ||
		o2.ShadowUnverdicted != 4 || s.ShadowNextBlockSeq != 9 {
		t.Fatalf("shadow stats: 0 = %+v, 2 = %+v, next %d", o0, o2, s.ShadowNextBlockSeq)
	}
	if want := (replay.TrackFrontier{IndexerID: 2, SafeBlockSeq: 4, AppliedPromotionSeq: 4}); o2.Entry != want || o2.PublishedEntry != want || !o2.BehindSince.IsZero() {
		t.Fatalf("owner 2 entry %+v published %+v behind since %v", o2.Entry, o2.PublishedEntry, o2.BehindSince)
	}
	if s.FeedAccepted != 1 || s.FeedFullFetches != 1 || s.FeedRefused != 0 {
		t.Fatalf("feed counters: accepted %d, full fetches %d, refused %d", s.FeedAccepted, s.FeedFullFetches, s.FeedRefused)
	}
}

func TestRoleListsUnconfiguredOwners(t *testing.T) {
	h := newHarness(t)
	h.registry = newStaticRegistry(testRegistry(0, 1, 2, 5, 7))
	r := h.role()
	if got := r.Stats().UnconfiguredOwners; !slices.Equal(got, []uint64{5, 7}) {
		t.Fatalf("UnconfiguredOwners = %v, want [5 7] (enrolled, neither a peer nor the host)", got)
	}
}

func TestRoleIsReadyOnlyAfterACleanAuditAndAManifest(t *testing.T) {
	h := newHarness(t)
	h.installer.setClean(false)
	h.copies.set(copyOf(0, "devuser1.t0", 7), copyOf(2, "devuser1.t2", 9))
	r := h.role()
	if r.Ready() {
		t.Fatal("ready before Run")
	}
	h.start(r)
	waitFor(t, "the startup audit", func() bool { return h.steps.has("audit startup") })
	if r.Ready() {
		t.Fatal("ready with an unclean audit and no manifest")
	}
	h.installer.setClean(true)
	time.Sleep(50 * time.Millisecond)
	if r.Ready() {
		t.Fatal("ready before any peer's copies reached a manifest")
	}
	h.arbiter.publish(child(t, h.genesis, 2, 0, 1, 2))
	waitFor(t, "readiness after the first install pass", r.Ready)
	if !r.Stats().Ready {
		t.Fatal("Stats.Ready disagrees with Ready")
	}
	h.installer.setClean(false)
	if r.Ready() {
		t.Fatal("still ready after an audit found the copies unclean")
	}
}

// TestRoleAuditsTheCopiesTheFirstCopysetPassMadeReady (Plan S3-C2 review fix
// 245-1): a fresh reconciler reports no copy until its first pass, so the
// startup audit runs after that pass and covers the copies it made ready.
func TestRoleAuditsTheCopiesTheFirstCopysetPassMadeReady(t *testing.T) {
	h := newHarness(t)
	h.copies.onReconcile = func() { h.copies.set(copyOf(0, "devuser1.t0", 7), copyOf(2, "devuser1.t2", 9)) }
	r := h.role()
	h.start(r)
	waitFor(t, "the startup audit", func() bool { return h.steps.has("audit startup") })
	if got := h.installer.auditSawList(); len(got) == 0 || got[0] != 2 {
		t.Fatalf("the startup audit saw %v ready copies, want 2: the copies of the first copyset pass", got)
	}
}

// TestRoleRepairsWhatAnAuditFoundWithoutANewManifest (Plan S3-C2 review fix
// 245-2): the watermark stands still and the copies do not change, yet an audit
// recorded damage the installer reports through NeedsPass; the copy watch
// re-applies the last update until the installer has nothing left, and not
// after.
func TestRoleRepairsWhatAnAuditFoundWithoutANewManifest(t *testing.T) {
	h := newHarness(t)
	h.copies.set(copyOf(0, "devuser1.t0", 7))
	m := child(t, h.genesis, 2, 0, 1, 2)
	h.arbiter.publish(m)
	r := h.role()
	h.start(r)
	waitFor(t, "the first install pass", func() bool { return len(h.installer.appliedList()) == 1 })
	time.Sleep(1500 * time.Millisecond)
	if got := h.installer.appliedList(); len(got) != 1 {
		t.Fatalf("install passes %v while nothing changed, want one", got)
	}
	h.installer.setNeedsPass(true)
	waitFor(t, "a repair pass of the last update", func() bool { return len(h.installer.appliedList()) == 2 })
	if got := h.installer.appliedList(); got[1] != m.SnapshotID {
		t.Fatalf("passes %v, want the last update re-applied", got)
	}
	time.Sleep(1500 * time.Millisecond)
	if got := h.installer.appliedList(); len(got) != 2 {
		t.Fatalf("install passes %v once the repair left nothing, want two", got)
	}
}

func TestRoleOffersAFailedUpdateAgain(t *testing.T) {
	h := newHarness(t)
	h.installer.failNext = 1
	h.copies.set(copyOf(0, "devuser1.t0", 7))
	m := child(t, h.genesis, 2, 0, 1, 2)
	h.arbiter.publish(m)
	r := h.role()
	h.start(r)
	waitFor(t, "the update offered twice", func() bool { return slices.Equal(h.installer.appliedList(), []string{m.SnapshotID, m.SnapshotID}) })
	time.Sleep(2500 * time.Millisecond)
	if got := h.installer.appliedList(); len(got) != 2 {
		t.Fatalf("install passes %v, want exactly the failed one and its retry", got)
	}
	if s := r.Stats(); s.FeedAccepted != 1 || !r.Ready() {
		t.Fatalf("FeedAccepted = %d, ready %v; want 1 and ready after the retry", s.FeedAccepted, r.Ready())
	}
}

// TestRoleRetriesTheNewestOfferedUpdateAfterAPartialPass (Plan S3-C2 review fix
// 245-5): m1 is applied; m2's pass commits owner 0 and fails for owner 2, whose
// repair stays outstanding (NeedsPass). The copy watch and the feed retry m2,
// never m1: install.Installer journals each owner on its own, so a pass of m1
// would take owner 0 back (Task 32's TestApplyNeverTakesAnOwnerBackAfterAPartialPass
// shows the parts it would drop). Once a pass of m2 succeeds the feed accepts it.
func TestRoleRetriesTheNewestOfferedUpdateAfterAPartialPass(t *testing.T) {
	h := newHarness(t)
	h.copies.set(copyOf(0, "devuser1.t0", 7), copyOf(2, "devuser1.t2", 9))
	m1 := child(t, h.genesis, 2, 0, 1, 2)
	h.arbiter.publish(m1)
	r := h.role()
	h.start(r)
	waitFor(t, "the first install pass", func() bool { return slices.Equal(h.installer.appliedList(), []string{m1.SnapshotID}) })
	m2 := child(t, m1, 5, 0, 1, 2)
	h.installer.setPartial(m2.SnapshotID, 0)
	h.arbiter.publish(m2)
	waitFor(t, "four passes after m2 was offered", func() bool { return len(h.installer.appliedList()) >= 5 })
	if got := h.installer.appliedList(); slices.Contains(got[1:], m1.SnapshotID) {
		t.Fatalf("passes %v: %s was re-applied after the newer %s was offered", got, m1.SnapshotID, m2.SnapshotID)
	}
	if j, _ := h.installer.Entry(0); j.SnapshotID != m2.SnapshotID {
		t.Fatalf("owner 0 is journaled at %s, want %s: a retry took it back", j.SnapshotID, m2.SnapshotID)
	}
	h.installer.setPartial("", 0)
	waitFor(t, "the feed accepting m2 once its pass succeeds", func() bool { return r.Stats().FeedAccepted == 2 && r.Ready() })
	if got := h.installer.appliedList(); slices.Contains(got[1:], m1.SnapshotID) {
		t.Fatalf("passes %v: %s was re-applied after the newer %s was offered", got, m1.SnapshotID, m2.SnapshotID)
	}
}

func TestRoleReappliesTheLastUpdateWhenCopiesChange(t *testing.T) {
	h := newHarness(t)
	h.copies.set(copyOf(0, "devuser1.t0", 7))
	m := child(t, h.genesis, 2, 0, 1, 2)
	h.arbiter.publish(m)
	r := h.role()
	h.start(r)
	waitFor(t, "the first install pass", func() bool { return len(h.installer.appliedList()) == 1 })
	time.Sleep(1500 * time.Millisecond)
	if got := h.installer.appliedList(); len(got) != 1 {
		t.Fatalf("install passes %v while nothing changed, want one", got)
	}
	h.copies.set(copyOf(0, "devuser1.t0", 7), copyOf(2, "devuser1.t2", 9))
	waitFor(t, "a second pass for the new copy", func() bool { return len(h.installer.appliedList()) == 2 })
	if got := h.installer.appliedList(); got[1] != m.SnapshotID {
		t.Fatalf("passes %v, want the last update re-applied", got)
	}
}

// TestRoleReappliesWhenACopyIsCreatedAgain (Codex review of PR #54,
// copyset reconciler.go:299): a copy whose table vanished and was created
// again keeps its incarnation and name and moves only its generation; the copy
// watch re-applies the last update for it, without a new manifest and without
// the installer reporting NeedsPass, so the installer compares the new table
// with its journal.
func TestRoleReappliesWhenACopyIsCreatedAgain(t *testing.T) {
	h := newHarness(t)
	c := copyOf(0, "devuser1.t0", 7)
	c.Generation = 1
	h.copies.set(c)
	m := child(t, h.genesis, 2, 0, 1, 2)
	h.arbiter.publish(m)
	r := h.role()
	h.start(r)
	waitFor(t, "the first install pass", func() bool { return len(h.installer.appliedList()) == 1 })
	c.Generation = 2
	h.copies.set(c)
	waitFor(t, "a second pass for the recreated copy", func() bool { return len(h.installer.appliedList()) == 2 })
	if got := h.installer.appliedList(); got[1] != m.SnapshotID {
		t.Fatalf("passes %v, want the last update re-applied", got)
	}
}

func TestRoleResumesFromTheJournaledManifest(t *testing.T) {
	h := newHarness(t)
	m := child(t, h.genesis, 2, 0, 1, 2)
	h.arbiter.publish(m)
	for _, id := range []uint64{0, 2} {
		h.installer.entries[id] = install.OwnerJournal{SnapshotID: m.SnapshotID, Entry: replay.TrackFrontier{IndexerID: id, SafeBlockSeq: 2, AppliedPromotionSeq: 2}}
	}
	h.copies.set(copyOf(0, "devuser1.t0", 7))
	r := h.role()
	h.start(r)
	waitFor(t, "readiness from the journal", r.Ready)
	// The copy watch re-applies the journaled manifest once (Apply is
	// idempotent), and the feed, already at the watermark, offers nothing.
	waitFor(t, "one pass of the journaled manifest", func() bool { return slices.Equal(h.installer.appliedList(), []string{m.SnapshotID}) })
	time.Sleep(1500 * time.Millisecond)
	if got := h.installer.appliedList(); len(got) != 1 {
		t.Fatalf("install passes %v, want only the journaled manifest's", got)
	}
	if s := r.Stats(); s.FeedFullFetches != 0 || s.FeedAccepted != 0 {
		t.Fatalf("feed counters after a resume: accepted %d full fetches %d", s.FeedAccepted, s.FeedFullFetches)
	}
	next := child(t, m, 5, 0, 1, 2)
	h.arbiter.publish(next)
	waitFor(t, "the next publication, walked from the journal", func() bool { return slices.Contains(h.installer.appliedList(), next.SnapshotID) })
	if s := r.Stats(); s.FeedFullFetches != 0 || s.FeedAccepted != 1 {
		t.Fatalf("feed counters after the walk: accepted %d full fetches %d, want 1 and 0", s.FeedAccepted, s.FeedFullFetches)
	}
}

func TestRoleCountsRefusedManifests(t *testing.T) {
	h := newHarness(t)
	h.arbiter.publish(child(t, h.genesis, 1))
	r := h.role()
	h.start(r)
	waitFor(t, "a refused manifest", func() bool { return r.Stats().FeedRefused > 0 })
	if got := h.installer.appliedList(); len(got) != 0 {
		t.Fatalf("installed from a refused manifest: %v", got)
	}
}

func TestRoleAuditsAfterAClickHouseRestart(t *testing.T) {
	h := newHarness(t)
	r := h.role()
	h.start(r)
	waitFor(t, "the restart watcher", func() bool { return h.steps.has("watch restarts every 1s") })
	h.restarts <- struct{}{}
	waitFor(t, "the restart audit", func() bool { return slices.Equal(h.installer.auditList(), []string{"startup", "clickhouse_restart"}) })
}
