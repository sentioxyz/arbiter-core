package member

import (
	"context"
	"errors"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/consensus"
	"github.com/sentioxyz/arbiter-core/member/copyset"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/manifestfeed"
	"github.com/sentioxyz/arbiter-core/member/serve"
	"github.com/sentioxyz/arbiter-core/wire"
)

// fakeMode is the consensus mode of the role tests: Run records what the
// role hands it and waits for its context.
type fakeMode struct {
	metrics *consensus.Metrics

	mu        sync.Mutex
	runs      int
	manifests replay.SnapshotStore
	inst      consensus.InstallView
	bootNonce string
}

func (f *fakeMode) Run(ctx context.Context, manifests replay.SnapshotStore, inst consensus.InstallView, bootNonce string) error {
	f.mu.Lock()
	f.runs++
	f.manifests, f.inst, f.bootNonce = manifests, inst, bootNonce
	f.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeMode) Status() consensus.Status {
	return consensus.Status{Mode: consensus.ModeConsensus, Registered: true, Groups: []consensus.GroupStatus{}}
}

func (f *fakeMode) Metrics() *consensus.Metrics { return f.metrics }

func (f *fakeMode) ran() (int, replay.SnapshotStore, consensus.InstallView, string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.runs, f.manifests, f.inst, f.bootNonce
}

// consensusTestConfig is a valid consensus mode for cfg: its identity, the
// authoritytest member-b keys, Register off.
func consensusTestConfig(cfg Config) *consensus.Config {
	return &consensus.Config{
		NodeID: cfg.NodeID, NetworkID: cfg.NetworkID, GenesisSnapshotID: cfg.GenesisSnapshotID, StateDir: cfg.StateDir, StoragePolicy: cfg.Pinned.StoragePolicy,
		Ed25519Seed: authoritytest.MemberKeyB().Seed(), ReplicaSignerKeyHex: authoritytest.ReplicaKeyHexB,
	}
}

// installerStep is the harness's record of the installer it built, with the
// source names of owners 0 and 2.
func installerStep(sources string) string {
	return "new installer budget 1073741824 ratio 0.8 sources " + sources
}

// TestConsensusConfigNilKeepsTheP2Role (plan S3-E1 K66): without
// Config.Consensus the role builds no mode, exposes no consensus series,
// keeps the P2 source order and the P2 source labels, and its serving view
// reports the mode off.
func TestConsensusConfigNilKeepsTheP2Role(t *testing.T) {
	h := newHarness(t)
	r := h.role()
	if r.mode != nil || r.ConsensusMetrics() != nil {
		t.Fatal("a role without Config.Consensus has a consensus mode")
	}
	h.start(r)
	p2 := installerStep("[[owner_pull peer_unsafe_pull] [owner_pull peer_unsafe_pull]]")
	waitFor(t, "the installer", func() bool { return h.steps.has(p2) })
	if keys := slices.Sorted(maps.Keys(r.Stats().Installs)); !slices.Equal(keys, []string{"owner_pull", "peer_unsafe_pull", "restore"}) {
		t.Fatalf("Stats().Installs keys = %v, want the P2 sources only", keys)
	}

	sr, err := serveTestRole(t, &serve.Config{Enabled: true, Token: "role-test-token"})
	if err != nil {
		t.Fatal(err)
	}
	if got := sr.Serving().Consensus; !reflect.DeepEqual(got, consensus.OffStatus()) {
		t.Fatalf("serving view consensus = %+v, want %+v", got, consensus.OffStatus())
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/serving", nil)
	req.Header.Set("Authorization", "Bearer role-test-token")
	rec := httptest.NewRecorder()
	sr.ServeHandler().ServeHTTP(rec, req)
	if !strings.Contains(rec.Body.String(), `"consensus":{"mode":"off","registered":false,"groups":[]}`) {
		t.Fatalf("GET /v1/serving = %s, want the consensus mode off", rec.Body.String())
	}
}

// TestConsensusConfigWiresTheModeIntoTheRole: with Config.Consensus the role
// refuses a mode of another identity or without payloads, puts the
// hg_member_unsafe hard-link source first, exports its label, hands the mode
// the feed's manifest store, the installer and the boot nonce of startup
// step (7) (the installer's) in startup step (9), exposes the mode's series,
// and serves its status.
func TestConsensusConfigWiresTheModeIntoTheRole(t *testing.T) {
	t.Run("refusals", func(t *testing.T) {
		h := newHarness(t)
		other := consensusTestConfig(h.cfg)
		other.NodeID = "member-c"
		h.cfg.Consensus = other
		client := h.arbiter.client(t)
		if _, err := New(h.cfg, Deps{Client: client, Conn: noConn{}, Registry: h.registry, Payloads: h.payloads}); err == nil ||
			err.Error() != `member: Config.Consensus.NodeID "member-c" differs from the member's "member-b"` {
			t.Fatalf("New with another node id = %v", err)
		}
		h.cfg.Consensus = consensusTestConfig(h.cfg)
		if _, err := New(h.cfg, Deps{Client: client, Conn: noConn{}, Registry: h.registry}); err == nil ||
			err.Error() != "member: the consensus mode's verify role needs Deps.Payloads" {
			t.Fatalf("New without payloads = %v", err)
		}
		h.cfg.Consensus.Ed25519Seed = []byte{1}
		if _, err := New(h.cfg, Deps{Client: client, Conn: noConn{}, Registry: h.registry, Payloads: h.payloads}); err == nil ||
			err.Error() != "member: member consensus: the ed25519 seed must be 32 bytes" {
			t.Fatalf("New with a short seed = %v", err)
		}
	})

	t.Run("wiring", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Consensus = consensusTestConfig(h.cfg)
		r := h.role()
		if r.mode == nil || r.ConsensusMetrics() == nil {
			t.Fatal("a role with Config.Consensus has no mode or no series")
		}
		fake := &fakeMode{metrics: consensus.NewMetrics()}
		r.mode = fake
		var nonceMu sync.Mutex
		installNonce := ""
		newInstaller := r.env.newInstaller
		r.env.newInstaller = func(cfg install.Config, d install.Deps) (installer, error) {
			nonceMu.Lock()
			installNonce = cfg.BootNonce
			nonceMu.Unlock()
			return newInstaller(cfg, d)
		}
		h.start(r)
		waitFor(t, "the consensus mode", func() bool { n, _, _, _ := fake.ran(); return n == 1 })
		_, manifests, inst, nonce := fake.ran()
		if _, ok := manifests.(*manifestfeed.Store); !ok || inst != consensus.InstallView(h.installer) {
			t.Fatalf("the mode ran with manifests %T and installer %T, want the feed's store and the role's installer", manifests, inst)
		}
		nonceMu.Lock()
		want := installNonce
		nonceMu.Unlock()
		if len(nonce) != 8 || nonce != want {
			t.Fatalf("the mode ran with boot nonce %q, want the role's own, the installer's %q", nonce, want)
		}
		if !h.steps.has(installerStep("[[member_unsafe_hardlink owner_pull peer_unsafe_pull] [member_unsafe_hardlink owner_pull peer_unsafe_pull]]")) {
			t.Fatalf("steps %v, want the hard-link source first for every owner", h.steps.list())
		}
		if n, ok := r.Stats().Installs["member_unsafe_hardlink"]; !ok || n != 0 {
			t.Fatalf("Stats().Installs = %v, want member_unsafe_hardlink at 0", r.Stats().Installs)
		}
		if r.ConsensusMetrics() != fake.metrics {
			t.Fatal("ConsensusMetrics is not the mode's")
		}
	})

	t.Run("serving view", func(t *testing.T) {
		client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "arbiter-0", GRPCAddr: "127.0.0.1:1"}}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(client.Close)
		cfg := Config{
			NodeID: "member-b", HostIndexerID: 1, NetworkID: "s3c4-serve-test", GenesisSnapshotID: "0xgenesis", StateDir: t.TempDir(),
			Peers: []Peer{{IndexerID: 0, Collection: "si_owner_0"}}, DiskBudgetBytes: 1 << 30,
			Serve: &serve.Config{Enabled: true, Token: "role-test-token"},
		}
		cfg.Consensus = consensusTestConfig(cfg)
		r, err := New(cfg, Deps{Client: client, Conn: serveTestConn{}, Registry: serveTestRegistry{}, Payloads: noPayloads{}, Logger: slog.New(slog.DiscardHandler)})
		if err != nil {
			t.Fatal(err)
		}
		want := consensus.Status{Mode: consensus.ModeConsensus, Groups: []consensus.GroupStatus{}}
		if got := r.Serving().Consensus; !reflect.DeepEqual(got, want) {
			t.Fatalf("serving view consensus = %+v, want %+v", got, want)
		}
	})
}

// TestConsensusModeAcceptsItsOwnRegistrationAtARestart (plan S3-C2 K29, S3-E1
// Task 63): once the member registered as a VERIFIER its id is in the purge
// node set, and a consensus-mode member restarted with that registration
// present, served by the registry as this host's group member, still starts
// and runs its mode; a P2 member with the same purge node set, a
// consensus-mode id that is an SNode's and one the registry serves as
// another host's group member are refused before any part is built.
func TestConsensusModeAcceptsItsOwnRegistrationAtARestart(t *testing.T) {
	registered := []string{"member-b", "snode-1", "snode-2", "snode-3", "verifier-1"}
	served := func(host uint64) dataplane.RegistryView {
		snap := testRegistry(0, 1, 2)
		snap.Groups = []wire.DatabaseGroup{{OwnerIndexerID: 0, DatabaseID: "dex", Generation: 1,
			Members: []wire.GroupMember{{NodeID: "member-b", JoinedGeneration: 1, HostIndexerID: &host}}}}
		return newStaticRegistry(snap)
	}
	t.Run("registered", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Consensus = consensusTestConfig(h.cfg)
		h.arbiter.setPurge(registered...)
		h.registry = served(h.cfg.HostIndexerID)
		r := h.role()
		fake := &fakeMode{metrics: consensus.NewMetrics()}
		r.mode = fake
		h.start(r)
		waitFor(t, "the consensus mode", func() bool { n, _, _, _ := fake.ran(); return n == 1 })
	})
	refused := func(t *testing.T, h *harness, want string) {
		t.Helper()
		err := h.runToError(h.role())
		if !errors.Is(err, copyset.ErrNodeIDInUse) || err.Error() != want {
			t.Fatalf("Run = %v, want %q wrapping copyset.ErrNodeIDInUse", err, want)
		}
		if got := h.steps.list(); hasPrefix(got, "new ", "drop", "audit") {
			t.Fatalf("a refused start built parts, dropped tables or audited: %v", got)
		}
	}
	t.Run("a P2 member", func(t *testing.T) {
		h := newHarness(t)
		h.arbiter.setPurge(registered...)
		refused(t, h, `copyset: node id "member-b" is already used by the purge node set (a registered SNode or verifier)`)
	})
	t.Run("an SNode's id", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.NodeID = "snode-3"
		h.cfg.Consensus = consensusTestConfig(h.cfg)
		h.arbiter.setPurge(registered...)
		refused(t, h, `copyset: node id "snode-3" is already used by si_indexers entry 2 (its SNode)`)
	})
	t.Run("another host's member", func(t *testing.T) {
		h := newHarness(t)
		h.cfg.Consensus = consensusTestConfig(h.cfg)
		h.arbiter.setPurge(registered...)
		h.registry = served(2)
		refused(t, h, `member: node id "member-b" is served as a member of group 0/dex hosted by indexer 2, not by this host 1: copyset: node id already in use`)
	})
}

func TestConsensusReplayPolicyMustMatchPinnedMemberPolicy(t *testing.T) {
	h := newHarness(t)
	h.cfg.Pinned = ddl.DefaultMemberPinned(h.cfg.NodeID, 0, "member_verify")
	h.cfg.Consensus = consensusTestConfig(h.cfg)
	client := h.arbiter.client(t)
	deps := Deps{Client: client, Conn: noConn{}, Registry: h.registry, Payloads: h.payloads}
	r, err := New(h.cfg, deps)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := r.mode.(*consensus.Mode); !ok {
		t.Fatalf("Role built %T, want actual consensus.Mode", r.mode)
	}
	h.cfg.Consensus.StoragePolicy = "default"
	if _, err := New(h.cfg, deps); err == nil || !strings.Contains(err.Error(), "Config.Consensus.StoragePolicy") {
		t.Fatalf("mismatching replay policy = %v", err)
	}
}
