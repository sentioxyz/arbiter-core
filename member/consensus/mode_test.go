package consensus

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/wire"
)

// modeRegistry is a registry follower whose view the test sets.
type modeRegistry struct {
	mu      sync.Mutex
	snap    wire.TableRegistrySnapshot
	enabled bool
	ready   chan struct{}
}

func newModeRegistry(snap wire.TableRegistrySnapshot) *modeRegistry {
	r := &modeRegistry{snap: snap, enabled: true, ready: make(chan struct{})}
	close(r.ready)
	return r
}

func (r *modeRegistry) View() (wire.TableRegistrySnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snap, r.enabled
}
func (r *modeRegistry) Changed() <-chan struct{} { return make(chan struct{}) }
func (r *modeRegistry) Ready() <-chan struct{}   { return r.ready }

func (r *modeRegistry) set(snap wire.TableRegistrySnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snap = snap
}

// modeTestConn is a ClickHouse connection the mode tests never use: every
// call the mode makes on it goes through a seam.
type modeTestConn struct{ clickhouse.Conn }

type modeTestPayloads struct{}

func (modeTestPayloads) GetPayload(context.Context, string) ([]byte, error) {
	return nil, errors.New("no payloads in this test")
}

// fakeVerifyRunner is the verify role of the mode tests: Register fails with
// each error of registerErrs in turn, then succeeds; Run waits for ctx.
type fakeVerifyRunner struct {
	mu           sync.Mutex
	registerErrs []error
	registers    int
	runs         int
}

func (f *fakeVerifyRunner) Register(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registers++
	if len(f.registerErrs) > 0 {
		err := f.registerErrs[0]
		f.registerErrs = f.registerErrs[1:]
		return err
	}
	return nil
}

func (f *fakeVerifyRunner) Run(ctx context.Context) error {
	f.mu.Lock()
	f.runs++
	f.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeVerifyRunner) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.registers, f.runs
}

// fakeInstall is the installer the checkpoint step reads.
type fakeInstall struct {
	mu      sync.Mutex
	entries map[uint64]install.OwnerJournal
	tables  map[uint64][]install.TableState
	stats   install.Stats
}

func (f *fakeInstall) Entry(owner uint64) (install.OwnerJournal, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	j, ok := f.entries[owner]
	return j, ok
}

func (f *fakeInstall) Tables(owner uint64) []install.TableState {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.tables[owner])
}

func (f *fakeInstall) Stats() install.Stats {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stats
}

// journalAt journals owner at snapshot with every listed table installed.
func (f *fakeInstall) journalAt(owner uint64, snapshot string, entry replay.TrackFrontier, keys ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.entries[owner] = install.OwnerJournal{SnapshotID: snapshot, Entry: entry}
	f.tables[owner] = nil
	for i, k := range keys {
		f.tables[owner] = append(f.tables[owner], install.TableState{Owner: owner, TableKey: k, IncarnationSeq: uint64(i + 1), SnapshotID: snapshot, Entry: entry})
	}
}

// modeHarness is one Mode on a fake registry with every ClickHouse and
// arbiter call replaced, recording what the mode sends.
type modeHarness struct {
	t        *testing.T
	reg      *modeRegistry
	mode     *Mode
	verify   *fakeVerifyRunner
	inst     *fakeInstall
	stateDir string

	mu          sync.Mutex
	local       LocalReplicas
	localPaths  []map[string]string
	ready       []wire.MemberReady
	readyErr    error
	checkpoints []wire.MemberCheckpoint
	cpErr       error
	held        map[uint64]bool
	replicas    map[uint64][]string
	reports     []uint64
	reportErr   error

	// realVerifyRole is the verify-role seam NewMode installed (Task 62's
	// NewVerifyRole), kept for the tests that build the real role.
	realVerifyRole func(Config, VerifyDeps, *Metrics) (verifyRunner, error)
}

func newModeHarness(t *testing.T, snap wire.TableRegistrySnapshot, register bool) *modeHarness {
	t.Helper()
	return newModeHarnessIn(t, t.TempDir(), snap, register)
}

func newModeHarnessIn(t *testing.T, stateDir string, snap wire.TableRegistrySnapshot, register bool) *modeHarness {
	t.Helper()
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	h := &modeHarness{t: t, reg: newModeRegistry(snap), verify: &fakeVerifyRunner{}, stateDir: stateDir,
		inst: &fakeInstall{entries: map[uint64]install.OwnerJournal{}, tables: map[uint64][]install.TableState{}, stats: install.Stats{AuditClean: true}},
		held: map[uint64]bool{}, replicas: map[uint64][]string{}}
	m, err := NewMode(Config{
		NodeID: fixtureNode, NetworkID: fixtureNetwork, GenesisSnapshotID: fixtureGenesis, StateDir: stateDir, StoragePolicy: "default",
		Ed25519Seed: authoritytest.MemberKeyB().Seed(), ReplicaSignerKeyHex: authoritytest.ReplicaKeyHexB, Register: register,
		ReadinessInterval: 10 * time.Millisecond, CheckpointInterval: 10 * time.Millisecond, ResolveInterval: 10 * time.Millisecond,
	}, ModeDeps{Client: client, Registry: h.reg, Conn: modeTestConn{}, Payloads: modeTestPayloads{}, MemberUnsafeDB: "hg_member_unsafe", Owners: []uint64{2, 0}})
	if err != nil {
		t.Fatalf("NewMode: %v", err)
	}
	h.realVerifyRole = m.env.newVerifyRole
	m.env.newVerifyRole = func(Config, VerifyDeps, *Metrics) (verifyRunner, error) { return h.verify, nil }
	m.env.localReplicas = func(_ context.Context, paths map[string]string) (LocalReplicas, error) {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.localPaths = append(h.localPaths, paths)
		return h.local, nil
	}
	m.env.submitReady = func(_ context.Context, cmd wire.MemberReady) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.ready = append(h.ready, cmd)
		return h.readyErr
	}
	m.env.submitCheckpoint = func(_ context.Context, cmd wire.MemberCheckpoint) error {
		h.mu.Lock()
		defer h.mu.Unlock()
		h.checkpoints = append(h.checkpoints, cmd)
		return h.cpErr
	}
	m.env.reporter = h
	m.env.probe = h
	h.mode = m
	return h
}

func (h *modeHarness) SubmitTablePurged(_ context.Context, nodeID string, seq uint64) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if nodeID != fixtureNode {
		h.t.Errorf("purge report for %s, want %s", nodeID, fixtureNode)
	}
	h.reports = append(h.reports, seq)
	return h.reportErr
}

func (h *modeHarness) MemberTableHeld(_ context.Context, inc wire.TableIncarnation) (bool, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.held[inc.Seq], nil
}

func (h *modeHarness) KeeperReplicas(_ context.Context, inc wire.TableIncarnation) ([]string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.replicas[inc.Seq]), nil
}

// prepare does what Run does before its loops, so a test drives the steps
// itself; testBootNonce (Task 62's fixture) stands for member.Role's.
func (h *modeHarness) prepare() {
	h.t.Helper()
	if _, err := h.mode.prepare(modeTestStore{}, testBootNonce); err != nil {
		h.t.Fatalf("prepare: %v", err)
	}
}

func (h *modeHarness) sent() ([]wire.MemberReady, []wire.MemberCheckpoint, []uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.ready), slices.Clone(h.checkpoints), slices.Clone(h.reports)
}

func (h *modeHarness) set(f func(h *modeHarness)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	f(h)
}

type modeTestStore struct{}

func (modeTestStore) GetSafeSnapshot(context.Context, string) (replay.SafeSnapshotManifest, error) {
	return replay.SafeSnapshotManifest{}, errors.New("no manifests in this test")
}

func switchOffError(text string) error {
	return fmt.Errorf("%w: %s", dataplane.ErrStage3SwitchOff, text)
}

// metricValues registers m on a fresh registry and returns every sample by
// name and labels, name{label=value}..., the precedent of member/serve's
// tests.
func metricValues(t *testing.T, m *Metrics) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := m.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := map[string]float64{}
	for _, mf := range families {
		for _, metric := range mf.GetMetric() {
			key := mf.GetName()
			for _, l := range metric.GetLabel() {
				key += "{" + l.GetName() + "=" + l.GetValue() + "}"
			}
			out[key] = metric.GetGauge().GetValue() + metric.GetCounter().GetValue()
		}
	}
	return out
}

// TestReadinessLoopIdlesUntilThePlacementSwitch: before the stage-3
// parameter, with the placement switch off, and before the member registered
// in this process, the readiness step reads no replica and sends nothing;
// then it reports every synced group, signed with the member's ed25519 key,
// and a switch refusal is retried at the next step.
func TestReadinessLoopIdlesUntilThePlacementSwitch(t *testing.T) {
	snap := readinessSnapshot()
	off := snap
	off.Stage3 = nil
	h := newModeHarness(t, off, true)
	h.prepare()
	h.set(func(h *modeHarness) {
		h.local = LocalReplicas{"dex__pools": true, "dex__swaps": true, "nft__mints": true}
	})
	ctx := context.Background()
	h.mode.markRegistered()
	h.mode.readinessStep(ctx)
	placementOff := snap
	placementOff.Stage3 = fixtureStage3("purge")
	h.reg.set(placementOff)
	h.mode.readinessStep(ctx)
	if ready, _, _ := h.sent(); len(ready) != 0 || len(h.localPaths) != 0 {
		t.Fatalf("before the placement switch the step sent %v and read replicas %v", ready, h.localPaths)
	}
	if got := metricValues(t, h.mode.metrics)["sentio_node_si_member_ready_groups"]; got != 1 {
		t.Fatalf("ready_groups = %v, want 1 (owner 2's lp group)", got)
	}

	unregistered := newModeHarness(t, snap, true)
	unregistered.prepare()
	unregistered.mode.readinessStep(ctx)
	if ready, _, _ := unregistered.sent(); len(ready) != 0 || len(unregistered.localPaths) != 0 {
		t.Fatalf("before the registration the step sent %v and read replicas %v", ready, unregistered.localPaths)
	}

	h.reg.set(snap)
	h.mode.readinessStep(ctx)
	ready, _, _ := h.sent()
	if len(ready) != 1 || len(h.localPaths) != 1 || !reflect.DeepEqual(h.localPaths[0], ReplicaPaths(snap, fixtureNode, 0)) {
		t.Fatalf("after the switch the step sent %v and read %v; want one MemberReady over ReplicaPaths", ready, h.localPaths)
	}
	cmd := ready[0]
	if cmd.NodeID != fixtureNode || cmd.OwnerIndexerID != 0 || cmd.DatabaseID != "dex" || cmd.Generation != 3 {
		t.Fatalf("MemberReady = %+v, want member-b, owner 0, dex, generation 3", cmd)
	}
	pub := authoritytest.MemberKeyB().Public().(ed25519.PublicKey)
	if err := authority.VerifyMemberReady(pub, fixtureContext(), cmd.Body(), cmd.Ed25519Signature); err != nil {
		t.Fatalf("the MemberReady signature does not verify under the member key: %v", err)
	}

	h.set(func(h *modeHarness) { h.readyErr = switchOffError(placementSwitchOffText) })
	h.mode.readinessStep(ctx)
	h.mode.readinessStep(ctx)
	if ready, _, _ := h.sent(); len(ready) != 3 {
		t.Fatalf("a switch refusal must be retried at every step; sent %d MemberReady, want 3", len(ready))
	}
}

// TestModeRegistersBeforeItReportsAndRetriesARefusedRegistration: with
// Register the mode registers through the verify role, retrying a refusal,
// before it runs the role and reports anything; without Register it runs the
// role at once, registers nothing and never reports readiness.
func TestModeRegistersBeforeItReportsAndRetriesARefusedRegistration(t *testing.T) {
	h := newModeHarness(t, readinessSnapshot(), true)
	h.verify.registerErrs = []error{errors.New("arbiter unavailable")}
	h.set(func(h *modeHarness) {
		h.local = LocalReplicas{"dex__pools": true, "dex__swaps": true, "nft__mints": true}
	})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.mode.Run(ctx, modeTestStore{}, h.inst, testBootNonce) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if ready, _, _ := h.sent(); len(ready) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the mode never reported readiness after registering")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if registers, runs := h.verify.counts(); registers != 2 || runs != 1 {
		t.Fatalf("Register called %d times and Run %d times; want 2 (one refused) and 1", registers, runs)
	}
	if got := metricValues(t, h.mode.metrics)["sentio_node_si_member_consensus_registered"]; got != 1 {
		t.Fatalf("consensus_registered = %v, want 1", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run = %v, want context.Canceled", err)
	}

	passive := newModeHarness(t, readinessSnapshot(), false)
	passive.set(func(h *modeHarness) {
		h.local = LocalReplicas{"dex__pools": true, "dex__swaps": true, "nft__mints": true}
	})
	ctx, cancel = context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := passive.mode.Run(ctx, modeTestStore{}, passive.inst, testBootNonce); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run = %v, want the deadline", err)
	}
	if registers, runs := passive.verify.counts(); registers != 0 || runs != 1 {
		t.Fatalf("without Register: Register called %d times and Run %d times; want 0 and 1", registers, runs)
	}
	if ready, _, _ := passive.sent(); len(ready) != 0 || metricValues(t, passive.mode.metrics)["sentio_node_si_member_consensus_registered"] != 0 {
		t.Fatalf("without Register the mode sent %v", ready)
	}
}

// TestModeBuildsTheRealVerifyRoleWithTheRolesBootNonce: prepare and Run hand
// Task 62's real verify-role constructor the boot nonce member.Role drew, so a
// member with Config.Consensus builds its verify role and runs with Register
// off; a nonce that is not install.NewBootNonce's, the empty one included,
// stops prepare with the constructor's refusal.
func TestModeBuildsTheRealVerifyRoleWithTheRolesBootNonce(t *testing.T) {
	h := newModeHarness(t, readinessSnapshot(), false)
	var nonces []string
	h.mode.env.newVerifyRole = func(cfg Config, d VerifyDeps, metrics *Metrics) (verifyRunner, error) {
		nonces = append(nonces, d.BootNonce)
		return h.realVerifyRole(cfg, d, metrics)
	}
	for _, bad := range []string{"", "nonce"} {
		want := fmt.Sprintf("member consensus: verify role: member consensus: boot nonce %q must be 8 lowercase hex characters (install.NewBootNonce)", bad)
		if _, err := h.mode.prepare(modeTestStore{}, bad); err == nil || err.Error() != want {
			t.Fatalf("prepare with boot nonce %q = %v, want %q", bad, err, want)
		}
	}
	vr, err := h.mode.prepare(modeTestStore{}, testBootNonce)
	if err != nil {
		t.Fatalf("prepare with the role's boot nonce: %v", err)
	}
	if _, ok := vr.(*VerifyRole); !ok {
		t.Fatalf("prepare built %T, want Task 62's *VerifyRole", vr)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := h.mode.Run(ctx, modeTestStore{}, h.inst, testBootNonce); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Run with the real verify role = %v, want the deadline", err)
	}
	if !slices.Equal(nonces, []string{"", "nonce", testBootNonce, testBootNonce}) {
		t.Fatalf("the verify role was built with boot nonces %q, want each one prepare and Run were given", nonces)
	}
}

// TestCheckpointLoopIdlesUntilTheCheckpointsSwitch: nothing is signed before
// the switch or the registration; then one checkpoint per journaled owner,
// verifiable under the replica_signer, and none while its content is what
// the arbiter holds (this process's last accepted one, or after a restart
// the registry's stored one); a new manifest signs the next seq; refusals
// are counted by kind.
func TestCheckpointLoopIdlesUntilTheCheckpointsSwitch(t *testing.T) {
	snap := checkpointSnapshot()
	before := snap
	before.Stage3 = fixtureStage3("purge", "placement")
	h := newModeHarness(t, before, true)
	h.prepare()
	h.inst.journalAt(0, "0xsnap", fixtureEntry(0, 90, 40), "dex.pools", "nft.mints")
	ctx := context.Background()
	h.mode.markRegistered()
	h.mode.checkpointStep(ctx, h.inst)
	h.reg.set(snap)
	unregistered := newModeHarness(t, snap, true)
	unregistered.prepare()
	unregistered.inst.journalAt(0, "0xsnap", fixtureEntry(0, 90, 40), "dex.pools", "nft.mints")
	unregistered.mode.checkpointStep(ctx, unregistered.inst)
	if _, cps, _ := h.sent(); len(cps) != 0 {
		t.Fatalf("before the checkpoints switch the step sent %v", cps)
	}
	if _, cps, _ := unregistered.sent(); len(cps) != 0 {
		t.Fatalf("before the registration the step sent %v", cps)
	}

	h.mode.checkpointStep(ctx, h.inst)
	_, cps, _ := h.sent()
	if len(cps) != 1 {
		t.Fatalf("after the switch the step sent %d checkpoints, want 1 (owner 2 has no journal)", len(cps))
	}
	first := cps[0]
	if first.Checkpoint.CheckpointSeq != 1 || first.Checkpoint.OwnerIndexerID != 0 || len(first.Checkpoint.Groups) != 2 {
		t.Fatalf("checkpoint = %+v, want seq 1 of owner 0 over dex and nft", first.Checkpoint)
	}
	if err := authority.VerifyMemberCheckpoint(fixtureContext(), first.Checkpoint, first.ReplicaJWS, authoritytest.ReplicaAddrB); err != nil {
		t.Fatalf("the checkpoint does not verify under the replica_signer: %v", err)
	}
	if got := metricValues(t, h.mode.metrics)["sentio_node_si_member_checkpoint_seq{owner=0}"]; got != 1 {
		t.Fatalf("checkpoint_seq{owner=0} = %v, want 1", got)
	}
	h.mode.checkpointStep(ctx, h.inst)
	if _, cps, _ := h.sent(); len(cps) != 1 {
		t.Fatalf("an unchanged plan was signed again: %d checkpoints", len(cps))
	}

	h.inst.journalAt(0, "0xsnap2", fixtureEntry(0, 95, 41), "dex.pools", "nft.mints")
	h.set(func(h *modeHarness) { h.cpErr = switchOffError(checkpointsSwitchOffText) })
	h.mode.checkpointStep(ctx, h.inst)
	h.set(func(h *modeHarness) {
		h.cpErr = fmt.Errorf("%w: member checkpoint: snapshot 0xsnap2 is not a published manifest", dataplane.ErrStage3SwitchOff)
	})
	h.mode.checkpointStep(ctx, h.inst)
	h.set(func(h *modeHarness) { h.cpErr = nil })
	h.mode.checkpointStep(ctx, h.inst)
	_, cps, _ = h.sent()
	if len(cps) != 4 || cps[1].Checkpoint.CheckpointSeq != 2 || cps[2].Checkpoint.CheckpointSeq != 3 || cps[3].Checkpoint.CheckpointSeq != 4 ||
		cps[3].Checkpoint.SnapshotID != "0xsnap2" {
		t.Fatalf("checkpoints after the new manifest = %+v, want seqs 2, 3 and 4 at 0xsnap2", cps)
	}
	for result, want := range map[string]float64{CheckpointAccepted: 2, CheckpointSwitchOff: 1, CheckpointRefused: 1} {
		if got := metricValues(t, h.mode.metrics)["sentio_node_si_member_checkpoints_total{owner=0}{result="+result+"}"]; got != want {
			t.Fatalf("checkpoints_total{owner=0,result=%s} = %v, want %v", result, got, want)
		}
	}

	// A restart on the same state dir, with the registry storing the
	// accepted checkpoint for both groups: nothing to sign.
	stored := snap
	stored.Groups = slices.Clone(snap.Groups)
	for i, g := range stored.Groups {
		if g.OwnerIndexerID == 0 && (g.DatabaseID == "dex" || g.DatabaseID == "nft") {
			g.Members = slices.Clone(g.Members)
			g.Members[0].Checkpoint = &wire.MemberCheckpointRecord{Checkpoint: cps[3].Checkpoint, ReplicaJWS: cps[3].ReplicaJWS, Epoch: 2}
			stored.Groups[i] = g
		}
	}
	restarted := newModeHarnessIn(t, h.stateDir, stored, true)
	restarted.prepare()
	restarted.mode.markRegistered()
	restarted.inst.journalAt(0, "0xsnap2", fixtureEntry(0, 95, 41), "dex.pools", "nft.mints")
	restarted.mode.checkpointStep(ctx, restarted.inst)
	if _, cps, _ := restarted.sent(); len(cps) != 0 {
		t.Fatalf("after a restart the registry's stored checkpoint was signed again: %v", cps)
	}
	restarted.inst.journalAt(0, "0xsnap3", fixtureEntry(0, 99, 42), "dex.pools", "nft.mints")
	restarted.mode.checkpointStep(ctx, restarted.inst)
	if _, cps, _ := restarted.sent(); len(cps) != 1 || cps[0].Checkpoint.CheckpointSeq != 5 {
		t.Fatalf("after a restart the next checkpoint = %v, want checkpoint_seq 5 from the journal", cps)
	}
}

// TestMemberThatLostItsStateResumesFromThePublishedCheckpointSeq (spec §17):
// a member whose state directory is gone signs its next checkpoint above the
// checkpoint_seq the registry publishes for it under its replica_signer, so
// Apply's "above the stored one" accepts it; a value published under another
// signer does not count.
func TestMemberThatLostItsStateResumesFromThePublishedCheckpointSeq(t *testing.T) {
	snap := checkpointSnapshot()
	snap.MemberCheckpointSeqs = []wire.MemberCheckpointSeq{
		{NodeID: fixtureNode, OwnerIndexerID: 0, CheckpointSeq: 7, ReplicaSigner: authoritytest.ReplicaAddrB},
		{NodeID: fixtureNode, OwnerIndexerID: 2, CheckpointSeq: 9, ReplicaSigner: otherSigner},
	}
	h := newModeHarness(t, snap, true)
	h.prepare()
	h.mode.markRegistered()
	h.inst.journalAt(0, "0xsnap", fixtureEntry(0, 90, 40), "dex.pools", "nft.mints")
	h.inst.journalAt(2, "0xsnap", fixtureEntry(2, 90, 40), "lp.pairs")
	h.mode.checkpointStep(context.Background(), h.inst)
	_, cps, _ := h.sent()
	seqs := map[uint64]uint64{}
	for _, cp := range cps {
		seqs[cp.Checkpoint.OwnerIndexerID] = cp.Checkpoint.CheckpointSeq
	}
	if !reflect.DeepEqual(seqs, map[uint64]uint64{0: 8, 2: 1}) {
		t.Fatalf("checkpoint_seq per owner = %v, want 8 above owner 0's published 7 and 1 for owner 2 (published under another signer)", seqs)
	}
	if got := journalText(t, h.stateDir); !strings.Contains(got, `"checkpoint_seq":8`) {
		t.Fatalf("the journal does not hold the resumed seq: %s", got)
	}
}

// purgeSnapshot: incarnation 3 of dex.pools is Purging with member-b in its
// purge set; incarnation 5 lists member-b as purged already; incarnation 6
// is Purging without member-b.
func purgeSnapshot() wire.TableRegistrySnapshot {
	pools := fixtureInc(3, "dex", "pools", 0, wire.TableStatusPurging)
	pools.PurgeMembers = []string{"member-a", fixtureNode}
	done := fixtureInc(5, "dex", "done", 0, wire.TableStatusPurging)
	done.PurgeMembers, done.PurgedBy = []string{fixtureNode}, []string{fixtureNode}
	other := fixtureInc(6, "dex", "other", 0, wire.TableStatusPurging)
	other.PurgeMembers = []string{"member-a"}
	return fixtureSnapshot(fixtureStage3("purge"), nil, pools, done, other)
}

// TestMemberReportsAPurgeOnlyAfterTheDropAndQuiescence: a Purging
// incarnation is reported only once its hg_member_unsafe table is gone, its
// own Keeper replica is gone and no unresolved block names it; then exactly
// once, signed with the member key.
func TestMemberReportsAPurgeOnlyAfterTheDropAndQuiescence(t *testing.T) {
	h := newModeHarness(t, purgeSnapshot(), true)
	q, err := OpenQuiescence(h.stateDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := q.Observe(41, []QuiescentTable{{TableID: "dex.pools", IncarnationSeq: 3}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	h.set(func(h *modeHarness) { h.held[3], h.replicas[3] = true, []string{"member-a", fixtureNode} })
	h.mode.purgeStep(ctx)
	h.set(func(h *modeHarness) { h.held[3] = false })
	h.mode.purgeStep(ctx)
	h.set(func(h *modeHarness) { h.replicas[3] = []string{"member-a"} })
	h.mode.purgeStep(ctx)
	if _, _, reports := h.sent(); len(reports) != 0 {
		t.Fatalf("reported %v before the table, the replica and the quiescence hook allowed it", reports)
	}
	// Task 62's Resolve removes the block once its verdict is evaluated; an
	// empty ledger is a missing file.
	if err := os.Remove(filepath.Join(h.stateDir, QuiescenceFile)); err != nil {
		t.Fatal(err)
	}
	h.set(func(h *modeHarness) { h.reportErr = fmt.Errorf("%w: not yet", dataplane.ErrTableNotPurging) })
	h.mode.purgeStep(ctx)
	h.set(func(h *modeHarness) { h.reportErr = nil })
	h.mode.purgeStep(ctx)
	h.mode.purgeStep(ctx)
	if _, _, reports := h.sent(); !reflect.DeepEqual(reports, []uint64{3, 3}) {
		t.Fatalf("reports = %v, want incarnation 3 refused once, then accepted once and never again", reports)
	}
	if got := metricValues(t, h.mode.metrics)["sentio_node_si_member_purge_reports_total"]; got != 1 {
		t.Fatalf("purge_reports_total = %v, want 1", got)
	}
	signed, err := h.mode.signTablePurged(fixtureNode, 3)
	if err != nil {
		t.Fatal(err)
	}
	pub := authoritytest.MemberKeyB().Public().(ed25519.PublicKey)
	if err := authority.VerifyVerifierMessage(pub, authority.VerifierMessageTablePurged, fixtureContext(),
		authority.TablePurgedBody{NodeID: fixtureNode, IncarnationSeq: 3}, signed.Ed25519Signature); err != nil {
		t.Fatalf("the purge report signature does not verify under the member key: %v", err)
	}
}

// TestMemberReportsAtOnceAPurgingIncarnationItNeverHeld: an incarnation the
// member never held (no table, no replica, no observed block) is reported at
// the first step that sees it Purging.
func TestMemberReportsAtOnceAPurgingIncarnationItNeverHeld(t *testing.T) {
	h := newModeHarness(t, purgeSnapshot(), false)
	h.mode.purgeStep(context.Background())
	if _, _, reports := h.sent(); !reflect.DeepEqual(reports, []uint64{3}) {
		t.Fatalf("reports = %v, want incarnation 3 at once", reports)
	}
}

// TestMemberStatusCarriesReadinessAndCheckpointSeq: the status lists every
// group that names the member, current or Leaving, in registry order, with
// its generation, readiness, class and stored checkpoint_seq.
func TestMemberStatusCarriesReadinessAndCheckpointSeq(t *testing.T) {
	snap := checkpointSnapshot()
	dex := slices.IndexFunc(snap.Groups, func(g wire.DatabaseGroup) bool { return g.OwnerIndexerID == 0 && g.DatabaseID == "dex" })
	snap.Groups[dex].Members[0].Checkpoint = &wire.MemberCheckpointRecord{Checkpoint: arbiter.MemberCheckpoint{CheckpointSeq: 7}}
	h := newModeHarness(t, snap, true)
	got := h.mode.Status()
	want := Status{Mode: ModeConsensus, Registered: false, Groups: []GroupStatus{
		{OwnerIndexerID: 0, DatabaseID: "nft", Generation: 2, Ready: true, Class: "behind"},
		{OwnerIndexerID: 0, DatabaseID: "dex", Generation: 3, Ready: true, Class: "caught_up", CheckpointSeq: 7},
		{OwnerIndexerID: 0, DatabaseID: "old", Generation: 5, Class: ClassLeaving},
		{OwnerIndexerID: 2, DatabaseID: "lp", Generation: 4, Ready: true, Class: "caught_up"},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Status = %+v, want %+v", got, want)
	}
	h.mode.markRegistered()
	b, err := json.Marshal(h.mode.Status().Groups[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"owner_indexer_id":0,"database_id":"dex","generation":3,"ready":true,"class":"caught_up","checkpoint_seq":7}` || !h.mode.Status().Registered {
		t.Fatalf("group status JSON = %s, registered %v", b, h.mode.Status().Registered)
	}
	b, err = json.Marshal(OffStatus())
	if err != nil || string(b) != `{"mode":"off","registered":false,"groups":[]}` {
		t.Fatalf("OffStatus JSON = %s, %v", b, err)
	}
}

// TestConsensusMetricsExportEveryLabelIncludingZeros: every series of the
// main process and of the reconciler process exists from the start, with
// every owner, result and kind.
func TestConsensusMetricsExportEveryLabelIncludingZeros(t *testing.T) {
	h := newModeHarness(t, checkpointSnapshot(), true)
	reg := prometheus.NewPedanticRegistry()
	if err := h.mode.Metrics().Register(reg); err != nil {
		t.Fatal(err)
	}
	rec := NewReconcilerMetrics(func() ReconcilerStats { return ReconcilerStats{Replicas: 3, ReplicaAlreadyExists: 1} })
	if err := rec.Register(reg); err != nil {
		t.Fatal(err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]int{}
	for _, f := range families {
		got[f.GetName()] = len(f.GetMetric())
	}
	want := map[string]int{
		"sentio_node_si_member_consensus_registered":         1,
		"sentio_node_si_member_ready_groups":                 1,
		"sentio_node_si_member_checkpoint_seq":               2,
		"sentio_node_si_member_checkpoints_total":            6,
		"sentio_node_si_member_evidence_total":               4,
		"sentio_node_si_member_purge_reports_total":          1,
		"sentio_node_si_member_unsafe_replicas":              1,
		"sentio_node_si_member_replica_already_exists_total": 1,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("series = %v, want %v", got, want)
	}
}

// TestVerifyRoleCountsItsEvidence: a dispatch that returned nil counts as
// submitted, one that failed as refused, one cut short by its context not at
// all, and a role without metrics counts nothing.
func TestVerifyRoleCountsItsEvidence(t *testing.T) {
	m := NewMetrics()
	v := &VerifyRole{metrics: m}
	failed := errors.New("replay verify failed")
	if v.countEvidence(EvidenceReplay, nil) != nil || v.countEvidence(EvidenceScan, failed) != failed ||
		!errors.Is(v.countEvidence(EvidenceReplay, context.Canceled), context.Canceled) {
		t.Fatal("countEvidence must return the dispatch's error unchanged")
	}
	for _, c := range []struct {
		kind, result string
		want         float64
	}{{EvidenceReplay, EvidenceSubmitted, 1}, {EvidenceReplay, EvidenceRefused, 0}, {EvidenceScan, EvidenceSubmitted, 0}, {EvidenceScan, EvidenceRefused, 1}} {
		if got := metricValues(t, m)["sentio_node_si_member_evidence_total{kind="+c.kind+"}{result="+c.result+"}"]; got != c.want {
			t.Fatalf("evidence_total{kind=%s,result=%s} = %v, want %v", c.kind, c.result, got, c.want)
		}
	}
	if err := (&VerifyRole{}).countEvidence(EvidenceScan, nil); err != nil {
		t.Fatal(err)
	}
}

// TestModeThreadsReplayStoragePolicy exercises the production prepare path
// using Task 62's actual verify role rather than the mode's usual runner fake.
func TestModeThreadsReplayStoragePolicy(t *testing.T) {
	h := newModeHarness(t, readinessSnapshot(), false)
	h.mode.env.newVerifyRole = h.realVerifyRole
	for _, policy := range []string{"", "member disk", "member_verify"} {
		h.mode.cfg.StoragePolicy = policy
		vr, err := h.mode.prepare(modeTestStore{}, testBootNonce)
		if policy != "member_verify" {
			if err == nil || !strings.Contains(err.Error(), "materializer storage policy") {
				t.Fatalf("prepare with policy %q = %v, want policy refusal", policy, err)
			}
		} else if err != nil {
			t.Fatalf("prepare with explicit policy: %v", err)
		} else if _, ok := vr.(*VerifyRole); !ok {
			t.Fatalf("prepare built %T, want the real verify role", vr)
		}
	}
}
