package consensus

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The leader's FAILED_PRECONDITION texts before a switch (plan S3-E1 K67).
// dataplane.ErrStage3SwitchOff wraps every leader precondition, the FSM's
// own refusals of a member command included, so the mode reads the switch
// refusal by its text.
const (
	placementSwitchOffText   = "member readiness is refused until the placement switch is on"
	checkpointsSwitchOffText = "member checkpoints are refused until the checkpoints switch is on"
)

// InstallView is what the mode reads of the member's installer
// (*install.Installer): the per-owner journal, the copied tables and the
// audit and disk state.
type InstallView interface {
	Entry(owner uint64) (install.OwnerJournal, bool)
	Tables(owner uint64) []install.TableState
	Stats() install.Stats
}

// ModeDeps are the mode's connections; member.Role passes its own. The mode
// opens and closes none of them.
type ModeDeps struct {
	Client   *dataplane.Client
	Registry dataplane.RegistryView
	// Conn is the member's ClickHouse as si_member.
	Conn clickhouse.Conn
	// Payloads reads DA payloads for the verify role's replays.
	Payloads replay.PayloadStore
	// MemberUnsafeDB is the member's hg_member_unsafe database
	// (ddl.MemberPinned.MemberUnsafeDB).
	MemberUnsafeDB string
	// Owners are the member's configured peers, the owners it checkpoints;
	// each is exported in the checkpoint series from the start.
	Owners []uint64
	Logger *slog.Logger
	Now    func() time.Time
}

// verifyRunner is what the mode uses of *VerifyRole.
type verifyRunner interface {
	Register(ctx context.Context) error
	Run(ctx context.Context) error
}

// modeSeams are the mode's calls on ClickHouse and the arbiter. NewMode
// installs the real ones; the package's tests replace them.
type modeSeams struct {
	newVerifyRole    func(cfg Config, d VerifyDeps, metrics *Metrics) (verifyRunner, error)
	localReplicas    func(ctx context.Context, paths map[string]string) (LocalReplicas, error)
	submitReady      func(ctx context.Context, cmd wire.MemberReady) error
	submitCheckpoint func(ctx context.Context, cmd wire.MemberCheckpoint) error
	reporter         purgeReporter
	probe            purgeProbe
}

// Mode is a member's consensus mode (plan S3-E1 K66): the VERIFIER
// registration and the verify role of Task 62, MemberReady once the
// placement switch is on, signed checkpoints once the checkpoints switch is
// on, and purge reports. member.Role builds it only with
// member.Config.Consensus and runs it beside its other parts.
type Mode struct {
	cfg     Config
	d       ModeDeps
	log     *slog.Logger
	now     func() time.Time
	priv    ed25519.PrivateKey
	msgCtx  authority.MessageContext
	owners  []uint64
	metrics *Metrics
	env     modeSeams

	mu         sync.Mutex
	registered bool
	signer     *CheckpointSigner
	accepted   map[uint64]arbiter.MemberCheckpoint // the last accepted checkpoint per owner
	reported   map[uint64]bool                     // incarnations whose purge report was accepted
}

// NewMode validates cfg and d and fills cfg's zero intervals with their
// defaults. It opens nothing and writes nothing.
func NewMode(cfg Config, d ModeDeps) (*Mode, error) {
	if cfg.NodeID == "" || cfg.NetworkID == "" || cfg.GenesisSnapshotID == "" || cfg.StateDir == "" {
		return nil, errors.New("member consensus: NodeID, NetworkID, GenesisSnapshotID and StateDir are required")
	}
	if len(cfg.Ed25519Seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("member consensus: the ed25519 seed must be %d bytes", ed25519.SeedSize)
	}
	if _, err := authority.NewSignerFromHex(cfg.ReplicaSignerKeyHex); err != nil {
		return nil, errReplicaSignerKey
	}
	if d.Client == nil || d.Registry == nil || d.Conn == nil || d.Payloads == nil {
		return nil, errors.New("member consensus: ModeDeps.Client, Registry, Conn and Payloads are required")
	}
	if d.MemberUnsafeDB == "" {
		return nil, errors.New("member consensus: ModeDeps.MemberUnsafeDB is required")
	}
	if cfg.ReadinessInterval < 0 || cfg.CheckpointInterval < 0 || cfg.ResolveInterval < 0 {
		return nil, errors.New("member consensus: intervals must not be negative")
	}
	if cfg.ReadinessInterval == 0 {
		cfg.ReadinessInterval = DefaultReadinessInterval
	}
	if cfg.CheckpointInterval == 0 {
		cfg.CheckpointInterval = DefaultCheckpointInterval
	}
	if cfg.ResolveInterval == 0 {
		cfg.ResolveInterval = DefaultResolveInterval
	}
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := d.Now
	if now == nil {
		now = time.Now
	}
	owners := slices.Clone(d.Owners)
	slices.Sort(owners)
	m := &Mode{
		cfg: cfg, d: d, log: logger.With("member_consensus", cfg.NodeID), now: now,
		priv:     ed25519.NewKeyFromSeed(cfg.Ed25519Seed),
		msgCtx:   authority.MessageContext{NetworkID: cfg.NetworkID, GenesisSnapshotID: cfg.GenesisSnapshotID},
		owners:   slices.Compact(owners),
		metrics:  NewMetrics(),
		accepted: map[uint64]arbiter.MemberCheckpoint{}, reported: map[uint64]bool{},
	}
	m.metrics.declareOwners(m.owners)
	m.env = modeSeams{
		newVerifyRole: func(cfg Config, d VerifyDeps, metrics *Metrics) (verifyRunner, error) {
			v, err := NewVerifyRole(cfg, d)
			if err != nil {
				return nil, err
			}
			v.metrics = metrics
			return v, nil
		},
		localReplicas: func(ctx context.Context, paths map[string]string) (LocalReplicas, error) {
			return ReadLocalReplicas(ctx, d.Conn, d.MemberUnsafeDB, cfg.NodeID, paths)
		},
		submitReady:      d.Client.SubmitMemberReady,
		submitCheckpoint: d.Client.SubmitMemberCheckpoint,
		reporter:         dataplane.PurgeReporter{Client: d.Client, Sign: m.signTablePurged},
		probe:            chPurgeProbe{conn: d.Conn, memberUnsafeDB: d.MemberUnsafeDB, keeperShardID: cfg.KeeperShardID},
	}
	return m, nil
}

// Metrics are the mode's series, for the si-member process's registry.
func (m *Mode) Metrics() *Metrics { return m.metrics }

// Status is the mode's part of the serving view: whether the member
// registered in this process and its standing in every group the registry
// names it in.
func (m *Mode) Status() Status {
	snap, enabled := m.d.Registry.View()
	if !enabled {
		return Status{Mode: ModeConsensus, Registered: m.isRegistered(), Groups: []GroupStatus{}}
	}
	return statusOf(snap, m.cfg.NodeID, m.isRegistered())
}

// Run runs the mode until ctx ends: the verify role (registering first while
// Config.Register is on, retried every ReadinessInterval until it succeeds),
// the readiness step every ReadinessInterval, the checkpoint step every
// CheckpointInterval and the purge step every ResolveInterval. manifests is
// the member feed's verified manifest store and inst its installer; both
// exist only once member.Role's startup reached step (9). bootNonce is the
// boot nonce member.Role drew in startup step (7) (install.NewBootNonce), which
// the verify role's replay scratch prefix carries (VerifyDeps.BootNonce), so
// the next start's drop of this node's stage tables removes that scratch too.
// Run returns ctx's error, or the verify role's failure. A step's failure is
// logged and retried at its next interval, and a switch refusal never pages.
func (m *Mode) Run(ctx context.Context, manifests replay.SnapshotStore, inst InstallView, bootNonce string) error {
	if manifests == nil || inst == nil {
		return errors.New("member consensus: Run needs the manifest store and the installer")
	}
	vr, err := m.prepare(manifests, bootNonce)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	failed := make(chan error, 1)
	var wg sync.WaitGroup
	wg.Go(func() {
		if err := m.runVerifyRole(runCtx, vr); err != nil && runCtx.Err() == nil {
			failed <- fmt.Errorf("member consensus: verify role: %w", err)
		}
	})
	wg.Go(func() { m.every(runCtx, m.cfg.ReadinessInterval, m.readinessStep) })
	wg.Go(func() {
		m.every(runCtx, m.cfg.CheckpointInterval, func(ctx context.Context) { m.checkpointStep(ctx, inst) })
	})
	wg.Go(func() { m.every(runCtx, m.cfg.ResolveInterval, m.purgeStep) })
	select {
	case <-ctx.Done():
		err = ctx.Err()
	case err = <-failed:
	}
	cancel()
	wg.Wait()
	return err
}

// prepare opens the checkpoint_seq journal, loads the checkpoint signer and
// builds the verify role with the role's boot nonce, which Task 62's
// NewVerifyRole refuses unless it is install.NewBootNonce's shape; it
// registers nothing.
func (m *Mode) prepare(manifests replay.SnapshotStore, bootNonce string) (verifyRunner, error) {
	store, err := OpenCheckpointSeqStore(m.cfg.StateDir)
	if err != nil {
		return nil, err
	}
	signer, err := NewCheckpointSigner(m.cfg.ReplicaSignerKeyHex, m.msgCtx, store)
	if err != nil {
		return nil, err
	}
	vr, err := m.env.newVerifyRole(m.cfg, VerifyDeps{
		Client: m.d.Client, Registry: m.d.Registry, Conn: m.d.Conn, Manifests: manifests, Payloads: m.d.Payloads, Logger: m.log, Now: m.now,
		BootNonce: bootNonce,
	}, m.metrics)
	if err != nil {
		return nil, fmt.Errorf("member consensus: verify role: %w", err)
	}
	m.mu.Lock()
	m.signer = signer
	m.mu.Unlock()
	return vr, nil
}

// every runs step at once and then every interval until ctx ends.
func (m *Mode) every(ctx context.Context, interval time.Duration, step func(context.Context)) {
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		step(ctx)
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
	}
}

// runVerifyRole registers the member as a VERIFIER while Config.Register is
// on (S3-F step 4), retrying every ReadinessInterval until it succeeds, and
// then runs the verify role. Without Register it runs the role at once,
// which subscribes to nothing (Task 62).
func (m *Mode) runVerifyRole(ctx context.Context, vr verifyRunner) error {
	if m.cfg.Register {
		for {
			err := vr.Register(ctx)
			if err == nil {
				break
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			m.log.Warn("member consensus: registration as a verifier failed; retrying", "retry_in", m.cfg.ReadinessInterval, "err", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(m.cfg.ReadinessInterval):
			}
		}
		m.markRegistered()
		m.log.Info("member consensus: registered and marked active as a verifier")
	}
	return vr.Run(ctx)
}

func (m *Mode) markRegistered() {
	m.mu.Lock()
	m.registered = true
	m.mu.Unlock()
	m.metrics.registered.Set(1)
}

func (m *Mode) isRegistered() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.registered
}

// switchOff reports whether err is the leader's refusal before the switch
// whose K67 text is text.
func switchOff(err error, text string) bool {
	return errors.Is(err, dataplane.ErrStage3SwitchOff) && strings.Contains(err.Error(), text)
}

// readinessStep reports readiness once the placement switch is on and the
// member registered in this process: it reads the local replicas of every
// group still waiting for the member, plans the MemberReady bodies and
// submits each, signed with the member's ed25519 key. The ready-groups gauge
// follows the registry.
func (m *Mode) readinessStep(ctx context.Context) {
	snap, enabled := m.d.Registry.View()
	if !enabled {
		return
	}
	m.metrics.readyGroups.Set(float64(readyGroupCount(snap, m.cfg.NodeID)))
	if snap.Stage3 == nil || !snap.Stage3.Placement || !m.isRegistered() {
		return
	}
	paths := ReplicaPaths(snap, m.cfg.NodeID, m.cfg.KeeperShardID)
	if len(paths) == 0 {
		return
	}
	local, err := m.env.localReplicas(ctx, paths)
	if err != nil {
		if ctx.Err() == nil {
			m.log.Warn("member consensus: reading the local hg_member_unsafe replicas failed; the next step retries", "err", err)
		}
		return
	}
	for _, body := range PlanReadiness(snap, m.cfg.NodeID, local) {
		sig, err := authority.SignMemberReady(m.priv, m.msgCtx, body)
		if err != nil {
			m.log.Warn("member consensus: signing a MemberReady failed", "owner", body.OwnerIndexerID, "database", body.DatabaseID, "err", err)
			continue
		}
		cmd := wire.MemberReady{NodeID: body.NodeID, OwnerIndexerID: body.OwnerIndexerID, DatabaseID: body.DatabaseID, Generation: body.Generation, Ed25519Signature: sig}
		switch err := m.env.submitReady(ctx, cmd); {
		case err == nil:
			m.log.Info("member consensus: reported the group's replicas ready", "owner", body.OwnerIndexerID, "database", body.DatabaseID, "generation", body.Generation)
		case switchOff(err, placementSwitchOffText):
			m.log.Info("member consensus: the arbiter refuses readiness until its placement switch is on; retrying", "owner", body.OwnerIndexerID, "database", body.DatabaseID)
		case ctx.Err() != nil:
			return
		default:
			m.log.Warn("member consensus: MemberReady refused; the next step retries", "owner", body.OwnerIndexerID, "database", body.DatabaseID, "generation", body.Generation, "err", err)
		}
	}
}

func (m *Mode) checkpointSigner() *CheckpointSigner {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.signer
}

// checkpointStep signs and submits, once the checkpoints switch is on and
// the member registered in this process, one checkpoint per configured owner
// whose plan differs from the last checkpoint the arbiter accepted for it
// (this process's, or after a restart the one the registry stores for every
// group of the plan). Its checkpoint_seq is above both the journal and the
// value the registry publishes under this signer.
func (m *Mode) checkpointStep(ctx context.Context, inst InstallView) {
	snap, enabled := m.d.Registry.View()
	if !enabled || snap.Stage3 == nil || !snap.Stage3.Checkpoints || !m.isRegistered() {
		return
	}
	signer := m.checkpointSigner()
	if signer == nil {
		return
	}
	stats := inst.Stats()
	for _, owner := range m.owners {
		if ctx.Err() != nil {
			return
		}
		j, ok := inst.Entry(owner)
		if !ok {
			continue
		}
		cp, ok := PlanCheckpoint(snap, m.cfg.NodeID, owner, InstallStateOf(snap, owner, j, inst.Tables(owner), stats))
		if !ok || m.checkpointCurrent(snap, cp) {
			continue
		}
		var published uint64
		if seq, ok := snap.PublishedCheckpointSeq(m.cfg.NodeID, owner); ok && seq.ReplicaSigner == signer.Address() {
			published = seq.CheckpointSeq
		}
		signed, err := signer.Sign(cp, published)
		if err != nil {
			m.log.Warn("member consensus: signing a checkpoint failed", "owner", owner, "err", err)
			continue
		}
		switch err := m.env.submitCheckpoint(ctx, signed); {
		case err == nil:
			m.mu.Lock()
			m.accepted[owner] = signed.Checkpoint
			m.mu.Unlock()
			m.metrics.countCheckpoint(owner, CheckpointAccepted)
			m.metrics.checkpointSeq.WithLabelValues(strconv.FormatUint(owner, 10)).Set(float64(signed.Checkpoint.CheckpointSeq))
			m.log.Info("member consensus: checkpoint accepted", "owner", owner, "checkpoint_seq", signed.Checkpoint.CheckpointSeq, "snapshot_id", cp.SnapshotID)
		case switchOff(err, checkpointsSwitchOffText):
			m.metrics.countCheckpoint(owner, CheckpointSwitchOff)
			m.log.Info("member consensus: the arbiter refuses checkpoints until its checkpoints switch is on; retrying", "owner", owner)
		case ctx.Err() != nil:
			return
		default:
			m.metrics.countCheckpoint(owner, CheckpointRefused)
			m.log.Warn("member consensus: checkpoint refused; the next step signs a new one", "owner", owner, "checkpoint_seq", signed.Checkpoint.CheckpointSeq, "err", err)
		}
	}
}

// checkpointCurrent reports whether the arbiter already holds cp's content
// (everything but checkpoint_seq): this process's last accepted checkpoint
// for the owner, or, after a restart, the stored checkpoint of every group cp
// lists.
func (m *Mode) checkpointCurrent(snap wire.TableRegistrySnapshot, cp arbiter.MemberCheckpoint) bool {
	m.mu.Lock()
	last, ok := m.accepted[cp.OwnerIndexerID]
	m.mu.Unlock()
	if ok && sameCheckpointContent(last, cp) {
		return true
	}
	for _, cg := range cp.Groups {
		g, ok := snap.Group(cp.OwnerIndexerID, cg.DatabaseID)
		if !ok {
			return false
		}
		i := slices.IndexFunc(g.Members, func(mem wire.GroupMember) bool { return mem.NodeID == cp.NodeID })
		if i < 0 || g.Members[i].Checkpoint == nil {
			return false
		}
		stored := g.Members[i].Checkpoint.Checkpoint
		j := slices.IndexFunc(stored.Groups, func(sg arbiter.CheckpointGroup) bool { return sg.DatabaseID == cg.DatabaseID })
		if stored.SnapshotID != cp.SnapshotID || stored.Entry != cp.Entry || j < 0 || !sameCheckpointGroup(stored.Groups[j], cg) {
			return false
		}
	}
	return true
}

func sameCheckpointContent(a, b arbiter.MemberCheckpoint) bool {
	return a.NodeID == b.NodeID && a.OwnerIndexerID == b.OwnerIndexerID && a.SnapshotID == b.SnapshotID && a.Entry == b.Entry &&
		slices.EqualFunc(a.Groups, b.Groups, sameCheckpointGroup)
}

func sameCheckpointGroup(a, b arbiter.CheckpointGroup) bool {
	return a.DatabaseID == b.DatabaseID && a.GroupGeneration == b.GroupGeneration && slices.Equal(a.Excluded, b.Excluded)
}
