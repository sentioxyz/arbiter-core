package snode

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// PayloadSpool is the intake's payload-before-write seam.
type PayloadSpool interface {
	Put(ctx context.Context, ref string, payload []byte) error
}

type Deps struct {
	Client   *dataplane.Client
	Conn     clickhouse.Conn
	Payloads PayloadSpool
	Logger   *slog.Logger
}

// contextMutex is a zero-value mutex whose acquisition can be canceled.
type contextMutex struct {
	once  sync.Once
	token chan struct{}
}

func (m *contextMutex) init() {
	m.once.Do(func() {
		m.token = make(chan struct{}, 1)
		m.token <- struct{}{}
	})
}

func (m *contextMutex) Lock() {
	_ = m.LockContext(context.Background())
}

func (m *contextMutex) LockContext(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	m.init()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-m.token:
		if err := ctx.Err(); err != nil {
			m.token <- struct{}{}
			return err
		}
		return nil
	}
}

func (m *contextMutex) Unlock() {
	m.init()
	m.token <- struct{}{}
}

type Role struct {
	cfg              Config
	d                Deps
	state            *stateStore
	journal          *intakeJournal
	authority        *authority.Validator
	intakeMu         contextMutex
	promotionLocksMu sync.Mutex
	promotionLocks   map[string]*sync.Mutex
}

func New(cfg Config, d Deps) (*Role, error) {
	if err := cfg.validate(); err != nil {
		return nil, fmt.Errorf("snode config: %w", err)
	}
	if d.Client == nil {
		return nil, fmt.Errorf("snode: dataplane client is required")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	st, err := openStateStore(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	journal, err := openIntakeJournal(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	return &Role{
		cfg:       cfg,
		d:         d,
		state:     st,
		journal:   journal,
		authority: authorityValidator(cfg.AuthorityAddresses),
	}, nil
}

func (r *Role) Register(ctx context.Context) error {
	if err := r.ensureProtocolTables(ctx); err != nil {
		return err
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).RegisterNode(ctx, &pb.NodeRegistration{
			NodeId: r.cfg.NodeID,
			Roles:  []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE},
		})
		return err
	}); err != nil {
		return fmt.Errorf("register snode: %w", err)
	}
	if err := r.d.Client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).MarkActive(ctx, &pb.NodeRef{NodeId: r.cfg.NodeID})
		return err
	}); err != nil {
		return fmt.Errorf("mark snode active: %w", err)
	}
	return nil
}

// PromotedUnsafeParts returns the hg_unsafe part names of tableID that an
// applied promotion already copied into hg_safe and whose cleanup has not
// yet been journaled. HouseGate's storage-integrity read surface excludes
// them in unsafe_latest mode (rewriter.StorageIntegrityReadState); pass the
// Role straight into housegate.Options.StorageIntegrityReadState.
func (r *Role) PromotedUnsafeParts(tableID string) ([]string, error) {
	return r.state.PromotedUnsafeParts(tableID)
}

// Prepare converges every durable non-terminal intake visible at the time of
// the call. Repeated calls are safe and re-scan the journal; a caller waiting
// behind another intake transition may cancel its acquisition through ctx.
func (r *Role) Prepare(ctx context.Context) error {
	if err := r.convergeStartup(ctx); err != nil {
		return fmt.Errorf("converge staged intake: %w", err)
	}
	return nil
}

// Run preserves the original Role lifecycle without publishing a readiness
// notification. Hosts that gate externally reachable services should use
// RunWithReady instead.
func (r *Role) Run(ctx context.Context) error {
	return r.RunWithReady(ctx, nil)
}

// RunWithReady prepares the Role and launches every configured long-lived
// worker to a local barrier before invoking ready at most once. The callback
// reports local lifecycle ownership only; it does not mean the remote promotion
// stream has registered or acknowledged the subscription. Cancellation can
// race the callback, so hosts with a transactional startup gate must re-check
// their own lifecycle after readiness. A cancellation already observed before
// the callback suppresses it. If ready panics, all barrier workers are canceled
// and joined before the panic continues.
func (r *Role) RunWithReady(ctx context.Context, ready func()) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	if err := r.Prepare(runCtx); err != nil {
		return err
	}
	runSubscription := func(ctx context.Context) error {
		return r.d.Client.RunPromotionSubscription(ctx, r.cfg.NodeID, func(cmd *pb.PromotionCommand) error {
			if cmd == nil {
				return nil
			}
			switch m := cmd.GetCmd().(type) {
			case *pb.PromotionCommand_Promote:
				return r.handlePromote(ctx, m.Promote, cmd.GetAuthorityJws())
			case *pb.PromotionCommand_Cleanup:
				return r.handleCleanup(ctx, m.Cleanup, cmd.GetAuthorityJws())
			default:
				r.d.Logger.Warn("unknown promotion command", "type", fmt.Sprintf("%T", cmd.GetCmd()))
				return nil
			}
		})
	}
	workers := roleWorkers{subscription: runSubscription}
	if r.cfg.ProtocolTables != ddl.ModeOff {
		workers.reconcile = r.reconcileProtocolTables
	}
	return newRoleWorkerCoordinator(runCtx, cancel, ready, workers).run().Err()
}

func (r *Role) pinned() ddl.Pinned {
	return ddl.Pinned{
		UnsafeDB: r.cfg.UnsafeDatabase, SafeDB: r.cfg.SafeDatabase, PromoteDB: r.cfg.PromoteDatabase,
		NodeID: r.cfg.NodeID, KeeperShardID: r.cfg.KeeperShardID,
	}
}

func (r *Role) ensureProtocolTables(ctx context.Context) error {
	return r.ensureProtocolTablesMode(ctx, r.cfg.ProtocolTables)
}

func (r *Role) ensureProtocolTablesMode(ctx context.Context, mode ddl.Mode) error {
	if mode == ddl.ModeOff {
		return nil
	}
	if r.d.Conn == nil {
		return errors.New("snode: clickhouse connection is required to ensure protocol tables")
	}
	if err := ddl.EnsureProtocolTables(ctx, r.d.Conn, r.pinned(), r.cfg.Tables, mode, r.d.Logger); err != nil {
		return fmt.Errorf("snode: ensure protocol tables: %w", err)
	}
	return nil
}

type roleWorkers struct {
	subscription func(context.Context) error
	reconcile    func(context.Context) error
}

type roleWorkerKind uint8

const (
	roleWorkerSubscription roleWorkerKind = iota
	roleWorkerReconcile
)

type roleWorker struct {
	kind       roleWorkerKind
	run        func(context.Context) error
	beforeExit func()
	exited     chan struct{}
}

type roleWorkerResult struct {
	kind roleWorkerKind
	err  error
}

type roleWorkerErrors struct {
	subscription error
	reconcile    error
}

func (e *roleWorkerErrors) add(result roleWorkerResult) {
	switch result.kind {
	case roleWorkerSubscription:
		e.subscription = result.err
	case roleWorkerReconcile:
		e.reconcile = result.err
	}
}

// Err preserves the historical error priority: a non-cancellation reconcile
// failure wins; otherwise the subscription result is authoritative.
func (e roleWorkerErrors) Err() error {
	if e.reconcile != nil && !errors.Is(e.reconcile, context.Canceled) {
		return e.reconcile
	}
	return e.subscription
}

type roleWorkerCoordinator struct {
	ctx      context.Context
	cancel   context.CancelFunc
	ready    func()
	workers  []roleWorker
	launched chan struct{}
	activate chan struct{}
	abort    chan struct{}
	results  chan roleWorkerResult
	workerWG sync.WaitGroup
	decided  bool
	stopped  bool
	joined   bool
}

func newRoleWorkerCoordinator(
	ctx context.Context,
	cancel context.CancelFunc,
	ready func(),
	configured roleWorkers,
) *roleWorkerCoordinator {
	workers := make([]roleWorker, 0, 2)
	if configured.subscription != nil {
		workers = append(workers, roleWorker{
			kind:   roleWorkerSubscription,
			run:    configured.subscription,
			exited: make(chan struct{}),
		})
	}
	if configured.reconcile != nil {
		workers = append(workers, roleWorker{
			kind:   roleWorkerReconcile,
			run:    configured.reconcile,
			exited: make(chan struct{}),
		})
	}
	return &roleWorkerCoordinator{
		ctx:      ctx,
		cancel:   cancel,
		ready:    ready,
		workers:  workers,
		launched: make(chan struct{}, len(workers)),
		activate: make(chan struct{}),
		abort:    make(chan struct{}),
		results:  make(chan roleWorkerResult, len(workers)),
	}
}

// roleWorkerCoordinator uses a launch barrier so readiness is published only after
// all local worker goroutines are owned by this invocation. Workers also honor
// context cancellation while parked at the barrier. The checks around ready
// narrow, but cannot remove, the race between a concurrent cancellation and a
// callback with external effects.
func (c *roleWorkerCoordinator) run() roleWorkerErrors {
	c.launch()
	defer func() {
		if c.joined {
			return
		}
		c.abortWorkers()
		_ = c.join(nil)
	}()

	if c.ctx.Err() != nil {
		c.abortWorkers()
		return c.join(nil)
	}
	if c.ready != nil {
		c.ready()
	}
	if c.ctx.Err() != nil {
		c.abortWorkers()
	} else {
		c.activateWorkers()
	}
	return c.join(nil)
}

func (c *roleWorkerCoordinator) launch() {
	c.workerWG.Add(len(c.workers))
	for _, worker := range c.workers {
		go func() {
			defer c.workerWG.Done()
			defer close(worker.exited)
			c.launched <- struct{}{}
			if c.awaitWorkerActivation() {
				c.results <- roleWorkerResult{kind: worker.kind, err: worker.run(c.ctx)}
			} else {
				c.results <- roleWorkerResult{kind: worker.kind, err: c.ctx.Err()}
			}
			if worker.beforeExit != nil {
				worker.beforeExit()
			}
		}()
	}
	for range c.workers {
		<-c.launched
	}
}

// awaitWorkerActivation observes cancellation while parked, but leaves the
// activate-or-abort decision to the coordinator. This makes its post-ready
// cancellation check authoritative even when both the context and a barrier
// channel become ready before a worker is scheduled.
func (c *roleWorkerCoordinator) awaitWorkerActivation() bool {
	select {
	case <-c.activate:
		return true
	case <-c.abort:
		return false
	case <-c.ctx.Done():
		select {
		case <-c.activate:
			return true
		case <-c.abort:
			return false
		}
	}
}

func (c *roleWorkerCoordinator) stop() {
	if c.stopped {
		return
	}
	c.stopped = true
	c.cancel()
}

func (c *roleWorkerCoordinator) abortWorkers() {
	if c.decided {
		return
	}
	c.decided = true
	c.stop()
	close(c.abort)
}

func (c *roleWorkerCoordinator) activateWorkers() {
	if c.decided {
		return
	}
	c.decided = true
	close(c.activate)
}

func (c *roleWorkerCoordinator) join(first *roleWorkerResult) roleWorkerErrors {
	var errs roleWorkerErrors
	received := 0
	if first != nil {
		errs.add(*first)
		received = 1
		c.stop()
	}
	for received < len(c.workers) {
		result := <-c.results
		errs.add(result)
		if received == 0 {
			c.stop()
		}
		received++
	}
	c.workerWG.Wait()
	c.joined = true
	return errs
}

func (r *Role) reconcileProtocolTables(ctx context.Context) error {
	ticker := time.NewTicker(r.cfg.ProtocolTablesReconcile)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := r.ensureProtocolTablesMode(ctx, ddl.ModeVerifyOnly); err != nil {
				return fmt.Errorf("snode: reconcile protocol tables: %w", err)
			}
		}
	}
}

func authorityValidator(addresses []string) *authority.Validator {
	allow := make(map[string]bool, len(addresses))
	for _, addr := range addresses {
		allow[strings.ToLower(addr)] = true
	}
	return &authority.Validator{AllowedAddresses: allow, MaxTokenAge: time.Minute}
}

func (r *Role) schemaFor(tableID string) (payloadexec.TableSchema, error) {
	for _, t := range r.cfg.Tables {
		if t.TableID == tableID {
			return t, nil
		}
	}
	return payloadexec.TableSchema{}, fmt.Errorf("no schema configured for table %s", tableID)
}

func (r *Role) promotionLock(k partitionKey) *sync.Mutex {
	ks := key(k.Table, k.Partition)
	r.promotionLocksMu.Lock()
	defer r.promotionLocksMu.Unlock()
	if r.promotionLocks == nil {
		r.promotionLocks = map[string]*sync.Mutex{}
	}
	if r.promotionLocks[ks] == nil {
		r.promotionLocks[ks] = &sync.Mutex{}
	}
	return r.promotionLocks[ks]
}
