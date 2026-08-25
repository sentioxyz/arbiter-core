package snode

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
)

func newPrepareTestRole(t *testing.T, server *snodeFakeServer) *Role {
	t.Helper()
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	role, err := New(testConfigS(t), Deps{Client: client})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}
	return role
}

func TestPrepare_PropagatesJournalAndConvergenceErrorsBeforeRunStarts(t *testing.T) {
	t.Run("journal", func(t *testing.T) {
		server := &snodeFakeServer{}
		role := newPrepareTestRole(t, server)
		if err := os.RemoveAll(role.journal.dir); err != nil {
			t.Fatalf("remove journal directory: %v", err)
		}
		if err := os.WriteFile(role.journal.dir, []byte("not a directory"), 0o600); err != nil {
			t.Fatalf("replace journal directory: %v", err)
		}

		err := role.Prepare(t.Context())
		if err == nil || !strings.Contains(err.Error(), "converge staged intake") || !strings.Contains(err.Error(), "list intake journal") {
			t.Fatalf("Prepare error = %v, want wrapped journal-list failure", err)
		}
		var readyCalls atomic.Int32
		if err := role.RunWithReady(t.Context(), func() { readyCalls.Add(1) }); err == nil || !strings.Contains(err.Error(), "list intake journal") {
			t.Fatalf("RunWithReady error = %v, want journal-list failure", err)
		}
		if starts, active := server.subscriptionSnapshot(); starts != 0 || active != 0 {
			t.Fatalf("subscription crossed failed Prepare: starts=%d active=%d", starts, active)
		}
		if got := readyCalls.Load(); got != 0 {
			t.Fatalf("ready calls after failed Prepare = %d, want 0", got)
		}
	})

	t.Run("convergence", func(t *testing.T) {
		server := &snodeFakeServer{}
		role := newPrepareTestRole(t, server)
		rec := testRecord("0xabc:8:convergence")
		rec.Envelope.TargetTableID = "unknown.table"
		if err := role.journal.save(rec); err != nil {
			t.Fatalf("seed intake record: %v", err)
		}

		err := role.Prepare(t.Context())
		if !errors.Is(err, ErrSchemaUnknown) || !strings.Contains(err.Error(), "converge staged intake") || !strings.Contains(err.Error(), "current binding") {
			t.Fatalf("Prepare error = %v, want wrapped convergence failure", err)
		}
		var readyCalls atomic.Int32
		if err := role.RunWithReady(t.Context(), func() { readyCalls.Add(1) }); !errors.Is(err, ErrSchemaUnknown) || !strings.Contains(err.Error(), "current binding") {
			t.Fatalf("RunWithReady error = %v, want convergence failure", err)
		}
		if starts, active := server.subscriptionSnapshot(); starts != 0 || active != 0 {
			t.Fatalf("subscription crossed failed Prepare: starts=%d active=%d", starts, active)
		}
		if got := readyCalls.Load(); got != 0 {
			t.Fatalf("ready calls after failed Prepare = %d, want 0", got)
		}
	})
}

func TestRunWithReady_SignalsOnceAfterLocalWorkerLaunch(t *testing.T) {
	server := &snodeFakeServer{}
	role := newPrepareTestRole(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	var readyCalls atomic.Int32
	var startsAtReady atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- role.RunWithReady(ctx, func() {
			readyCalls.Add(1)
			starts, _ := server.subscriptionSnapshot()
			startsAtReady.Store(int32(starts))
			close(ready)
		})
	}()

	select {
	case <-ready:
	case <-time.After(2 * time.Second):
		t.Fatal("RunWithReady did not signal local readiness")
	}
	if got := readyCalls.Load(); got != 1 {
		t.Fatalf("ready calls = %d, want 1", got)
	}
	if got := startsAtReady.Load(); got != 0 {
		t.Fatalf("remote subscription starts at local ready = %d, want 0", got)
	}
	waitSNodeSubscriptions(t, server, 1, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithReady exit = %v, want context canceled", err)
	}
	if got := readyCalls.Load(); got != 1 {
		t.Fatalf("ready calls after worker exit = %d, want 1", got)
	}
}

func TestRunWithReady_CanceledBeforeWorkerReleaseDoesNotSignal(t *testing.T) {
	server := &snodeFakeServer{}
	role := newPrepareTestRole(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var readyCalls atomic.Int32

	err := role.RunWithReady(ctx, func() { readyCalls.Add(1) })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithReady exit = %v, want context canceled", err)
	}
	if got := readyCalls.Load(); got != 0 {
		t.Fatalf("ready calls after pre-launch cancellation = %d, want 0", got)
	}
	if starts, active := server.subscriptionSnapshot(); starts != 0 || active != 0 {
		t.Fatalf("subscription crossed pre-launch cancellation: starts=%d active=%d", starts, active)
	}
}

func TestRunWithReady_CancelRacingReadyTerminates(t *testing.T) {
	server := &snodeFakeServer{}
	role := newPrepareTestRole(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	var readyCalls atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- role.RunWithReady(ctx, func() {
			readyCalls.Add(1)
			cancel()
		})
	}()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("RunWithReady exit = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RunWithReady did not terminate after cancellation raced ready")
	}
	if got := readyCalls.Load(); got != 1 {
		t.Fatalf("ready calls = %d, want 1", got)
	}
	if starts, active := server.subscriptionSnapshot(); starts != 0 || active != 0 {
		t.Fatalf("remote subscription crossed canceled readiness: starts=%d active=%d", starts, active)
	}
}

func TestRunWithReady_ReadyPanicJoinsLaunchBarrierWorkers(t *testing.T) {
	server := &snodeFakeServer{}
	role := newPrepareTestRole(t, server)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const panicValue = "ready callback panic"
	var recovered any
	func() {
		defer func() { recovered = recover() }()
		_ = role.RunWithReady(ctx, func() { panic(panicValue) })
	}()
	if recovered != panicValue {
		t.Fatalf("recovered panic = %#v, want %q", recovered, panicValue)
	}
	if starts, active := server.subscriptionSnapshot(); starts != 0 || active != 0 {
		t.Fatalf("remote subscription crossed ready panic: starts=%d active=%d", starts, active)
	}
}

func TestRoleWorkerCoordinator_ReadyPanicDirectlyJoinsWorkers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		const panicValue = "coordinator ready panic"
		var workerCalls atomic.Int32
		controlledWorker := func(context.Context) error {
			workerCalls.Add(1)
			return nil
		}
		coordinator := newRoleWorkerCoordinator(ctx, cancel, func() { panic(panicValue) }, roleWorkers{
			subscription: controlledWorker,
			reconcile:    controlledWorker,
		})
		published := make(chan roleWorkerKind, len(coordinator.workers))
		releaseExit := make(chan struct{})
		for i := range coordinator.workers {
			kind := coordinator.workers[i].kind
			coordinator.workers[i].beforeExit = func() {
				published <- kind
				<-releaseExit
			}
		}
		recovered := make(chan any, 1)
		go func() {
			var value any
			func() {
				defer func() { value = recover() }()
				_ = coordinator.run()
			}()
			recovered <- value
		}()

		for range coordinator.workers {
			<-published
		}
		synctest.Wait()
		select {
		case value := <-recovered:
			close(releaseExit)
			synctest.Wait()
			t.Fatalf("ready panic recovered as %#v before held workers exited", value)
		default:
		}
		close(releaseExit)
		synctest.Wait()
		value := <-recovered
		if value != panicValue {
			t.Fatalf("recovered panic = %#v, want %q", value, panicValue)
		}
		if !errors.Is(ctx.Err(), context.Canceled) {
			t.Fatalf("coordinator context error = %v, want context canceled by panic cleanup", ctx.Err())
		}
		requireRoleWorkersJoined(t, coordinator, roleWorkerSubscription, roleWorkerReconcile)
		if got := workerCalls.Load(); got != 0 {
			t.Fatalf("ready panic activated %d configured workers, want 0", got)
		}
	})
}

func TestRoleWorkerCoordinator_PreCanceledContextAbortsBeforeReady(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var readyCalls atomic.Int32
	var workerCalls atomic.Int32
	contextAgnosticWorker := func(context.Context) error {
		workerCalls.Add(1)
		return errors.New("worker crossed pre-canceled barrier")
	}
	coordinator := newRoleWorkerCoordinator(ctx, cancel, func() { readyCalls.Add(1) }, roleWorkers{
		subscription: contextAgnosticWorker,
		reconcile:    contextAgnosticWorker,
	})

	err := coordinator.run().Err()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("coordinator error = %v, want context canceled", err)
	}
	if got := readyCalls.Load(); got != 0 {
		t.Fatalf("pre-canceled coordinator called ready %d times, want 0", got)
	}
	if got := workerCalls.Load(); got != 0 {
		t.Fatalf("pre-canceled coordinator activated %d context-agnostic workers, want 0", got)
	}
	requireRoleWorkersJoined(t, coordinator, roleWorkerSubscription, roleWorkerReconcile)
}

func TestRoleWorkerCoordinator_PostReadyCancellationAbortsBeforeActivation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var workerCalls atomic.Int32
	contextAgnosticWorker := func(context.Context) error {
		workerCalls.Add(1)
		return errors.New("worker crossed canceled ready barrier")
	}
	coordinator := newRoleWorkerCoordinator(ctx, cancel, cancel, roleWorkers{
		subscription: contextAgnosticWorker,
		reconcile:    contextAgnosticWorker,
	})

	err := coordinator.run().Err()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("coordinator error = %v, want context canceled", err)
	}
	if got := workerCalls.Load(); got != 0 {
		t.Fatalf("post-ready cancellation activated %d context-agnostic workers, want 0", got)
	}
	requireRoleWorkersJoined(t, coordinator, roleWorkerSubscription, roleWorkerReconcile)
}

func TestRoleWorkerCoordinator_EarlyNilStopsAndJoinsPeer(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	reconcileStarted := make(chan struct{})
	coordinator := newRoleWorkerCoordinator(ctx, cancel, nil, roleWorkers{
		subscription: func(context.Context) error {
			<-reconcileStarted
			return nil
		},
		reconcile: func(ctx context.Context) error {
			close(reconcileStarted)
			<-ctx.Done()
			return ctx.Err()
		},
	})

	if err := coordinator.run().Err(); err != nil {
		t.Fatalf("coordinator error = %v, want early nil subscription result", err)
	}
	requireRoleWorkersJoined(t, coordinator, roleWorkerSubscription, roleWorkerReconcile)
}

func TestRoleWorkerCoordinator_SubscriptionFirstCauseWinsForRole(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	subscriptionErr := errors.New("subscription failed")
	reconcileArtifact := errors.New("reconcile cancellation artifact")
	reconcileStarted := make(chan struct{})
	coordinator := newRoleWorkerCoordinator(ctx, cancel, nil, roleWorkers{
		subscription: func(context.Context) error {
			<-reconcileStarted
			return subscriptionErr
		},
		reconcile: func(ctx context.Context) error {
			close(reconcileStarted)
			<-ctx.Done()
			return reconcileArtifact
		},
	})

	errs := coordinator.run()
	role := &Role{d: Deps{Logger: slog.Default()}}
	if err := role.resolveWorkerErrors(errs); !errors.Is(err, subscriptionErr) {
		t.Fatalf("role worker error = %v, want original subscription cause", err)
	}
	if err := errs.Err(); !errors.Is(err, reconcileArtifact) {
		t.Fatalf("generic coordinator error = %v, want established reconcile priority", err)
	}
	requireRoleWorkersJoined(t, coordinator, roleWorkerSubscription, roleWorkerReconcile)
}

func TestRoleWorkerCoordinator_ReconcileErrorKeepsLegacyPriority(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	subscriptionErr := errors.New("subscription failed")
	reconcileErr := errors.New("reconcile failed")
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	worker := func(err error) func(context.Context) error {
		return func(context.Context) error {
			started <- struct{}{}
			<-release
			return err
		}
	}
	coordinator := newRoleWorkerCoordinator(ctx, cancel, nil, roleWorkers{
		subscription: worker(subscriptionErr),
		reconcile:    worker(reconcileErr),
	})
	done := make(chan roleWorkerErrors, 1)
	go func() { done <- coordinator.run() }()
	for range 2 {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			t.Fatal("both workers did not reach the simultaneous error barrier")
		}
	}
	close(release)
	var errs roleWorkerErrors
	select {
	case errs = <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not collect simultaneous worker errors")
	}
	if err := errs.Err(); !errors.Is(err, reconcileErr) {
		t.Fatalf("coordinator error = %v, want reconcile priority %v", err, reconcileErr)
	}
	requireRoleWorkersJoined(t, coordinator, roleWorkerSubscription, roleWorkerReconcile)
}

func requireRoleWorkersJoined(t *testing.T, coordinator *roleWorkerCoordinator, kinds ...roleWorkerKind) {
	t.Helper()
	if !coordinator.joined {
		t.Fatal("coordinator returned before synchronously joining workers")
	}
	exited := make(map[roleWorkerKind]<-chan struct{}, len(coordinator.workers))
	for _, worker := range coordinator.workers {
		exited[worker.kind] = worker.exited
	}
	for _, kind := range kinds {
		workerExited, ok := exited[kind]
		if !ok {
			t.Fatalf("coordinator did not configure worker kind %d", kind)
		}
		select {
		case <-workerExited:
		default:
			t.Fatalf("coordinator returned before worker kind %d exited", kind)
		}
	}
}

func TestPrepare_RechecksJournalAcrossCallsAndRuns(t *testing.T) {
	server := &snodeFakeServer{}
	role := newPrepareTestRole(t, server)

	const callers = 32
	start := make(chan struct{})
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			errs <- role.Prepare(context.Background())
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Prepare: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()
	waitSNodeSubscriptions(t, server, 1, 1)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("Run exit = %v, want context canceled", err)
	}
	waitSNodeSubscriptions(t, server, 1, 0)

	rec := testRecord("0xabc:9:later-run")
	rec.Envelope.TargetTableID = "unknown.table"
	if err := role.journal.save(rec); err != nil {
		t.Fatalf("seed later intake record: %v", err)
	}
	if err := role.Prepare(t.Context()); !errors.Is(err, ErrSchemaUnknown) || !strings.Contains(err.Error(), "current binding") {
		t.Fatalf("repeat Prepare error = %v, want new convergence failure", err)
	}
	var readyCalls atomic.Int32
	if err := role.RunWithReady(t.Context(), func() { readyCalls.Add(1) }); !errors.Is(err, ErrSchemaUnknown) || !strings.Contains(err.Error(), "current binding") {
		t.Fatalf("second RunWithReady error = %v, want new convergence failure", err)
	}
	if got := readyCalls.Load(); got != 0 {
		t.Fatalf("ready calls after later convergence failure = %d, want 0", got)
	}
	if starts, active := server.subscriptionSnapshot(); starts != 1 || active != 0 {
		t.Fatalf("second run crossed failed convergence: starts=%d active=%d", starts, active)
	}
}

type blockingPrepareConn struct {
	clickhouse.Conn
	mu          sync.Mutex
	entries     int
	active      int
	maxActive   int
	entryEvents chan int
	release     chan struct{}
	releaseOnce sync.Once
}

func newBlockingPrepareConn() *blockingPrepareConn {
	return &blockingPrepareConn{entryEvents: make(chan int, 4), release: make(chan struct{})}
}

func (c *blockingPrepareConn) Query(ctx context.Context, _ string, _ ...any) (driver.Rows, error) {
	c.mu.Lock()
	c.entries++
	c.active++
	if c.active > c.maxActive {
		c.maxActive = c.active
	}
	entry := c.entries
	c.mu.Unlock()
	c.entryEvents <- entry
	defer func() {
		c.mu.Lock()
		c.active--
		c.mu.Unlock()
	}()
	select {
	case <-c.release:
		return nil, errors.New("release blocked startup convergence")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *blockingPrepareConn) unblock() {
	c.releaseOnce.Do(func() { close(c.release) })
}

func (c *blockingPrepareConn) stats() (entries, maxActive int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.entries, c.maxActive
}

func (c *blockingPrepareConn) waitEntry(t *testing.T, want int) {
	t.Helper()
	select {
	case got := <-c.entryEvents:
		if got != want {
			t.Fatalf("Query entry = %d, want %d", got, want)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Query entry %d did not occur", want)
	}
}

func TestPrepare_SecondCallerCanCancelWhileConvergenceIsSerialized(t *testing.T) {
	role := newPrepareTestRole(t, &snodeFakeServer{})
	probe := newBlockingPrepareConn()
	role.d.Conn = probe
	rec := testRecord("0xabc:10:blocking")
	rec.Envelope.TargetTableID = role.cfg.Tables[0].TableID
	rec.Envelope.SchemaHash = payloadexec.TableSchemaHash(role.cfg.NetworkID, role.cfg.Tables[0])
	if err := role.journal.save(rec); err != nil {
		t.Fatalf("seed intake record: %v", err)
	}

	firstDone := make(chan error, 1)
	go func() { firstDone <- role.Prepare(context.Background()) }()
	probe.waitEntry(t, 1)

	baseCtx, cancel := context.WithCancel(context.Background())
	observedCtx := newObservedDoneContext(baseCtx)
	secondDone := make(chan error, 1)
	go func() { secondDone <- role.Prepare(observedCtx) }()
	select {
	case <-observedCtx.observed:
	case <-time.After(2 * time.Second):
		probe.unblock()
		<-firstDone
		t.Fatal("second Prepare never reached the cancellable convergence wait")
	}
	cancel()
	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("second Prepare error = %v, want context canceled", err)
		}
	case <-time.After(2 * time.Second):
		probe.unblock()
		<-firstDone
		t.Fatal("second Prepare ignored cancellation while waiting for convergence")
	}
	if entries, maxActive := probe.stats(); entries != 1 || maxActive != 1 {
		t.Fatalf("canceled waiter entered Query: entries=%d max_concurrent=%d, want 1/1", entries, maxActive)
	}

	thirdCtx := newObservedDoneContext(context.Background())
	thirdDone := make(chan error, 1)
	go func() { thirdDone <- role.Prepare(thirdCtx) }()
	select {
	case <-thirdCtx.observed:
	case <-time.After(2 * time.Second):
		probe.unblock()
		<-firstDone
		t.Fatal("third Prepare never reached serialized convergence wait")
	}
	if entries, maxActive := probe.stats(); entries != 1 || maxActive != 1 {
		t.Fatalf("third waiter entered before first released: entries=%d max_concurrent=%d, want 1/1", entries, maxActive)
	}

	probe.unblock()
	if err := <-firstDone; err == nil || !strings.Contains(err.Error(), "release blocked startup convergence") {
		t.Fatalf("first Prepare error = %v, want injected convergence release", err)
	}
	probe.waitEntry(t, 2)
	if err := <-thirdDone; err == nil || !strings.Contains(err.Error(), "release blocked startup convergence") {
		t.Fatalf("third Prepare error = %v, want post-acquisition convergence release", err)
	}
	if entries, maxActive := probe.stats(); entries != 2 || maxActive != 1 {
		t.Fatalf("serialized Query stats = entries %d max_concurrent %d, want 2/1", entries, maxActive)
	}
}
