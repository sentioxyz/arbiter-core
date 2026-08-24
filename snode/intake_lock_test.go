package snode

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type observedDoneContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func newObservedDoneContext(ctx context.Context) *observedDoneContext {
	return &observedDoneContext{Context: ctx, observed: make(chan struct{})}
}

func (c *observedDoneContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

type canceledAfterAcquireContext struct {
	context.Context
	done       chan struct{}
	cancelOnce sync.Once
	errCalls   atomic.Int32
}

func newCanceledAfterAcquireContext() *canceledAfterAcquireContext {
	return &canceledAfterAcquireContext{
		Context: context.Background(),
		done:    make(chan struct{}),
	}
}

func (c *canceledAfterAcquireContext) Done() <-chan struct{} { return c.done }

func (c *canceledAfterAcquireContext) Err() error {
	if c.errCalls.Add(1) == 1 {
		return nil
	}
	c.cancelOnce.Do(func() { close(c.done) })
	return context.Canceled
}

func TestContextMutex_ReturnsTokenWhenCancellationWinsAfterAcquire(t *testing.T) {
	var mu contextMutex
	ctx := newCanceledAfterAcquireContext()
	done := ctx.Done()
	if done == nil || done != ctx.Done() {
		t.Fatal("cancellable test context must return one stable, non-nil Done channel")
	}
	if err := mu.LockContext(ctx); !errors.Is(err, context.Canceled) {
		if err == nil {
			mu.Unlock()
		}
		t.Fatalf("LockContext error = %v, want context canceled after token acquisition", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("test context Done channel remained open after Err returned context canceled")
	}

	acquired := make(chan error, 1)
	go func() { acquired <- mu.LockContext(context.Background()) }()
	select {
	case err := <-acquired:
		if err != nil {
			t.Fatalf("next LockContext after canceled acquisition: %v", err)
		}
		mu.Unlock()
	case <-time.After(2 * time.Second):
		t.Fatal("canceled acquisition did not return the mutex token")
	}
}

func TestIntakeAPIs_CanceledWaitersReturnWithoutSideEffects(t *testing.T) {
	server := &snodeFakeServer{}
	role := newPrepareTestRole(t, server)
	before, err := role.journal.list()
	if err != nil {
		t.Fatalf("list journal before canceled calls: %v", err)
	}

	payload := []byte("not reached because intake acquisition is canceled")
	req := stagedRequest(payload)
	calls := []struct {
		name string
		call func(context.Context) error
	}{
		{
			name: "PrepareLocalStatement",
			call: func(ctx context.Context) error {
				_, err := role.PrepareLocalStatement(ctx, req, payload)
				return err
			},
		},
		{
			name: "LookupPreparedStatement",
			call: func(ctx context.Context) error {
				_, _, err := role.LookupPreparedStatement(ctx, req.Envelope.StatementID.Flat())
				return err
			},
		},
		{
			name: "RegisterPreparedClaim",
			call: func(ctx context.Context) error {
				_, err := role.RegisterPreparedClaim(ctx, req.Envelope.StatementID.Flat())
				return err
			},
		},
		{
			name: "AbortPreparedStatement",
			call: func(ctx context.Context) error {
				return role.AbortPreparedStatement(ctx, req.Envelope.StatementID.Flat(), nil, "canceled")
			},
		},
	}

	role.intakeMu.Lock()
	locked := true
	defer func() {
		if locked {
			role.intakeMu.Unlock()
		}
	}()
	type waiter struct {
		name     string
		ctx      *observedDoneContext
		cancel   context.CancelFunc
		finished chan error
	}
	waiters := make([]waiter, 0, len(calls))
	for _, tc := range calls {
		baseCtx, cancel := context.WithCancel(context.Background())
		observedCtx := newObservedDoneContext(baseCtx)
		finished := make(chan error, 1)
		go func() { finished <- tc.call(observedCtx) }()
		waiters = append(waiters, waiter{name: tc.name, ctx: observedCtx, cancel: cancel, finished: finished})
	}

	for _, w := range waiters {
		select {
		case <-w.ctx.observed:
		case <-time.After(2 * time.Second):
			role.intakeMu.Unlock()
			locked = false
			for _, pending := range waiters {
				pending.cancel()
				<-pending.finished
			}
			t.Fatalf("%s never reached its cancellable intake wait", w.name)
		}
	}
	for _, w := range waiters {
		w.cancel()
	}
	for _, w := range waiters {
		select {
		case err := <-w.finished:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s error = %v, want context canceled", w.name, err)
			}
		case <-time.After(2 * time.Second):
			t.Errorf("%s did not return after cancellation", w.name)
		}
	}
	role.intakeMu.Unlock()
	locked = false

	after, err := role.journal.list()
	if err != nil {
		t.Fatalf("list journal after canceled calls: %v", err)
	}
	if len(after) != len(before) {
		t.Fatalf("canceled intake waiters changed journal records: before=%d after=%d", len(before), len(after))
	}
	if regs, active := server.snapshot(); len(regs) != 0 || len(active) != 0 {
		t.Fatalf("canceled intake waiters reached membership side effects: regs=%d active=%d", len(regs), len(active))
	}
	if starts, active := server.subscriptionSnapshot(); starts != 0 || active != 0 {
		t.Fatalf("canceled intake waiters reached subscription side effects: starts=%d active=%d", starts, active)
	}
}
