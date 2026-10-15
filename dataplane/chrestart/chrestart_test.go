package chrestart

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var errServerDown = errors.New("dial tcp 127.0.0.1:9000: connect: connection refused")

// fakeServer is a clickhouse.Conn whose only query is uptime(), answered from a
// start time the test sets (inside a synctest bubble time is synthetic). Any
// other method is unimplemented (the embedded nil Conn panics).
type fakeServer struct {
	clickhouse.Conn
	mu      sync.Mutex
	started time.Time
	down    bool
	queries []string
}

func (s *fakeServer) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries = append(s.queries, query)
	if s.down {
		return uptimeRow{err: errServerDown}
	}
	return uptimeRow{uptime: uint64(time.Since(s.started) / time.Second)}
}

func (s *fakeServer) setStarted(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.started = at
}

func (s *fakeServer) startedAt() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.started
}

func (s *fakeServer) setDown(down bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.down = down
}

func (s *fakeServer) probes() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.queries...)
}

type uptimeRow struct {
	uptime uint64
	err    error
}

func (r uptimeRow) Err() error { return r.err }

func (r uptimeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != 1 {
		return fmt.Errorf("scan into %d destinations, want 1", len(dest))
	}
	p, ok := dest[0].(*uint64)
	if !ok {
		return fmt.Errorf("scan into %T, want *uint64", dest[0])
	}
	*p = r.uptime
	return nil
}

func (r uptimeRow) ScanStruct(any) error { return errors.New("uptimeRow: ScanStruct is not supported") }

// restarts records every OnRestart call.
type restarts struct {
	mu   sync.Mutex
	seen [][2]time.Time
}

func (r *restarts) record(_ context.Context, previous, current time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, [2]time.Time{previous, current})
}

func (r *restarts) all() [][2]time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([][2]time.Time(nil), r.seen...)
}

// run runs w on conn in the background; stop cancels it and waits for Run to
// return.
func run(w *Watcher, conn clickhouse.Conn) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		w.Run(ctx, conn)
	}()
	return func() {
		cancel()
		<-done
	}
}

// TestStartedAtIsNowMinusUptime: the start time is the caller's clock minus
// uptime(), from one query; a failed probe is an error, never a time.
func TestStartedAtIsNowMinusUptime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &fakeServer{started: time.Now().Add(-42 * time.Second)}
		got, err := StartedAt(context.Background(), server, time.Now())
		if err != nil || !got.Equal(server.startedAt()) {
			t.Fatalf("StartedAt = %v, %v; want %v", got, err, server.startedAt())
		}
		if q := server.probes(); len(q) != 1 || q[0] != "SELECT toUInt64(uptime())" {
			t.Fatalf("queries = %q, want the one uptime() probe", q)
		}
		server.setDown(true)
		if got, err := StartedAt(context.Background(), server, time.Now()); !errors.Is(err, errServerDown) || !got.IsZero() {
			t.Fatalf("StartedAt of a down server = %v, %v; want the probe error", got, err)
		}
	})
}

// TestWatcherReportsARestart: a start time that moves later by more than the
// skew is a restart, reported once with both start times (housegate spec
// 2026-10-14 §14: the audit reruns after every ClickHouse restart).
func TestWatcherReportsARestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &fakeServer{started: time.Now().Add(-time.Hour)}
		var log restarts
		stop := run(&Watcher{Interval: time.Second, OnRestart: log.record}, server)
		defer stop()
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if got := log.all(); len(got) != 0 {
			t.Fatalf("restarts while the server ran = %v", got)
		}
		before := server.startedAt()
		server.setStarted(time.Now())
		after := server.startedAt()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		got := log.all()
		if len(got) != 1 || !got[0][0].Equal(before) || !got[0][1].Equal(after) {
			t.Fatalf("restarts = %v, want one from %v to %v", got, before, after)
		}
	})
}

// TestWatcherDetectsARestartEvenWhenTheNewUptimeIsLarger: with a probe interval
// longer than the uptime the server had when it restarted, the next probe finds
// a larger uptime than the previous one. A rule of "uptime decreased" misses
// that restart; the start time does not.
func TestWatcherDetectsARestartEvenWhenTheNewUptimeIsLarger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &fakeServer{started: time.Now().Add(-30 * time.Second)}
		var log restarts
		stop := run(&Watcher{Interval: time.Minute, OnRestart: log.record}, server)
		defer stop()
		synctest.Wait() // the first probe: uptime 30s
		time.Sleep(time.Second)
		server.setStarted(time.Now()) // up 31s, then restarted
		time.Sleep(time.Minute)       // the next probe finds uptime 59s
		synctest.Wait()
		if got := log.all(); len(got) != 1 {
			t.Fatalf("restarts = %v, want the one restart", got)
		}
	})
}

// TestWatcherIgnoresProbeErrorsAndJitter: a failed probe (the server is down)
// is not a restart and keeps the previous start time; a start time that moves
// by less than the skew (uptime()'s one-second resolution, a clock adjustment)
// is not one either. The watcher keeps probing every interval meanwhile.
func TestWatcherIgnoresProbeErrorsAndJitter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &fakeServer{started: time.Now().Add(-time.Hour)}
		var log restarts
		stop := run(&Watcher{Interval: time.Second, OnRestart: log.record}, server)
		defer stop()
		time.Sleep(2 * time.Second)
		server.setDown(true)
		time.Sleep(5 * time.Second)
		server.setDown(false)
		time.Sleep(2 * time.Second)
		server.setStarted(server.startedAt().Add(DefaultRestartSkew - time.Second))
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if got := log.all(); len(got) != 0 {
			t.Fatalf("restarts = %v, want none", got)
		}
		if n := len(server.probes()); n < 12 {
			t.Fatalf("probes = %d, want one a second", n)
		}
	})
}

// TestWatcherReportsARestartBeforeItsFirstProbe: a Baseline the caller observed
// earlier (before the startup audit) makes a restart between that observation
// and the first probe a restart too. Without a Baseline the first probe only
// sets the baseline.
func TestWatcherReportsARestartBeforeItsFirstProbe(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		baseline := time.Now().Add(-time.Hour)
		server := &fakeServer{started: time.Now().Add(-10 * time.Second)}
		var log restarts
		stop := run(&Watcher{Interval: time.Second, OnRestart: log.record, Baseline: baseline}, server)
		synctest.Wait()
		stop()
		got := log.all()
		if len(got) != 1 || !got[0][0].Equal(baseline) || !got[0][1].Equal(server.startedAt()) {
			t.Fatalf("restarts = %v, want one from the baseline %v to %v", got, baseline, server.startedAt())
		}
		var none restarts
		stopNone := run(&Watcher{Interval: time.Second, OnRestart: none.record}, server)
		synctest.Wait()
		stopNone()
		if got := none.all(); len(got) != 0 {
			t.Fatalf("restarts without a baseline = %v", got)
		}
	})
}

// TestWatcherStopsWithItsContext: Run returns once its context ends, also while
// it waits for the next probe, and the zero Watcher probes every second.
func TestWatcherStopsWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		server := &fakeServer{started: time.Now().Add(-time.Hour)}
		var w Watcher
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			w.Run(ctx, server)
		}()
		time.Sleep(3 * time.Second)
		synctest.Wait()
		cancel()
		<-done
		if n := len(server.probes()); n != 4 {
			t.Fatalf("probes of the zero Watcher in 3s = %d, want 4 (at 0s, 1s, 2s and 3s)", n)
		}
	})
}
