// Package chrestart detects that a ClickHouse server restarted, from its
// uptime(). A restart inside old_parts_lifetime reactivates the parts a
// REPLACE PARTITION retired (housegate spec 2026-10-14 §2), so every hg_safe
// audit must run again after one (spec §14; the stage-3 member's startup
// audit, §8.7, reruns on the same signal).
//
// The rule is the server's start time, now minus uptime(), not "uptime
// decreased": a restart moves the start time later by at least the uptime
// the server had when it went down, even when the next probe finds a larger
// uptime than the previous one. A probe that fails (the server is down) is
// not a restart; the next successful probe decides. One connection must
// reach one server: a pool spanning several would mix their start times.
package chrestart

import (
	"context"
	"fmt"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// DefaultRestartSkew is how far the start time must move before a Watcher
// with a zero Skew reports a restart: uptime() has a one-second resolution and
// the caller's clock may be adjusted, so smaller movements are noise.
const DefaultRestartSkew = 5 * time.Second

// StartedAt returns when the server behind conn started: now minus its
// uptime().
func StartedAt(ctx context.Context, conn clickhouse.Conn, now time.Time) (time.Time, error) {
	var uptime uint64
	if err := conn.QueryRow(ctx, "SELECT toUInt64(uptime())").Scan(&uptime); err != nil {
		return time.Time{}, fmt.Errorf("chrestart: read the server uptime: %w", err)
	}
	return now.Add(-time.Duration(uptime) * time.Second), nil
}

// Watcher probes one server's start time and reports when it moved later.
type Watcher struct {
	// Interval paces the probes and bounds each one; non-positive probes
	// every second.
	Interval time.Duration
	// Skew is the movement that counts as a restart; non-positive is
	// DefaultRestartSkew.
	Skew time.Duration
	// Now is the clock; nil is time.Now.
	Now func() time.Time
	// OnRestart is called on the watcher's goroutine with the start times
	// before and after a restart; the next probe waits for it.
	OnRestart func(ctx context.Context, previous, current time.Time)
	// Baseline is a start time the caller already observed (before a startup
	// audit, say), so a restart between that observation and the first probe
	// is reported too. Zero makes the first successful probe the baseline.
	Baseline time.Time
}

// Run probes conn until ctx ends: once at once, then every Interval.
func (w *Watcher) Run(ctx context.Context, conn clickhouse.Conn) {
	interval := w.Interval
	if interval <= 0 {
		interval = time.Second
	}
	skew := w.Skew
	if skew <= 0 {
		skew = DefaultRestartSkew
	}
	now := w.Now
	if now == nil {
		now = time.Now
	}
	previous := w.Baseline
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		probeCtx, cancel := context.WithTimeout(ctx, interval)
		current, err := StartedAt(probeCtx, conn, now())
		cancel()
		if err == nil {
			if !previous.IsZero() && current.After(previous.Add(skew)) && w.OnRestart != nil {
				w.OnRestart(ctx, previous, current)
			}
			previous = current
		}
		timer.Reset(interval)
	}
}
