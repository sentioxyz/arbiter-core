// Package chrestart detects that a ClickHouse server restarted, from its
// uptime(). A restart inside old_parts_lifetime reactivates the parts a
// REPLACE PARTITION retired (housegate spec 2026-10-14 §2), so every hg_safe
// audit must run again after one (spec §14; the stage-3 member's startup
// audit, §8.7, reruns on the same signal).
//
// The main rule is the server's start time, now minus uptime(), not "uptime
// decreased": a restart moves the start time later by at least the uptime
// the server had when it went down, even when the next probe finds a larger
// uptime than the previous one. A movement within the skew is kept as
// evidence against the last baseline rather than replacing it, and an uptime
// smaller than the previous probe's is a restart whatever the start time
// says: uptime() is the server's monotonic clock, so it never falls without
// one. A probe that fails (the server is down) is not a restart; the next
// successful probe decides. One connection must reach one server: a pool
// spanning several would mix their start times.
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
	started, _, err := probe(ctx, conn, now)
	return started, err
}

// probe returns the server's start time and its uptime() in seconds.
func probe(ctx context.Context, conn clickhouse.Conn, now time.Time) (time.Time, uint64, error) {
	var uptime uint64
	if err := conn.QueryRow(ctx, "SELECT toUInt64(uptime())").Scan(&uptime); err != nil {
		return time.Time{}, 0, fmt.Errorf("chrestart: read the server uptime: %w", err)
	}
	return now.Add(-time.Duration(uptime) * time.Second), uptime, nil
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

// Run probes conn until ctx ends: once at once, then every Interval. It
// reports a restart when the start time moved later than the baseline plus
// Skew, or when uptime() fell below the previous probe's. The baseline moves
// forward only with a reported restart, so forward movements within the skew
// add up instead of being forgotten; it follows a start time that moved
// earlier, which only the caller's clock or jitter can do.
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
	var lastUptime uint64
	probed := false
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		probeCtx, cancel := context.WithTimeout(ctx, interval)
		current, uptime, err := probe(probeCtx, conn, now())
		cancel()
		if err == nil {
			restarted := !previous.IsZero() && (current.After(previous.Add(skew)) || (probed && uptime < lastUptime))
			if restarted && w.OnRestart != nil {
				w.OnRestart(ctx, previous, current)
			}
			if restarted || previous.IsZero() || current.Before(previous) {
				previous = current
			}
			lastUptime, probed = uptime, true
		}
		timer.Reset(interval)
	}
}
