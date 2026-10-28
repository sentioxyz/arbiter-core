package serve

import (
	"context"
	"strconv"
	"time"
)

// probeLoop dials owner's HouseGate at once and then every
// OwnerProbeInterval, each dial bounded by OwnerProbeTimeout, until ctx ends.
func (s *Server) probeLoop(ctx context.Context, owner uint64, addr string) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		dctx, cancel := context.WithTimeout(ctx, OwnerProbeTimeout)
		conn, err := s.dial(dctx, "tcp", addr)
		cancel()
		if err == nil {
			_ = conn.Close()
		}
		if ctx.Err() != nil {
			return
		}
		s.recordProbe(owner, err)
		timer.Reset(s.cfg.OwnerProbeInterval)
	}
}

// recordProbe keeps the start of the current failure streak: a success
// clears it, the first failure after a success starts it.
func (s *Server) recordProbe(owner uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	o := s.ownerLocked(owner)
	switch {
	case err == nil && !o.firstFailure.IsZero():
		o.firstFailure = time.Time{}
		s.logger.Info("member serving: owner housegate reachable again", "owner", owner, "addr", o.addr)
	case err != nil && o.firstFailure.IsZero():
		o.firstFailure = now
		s.logger.Warn("member serving: owner housegate probe failed", "owner", owner, "addr", o.addr, "err", err)
	}
	s.metrics.ownerUnreachable.WithLabelValues(strconv.FormatUint(owner, 10)).Set(boolGauge(s.unreachableLocked(owner, now)))
}

// unreachableLocked: every probe of owner failed since a first failure at
// least OwnerUnreachableAfter ago. An owner without an address never is.
func (s *Server) unreachableLocked(owner uint64, now time.Time) bool {
	o := s.owners[owner]
	return o != nil && o.addr != "" && !o.firstFailure.IsZero() && now.Sub(o.firstFailure) >= s.cfg.OwnerUnreachableAfter
}

func boolGauge(b bool) float64 {
	if b {
		return 1
	}
	return 0
}
