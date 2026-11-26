package dataplane

import (
	"context"
	"math/rand/v2"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxImmediateHintHops is how many NotLeader hints a retry round follows
// without waiting. During an election two followers can each name the other
// as the leader; following every hint at once turns that into a tight loop
// (devnet2-si-v3, 2026-11-26: about 8k stream opens per second per method
// while the leader was replaced). One hop still re-homes a client to a known
// leader without delay.
const maxImmediateHintHops = 1

// retryPacer paces the leader-seeking loops of WithLeaderRetry and
// runSubscription: an exponential backoff between RetryBackoffMin and
// RetryBackoffMax, with jitter, and a bound on hints followed without waiting.
type retryPacer struct {
	min, max time.Duration
	backoff  time.Duration
	hops     int
}

func newRetryPacer(cfg Config) *retryPacer {
	return &retryPacer{min: cfg.RetryBackoffMin, max: cfg.RetryBackoffMax, backoff: cfg.RetryBackoffMin}
}

// hop reports whether a NotLeader hint may be followed now. Once the round's
// hops are spent, the caller waits before following it.
func (p *retryPacer) hop() bool {
	if p.hops < maxImmediateHintHops {
		p.hops++
		return true
	}
	return false
}

// reset returns to the minimum backoff after progress (a stream delivered).
func (p *retryPacer) reset() {
	p.backoff = p.min
	p.hops = 0
}

// wait sleeps for the jittered backoff, then doubles it up to max and starts
// a new round of hops. It returns ctx's error if ctx ends first.
func (p *retryPacer) wait(ctx context.Context) error {
	timer := time.NewTimer(jittered(p.backoff))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}
	p.backoff *= 2
	if p.backoff > p.max {
		p.backoff = p.max
	}
	p.hops = 0
	return nil
}

// jittered returns a duration in [d/2, d] ("equal jitter"), so that the
// clients of one network that lost the same leader do not retry in lockstep.
func jittered(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + rand.N(d-half+1)
}

// perPeerLimited reports whether err is the arbiter's per-peer limit refusal
// (arbiter transportsec.Limits with limits.mode enforce): ResourceExhausted
// with "per-peer <class> limit reached". It is retried after a backoff. Other
// ResourceExhausted errors, such as a message over the client's receive limit
// or a busy snapshot-query reservation, keep their own handling.
func perPeerLimited(err error) bool {
	if err == nil {
		return false
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		return false
	}
	msg := st.Message()
	return strings.HasPrefix(msg, "per-peer ") && strings.Contains(msg, " limit reached")
}
