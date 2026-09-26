package dataplane

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core/wire"
)

// TableRegistryDisabledMessage is the FAILED_PRECONDITION message the arbiter
// leader answers GetTableRegistry with until governance enables the registry.
const TableRegistryDisabledMessage = "table registry is disabled"

// DefaultRegistryStartupTimeout bounds how long a role waits for the
// follower's first GetTableRegistry answer before its startup fails.
const DefaultRegistryStartupTimeout = 2 * time.Minute

// ErrRegistryNotReady reports that the follower had no answer from the
// arbiter within the startup timeout.
var ErrRegistryNotReady = errors.New("dataplane: table registry follower is not ready")

// RegistryView is the read side of a RegistryFollower. Roles and hosts take
// this interface so tests can supply a fixed view.
type RegistryView interface {
	// View returns the last accepted snapshot and whether the registry is
	// enabled. Before Ready, and while the registry is disabled, it returns
	// the zero snapshot and false. The snapshot is shared and read-only.
	View() (wire.TableRegistrySnapshot, bool)
	// Changed returns a channel closed when the next version is accepted
	// after this call.
	Changed() <-chan struct{}
	// Ready is closed once the first GetTableRegistry answered (with a
	// snapshot or "disabled").
	Ready() <-chan struct{}
}

// RegistryFollower follows the arbiter's dynamic SI table registry: one
// GetTableRegistry through WithLeaderRetry, then a resumable
// WatchTableRegistry stream built on the subscription loop (leader hints,
// reconnect, backoff, no per-call timeout). Each accepted message is the
// whole registry; only a strictly greater version is accepted.
type RegistryFollower struct {
	c      *Client
	logger *slog.Logger

	mu        sync.Mutex
	snap      wire.TableRegistrySnapshot
	enabled   bool
	changed   chan struct{}
	ready     chan struct{}
	readyOnce sync.Once
	connected atomic.Bool
}

// NewRegistryFollower returns a follower that reads through c. Call Run to
// start it; one follower serves every consumer in a process.
func NewRegistryFollower(c *Client, logger *slog.Logger) *RegistryFollower {
	if logger == nil {
		logger = slog.Default()
	}
	return &RegistryFollower{c: c, logger: logger, changed: make(chan struct{}), ready: make(chan struct{})}
}

// View implements RegistryView.
func (f *RegistryFollower) View() (wire.TableRegistrySnapshot, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.snap, f.enabled
}

// Changed implements RegistryView.
func (f *RegistryFollower) Changed() <-chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.changed
}

// Ready implements RegistryView.
func (f *RegistryFollower) Ready() <-chan struct{} { return f.ready }

// Connected reports whether a WatchTableRegistry stream is currently open.
// Hosts export it as the follower connection metric.
func (f *RegistryFollower) Connected() bool { return f.connected.Load() }

// WaitReady blocks until Ready or until timeout elapses (ErrRegistryNotReady).
func WaitReady(ctx context.Context, v RegistryView, timeout time.Duration) error {
	if timeout <= 0 {
		timeout = DefaultRegistryStartupTimeout
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-v.Ready():
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("%w after %s", ErrRegistryNotReady, timeout)
	}
}

// Run answers the startup GetTableRegistry and then follows
// WatchTableRegistry until ctx ends. It returns ctx's error, or a
// non-retryable transport error. An arbiter without the TableRegistry service
// (Unimplemented) counts as a disabled registry; the watch keeps retrying it
// at the maximum backoff.
func (f *RegistryFollower) Run(ctx context.Context) error {
	if err := f.initial(ctx); err != nil {
		return err
	}
	for {
		err := runSubscription(ctx, f.c, f.openWatch, f.deliver)
		f.connected.Store(false)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if status.Code(err) != codes.Unimplemented {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(f.c.cfg.RetryBackoffMax):
		}
	}
}

func (f *RegistryFollower) initial(ctx context.Context) error {
	var got *pb.TableRegistrySnapshot
	err := f.c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		m, err := pb.NewTableRegistryClient(conn).GetTableRegistry(ctx, &emptypb.Empty{})
		switch {
		case err == nil:
			// Decode before accepting: an undecodable answer (e.g. version
			// skew introducing an unknown status/origin/retire-reason) must
			// not be read as a disabled registry. Returning an error here
			// (via the callback) makes WithLeaderRetry retry the Get with
			// its existing backoff instead of closing Ready.
			if _, decodeErr := wire.TableRegistrySnapshotFromPB(m); decodeErr != nil {
				return fmt.Errorf("decode initial table registry snapshot: %w", decodeErr)
			}
			got = m
			return nil
		case registryDisabled(err):
			return nil
		case status.Code(err) == codes.Unimplemented:
			f.logger.Warn("arbiter has no TableRegistry service; following the configured genesis tables only")
			return nil
		}
		return err
	})
	if err != nil {
		return fmt.Errorf("get table registry: %w", err)
	}
	if got != nil {
		f.deliver(got)
	}
	f.readyOnce.Do(func() { close(f.ready) })
	return nil
}

func (f *RegistryFollower) openWatch(ctx context.Context, conn *grpc.ClientConn) (recvStream[*pb.TableRegistrySnapshot], error) {
	f.mu.Lock()
	since := f.snap.Version
	f.mu.Unlock()
	stream, err := pb.NewTableRegistryClient(conn).WatchTableRegistry(ctx, &pb.WatchTableRegistryRequest{SinceVersion: since})
	if err != nil {
		return nil, err
	}
	f.connected.Store(true)
	return connectedStream{inner: stream, connected: &f.connected}, nil
}

// deliver accepts m when it decodes and its version is strictly greater than
// the current one. It never returns an error: the subscription loop ignores
// deliver's result, and a bad message must not end the stream.
func (f *RegistryFollower) deliver(m *pb.TableRegistrySnapshot) error {
	snap, err := wire.TableRegistrySnapshotFromPB(m)
	if err != nil {
		f.logger.Error("table registry snapshot refused", "version", m.GetVersion(), "err", err)
		return nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.enabled && snap.Version <= f.snap.Version {
		return nil
	}
	f.snap, f.enabled = snap, true
	close(f.changed)
	f.changed = make(chan struct{})
	return nil
}

type connectedStream struct {
	inner     recvStream[*pb.TableRegistrySnapshot]
	connected *atomic.Bool
}

func (s connectedStream) Recv() (*pb.TableRegistrySnapshot, error) {
	m, err := s.inner.Recv()
	if err != nil {
		s.connected.Store(false)
	}
	return m, err
}

// registryDisabled reports the leader's "table registry is disabled" answer:
// FAILED_PRECONDITION without a NotLeader detail.
func registryDisabled(err error) bool {
	st, ok := leaderPrecondition(err)
	return ok && st.Message() == TableRegistryDisabledMessage
}

// leaderPrecondition returns err's status when it is a FAILED_PRECONDITION
// that carries no NotLeader detail, i.e. a genuine precondition answered by
// the leader rather than a redirect. WithLeaderRetry treats every
// FAILED_PRECONDITION as a leader miss, so callers must intercept these.
func leaderPrecondition(err error) (*status.Status, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return nil, false
	}
	for _, detail := range st.Details() {
		if _, isNotLeader := detail.(*pb.NotLeader); isNotLeader {
			return nil, false
		}
	}
	return st, true
}
