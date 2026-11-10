package dataplane

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

// DefaultLeaseRenewInterval is the membership lease renewal interval of a
// role whose config leaves it zero. The committed lease
// (membership_lease_l2_blocks) must be far longer than this interval
// expressed in L2 blocks of the registry chain.
const DefaultLeaseRenewInterval = 30 * time.Second

// LeaseSignFunc signs one renewal body with the role's own key: the SNode's
// indexer key (authority.SNodeMessageLeaseRenewal, into SignerJWS) or the
// verifier's ed25519 key (authority.VerifierMessageLeaseRenewal, into
// Ed25519Signature).
type LeaseSignFunc func(body authority.LeaseRenewalBody) (wire.RenewNodeLease, error)

// LeaseOutcome is what one renewal attempt did.
type LeaseOutcome int

const (
	// LeaseIdle: the role is not active (never registered here, or
	// deactivated); nothing was sent.
	LeaseIdle LeaseOutcome = iota
	// LeaseRenewed: the renewal committed.
	LeaseRenewed
	// LeaseOff: the leader refused the renewal because no lease is committed
	// (arbiter.MembershipLeaseOffReason), or it predates RenewNodeLease.
	LeaseOff
	// LeaseReactivated: the node was no longer Active (its lease expired
	// while this role still ran, e.g. cut off from the leader) and the role's
	// MarkActive at its current registration_seq committed.
	LeaseReactivated
	// LeaseFailed: the attempt failed; the next tick tries again.
	LeaseFailed
)

// LeaseRenewer keeps one data-plane role's membership lease alive (Raft tag
// 44). The role calls Activate after its MarkActive committed and Deactivate
// before it withdraws; Run renews every interval while active. Each renewal
// carries the registration's seq and a lease_seq that strictly increases
// within it. When the arbiter answers arbiter.MembershipLeaseNotActiveReason
// the renewer resends the role's MarkActive of the same registration: the
// process is alive and was not deactivated, so it returns itself to Active.
//
// No network call holds the role's membership mutex: a renewal racing a
// Deactivate is refused by the arbiter, which binds every renewal and
// MarkActive to the registration_seq the Deactivate replaced.
type LeaseRenewer struct {
	client   *Client
	nodeID   string
	interval time.Duration
	sign     LeaseSignFunc
	log      *slog.Logger

	mu       sync.Mutex
	active   bool
	regSeq   uint64
	leaseSeq uint64
	mark     *pb.NodeRef
}

// NewLeaseRenewer returns an inactive renewer. interval <= 0 means
// DefaultLeaseRenewInterval; a nil logger logs nothing.
func NewLeaseRenewer(client *Client, nodeID string, interval time.Duration, sign LeaseSignFunc, log *slog.Logger) *LeaseRenewer {
	if interval <= 0 {
		interval = DefaultLeaseRenewInterval
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &LeaseRenewer{client: client, nodeID: nodeID, interval: interval, sign: sign, log: log}
}

// Interval is the renewal interval.
func (l *LeaseRenewer) Interval() time.Duration { return l.interval }

// Activate starts renewing for the registration at registrationSeq whose
// MarkActive request (already committed) is mark; lease_seq restarts.
func (l *LeaseRenewer) Activate(registrationSeq uint64, mark *pb.NodeRef) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active, l.regSeq, l.leaseSeq, l.mark = true, registrationSeq, 0, mark
}

// Deactivate stops renewing and reactivating until the next Activate.
func (l *LeaseRenewer) Deactivate() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.active, l.mark = false, nil
}

// Run renews every interval until ctx ends.
func (l *LeaseRenewer) Run(ctx context.Context) {
	t := time.NewTicker(l.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			l.RenewOnce(ctx)
		}
	}
}

// RenewOnce makes one renewal attempt, bounded by one interval.
func (l *LeaseRenewer) RenewOnce(ctx context.Context) LeaseOutcome {
	l.mu.Lock()
	if !l.active {
		l.mu.Unlock()
		return LeaseIdle
	}
	l.leaseSeq++
	body := authority.LeaseRenewalBody{NodeID: l.nodeID, RegistrationSeq: l.regSeq, LeaseSeq: l.leaseSeq}
	mark := l.mark
	l.mu.Unlock()

	ctx, cancel := context.WithTimeout(ctx, l.interval)
	defer cancel()
	renewal, err := l.sign(body)
	if err != nil {
		l.log.Warn("membership lease: sign renewal", "node_id", l.nodeID, "err", err)
		return LeaseFailed
	}
	req := wire.RenewNodeLeaseToRequest(renewal)
	err = l.client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewMembershipClient(conn).RenewNodeLease(ctx, req)
		return err
	})
	switch {
	case err == nil:
		return LeaseRenewed
	case leaseOff(err):
		l.log.Debug("membership lease: not enabled on the arbiter", "node_id", l.nodeID, "err", err)
		return LeaseOff
	case leaseNotActive(err):
		if !l.stillActive(body.RegistrationSeq) {
			return LeaseIdle // deactivated meanwhile: do not come back
		}
		l.log.Warn("membership lease: node is no longer active; re-activating", "node_id", l.nodeID,
			"registration_seq", body.RegistrationSeq)
		if err := l.client.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := pb.NewMembershipClient(conn).MarkActive(ctx, mark)
			return err
		}); err != nil {
			l.log.Warn("membership lease: re-activate", "node_id", l.nodeID, "err", err)
			return LeaseFailed
		}
		return LeaseReactivated
	default:
		if !errors.Is(err, context.Canceled) {
			l.log.Warn("membership lease: renew", "node_id", l.nodeID, "lease_seq", body.LeaseSeq, "err", err)
		}
		return LeaseFailed
	}
}

func (l *LeaseRenewer) stillActive(registrationSeq uint64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.active && l.regSeq == registrationSeq && l.mark != nil
}

// leaseOff reports the "no lease committed" refusal (INVALID_ARGUMENT, never
// FAILED_PRECONDITION, which Client.WithLeaderRetry reads as "not the
// leader" and retries), and an arbiter that predates RenewNodeLease
// (UNIMPLEMENTED), which has no lease either.
func leaseOff(err error) bool {
	st := status.Convert(err)
	return st.Code() == codes.Unimplemented || strings.Contains(st.Message(), arbiter.MembershipLeaseOffReason)
}

func leaseNotActive(err error) bool {
	return strings.Contains(status.Convert(err).Message(), arbiter.MembershipLeaseNotActiveReason)
}
