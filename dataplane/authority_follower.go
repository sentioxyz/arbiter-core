package dataplane

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
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

// AuthorityChainFile is the follower's state file in its directory.
const AuthorityChainFile = "authority_chain.json"

// DefaultAuthorityPollInterval is AuthorityFollowerConfig.PollInterval's
// default. Polling only shortens the time to notice a rotation without a
// token; a token of a newer epoch refreshes on demand.
const DefaultAuthorityPollInterval = time.Minute

// authorityRefreshTimeout bounds one GetConsensusHistory call.
const authorityRefreshTimeout = 10 * time.Second

// ErrAuthorityHistoryUnimplemented reports an Arbiter without the
// ConsensusHistoryService: upgrade every voter before following.
var ErrAuthorityHistoryUnimplemented = errors.New("dataplane: the arbiter does not serve GetConsensusHistory; upgrade every arbiter voter before enabling follow_consensus_authority")

// AuthorityFollowerConfig configures an AuthorityFollower.
type AuthorityFollowerConfig struct {
	// Anchor is the configured trust anchor (authority_addresses at
	// authority_trust_epoch).
	Anchor authority.AuthorityAnchor
	// StateDir holds AuthorityChainFile.
	StateDir string
	// PollInterval paces background refreshes; 0 = DefaultAuthorityPollInterval.
	PollInterval time.Duration
	// MaxTokenAge bounds promotion and cleanup token age; must be positive.
	MaxTokenAge time.Duration
}

// AuthorityFollower follows the committed authority set as a light client
// (decision record for arbiter#141). It verifies the Arbiter's signed
// consensus history from the configured anchor, persists the verified chain,
// and authorizes promotion and cleanup tokens against the set of the epoch it
// followed, requiring each token's consensus context to name exactly that
// epoch.
//
// It never rolls back: an answer behind the persisted chain is ignored and the
// last verified set stays in force, as it does while the Arbiter is
// unreachable. It fails closed only on a fork (two different updates for one
// epoch), which it persists, so a restart does not clear it; an operator
// clears it by configuring a different anchor.
type AuthorityFollower struct {
	c      *Client
	cfg    AuthorityFollowerConfig
	path   string
	logger *slog.Logger

	// fetch is the GetConsensusHistory seam; tests replace it.
	fetch func(ctx context.Context, from uint64) (*pb.ConsensusHistory, error)

	refreshMu sync.Mutex
	mu        sync.RWMutex
	chain     authority.AuthorityChain
	forked    string
}

type authorityChainFile struct {
	Chain authority.AuthorityChain `json:"chain"`
	// Fork is the persisted fork report; non-empty stops authorization.
	Fork string `json:"fork,omitempty"`
}

// NewAuthorityFollower loads the persisted chain when it starts at cfg.Anchor,
// and otherwise starts a fresh chain at cfg.Anchor (a changed anchor is the
// operator's re-anchor and replaces the persisted chain and any fork report).
func NewAuthorityFollower(c *Client, cfg AuthorityFollowerConfig, logger *slog.Logger) (*AuthorityFollower, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.StateDir == "" {
		return nil, errors.New("authority follower: state dir is required")
	}
	if cfg.MaxTokenAge <= 0 {
		return nil, errors.New("authority follower: max token age must be positive")
	}
	if cfg.PollInterval < 0 {
		return nil, errors.New("authority follower: poll interval must not be negative")
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = DefaultAuthorityPollInterval
	}
	fresh, err := authority.NewAuthorityChain(cfg.Anchor)
	if err != nil {
		return nil, err
	}
	f := &AuthorityFollower{c: c, cfg: cfg, path: filepath.Join(cfg.StateDir, AuthorityChainFile), logger: logger, chain: fresh}
	f.fetch = f.fetchHistory
	b, err := os.ReadFile(f.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return f, nil
	case err != nil:
		return nil, fmt.Errorf("authority follower: read %s: %w", f.path, err)
	}
	var stored authorityChainFile
	if err := json.Unmarshal(b, &stored); err != nil {
		return nil, fmt.Errorf("authority follower: decode %s: %w", f.path, err)
	}
	if !stored.Chain.SameAnchor(cfg.Anchor) {
		logger.Warn("authority trust anchor changed: discarding the persisted authority chain and any fork report",
			"persisted_anchor_epoch", stored.Chain.Anchor.Epoch, "persisted_epoch", stored.Chain.Epoch,
			"anchor_epoch", cfg.Anchor.Epoch, "fork", stored.Fork)
		return f, nil
	}
	if err := stored.Chain.Validate(); err != nil {
		return nil, fmt.Errorf("authority follower: %s: %w", f.path, err)
	}
	f.chain, f.forked = stored.Chain, stored.Fork
	return f, nil
}

// Current returns the followed epoch and set, and the fork report if any.
func (f *AuthorityFollower) Current() (epoch uint64, addresses []string, fork string) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.chain.Epoch, append([]string(nil), f.chain.Addresses...), f.forked
}

// Refresh fetches the history from the anchor epoch and extends the chain.
// A fork is persisted and returned as authority.ErrAuthorityFork; an answer
// behind the chain is logged and ignored; ErrAuthorityHistoryUnimplemented
// reports an Arbiter without the RPC. Other errors leave the chain as it was.
func (f *AuthorityFollower) Refresh(ctx context.Context) error {
	f.refreshMu.Lock()
	defer f.refreshMu.Unlock()
	f.mu.RLock()
	chain, forked := f.chain, f.forked
	f.mu.RUnlock()
	if forked != "" {
		return fmt.Errorf("%w (persisted: %s)", authority.ErrAuthorityFork, forked)
	}
	ctx, cancel := context.WithTimeout(ctx, authorityRefreshTimeout)
	defer cancel()
	from := chain.Anchor.Epoch
	h, err := f.fetch(ctx, from)
	if status.Code(err) == codes.Unimplemented {
		return ErrAuthorityHistoryUnimplemented
	}
	if err != nil {
		return fmt.Errorf("authority follower: get consensus history: %w", err)
	}
	if h.GetNetworkId() != chain.Anchor.NetworkID || h.GetGenesisSnapshotId() != chain.Anchor.GenesisSnapshotID {
		return fmt.Errorf("authority follower: arbiter serves network %q genesis %q, want %q %q",
			h.GetNetworkId(), h.GetGenesisSnapshotId(), chain.Anchor.NetworkID, chain.Anchor.GenesisSnapshotID)
	}
	updates := make([]authority.SignedConsensusUpdate, len(h.GetUpdates()))
	for i, u := range h.GetUpdates() {
		updates[i] = authority.SignedConsensusUpdate{Update: wire.ConsensusParamsUpdateFromPB(u.GetUpdate()), AuthorityJWS: u.GetAuthorityJws()}
	}
	if want := from + uint64(len(updates)); h.GetCurrentEpoch() != want && h.GetCurrentEpoch() >= from {
		return fmt.Errorf("authority follower: arbiter reports epoch %d but served history up to %d", h.GetCurrentEpoch(), want)
	}
	next, err := chain.Extend(from, updates)
	if errors.Is(err, authority.ErrAuthorityFork) {
		f.recordFork(err.Error())
		return err
	}
	if err != nil {
		return fmt.Errorf("authority follower: %w", err)
	}
	if h.GetCurrentEpoch() < chain.Epoch {
		f.logger.Warn("arbiter answered an authority history behind the verified chain; keeping the verified set",
			"served_epoch", h.GetCurrentEpoch(), "verified_epoch", chain.Epoch)
		return nil
	}
	if next.Epoch == chain.Epoch {
		return nil
	}
	if err := f.persist(authorityChainFile{Chain: next}); err != nil {
		return fmt.Errorf("authority follower: persist: %w", err)
	}
	f.mu.Lock()
	f.chain = next
	f.mu.Unlock()
	f.logger.Info("followed the committed authority set", "epoch", next.Epoch, "authority_addresses", next.Addresses, "previous_epoch", chain.Epoch)
	return nil
}

func (f *AuthorityFollower) recordFork(report string) {
	f.mu.Lock()
	f.forked = report
	chain := f.chain
	f.mu.Unlock()
	f.logger.Error("authority history fork: refusing every authority command until the trust anchor is re-configured", "err", report)
	if err := f.persist(authorityChainFile{Chain: chain, Fork: report}); err != nil {
		f.logger.Error("authority follower: persist fork report", "err", err)
	}
}

// Run refreshes every PollInterval until ctx ends. Refresh failures other
// than a fork are logged at warn level and retried; the last verified set
// stays in force. It returns only ctx's error.
func (f *AuthorityFollower) Run(ctx context.Context) error {
	t := time.NewTicker(f.cfg.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := f.Refresh(ctx); err != nil && ctx.Err() == nil && !errors.Is(err, authority.ErrAuthorityFork) {
				f.logger.Warn("authority history refresh failed; keeping the last verified set", "err", err)
			}
		}
	}
}

// AuthorizePromotion authorizes a promotion token against the followed set.
func (f *AuthorityFollower) AuthorizePromotion(ctx context.Context, cmd arbiter.PromoteSafePartition, token string) (string, error) {
	return f.authorize(ctx, token, func(v *authority.Validator, cctx authority.ConsensusContext) (string, error) {
		return v.AuthorizePromotionInContext(cmd, token, cctx)
	})
}

// AuthorizeCleanup authorizes a cleanup token against the followed set.
func (f *AuthorityFollower) AuthorizeCleanup(ctx context.Context, cmd arbiter.UnsafeCleanup, token string) (string, error) {
	return f.authorize(ctx, token, func(v *authority.Validator, cctx authority.ConsensusContext) (string, error) {
		return v.AuthorizeCleanupInContext(cmd, token, cctx)
	})
}

func (f *AuthorityFollower) authorize(ctx context.Context, token string, check func(*authority.Validator, authority.ConsensusContext) (string, error)) (string, error) {
	f.mu.RLock()
	epoch, forked := f.chain.Epoch, f.forked
	f.mu.RUnlock()
	if forked != "" {
		return "", fmt.Errorf("%w: %s", authority.ErrAuthorityFork, forked)
	}
	// The claimed epoch is unverified: it only decides whether to refresh.
	if claimed, ok := authority.TokenAuthorityEpoch(token); ok && claimed > epoch {
		if err := f.Refresh(ctx); err != nil {
			f.logger.Warn("authority history refresh for a newer token failed", "token_epoch", claimed, "verified_epoch", epoch, "err", err)
		}
	}
	f.mu.RLock()
	chain, forked := f.chain, f.forked
	f.mu.RUnlock()
	if forked != "" {
		return "", fmt.Errorf("%w: %s", authority.ErrAuthorityFork, forked)
	}
	addr, err := check(chain.Validator(f.cfg.MaxTokenAge), chain.Context())
	if err != nil {
		return "", fmt.Errorf("authority epoch %d: %w", chain.Epoch, err)
	}
	return addr, nil
}

func (f *AuthorityFollower) fetchHistory(ctx context.Context, from uint64) (*pb.ConsensusHistory, error) {
	var out *pb.ConsensusHistory
	err := f.c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		h, err := pb.NewConsensusHistoryServiceClient(conn).GetConsensusHistory(ctx, &pb.GetConsensusHistoryRequest{FromEpoch: from})
		if err != nil {
			return err
		}
		out = h
		return nil
	})
	return out, err
}

func (f *AuthorityFollower) persist(state authorityChainFile) error {
	b, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(f.path), ".authority-chain-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	if _, err := tmp.Write(b); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp.Name(), f.path); err != nil {
		return err
	}
	dir, err := os.Open(filepath.Dir(f.path))
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
