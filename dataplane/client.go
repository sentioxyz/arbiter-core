package dataplane

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Peer struct {
	ID       string
	GRPCAddr string
}

type Config struct {
	Peers           []Peer
	DialTimeout     time.Duration
	RetryBackoffMin time.Duration
	RetryBackoffMax time.Duration
}

func (cfg Config) withDefaults() Config {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.RetryBackoffMin <= 0 {
		cfg.RetryBackoffMin = 100 * time.Millisecond
	}
	if cfg.RetryBackoffMax <= 0 {
		cfg.RetryBackoffMax = 3 * time.Second
	}
	if cfg.RetryBackoffMax < cfg.RetryBackoffMin {
		cfg.RetryBackoffMax = cfg.RetryBackoffMin
	}
	return cfg
}

type Client struct {
	cfg   Config
	order []string

	mu         sync.Mutex
	conns      map[string]*grpc.ClientConn
	addrs      map[string]string
	lastLeader string
}

func New(cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if len(cfg.Peers) == 0 {
		return nil, fmt.Errorf("dataplane: at least one peer is required")
	}
	c := &Client{
		cfg:   cfg,
		conns: make(map[string]*grpc.ClientConn),
		addrs: make(map[string]string),
	}
	for _, p := range cfg.Peers {
		if p.ID == "" || p.GRPCAddr == "" {
			return nil, fmt.Errorf("dataplane: peer id and addr are required")
		}
		if _, ok := c.addrs[p.ID]; ok {
			return nil, fmt.Errorf("dataplane: duplicate peer id %q", p.ID)
		}
		c.addrs[p.ID] = p.GRPCAddr
		c.order = append(c.order, p.ID)
	}
	return c, nil
}

func (c *Client) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for id, conn := range c.conns {
		_ = conn.Close()
		delete(c.conns, id)
	}
}

func (c *Client) conn(id string) (*grpc.ClientConn, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, ok := c.conns[id]; ok {
		return conn, nil
	}
	addr, ok := c.addrs[id]
	if !ok {
		return nil, fmt.Errorf("dataplane: unknown peer %q", id)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dataplane: dial %s: %w", addr, err)
	}
	c.conns[id] = conn
	return conn, nil
}

func (c *Client) setLeader(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.addrs[id]; ok {
		c.lastLeader = id
	}
}

func (c *Client) hasPeer(id string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	_, ok := c.addrs[id]
	return ok
}

func (c *Client) candidateOrder(preferred string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if preferred == "" {
		preferred = c.lastLeader
	}
	out := make([]string, 0, len(c.order))
	if preferred != "" {
		if _, ok := c.addrs[preferred]; ok {
			out = append(out, preferred)
		}
	}
	for _, id := range c.order {
		if id != preferred {
			out = append(out, id)
		}
	}
	return out
}

func (c *Client) WithLeaderRetry(ctx context.Context, fn func(ctx context.Context, conn *grpc.ClientConn) error) error {
	backoff := c.cfg.RetryBackoffMin
	preferred := ""
	for {
		followHint := false
		for _, id := range c.candidateOrder(preferred) {
			if err := ctx.Err(); err != nil {
				return err
			}
			conn, err := c.conn(id)
			if err != nil {
				continue
			}
			callCtx, cancel := context.WithTimeout(ctx, c.cfg.DialTimeout)
			err = fn(callCtx, conn)
			cancel()
			if err == nil {
				c.setLeader(id)
				return nil
			}
			if hint, ok := notLeaderHint(err); ok {
				if hint != "" && hint != id && c.hasPeer(hint) {
					preferred = hint
					followHint = true
					break
				}
				continue
			}
			if !retryableStatus(status.Code(err)) {
				return err
			}
		}
		if followHint {
			backoff = c.cfg.RetryBackoffMin
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > c.cfg.RetryBackoffMax {
			backoff = c.cfg.RetryBackoffMax
		}
	}
}

func (c *Client) LeaderConn(ctx context.Context) (*grpc.ClientConn, error) {
	var out *grpc.ClientConn
	err := c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
		_, err := pb.NewSafeStateClient(conn).GetSafeWatermark(ctx, &pb.GetSafeWatermarkRequest{})
		if err != nil {
			return err
		}
		out = conn
		return nil
	})
	return out, err
}

func notLeaderHint(err error) (string, bool) {
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.FailedPrecondition {
		return "", false
	}
	for _, detail := range st.Details() {
		if nl, ok := detail.(*pb.NotLeader); ok {
			return nl.GetLeaderAddr(), true
		}
	}
	return "", true
}

func retryableStatus(code codes.Code) bool {
	return code == codes.Unavailable || code == codes.DeadlineExceeded || code == codes.Canceled || code == codes.Unknown
}
