// Package dastore is the arbiter repo's client for the da.proto payload
// store: snode put-spool, verifier fetch, and custody-chain pin/release.
//
// It is deliberately hash-silent on reads. Content verification against
// the sequenced envelope remains in housegate's replay.Verifier.
package dastore

import (
	"errors"
	"fmt"
	"sync"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const holderID = "arbiter"

var errClientClosed = errors.New("dastore: client is closed")

const (
	defaultDialTimeout = 5 * time.Second
	defaultCallTimeout = 10 * time.Second
)

// Config describes the data and lifecycle service endpoints.
type Config struct {
	DataAddr    string
	ControlAddr string
	DialTimeout time.Duration
	CallTimeout time.Duration
}

func (cfg Config) withDefaults() Config {
	if cfg.DialTimeout <= 0 {
		cfg.DialTimeout = defaultDialTimeout
	}
	if cfg.CallTimeout <= 0 {
		cfg.CallTimeout = defaultCallTimeout
	}
	return cfg
}

// Client lazily constructs connections to the payload-store services.
type Client struct {
	cfg Config

	connMu   sync.Mutex
	dataConn *grpc.ClientConn
	ctlConn  *grpc.ClientConn
	closed   bool

	limitsMu sync.Mutex
	limits   *pb.StoreLimits
}

// New constructs a lazy payload-store client without performing I/O.
func New(cfg Config) (*Client, error) {
	cfg = cfg.withDefaults()
	if cfg.DataAddr == "" && cfg.ControlAddr == "" {
		return nil, fmt.Errorf("dastore: at least one of data_addr or control_addr is required")
	}
	return &Client{cfg: cfg}, nil
}

// Close closes any connections opened by the client.
func (c *Client) Close() error {
	c.connMu.Lock()
	defer c.connMu.Unlock()
	if c.closed {
		return nil
	}
	c.closed = true

	var errs []error
	if c.dataConn != nil {
		errs = append(errs, c.dataConn.Close())
		c.dataConn = nil
	}
	if c.ctlConn != nil {
		errs = append(errs, c.ctlConn.Close())
		c.ctlConn = nil
	}
	return errors.Join(errs...)
}

func (c *Client) dataConnection() (*grpc.ClientConn, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.closed {
		return nil, errClientClosed
	}
	if c.dataConn != nil {
		return c.dataConn, nil
	}
	if c.cfg.DataAddr == "" {
		return nil, fmt.Errorf("dastore: data_addr is not configured")
	}
	conn, err := grpc.NewClient(c.cfg.DataAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dastore: data connection %s: %w", c.cfg.DataAddr, err)
	}
	c.dataConn = conn
	return conn, nil
}

func (c *Client) controlConnection() (*grpc.ClientConn, error) {
	c.connMu.Lock()
	defer c.connMu.Unlock()

	if c.closed {
		return nil, errClientClosed
	}
	if c.ctlConn != nil {
		return c.ctlConn, nil
	}
	if c.cfg.ControlAddr == "" {
		return nil, fmt.Errorf("dastore: control_addr is not configured")
	}
	conn, err := grpc.NewClient(c.cfg.ControlAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dastore: control connection %s: %w", c.cfg.ControlAddr, err)
	}
	c.ctlConn = conn
	return conn, nil
}
