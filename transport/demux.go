package transport

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

// tlsHandshakeRecord is the first byte of every TLS connection (a handshake
// record). Plaintext gRPC opens with the HTTP/2 preface "PRI " and the
// hashicorp raft transport with an RPC type byte 0-3, so neither collides.
const tlsHandshakeRecord = 0x16

const (
	// DefaultFirstByteTimeout bounds the wait for a client's first byte.
	DefaultFirstByteTimeout = 5 * time.Second
	// DefaultHandshakeTimeout bounds a TLS handshake.
	DefaultHandshakeTimeout = 10 * time.Second
	// DefaultMaxPending bounds connections between accept and delivery.
	DefaultMaxPending = 256
)

// Refusal reasons reported through ListenerOptions.OnRefused.
const (
	RefusedFirstByte = "first_byte"
	RefusedHandshake = "handshake"
	RefusedPlaintext = "plaintext"
	RefusedPending   = "pending_cap"
	RefusedPerIP     = "per_ip_cap"
)

// Connection kinds reported through ListenerOptions.OnAccepted.
const (
	KindPlaintext = "plaintext"
	KindTLS       = "tls"
)

// ListenerOptions configures NewListener.
type ListenerOptions struct {
	Mode Mode
	// TLS is the server config (ServerTLSConfig). Required unless Mode is off.
	TLS *tls.Config
	// FirstByteTimeout and HandshakeTimeout bound how long one connection may
	// take before it is delivered; zero selects the defaults. They cannot be
	// disabled: a client that sends nothing, or stalls mid-handshake, is
	// closed when they expire.
	FirstByteTimeout time.Duration
	HandshakeTimeout time.Duration
	// MaxPending bounds connections being classified or handshaken at once;
	// zero selects DefaultMaxPending. Beyond it, new connections are closed.
	MaxPending int
	// MaxConnsPerIP bounds open connections per source IP, counted from
	// accept to close; zero is unlimited.
	MaxConnsPerIP int
	// OnAccepted and OnRefused observe each connection's outcome.
	OnAccepted func(kind string)
	OnRefused  func(reason string)
}

// Conn is a connection delivered by a Listener.
type Conn struct {
	net.Conn
	tls     *tls.Conn
	release func()
	once    sync.Once
}

// TLSState returns the handshake state of a TLS connection.
func (c *Conn) TLSState() (tls.ConnectionState, bool) {
	if c.tls == nil {
		return tls.ConnectionState{}, false
	}
	return c.tls.ConnectionState(), true
}

// Close closes the connection and releases its per-IP slot.
func (c *Conn) Close() error {
	err := c.Conn.Close()
	c.once.Do(func() {
		if c.release != nil {
			c.release()
		}
	})
	return err
}

// prefixConn replays bytes already read from the connection.
type prefixConn struct {
	net.Conn
	prefix []byte
}

func (c *prefixConn) Read(b []byte) (int, error) {
	if len(c.prefix) > 0 {
		n := copy(b, c.prefix)
		c.prefix = c.prefix[n:]
		return n, nil
	}
	return c.Conn.Read(b)
}

type listener struct {
	inner net.Listener
	opts  ListenerOptions

	conns   chan *Conn
	pending chan struct{}
	done    chan struct{}
	failed  chan struct{}
	failErr error
	close   sync.Once

	ipMu  sync.Mutex
	perIP map[string]int
}

// NewListener wraps inner. In ModeOff it delivers plaintext connections as
// they come (only MaxConnsPerIP applies). Otherwise each connection is
// classified off the accept loop: a first byte of 0x16 starts a TLS handshake;
// any other byte is a plaintext connection, delivered in ModeDual and closed
// in ModeTLS and ModeMTLS. Connections are delivered as *Conn.
func NewListener(inner net.Listener, opts ListenerOptions) (net.Listener, error) {
	if opts.Mode == "" {
		opts.Mode = ModeOff
	}
	if _, err := ParseMode(string(opts.Mode)); err != nil {
		return nil, err
	}
	if opts.Mode.UsesTLS() && opts.TLS == nil {
		return nil, fmt.Errorf("transport: mode %q needs a TLS config", opts.Mode)
	}
	if opts.FirstByteTimeout <= 0 {
		opts.FirstByteTimeout = DefaultFirstByteTimeout
	}
	if opts.HandshakeTimeout <= 0 {
		opts.HandshakeTimeout = DefaultHandshakeTimeout
	}
	if opts.MaxPending <= 0 {
		opts.MaxPending = DefaultMaxPending
	}
	l := &listener{
		inner:   inner,
		opts:    opts,
		conns:   make(chan *Conn),
		pending: make(chan struct{}, opts.MaxPending),
		done:    make(chan struct{}),
		failed:  make(chan struct{}),
		perIP:   make(map[string]int),
	}
	go l.acceptLoop()
	return l, nil
}

func (l *listener) Addr() net.Addr { return l.inner.Addr() }

func (l *listener) Close() error {
	var err error
	l.close.Do(func() {
		close(l.done)
		err = l.inner.Close()
	})
	return err
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	case <-l.failed:
		return nil, l.failErr
	}
}

func (l *listener) refused(reason string) {
	if l.opts.OnRefused != nil {
		l.opts.OnRefused(reason)
	}
}

func (l *listener) acceptLoop() {
	var backoff time.Duration
	for {
		c, err := l.inner.Accept()
		if err != nil {
			select {
			case <-l.done:
				return
			default:
			}
			var te interface{ Temporary() bool }
			if errors.As(err, &te) && te.Temporary() {
				if backoff == 0 {
					backoff = 5 * time.Millisecond
				} else if backoff *= 2; backoff > time.Second {
					backoff = time.Second
				}
				select {
				case <-time.After(backoff):
				case <-l.done:
					return
				}
				continue
			}
			l.failErr = err
			close(l.failed)
			return
		}
		backoff = 0
		release, ok := l.admitIP(c)
		if !ok {
			l.refused(RefusedPerIP)
			_ = c.Close()
			continue
		}
		select {
		case l.pending <- struct{}{}:
			go l.handle(c, release)
		default:
			release()
			l.refused(RefusedPending)
			_ = c.Close()
		}
	}
}

func (l *listener) admitIP(c net.Conn) (func(), bool) {
	if l.opts.MaxConnsPerIP <= 0 {
		return func() {}, true
	}
	ip := remoteIP(c)
	l.ipMu.Lock()
	defer l.ipMu.Unlock()
	if l.perIP[ip] >= l.opts.MaxConnsPerIP {
		return nil, false
	}
	l.perIP[ip]++
	var once sync.Once
	return func() {
		once.Do(func() {
			l.ipMu.Lock()
			defer l.ipMu.Unlock()
			if l.perIP[ip]--; l.perIP[ip] <= 0 {
				delete(l.perIP, ip)
			}
		})
	}, true
}

func remoteIP(c net.Conn) string {
	addr := c.RemoteAddr()
	if addr == nil {
		return ""
	}
	if host, _, err := net.SplitHostPort(addr.String()); err == nil {
		return host
	}
	return addr.String()
}

func (l *listener) handle(c net.Conn, release func()) {
	defer func() { <-l.pending }()
	conn, kind, reason := l.classify(c)
	if conn == nil {
		release()
		l.refused(reason)
		_ = c.Close()
		return
	}
	conn.release = release
	select {
	case l.conns <- conn:
		if l.opts.OnAccepted != nil {
			l.opts.OnAccepted(kind)
		}
	case <-l.done:
		_ = conn.Close()
	}
}

func (l *listener) classify(c net.Conn) (*Conn, string, string) {
	if l.opts.Mode == ModeOff {
		return &Conn{Conn: c}, KindPlaintext, ""
	}
	if err := c.SetReadDeadline(time.Now().Add(l.opts.FirstByteTimeout)); err != nil {
		return nil, "", RefusedFirstByte
	}
	var first [1]byte
	if _, err := io.ReadFull(c, first[:]); err != nil {
		return nil, "", RefusedFirstByte
	}
	pc := &prefixConn{Conn: c, prefix: first[:]}
	if first[0] != tlsHandshakeRecord {
		if !l.opts.Mode.AcceptsPlaintext() {
			return nil, "", RefusedPlaintext
		}
		if err := c.SetReadDeadline(time.Time{}); err != nil {
			return nil, "", RefusedFirstByte
		}
		return &Conn{Conn: pc}, KindPlaintext, ""
	}
	tc := tls.Server(pc, l.opts.TLS)
	ctx, cancel := context.WithTimeout(context.Background(), l.opts.HandshakeTimeout)
	defer cancel()
	if err := c.SetDeadline(time.Now().Add(l.opts.HandshakeTimeout)); err != nil {
		return nil, "", RefusedHandshake
	}
	if err := tc.HandshakeContext(ctx); err != nil {
		return nil, "", RefusedHandshake
	}
	if err := c.SetDeadline(time.Time{}); err != nil {
		return nil, "", RefusedHandshake
	}
	return &Conn{Conn: tc, tls: tc}, KindTLS, ""
}
