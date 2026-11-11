package transport_test

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	"github.com/sentioxyz/arbiter-core/transport"
	"github.com/sentioxyz/arbiter-core/transport/transporttest"
)

const network = "devnet2"

func voterID(id string) transport.Identity {
	return transport.Identity{NetworkID: network, Role: transport.RoleVoter, ID: id}
}

func snodeID(id string) transport.Identity {
	return transport.Identity{NetworkID: network, Role: transport.RoleSNode, ID: id}
}

func reloader(t *testing.T, f transport.Files) *transport.Reloader {
	t.Helper()
	r, err := transport.NewReloader(f, transport.ReloaderOptions{Interval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return r
}

// recorder is a health server that records each caller.
type recorder struct {
	healthpb.UnimplementedHealthServer
	mu    sync.Mutex
	peers []transport.Peer
}

func (r *recorder) Check(ctx context.Context, _ *healthpb.HealthCheckRequest) (*healthpb.HealthCheckResponse, error) {
	r.mu.Lock()
	r.peers = append(r.peers, transport.PeerFromContext(ctx))
	r.mu.Unlock()
	return &healthpb.HealthCheckResponse{Status: healthpb.HealthCheckResponse_SERVING}, nil
}

func (r *recorder) last() transport.Peer {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.peers[len(r.peers)-1]
}

type server struct {
	addr     string
	rec      *recorder
	mu       sync.Mutex
	refused  []string
	accepted []string
}

func (s *server) refusals() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.refused...)
}

func startServer(t *testing.T, mode transport.Mode, files transport.Files, tune func(*transport.ListenerOptions)) *server {
	t.Helper()
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &server{addr: inner.Addr().String(), rec: &recorder{}}
	opts := transport.ListenerOptions{
		Mode:       mode,
		OnRefused:  func(r string) { s.mu.Lock(); s.refused = append(s.refused, r); s.mu.Unlock() },
		OnAccepted: func(k string) { s.mu.Lock(); s.accepted = append(s.accepted, k); s.mu.Unlock() },
	}
	if mode.UsesTLS() {
		cfg, err := transport.ServerTLSConfig(reloader(t, files), transport.ServerOptions{Mode: mode, NetworkID: network, NextProtos: []string{"h2"}})
		if err != nil {
			t.Fatal(err)
		}
		opts.TLS = cfg
	}
	if tune != nil {
		tune(&opts)
	}
	lis, err := transport.NewListener(inner, opts)
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(transport.ServerCredentials()))
	healthpb.RegisterHealthServer(g, s.rec)
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return s
}

func check(t *testing.T, addr string, creds credentials.TransportCredentials) error {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///"+addr, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err = healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
	return err
}

func clientCreds(t *testing.T, files transport.Files, expect transport.Expect, serverName string) credentials.TransportCredentials {
	t.Helper()
	cfg := transport.ClientConfig{CAFile: files.CAFile, CertFile: files.CertFile, KeyFile: files.KeyFile, NetworkID: network, ServerName: serverName}
	r, err := cfg.NewClientReloader(transport.ReloaderOptions{Interval: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	c, err := cfg.Credentials(r, expect)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func voterExpect(id string) transport.Expect {
	return transport.Expect{Roles: []transport.Role{transport.RoleVoter}, ID: id}
}

func TestDualServesPlaintextAndTLSOnOnePort(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	srv := startServer(t, transport.ModeDual, transporttest.Node(t, ca, voterID("v0")), nil)

	if err := check(t, srv.addr, insecure.NewCredentials()); err != nil {
		t.Fatalf("plaintext: %v", err)
	}
	if p := srv.rec.last(); p.TLS || p.HasIdentity || p.RoleLabel() != "anonymous" {
		t.Fatalf("plaintext peer = %+v", p)
	}

	client := transporttest.Node(t, ca, snodeID("s1"))
	if err := check(t, srv.addr, clientCreds(t, client, voterExpect("v0"), "localhost")); err != nil {
		t.Fatalf("tls: %v", err)
	}
	if p := srv.rec.last(); !p.TLS || !p.HasIdentity || p.Identity != snodeID("s1") {
		t.Fatalf("tls peer = %+v", p)
	}

	// TLS without a client certificate is allowed in dual mode.
	anon := transporttest.Write(t, t.TempDir(), transporttest.Issued{}, ca)
	if err := check(t, srv.addr, clientCreds(t, anon, voterExpect("v0"), "localhost")); err != nil {
		t.Fatalf("anonymous tls: %v", err)
	}
	if p := srv.rec.last(); !p.TLS || p.HasIdentity {
		t.Fatalf("anonymous tls peer = %+v", p)
	}
}

func TestTLSModeRefusesPlaintext(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	srv := startServer(t, transport.ModeTLS, transporttest.Node(t, ca, voterID("v0")), nil)
	if err := check(t, srv.addr, insecure.NewCredentials()); err == nil {
		t.Fatal("plaintext served in tls mode")
	}
	waitFor(t, func() bool { return contains(srv.refusals(), transport.RefusedPlaintext) })
	anon := transporttest.Write(t, t.TempDir(), transporttest.Issued{}, ca)
	if err := check(t, srv.addr, clientCreds(t, anon, voterExpect("v0"), "localhost")); err != nil {
		t.Fatalf("tls without client cert in tls mode: %v", err)
	}
}

func TestMTLS(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	other := transporttest.NewCA(t, "other")
	srv := startServer(t, transport.ModeMTLS, transporttest.Node(t, ca, voterID("v0")), nil)

	good := transporttest.Node(t, ca, snodeID("s1"))
	if err := check(t, srv.addr, clientCreds(t, good, voterExpect("v0"), "localhost")); err != nil {
		t.Fatalf("mtls: %v", err)
	}

	cases := map[string]transport.Files{
		"no client certificate": transporttest.Write(t, t.TempDir(), transporttest.Issued{}, ca),
		"client from another CA": transporttest.Node(t, other, snodeID("s1"), ca),
		"client of another network": transporttest.Node(t, ca,
			transport.Identity{NetworkID: "mainnet", Role: transport.RoleSNode, ID: "s1"}),
	}
	for name, files := range cases {
		t.Run(name, func(t *testing.T) {
			if err := check(t, srv.addr, clientCreds(t, files, voterExpect("v0"), "localhost")); err == nil {
				t.Fatal("served")
			}
		})
	}
	// A Reloader refuses to load a certificate naming two identities, so
	// present one by hand: the server must refuse it too.
	two := ca.Issue(t, transporttest.Leaf{URIs: []string{snodeID("s1").String(), snodeID("s2").String()}})
	pair, err := tls.X509KeyPair(two.CertPEM, two.KeyPEM)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca.Cert)
	raw := credentials.NewTLS(&tls.Config{Certificates: []tls.Certificate{pair}, RootCAs: roots, ServerName: "localhost", MinVersion: tls.VersionTLS13})
	if err := check(t, srv.addr, raw); err == nil {
		t.Fatal("served a client certificate naming two identities")
	}
}

func TestClientVerifiesServer(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	other := transporttest.NewCA(t, "other")
	client := transporttest.Node(t, ca, snodeID("s1"))

	cases := map[string]struct {
		server     transport.Files
		expect     transport.Expect
		serverName string
	}{
		"server is another voter": {transporttest.Node(t, ca, voterID("v1")), voterExpect("v0"), "localhost"},
		"server is an snode":      {transporttest.Node(t, ca, snodeID("v0")), voterExpect("v0"), "localhost"},
		"server of another network": {transporttest.Node(t, ca,
			transport.Identity{NetworkID: "mainnet", Role: transport.RoleVoter, ID: "v0"}), voterExpect("v0"), "localhost"},
		"server from another CA": {transporttest.Node(t, other, voterID("v0"), other), voterExpect("v0"), "localhost"},
		"server name mismatch":   {transporttest.Node(t, ca, voterID("v0")), voterExpect("v0"), "voter-0.example"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			srv := startServer(t, transport.ModeDual, c.server, nil)
			if err := check(t, srv.addr, clientCreds(t, client, c.expect, c.serverName)); err == nil {
				t.Fatal("client accepted the server")
			}
		})
	}
}

func dialRaw(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// readsEOF reports whether the server closes c within d.
func closedWithin(c net.Conn, d time.Duration) bool {
	_ = c.SetReadDeadline(time.Now().Add(d))
	var b [64]byte
	for {
		_, err := c.Read(b[:])
		if err == nil {
			continue
		}
		var ne net.Error
		return !(errors.As(err, &ne) && ne.Timeout())
	}
}

func TestSlowloris(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	files := transporttest.Node(t, ca, voterID("v0"))
	srv := startServer(t, transport.ModeDual, files, func(o *transport.ListenerOptions) {
		o.FirstByteTimeout = 200 * time.Millisecond
		o.HandshakeTimeout = 300 * time.Millisecond
	})

	// Clients that send nothing, and clients that open a TLS record and stall.
	var silent, stalled []net.Conn
	for i := 0; i < 20; i++ {
		silent = append(silent, dialRaw(t, srv.addr))
		c := dialRaw(t, srv.addr)
		if _, err := c.Write([]byte{0x16, 0x03, 0x01}); err != nil {
			t.Fatal(err)
		}
		stalled = append(stalled, c)
	}

	// They do not hold up anyone else.
	start := time.Now()
	if err := check(t, srv.addr, insecure.NewCredentials()); err != nil {
		t.Fatalf("plaintext while stalled: %v", err)
	}
	if err := check(t, srv.addr, clientCreds(t, transporttest.Node(t, ca, snodeID("s1")), voterExpect("v0"), "localhost")); err != nil {
		t.Fatalf("tls while stalled: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 150*time.Millisecond {
		t.Fatalf("good clients waited %v behind stalled ones", elapsed)
	}

	// And they are closed when their timeout expires.
	for _, c := range silent {
		if !closedWithin(c, 2*time.Second) {
			t.Fatal("silent connection not closed after the first-byte timeout")
		}
	}
	for _, c := range stalled {
		if !closedWithin(c, 2*time.Second) {
			t.Fatal("stalled handshake not closed after the handshake timeout")
		}
	}
	r := srv.refusals()
	if count(r, transport.RefusedFirstByte) < 20 || count(r, transport.RefusedHandshake) < 20 {
		t.Fatalf("refusals = %v", r)
	}
}

func TestDefaultsCannotBeDisabled(t *testing.T) {
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ca := transporttest.NewCA(t, "ca")
	cfg, err := transport.ServerTLSConfig(reloader(t, transporttest.Node(t, ca, voterID("v0"))), transport.ServerOptions{Mode: transport.ModeDual, NetworkID: network})
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var refused []string
	lis, err := transport.NewListener(inner, transport.ListenerOptions{Mode: transport.ModeDual, TLS: cfg, FirstByteTimeout: -1, HandshakeTimeout: 0,
		OnRefused: func(r string) { mu.Lock(); refused = append(refused, r); mu.Unlock() }})
	if err != nil {
		t.Fatal(err)
	}
	defer lis.Close()
	c := dialRaw(t, inner.Addr().String())
	if !closedWithin(c, transport.DefaultFirstByteTimeout+3*time.Second) {
		t.Fatal("silent connection kept past the default first-byte timeout")
	}
}

func TestPendingCap(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	srv := startServer(t, transport.ModeDual, transporttest.Node(t, ca, voterID("v0")), func(o *transport.ListenerOptions) {
		o.MaxPending = 2
		o.FirstByteTimeout = 2 * time.Second
	})
	dialRaw(t, srv.addr)
	dialRaw(t, srv.addr)
	third := dialRaw(t, srv.addr)
	if !closedWithin(third, time.Second) {
		t.Fatal("connection beyond the pending cap was kept")
	}
	waitFor(t, func() bool { return contains(srv.refusals(), transport.RefusedPending) })
}

func TestPerIPCap(t *testing.T) {
	srv := startServer(t, transport.ModeOff, transport.Files{}, func(o *transport.ListenerOptions) { o.MaxConnsPerIP = 1 })
	first := dialRaw(t, srv.addr)
	second := dialRaw(t, srv.addr)
	if !closedWithin(second, time.Second) {
		t.Fatal("second connection from one IP was kept")
	}
	waitFor(t, func() bool { return contains(srv.refusals(), transport.RefusedPerIP) })
	_ = first.Close()
	// The slot is released when the connection closes (the gRPC server closes
	// its side once the client is gone).
	waitFor(t, func() bool { return check(t, srv.addr, insecure.NewCredentials()) == nil })
}

func TestOffModeIsPlaintext(t *testing.T) {
	srv := startServer(t, transport.ModeOff, transport.Files{}, nil)
	if err := check(t, srv.addr, insecure.NewCredentials()); err != nil {
		t.Fatal(err)
	}
}

func TestCARotation(t *testing.T) {
	oldCA := transporttest.NewCA(t, "old")
	newCA := transporttest.NewCA(t, "new")
	dir := t.TempDir()
	// Step 1: server on an old leaf, both trust the double bundle.
	serverFiles := transporttest.Write(t, dir, oldCA.Issue(t, transporttest.Leaf{Identity: ptr(voterID("v0")), DNSNames: []string{"localhost"}}), oldCA, newCA)
	srv := startServer(t, transport.ModeMTLS, serverFiles, nil)
	client := transporttest.Node(t, newCA, snodeID("s1"), oldCA, newCA)
	if err := check(t, srv.addr, clientCreds(t, client, voterExpect("v0"), "localhost")); err != nil {
		t.Fatalf("mixed leaves under the double bundle: %v", err)
	}
	// Step 3: server renewed from the new CA, bundle narrowed to the new CA,
	// without a restart.
	transporttest.Write(t, dir, newCA.Issue(t, transporttest.Leaf{Identity: ptr(voterID("v0")), DNSNames: []string{"localhost"}}), newCA)
	time.Sleep(5 * time.Millisecond)
	oldClient := transporttest.Node(t, oldCA, snodeID("s2"), oldCA, newCA)
	waitFor(t, func() bool {
		return check(t, srv.addr, clientCreds(t, oldClient, voterExpect("v0"), "localhost")) != nil
	})
	if err := check(t, srv.addr, clientCreds(t, client, voterExpect("v0"), "localhost")); err != nil {
		t.Fatalf("new leaf after rotation: %v", err)
	}
}

func TestServerConfigRequiresCertificate(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	r := reloader(t, transporttest.Write(t, t.TempDir(), transporttest.Issued{}, ca))
	if _, err := transport.ServerTLSConfig(r, transport.ServerOptions{Mode: transport.ModeDual, NetworkID: network}); err == nil {
		t.Fatal("server config without a certificate")
	}
	if _, err := transport.ServerTLSConfig(r, transport.ServerOptions{Mode: transport.ModeOff, NetworkID: network}); err == nil {
		t.Fatal("server config for mode off")
	}
}

func TestReloaderKeepsLastGoodMaterial(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	files := transporttest.Node(t, ca, voterID("v0"))
	var errs []error
	var mu sync.Mutex
	r, err := transport.NewReloader(files, transport.ReloaderOptions{Interval: time.Millisecond, OnError: func(err error) {
		mu.Lock()
		errs = append(errs, err)
		mu.Unlock()
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(files.CertFile, []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if id, ok := r.Identity(); !ok || id != voterID("v0") {
		t.Fatalf("identity after a bad reload = %v %v", id, ok)
	}
	mu.Lock()
	n := len(errs)
	mu.Unlock()
	if n == 0 {
		t.Fatal("bad reload not reported")
	}
	if r.CACount() != 1 {
		t.Fatalf("ca count = %d", r.CACount())
	}
	// Raw TLS handshake still uses the old certificate.
	cfg, err := transport.ServerTLSConfig(r, transport.ServerOptions{Mode: transport.ModeTLS, NetworkID: network})
	if err != nil {
		t.Fatal(err)
	}
	sc, cc := net.Pipe()
	go func() { _ = tls.Server(sc, cfg).Handshake() }()
	clientCfg, err := transport.ClientTLSConfig(r, transport.Expect{NetworkID: network, Roles: []transport.Role{transport.RoleVoter}, ID: "v0"}, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if err := tls.Client(cc, clientCfg).Handshake(); err != nil {
		t.Fatal(err)
	}
}

func ptr[T any](v T) *T { return &v }

func contains(s []string, v string) bool { return count(s, v) > 0 }

func count(s []string, v string) int {
	n := 0
	for _, x := range s {
		if x == v {
			n++
		}
	}
	return n
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestReloaderRefusesAmbiguousOwnCertificate(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	two := ca.Issue(t, transporttest.Leaf{URIs: []string{snodeID("s1").String(), snodeID("s2").String()}})
	if _, err := transport.NewReloader(transporttest.Write(t, t.TempDir(), two, ca), transport.ReloaderOptions{}); err == nil {
		t.Fatal("loaded a certificate naming two identities")
	}
	if _, err := transport.NewReloader(transport.Files{CAFile: "x", CertFile: "y"}, transport.ReloaderOptions{}); err == nil {
		t.Fatal("cert_file without key_file accepted")
	}
}
