package dataplane

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/health"

	"github.com/sentioxyz/arbiter-core/transport"
	"github.com/sentioxyz/arbiter-core/transport/transporttest"
)

func startTLSVoter(t *testing.T, ca *transporttest.CA, id string) string {
	t.Helper()
	files := transporttest.Node(t, ca, transport.Identity{NetworkID: "devnet2", Role: transport.RoleVoter, ID: id})
	r, err := transport.NewReloader(files, transport.ReloaderOptions{})
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := transport.ServerTLSConfig(r, transport.ServerOptions{Mode: transport.ModeTLS, NetworkID: "devnet2", NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	lis, err := transport.NewListener(inner, transport.ListenerOptions{Mode: transport.ModeTLS, TLS: cfg})
	if err != nil {
		t.Fatal(err)
	}
	g := grpc.NewServer(grpc.Creds(transport.ServerCredentials()))
	healthpb.RegisterHealthServer(g, health.NewServer())
	go func() { _ = g.Serve(lis) }()
	t.Cleanup(g.Stop)
	return inner.Addr().String()
}

func TestClientTLSPinsThePeerVoter(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	addr := startTLSVoter(t, ca, "v0")
	clientFiles := transporttest.Node(t, ca, transport.Identity{NetworkID: "devnet2", Role: transport.RoleSNode, ID: "s1"})
	tlsCfg := &transport.ClientConfig{CAFile: clientFiles.CAFile, CertFile: clientFiles.CertFile, KeyFile: clientFiles.KeyFile, NetworkID: "devnet2"}

	call := func(peerID string) error {
		c, err := New(Config{Peers: []Peer{{ID: peerID, GRPCAddr: addr}}, TLS: tlsCfg, DialTimeout: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		return c.WithLeaderRetry(ctx, func(ctx context.Context, conn *grpc.ClientConn) error {
			_, err := healthpb.NewHealthClient(conn).Check(ctx, &healthpb.HealthCheckRequest{})
			return err
		})
	}
	if err := call("v0"); err != nil {
		t.Fatalf("peer v0 over TLS: %v", err)
	}
	if err := call("v1"); err == nil {
		t.Fatal("peer entry v1 accepted the server certified as v0")
	}
}

func TestClientTLSConfigValidated(t *testing.T) {
	if _, err := New(Config{Peers: []Peer{{ID: "v0", GRPCAddr: "127.0.0.1:1"}}, TLS: &transport.ClientConfig{NetworkID: "devnet2"}}); err == nil {
		t.Fatal("TLS without a CA file accepted")
	}
}
