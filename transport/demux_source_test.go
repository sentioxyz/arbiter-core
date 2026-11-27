package transport_test

import (
	"net"
	"net/netip"
	"testing"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"github.com/sentioxyz/arbiter-core/transport"
	"github.com/sentioxyz/arbiter-core/transport/transporttest"
)

func TestDualRefusesPlaintextFromOutsideTheSources(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	srv := startServer(t, transport.ModeDual, transporttest.Node(t, ca, voterID("v0")), func(o *transport.ListenerOptions) {
		o.PlaintextSources = []netip.Prefix{netip.MustParsePrefix("10.0.0.0/8")}
	})
	if err := check(t, srv.addr, insecure.NewCredentials()); err == nil {
		t.Fatal("plaintext from 127.0.0.1 served with sources 10.0.0.0/8")
	}
	waitFor(t, func() bool { return contains(srv.refusals(), transport.RefusedPlaintextSource) })
	// TLS from the same source is not affected.
	client := transporttest.Node(t, ca, snodeID("s1"))
	if err := check(t, srv.addr, clientCreds(t, client, voterExpect("v0"), "localhost")); err != nil {
		t.Fatalf("tls: %v", err)
	}
}

func TestDualServesPlaintextFromTheSources(t *testing.T) {
	ca := transporttest.NewCA(t, "ca")
	srv := startServer(t, transport.ModeDual, transporttest.Node(t, ca, voterID("v0")), func(o *transport.ListenerOptions) {
		o.PlaintextSources = []netip.Prefix{netip.MustParsePrefix("127.0.0.1/32")}
	})
	if err := check(t, srv.addr, insecure.NewCredentials()); err != nil {
		t.Fatalf("plaintext from a listed source: %v", err)
	}
}

func TestDualDefaultServesIPv6Loopback(t *testing.T) {
	inner, err := net.Listen("tcp", "[::1]:0")
	if err != nil {
		t.Skipf("no IPv6 loopback: %v", err)
	}
	_ = inner.Close()
	ca := transporttest.NewCA(t, "ca")
	files := transporttest.Node(t, ca, voterID("v0"))
	cfg, err := transport.ServerTLSConfig(reloader(t, files), transport.ServerOptions{Mode: transport.ModeDual, NetworkID: network, NextProtos: []string{"h2"}})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		sources []netip.Prefix
		serve   bool
	}{
		{"default", nil, true},
		{"ipv4 loopback only", []netip.Prefix{netip.MustParsePrefix("127.0.0.0/8")}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inner, err := net.Listen("tcp", "[::1]:0")
			if err != nil {
				t.Fatal(err)
			}
			lis, err := transport.NewListener(inner, transport.ListenerOptions{Mode: transport.ModeDual, TLS: cfg, PlaintextSources: tc.sources})
			if err != nil {
				t.Fatal(err)
			}
			defer lis.Close()
			go func() {
				for {
					c, err := lis.Accept()
					if err != nil {
						return
					}
					_, _ = c.Write([]byte("ok"))
					_ = c.Close()
				}
			}()
			c, err := net.Dial("tcp", inner.Addr().String())
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			if _, err := c.Write([]byte("PRI ")); err != nil {
				t.Fatal(err)
			}
			_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
			buf := make([]byte, 2)
			_, err = c.Read(buf)
			if served := err == nil && string(buf) == "ok"; served != tc.serve {
				t.Fatalf("served=%t (err %v), want %t", served, err, tc.serve)
			}
		})
	}
}

func TestDefaultPlaintextSources(t *testing.T) {
	in := func(s string) bool {
		a := netip.MustParseAddr(s)
		for _, p := range transport.DefaultPlaintextSources() {
			if p.Contains(a) {
				return true
			}
		}
		return false
	}
	for _, s := range []string{"127.0.0.1", "127.8.9.10", "::1", "10.234.7.245", "172.16.0.1", "172.31.255.255", "192.168.1.1", "fd00::1"} {
		if !in(s) {
			t.Errorf("%s not in the default plaintext sources", s)
		}
	}
	for _, s := range []string{"8.8.8.8", "172.32.0.1", "100.64.0.1", "169.254.1.1", "2001:db8::1", "fe80::1"} {
		if in(s) {
			t.Errorf("%s in the default plaintext sources", s)
		}
	}
}

func TestParsePlaintextSources(t *testing.T) {
	got, err := transport.ParsePlaintextSources([]string{"10.1.2.3/8", "192.0.2.7", "::1", "0.0.0.0/0"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"10.0.0.0/8", "192.0.2.7/32", "::1/128", "0.0.0.0/0"}
	if len(got) != len(want) {
		t.Fatalf("got %v", got)
	}
	for i := range want {
		if got[i].String() != want[i] {
			t.Errorf("entry %d = %s, want %s", i, got[i], want[i])
		}
	}
	if got, err := transport.ParsePlaintextSources(nil); err != nil || got != nil {
		t.Fatalf("empty: %v %v", got, err)
	}
	for _, bad := range []string{"10.0.0.0/33", "example.com", "", "::ffff:10.0.0.0/104"} {
		if _, err := transport.ParsePlaintextSources([]string{bad}); err == nil {
			t.Errorf("%q accepted", bad)
		}
	}
}
