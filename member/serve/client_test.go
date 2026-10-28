package serve

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

// polledServer is a Server of the test world (owner 0 quarantined, the copy
// at entry40) polled twice by hand, so dex serves, behind a real listener.
func polledServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()
	reg, pub := &fakeRegistry{}, &fakePublished{}
	copies := &fakeCopies{copies: map[OwnerDatabase]DatabaseCopy{}}
	reg.set(registrySnapshot(testOwner))
	pub.set(manifestAt("0xs40", entry40), nil)
	copies.set(testOwner, testDatabase, copyAt("0xs40", entry40))
	srv, err := New(testConfig(), Deps{Registry: reg, Published: pub, Copies: copies, Logger: slog.New(slog.DiscardHandler), NodeID: "member-b", StateDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	srv.poll(t.Context())
	srv.poll(t.Context())
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	return srv, ts
}

// TestClientRoundTripsTheServingView: serve.Client reads the server's view
// unchanged over HTTP and maps every answer of the endpoint to its result or
// error: a released or revoked lease, a database that is not served, a wrong
// token and an unreachable member; no error carries a token.
func TestClientRoundTripsTheServingView(t *testing.T) {
	srv, ts := polledServer(t)
	c, err := NewClient(ts.URL, testToken, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	ctx := t.Context()

	view, err := c.Serving(ctx)
	if err != nil {
		t.Fatalf("Serving: %v", err)
	}
	if want := srv.Serving(); !reflect.DeepEqual(view, want) || len(view.Databases) != 1 || !view.Databases[0].Serving {
		t.Fatalf("client view %+v, server view %+v", view, want)
	}

	l, err := c.Acquire(ctx, "dex", "0xs40")
	if err != nil || len(l.ID) != 32 || l.TTL != DefaultLeaseTTL {
		t.Fatalf("Acquire: %+v %v", l, err)
	}
	if view, _ = c.Serving(ctx); view.Databases[0].Lease == nil || view.Databases[0].Lease.LeaseID != l.ID {
		t.Fatalf("the view does not show lease %s: %+v", l.ID, view.Databases[0].Lease)
	}
	if r, err := c.Renew(ctx, l.ID); err != nil || r != l {
		t.Fatalf("Renew: %+v %v, want %+v", r, err, l)
	}
	if err := c.Release(ctx, l.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	if err := c.Release(ctx, l.ID); err != nil {
		t.Fatalf("a second Release: %v", err)
	}
	_, err = c.Renew(ctx, l.ID)
	if !errors.Is(err, ErrLeaseRevoked) || !strings.Contains(err.Error(), "member serving: unknown lease "+l.ID) {
		t.Fatalf("Renew of a released lease: %v, want ErrLeaseRevoked with the member's 404 text", err)
	}

	// A revoked lease: the owner leaves quarantine.
	l, err = c.Acquire(ctx, "dex", "0xs40")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	srv.reg.(*fakeRegistry).set(registrySnapshot())
	srv.poll(ctx)
	_, err = c.Renew(ctx, l.ID)
	if !errors.Is(err, ErrLeaseRevoked) || !strings.Contains(err.Error(), "member serving: lease "+l.ID+" is revoked") {
		t.Fatalf("Renew of a revoked lease: %v, want ErrLeaseRevoked with the member's 409 text", err)
	}
	_, err = c.Acquire(ctx, "dex", "0xs40")
	if !errors.Is(err, ErrNotServed) || !strings.Contains(err.Error(), "member serving: database dex is not served: owner_available") {
		t.Fatalf("Acquire of a database that is not served: %v", err)
	}

	// A wrong token is ErrUnauthorized on every call, and no error names it.
	bad, err := NewClient(ts.URL+"/", "another-token", nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	for name, call := range map[string]func() error{
		"Serving": func() error { _, err := bad.Serving(ctx); return err },
		"Acquire": func() error { _, err := bad.Acquire(ctx, "dex", "0xs40"); return err },
		"Renew":   func() error { _, err := bad.Renew(ctx, l.ID); return err },
		"Release": func() error { return bad.Release(ctx, l.ID) },
	} {
		err := call()
		if !errors.Is(err, ErrUnauthorized) || strings.Contains(err.Error(), "another-token") || strings.Contains(err.Error(), testToken) {
			t.Errorf("%s with a wrong token: %v", name, err)
		}
	}

	// An unreachable member is a transport error that names neither token.
	ts.Close()
	if _, err := c.Serving(ctx); err == nil || strings.Contains(err.Error(), testToken) {
		t.Fatalf("Serving of a closed endpoint: %v", err)
	}
}

// TestNewClientRefusesABadURLOrNoToken: a client needs a token and an
// http://host:port base URL.
func TestNewClientRefusesABadURLOrNoToken(t *testing.T) {
	if _, err := NewClient("http://127.0.0.1:9096", "", nil); err == nil || err.Error() != "serve: a bearer token is required" {
		t.Fatalf("no token: %v", err)
	}
	for _, url := range []string{
		"", "127.0.0.1:9096", "https://127.0.0.1:9096", "http://127.0.0.1", "http://127.0.0.1:0", "http://127.0.0.1:http",
		"http://:9096", "http://127.0.0.1:9096/v1", "http://u:p@127.0.0.1:9096", "http://127.0.0.1:9096?x=1",
	} {
		_, err := NewClient(url, testToken, nil)
		if want := `serve: member serving URL "` + url + `" is not http://host:port`; err == nil || err.Error() != want {
			t.Errorf("NewClient(%q): %v, want %s", url, err, want)
		}
	}
	for _, url := range []string{"http://127.0.0.1:9096", "http://127.0.0.1:9096/", "http://sentio-node-devnet2-indexer-b-si-member:9096"} {
		c, err := NewClient(url, testToken, nil)
		if err != nil || c.http.Timeout != 5*time.Second {
			t.Errorf("NewClient(%q): %v", url, err)
		}
	}
}

// TestClientNeverFollowsAnEndpointRedirect keeps the bearer token at the
// configured endpoint even when another server shares its hostname.
func TestClientNeverFollowsAnEndpointRedirect(t *testing.T) {
	leaked := false
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		leaked = r.Header.Get("Authorization") != ""
		_, _ = w.Write([]byte(`{"node_id":"other","databases":[]}`))
	}))
	defer other.Close()
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusTemporaryRedirect)
	}))
	defer endpoint.Close()
	c, err := NewClient(endpoint.URL, testToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Serving(t.Context())
	if err == nil || leaked {
		t.Fatalf("redirect: err %v, credential forwarded %t", err, leaked)
	}
}

type tokenErrorTransport struct{}

func (tokenErrorTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return nil, fmt.Errorf("transport refused %s", r.Header.Get("Authorization"))
}

// TestClientErrorsRedactTheToken covers remote errors and errors returned by
// an injected HTTP transport, not only the endpoint's fixed 401 message.
func TestClientErrorsRedactTheToken(t *testing.T) {
	endpoint := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = fmt.Fprintf(w, `{"error":%q}`, "invalid token "+testToken)
	}))
	defer endpoint.Close()
	for name, hc := range map[string]*http.Client{"remote": nil, "transport": {Transport: tokenErrorTransport{}}} {
		t.Run(name, func(t *testing.T) {
			c, err := NewClient(endpoint.URL, testToken, hc)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Serving(context.Background())
			if err == nil || strings.Contains(err.Error(), testToken) {
				t.Fatal("client error exposed its bearer token")
			}
		})
	}
}

// TestClientRejectsMalformedLeaseAnswers refuses duration overflow and a
// credential reflected as a malformed lease id, without printing it.
func TestClientRejectsMalformedLeaseAnswers(t *testing.T) {
	for name, body := range map[string]string{
		"overflow": `{"lease_id":"00112233445566778899aabbccddeeff","ttl_ms":9223372036854775807}`,
		"token id": `{"lease_id":"` + testToken + `","ttl_ms":30000}`,
	} {
		t.Run(name, func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(body)) }))
			defer ts.Close()
			c, err := NewClient(ts.URL, testToken, nil)
			if err != nil {
				t.Fatal(err)
			}
			_, err = c.Acquire(t.Context(), "dex", "s40")
			if err == nil || strings.Contains(err.Error(), testToken) {
				t.Fatal("malformed lease accepted or credential printed")
			}
		})
	}
}

func TestClientBoundsResponsesAndPreservesCancellation(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"databases":[]}` + strings.Repeat(" ", maxResponseBytes)))
	}))
	defer ts.Close()
	c, err := NewClient(ts.URL, testToken, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Serving(t.Context()); err == nil {
		t.Fatal("oversized response accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := c.Serving(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation identity lost: %v", err)
	}
}
