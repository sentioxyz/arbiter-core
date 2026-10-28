package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// maxResponseBytes bounds an answer of the endpoint.
const maxResponseBytes = 4 << 20

var (
	// ErrLeaseRevoked: renew was refused because the member revoked the
	// lease (409) or no longer knows it (404: it expired or was replaced);
	// the member's text says which. The host stops answering the database
	// Replica.
	ErrLeaseRevoked = errors.New("serve: lease revoked")
	// ErrNotServed: acquire was refused; the database is not served, or it
	// moved to another snapshot (the message says which).
	ErrNotServed = errors.New("serve: database not served")
	// ErrUnauthorized: the member refused the bearer token.
	ErrUnauthorized = errors.New("serve: unauthorized")
)

// Lease is a granted or renewed lease. TTL counts from the moment the request
// was sent; a host's deadline is that moment plus TTL.
type Lease struct {
	ID  string
	TTL time.Duration
}

// Client calls a member's serving endpoint. Its errors never carry the token.
type Client struct {
	base  string
	token string
	http  *http.Client
}

// NewClient returns a client of the endpoint at baseURL (http://host:port).
// A nil httpClient is an http.Client with a 5s timeout.
func NewClient(baseURL, token string, httpClient *http.Client) (*Client, error) {
	if token == "" {
		return nil, errors.New("serve: a bearer token is required")
	}
	u, err := url.Parse(baseURL)
	if err != nil || u.Scheme != "http" || u.Hostname() == "" || u.User != nil ||
		(u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" || !validPort(u.Port()) {
		return nil, fmt.Errorf("serve: member serving URL %q is not http://host:port", baseURL)
	}
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 5 * time.Second}
	}
	// A redirect to another port of the same hostname otherwise forwards
	// Authorization. Keep the token at the configured member endpoint and
	// leave the caller's HTTP client unchanged.
	hc := *httpClient
	hc.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &Client{base: "http://" + u.Host, token: token, http: &hc}, nil
}

func validPort(port string) bool {
	n, err := strconv.ParseUint(port, 10, 16)
	return err == nil && n != 0
}

// Serving reads the member's serving view.
func (c *Client) Serving(ctx context.Context) (ServingView, error) {
	var view ServingView
	status, msg, err := c.do(ctx, http.MethodGet, "/v1/serving", nil, &view)
	if err != nil {
		return ServingView{}, err
	}
	if status != http.StatusOK {
		return ServingView{}, c.statusError(http.MethodGet, "/v1/serving", status, msg)
	}
	return view, nil
}

// Acquire asks for a lease of database at snapshotID (the view's
// SnapshotID of that database).
func (c *Client) Acquire(ctx context.Context, database, snapshotID string) (Lease, error) {
	var out acquireResponse
	status, msg, err := c.do(ctx, http.MethodPost, "/v1/serving/acquire", acquireRequest{Database: database, SnapshotID: snapshotID}, &out)
	switch {
	case err != nil:
		return Lease{}, err
	case status == http.StatusConflict:
		return Lease{}, fmt.Errorf("%w: %s", ErrNotServed, msg)
	case status != http.StatusOK:
		return Lease{}, c.statusError(http.MethodPost, "/v1/serving/acquire", status, msg)
	case len(out.LeaseID) != 32 || strings.Trim(out.LeaseID, "0123456789abcdef") != "" || out.TTLMs <= 0 || out.TTLMs > maxLeaseTTLMs:
		return Lease{}, fmt.Errorf("serve: acquire answered lease %q with ttl %dms", strings.ReplaceAll(out.LeaseID, c.token, "<redacted>"), out.TTLMs)
	}
	return Lease{ID: out.LeaseID, TTL: time.Duration(out.TTLMs) * time.Millisecond}, nil
}

// Renew extends a lease by its TTL.
func (c *Client) Renew(ctx context.Context, leaseID string) (Lease, error) {
	var out renewResponse
	status, msg, err := c.do(ctx, http.MethodPost, "/v1/serving/renew", leaseRequest{LeaseID: leaseID}, &out)
	switch {
	case err != nil:
		return Lease{}, err
	case status == http.StatusConflict || status == http.StatusNotFound:
		return Lease{}, fmt.Errorf("%w: %s", ErrLeaseRevoked, msg)
	case status != http.StatusOK:
		return Lease{}, c.statusError(http.MethodPost, "/v1/serving/renew", status, msg)
	case out.TTLMs <= 0 || out.TTLMs > maxLeaseTTLMs:
		return Lease{}, fmt.Errorf("serve: renew answered ttl %dms", out.TTLMs)
	}
	return Lease{ID: leaseID, TTL: time.Duration(out.TTLMs) * time.Millisecond}, nil
}

// Release gives a lease back; an unknown or released id is not an error.
func (c *Client) Release(ctx context.Context, leaseID string) error {
	status, msg, err := c.do(ctx, http.MethodPost, "/v1/serving/release", leaseRequest{LeaseID: leaseID}, &struct{}{})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return c.statusError(http.MethodPost, "/v1/serving/release", status, msg)
	}
	return nil
}

func (c *Client) statusError(method, path string, status int, msg string) error {
	if status == http.StatusUnauthorized {
		return fmt.Errorf("%w: %s", ErrUnauthorized, msg)
	}
	return fmt.Errorf("serve: %s %s answered %d: %s", method, path, status, msg)
}

// do sends one request and decodes a 200 answer into out; any other answer
// returns its status and error text.
// redactedClientError preserves errors.Is (including cancellation) while
// refusing to print a credential echoed by an HTTP transport or a peer.
type redactedClientError struct {
	cause error
	token string
}

func (e *redactedClientError) Error() string {
	return strings.ReplaceAll(e.cause.Error(), e.token, "<redacted>")
}
func (e *redactedClientError) Unwrap() error { return e.cause }

func (c *Client) do(ctx context.Context, method, path string, in, out any) (status int, message string, err error) {
	defer func() {
		message = strings.ReplaceAll(message, c.token, "<redacted>")
		if err != nil {
			err = &redactedClientError{cause: err, token: c.token}
		}
	}()
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return 0, "", fmt.Errorf("serve: %s %s: %w", method, path, err)
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return 0, "", fmt.Errorf("serve: %s %s: %w", method, path, err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("serve: %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil {
		return 0, "", fmt.Errorf("serve: %s %s: %w", method, path, err)
	}
	if len(data) > maxResponseBytes {
		return 0, "", fmt.Errorf("serve: %s %s answered a body larger than %d bytes", method, path, maxResponseBytes)
	}
	if resp.StatusCode != http.StatusOK {
		var e errorResponse
		if json.Unmarshal(data, &e) != nil || e.Error == "" {
			e.Error = http.StatusText(resp.StatusCode)
		}
		return resp.StatusCode, e.Error, nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return 0, "", fmt.Errorf("serve: %s %s answered an undecodable body: %w", method, path, err)
	}
	return resp.StatusCode, "", nil
}
