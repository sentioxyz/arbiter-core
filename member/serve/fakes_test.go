package serve

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/sentioxyz/arbiter-core/wire"
)

const testToken = "member-serving-test-token"

// The test world: owner 0 hosts database dex with the Active incarnations
// dex.swaps (seq 7) and dex.pools (seq 8); the member holds a copy of both.
const (
	testOwner    = uint64(0)
	testDatabase = "dex"
)

var (
	entry40 = replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 40, AppliedPromotionSeq: 12}
	entry41 = replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 41, AppliedPromotionSeq: 13}
)

func ownerID(id uint64) *uint64 { return &id }

// registrySnapshot is the registry of the test world; quarantined lists the
// quarantined indexers.
func registrySnapshot(quarantined ...uint64) wire.TableRegistrySnapshot {
	return wire.TableRegistrySnapshot{
		Version: 9,
		Incarnations: []wire.TableIncarnation{
			{Seq: 7, DatabaseID: "dex", TableID: "swaps", Origin: wire.TableOriginChain, Status: wire.TableStatusActive, SchemaHash: "0xswaps", AddBlockSeq: 30, OwnerIndexerID: ownerID(0)},
			{Seq: 8, DatabaseID: "dex", TableID: "pools", Origin: wire.TableOriginChain, Status: wire.TableStatusActive, SchemaHash: "0xpools", AddBlockSeq: 31, OwnerIndexerID: ownerID(0)},
			{Seq: 9, DatabaseID: "nft", TableID: "mints", Origin: wire.TableOriginChain, Status: wire.TableStatusActive, SchemaHash: "0xmints", AddBlockSeq: 32, OwnerIndexerID: ownerID(1)},
		},
		QuarantinedIndexers: quarantined,
	}
}

// manifestAt is a published manifest whose owner-0 entry is e.
func manifestAt(snapshotID string, e replay.TrackFrontier) replay.SafeSnapshotManifest {
	return replay.SafeSnapshotManifest{
		SnapshotID: snapshotID, ManifestRoot: snapshotID, SafeBlockSeq: e.SafeBlockSeq,
		TrackFrontiers: []replay.TrackFrontier{e, {IndexerID: 1, SafeBlockSeq: 39, AppliedPromotionSeq: 4}},
	}
}

// copyAt is the member's copy of dex at snapshotID and e, both tables installed.
func copyAt(snapshotID string, e replay.TrackFrontier) DatabaseCopy {
	return DatabaseCopy{
		SnapshotID: snapshotID, Entry: e, AuditClean: true,
		Tables: []TableCopy{
			{TableID: "dex.pools", IncarnationSeq: 8, CopyTable: "dex__pools__i8", SchemaHash: "0xpools", Condition: ConditionInstalled},
			{TableID: "dex.swaps", IncarnationSeq: 7, CopyTable: "dex__swaps__i7", SchemaHash: "0xswaps", Condition: ConditionInstalled},
		},
	}
}

type fakeRegistry struct {
	mu   sync.Mutex
	snap wire.TableRegistrySnapshot
	on   bool
}

func (r *fakeRegistry) View() (wire.TableRegistrySnapshot, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snap, r.on
}
func (r *fakeRegistry) Changed() <-chan struct{} { return make(chan struct{}) }
func (r *fakeRegistry) Ready() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}
func (r *fakeRegistry) set(snap wire.TableRegistrySnapshot) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.snap, r.on = snap, true
}

type fakePublished struct {
	mu    sync.Mutex
	m     replay.SafeSnapshotManifest
	err   error
	reads int
}

func (p *fakePublished) Latest(context.Context) (replay.SafeSnapshotManifest, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.reads++
	return p.m, p.err
}
func (p *fakePublished) readCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.reads
}
func (p *fakePublished) set(m replay.SafeSnapshotManifest, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.m, p.err = m, err
}

type fakeCopies struct {
	mu     sync.Mutex
	copies map[OwnerDatabase]DatabaseCopy
}

func (c *fakeCopies) Databases() []OwnerDatabase {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []OwnerDatabase
	for key := range c.copies {
		out = append(out, key)
	}
	slices.SortFunc(out, func(a, b OwnerDatabase) int { return strings.Compare(a.Database, b.Database) })
	return out
}
func (c *fakeCopies) Database(owner uint64, database string) (DatabaseCopy, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	d, ok := c.copies[OwnerDatabase{Owner: owner, Database: database}]
	return d, ok
}
func (c *fakeCopies) set(owner uint64, database string, d DatabaseCopy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.copies[OwnerDatabase{Owner: owner, Database: database}] = d
}

// fakeDialer answers every probe with err (nil: a connection that closes).
type fakeDialer struct {
	mu    sync.Mutex
	err   error
	dials int
}

func (d *fakeDialer) Dial(context.Context, string, string) (net.Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.dials++
	if d.err != nil {
		return nil, d.err
	}
	client, server := net.Pipe()
	_ = server.Close()
	return client, nil
}
func (d *fakeDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials
}
func (d *fakeDialer) set(err error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.err = err
}

var errRefused = errors.New("dial tcp 10.0.0.1:32001: connect: connection refused")

// harness runs one Server against the fakes. Inside a synctest bubble its
// clock is the bubble's.
type harness struct {
	t       *testing.T
	srv     *Server
	reg     *fakeRegistry
	pub     *fakePublished
	copies  *fakeCopies
	dialer  *fakeDialer
	metrics *Metrics
	// stateDir is the member's state directory, where the server records
	// its leases; restart builds the next server on the same one.
	stateDir string
	// stop ends the current server's Run.
	stop context.CancelFunc
}

// testConfig is serving on, the test token, polls every second.
func testConfig() Config {
	return Config{Enabled: true, Token: testToken, PollInterval: time.Second}
}

// newHarness builds the test world (owner 0 quarantined, entry40 published
// and copied), applies setup and starts Run with t.Context: its first poll
// runs at once, the next ones every PollInterval.
func newHarness(t *testing.T, cfg Config, setup ...func(*harness)) *harness {
	t.Helper()
	h := &harness{
		t: t, reg: &fakeRegistry{}, pub: &fakePublished{}, dialer: &fakeDialer{},
		copies: &fakeCopies{copies: map[OwnerDatabase]DatabaseCopy{}}, metrics: NewMetrics(),
		stateDir: t.TempDir(),
	}
	h.reg.set(registrySnapshot(testOwner))
	h.pub.set(manifestAt("0xs40", entry40), nil)
	h.copies.set(testOwner, testDatabase, copyAt("0xs40", entry40))
	for _, f := range setup {
		f(h)
	}
	h.start(cfg)
	return h
}

// start builds a server on the harness's ports and state directory and runs
// it until restart or the end of the test.
func (h *harness) start(cfg Config) {
	h.t.Helper()
	srv, err := New(cfg, Deps{
		Registry: h.reg, Published: h.pub, Copies: h.copies, Dial: h.dialer.Dial,
		Metrics: h.metrics, Logger: slog.New(slog.DiscardHandler), NodeID: "member-b",
		StateDir: h.stateDir,
	})
	if err != nil {
		h.t.Fatalf("New: %v", err)
	}
	ctx, stop := context.WithCancel(h.t.Context())
	h.srv, h.stop = srv, stop
	go func() { _ = srv.Run(ctx) }()
	synctest.Wait()
}

// restart stops the server as a crash would, losing everything it held in
// memory, and starts a new one on the same state directory: the si-member
// process restarting while its host keeps its lease.
func (h *harness) restart(cfg Config) {
	h.t.Helper()
	h.stop()
	synctest.Wait()
	h.metrics = NewMetrics()
	h.start(cfg)
}

// advance moves the bubble's clock by d and lets every goroutine settle.
func advance(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

// database is dex's entry of the serving view.
func (h *harness) database() DatabaseView {
	h.t.Helper()
	for _, d := range h.srv.Serving().Databases {
		if d.Database == testDatabase && d.Owner == testOwner {
			return d
		}
	}
	h.t.Fatalf("the serving view has no database %s of owner %d: %+v", testDatabase, testOwner, h.srv.Serving())
	return DatabaseView{}
}

func (h *harness) requireServing() {
	h.t.Helper()
	if d := h.database(); !d.Serving || d.Reason != "" {
		h.t.Fatalf("dex: serving %t reason %q, want serving", d.Serving, d.Reason)
	}
}

func (h *harness) requireReason(want Reason) {
	h.t.Helper()
	if d := h.database(); d.Serving || d.Reason != want {
		h.t.Fatalf("dex: serving %t reason %q, want not serving with %q", d.Serving, d.Reason, want)
	}
}

// call sends one request to the handler, with the test token unless
// token is set, and returns the status and the decoded JSON body.
func (h *harness) call(method, path, body string, token ...string) (int, map[string]any) {
	h.t.Helper()
	tok := testToken
	if len(token) > 0 {
		tok = token[0]
	}
	var rd io.Reader
	if body != "" {
		rd = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, rd)
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	rec := httptest.NewRecorder()
	h.srv.Handler().ServeHTTP(rec, req)
	var out map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(rec.Body.Bytes()), &out); err != nil {
		h.t.Fatalf("%s %s answered %d with a body that is not a JSON object: %q", method, path, rec.Code, rec.Body.String())
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		h.t.Fatalf("%s %s: Content-Type %q", method, path, ct)
	}
	return rec.Code, out
}

// acquire takes a lease of dex at snapshotID and returns its id.
func (h *harness) acquire(snapshotID string) string {
	h.t.Helper()
	status, out := h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"`+snapshotID+`"}`)
	if status != http.StatusOK {
		h.t.Fatalf("acquire: %d %v", status, out)
	}
	id, _ := out["lease_id"].(string)
	if len(id) != 32 || strings.Trim(id, "0123456789abcdef") != "" {
		h.t.Fatalf("lease id %q is not 128 bits of lowercase hex", id)
	}
	if ttl, _ := out["ttl_ms"].(float64); ttl != float64(h.srv.cfg.LeaseTTL.Milliseconds()) {
		h.t.Fatalf("ttl_ms %v, want %d", out["ttl_ms"], h.srv.cfg.LeaseTTL.Milliseconds())
	}
	return id
}

func (h *harness) renew(id string) (int, map[string]any) {
	h.t.Helper()
	return h.call(http.MethodPost, "/v1/serving/renew", `{"lease_id":"`+id+`"}`)
}

func (h *harness) release(id string) {
	h.t.Helper()
	if status, out := h.call(http.MethodPost, "/v1/serving/release", `{"lease_id":"`+id+`"}`); status != http.StatusOK || len(out) != 0 {
		h.t.Fatalf("release: %d %v, want 200 {}", status, out)
	}
}

// requireError checks a refusal's status and exact error text.
func requireError(t *testing.T, status int, out map[string]any, wantStatus int, wantText string) {
	t.Helper()
	if status != wantStatus || out["error"] != wantText || len(out) != 1 {
		t.Fatalf("answered %d %v, want %d {\"error\": %q}", status, out, wantStatus, wantText)
	}
}

// metricValues gathers m into "name{label=value}" (or "name") -> value.
func metricValues(t *testing.T, m *Metrics) map[string]float64 {
	t.Helper()
	reg := prometheus.NewPedanticRegistry()
	if err := m.Register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}
	families, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	out := map[string]float64{}
	for _, mf := range families {
		for _, metric := range mf.GetMetric() {
			key := mf.GetName()
			for _, l := range metric.GetLabel() {
				key += "{" + l.GetName() + "=" + l.GetValue() + "}"
			}
			out[key] = metric.GetGauge().GetValue() + metric.GetCounter().GetValue()
		}
	}
	return out
}

// requireMetrics checks the listed series' values.
func requireMetrics(t *testing.T, m *Metrics, want map[string]float64) {
	t.Helper()
	got := metricValues(t, m)
	for key, v := range want {
		if g, ok := got[key]; !ok || g != v {
			t.Errorf("%s = %v (present %t), want %v", key, g, ok, v)
		}
	}
}
