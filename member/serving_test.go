package member

import (
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/serve"
	"github.com/sentioxyz/arbiter-core/wire"
)

// serveTestRegistry is an enabled, empty registry.
type serveTestRegistry struct{}

func (serveTestRegistry) View() (wire.TableRegistrySnapshot, bool) {
	return wire.TableRegistrySnapshot{}, true
}
func (serveTestRegistry) Changed() <-chan struct{} { return make(chan struct{}) }
func (serveTestRegistry) Ready() <-chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}

// serveTestConn is a ClickHouse connection New never uses.
type serveTestConn struct{ clickhouse.Conn }

// serveTestJournal is an install journal of owner 0 (dex.pools, dex.swaps)
// and owner 2 (nft.mints).
type serveTestJournal struct {
	entries map[uint64]install.OwnerJournal
	tables  map[uint64][]install.TableState
	clean   bool
}

func (j serveTestJournal) Entry(owner uint64) (install.OwnerJournal, bool) {
	e, ok := j.entries[owner]
	return e, ok
}
func (j serveTestJournal) Tables(owner uint64) []install.TableState { return j.tables[owner] }
func (j serveTestJournal) Stats() install.Stats                     { return install.Stats{AuditClean: j.clean} }

var (
	serveEntry0 = replay.TrackFrontier{IndexerID: 0, SafeBlockSeq: 40, AppliedPromotionSeq: 12}
	serveEntry2 = replay.TrackFrontier{IndexerID: 2, SafeBlockSeq: 17, AppliedPromotionSeq: 3}
)

func newServeTestJournal() serveTestJournal {
	return serveTestJournal{
		clean: true,
		entries: map[uint64]install.OwnerJournal{
			0: {SnapshotID: "0xs40", Entry: serveEntry0},
			2: {SnapshotID: "0xs40", Entry: serveEntry2},
		},
		tables: map[uint64][]install.TableState{
			0: {
				{Owner: 0, TableKey: "dex.swaps", IncarnationSeq: 7, Copy: "dex__swaps__i7", SchemaHash: "0xswaps", Entry: serveEntry0, SnapshotID: "0xs40"},
				{Owner: 0, TableKey: "dex.pools", IncarnationSeq: 8, Copy: "dex__pools__i8", SchemaHash: "0xpools", Entry: serveEntry0, SnapshotID: "0xs40"},
			},
			2: {
				{Owner: 2, TableKey: "nft.mints", IncarnationSeq: 9, Copy: "nft__mints__i9", SchemaHash: "0xmints", Entry: serveEntry2, SnapshotID: "0xs40"},
			},
		},
	}
}

// serveTestRole is a valid member of host indexer 1 copying owners 0 and 2
// (only the fields of Plan S3-C2 contract §23.1), with serveCfg as its
// Config.Serve.
func serveTestRole(t *testing.T, serveCfg *serve.Config) (*Role, error) {
	t.Helper()
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "arbiter-0", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatalf("dataplane.New: %v", err)
	}
	t.Cleanup(client.Close)
	cfg := Config{
		NodeID: "member-b", HostIndexerID: 1, NetworkID: "s3c4-serve-test", GenesisSnapshotID: "0xgenesis", StateDir: t.TempDir(),
		Peers:           []Peer{{IndexerID: 2, Collection: "si_owner_2"}, {IndexerID: 0, Collection: "si_owner_0"}},
		DiskBudgetBytes: 1 << 30, Serve: serveCfg,
	}
	return New(cfg, Deps{Client: client, Conn: serveTestConn{}, Registry: serveTestRegistry{}, Logger: slog.New(slog.DiscardHandler)})
}

// TestRoleWiresServeIntoTheInstaller: Config.Serve gives the role a serving
// server whose BeforeInstall and AfterInstall are the installer's seams and
// whose handler, view and metrics the role exposes; without it the role is
// P2's. The package's sources pass the two seams to every install.Deps they
// build, bind the installer's journal and run the server in startup step (9).
func TestRoleWiresServeIntoTheInstaller(t *testing.T) {
	t.Run("without Config.Serve the role is P2's", func(t *testing.T) {
		r, err := serveTestRole(t, nil)
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		before, after := r.installHooks()
		if before != nil || after != nil {
			t.Fatal("a role without Config.Serve passes install seams")
		}
		r.attachServing(newServeTestJournal())
		if err := r.runServing(t.Context()); err != nil {
			t.Fatalf("runServing without Config.Serve: %v", err)
		}
		if r.ServeHandler() != nil || r.ServeMetrics() != nil || !reflect.DeepEqual(r.Serving(), serve.ServingView{}) {
			t.Fatal("a role without Config.Serve has a serving side")
		}
	})

	t.Run("a bad Config.Serve is refused by New", func(t *testing.T) {
		if _, err := serveTestRole(t, &serve.Config{Enabled: true}); err == nil || err.Error() != "member: serve: a bearer token is required" {
			t.Fatalf("New with a tokenless Config.Serve: %v", err)
		}
	})

	t.Run("with Config.Serve the seams are the server's", func(t *testing.T) {
		r, err := serveTestRole(t, &serve.Config{Enabled: true, Token: "role-test-token", PollInterval: 10 * time.Millisecond})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		before, after := r.installHooks()
		if before == nil || after == nil || r.ServeMetrics() == nil || r.ServeHandler() == nil {
			t.Fatal("a role with Config.Serve lacks its seams, metrics or handler")
		}
		if err := before(t.Context(), 0, "dex"); err != nil {
			t.Fatalf("BeforeInstall without a lease: %v", err)
		}
		if d := r.Serving().Databases; len(d) != 1 || d[0].Database != "dex" || d[0].Reason != serve.ReasonInstalling {
			t.Fatalf("BeforeInstall did not reach the server: %+v", d)
		}
		after(0, "dex")
		r.attachServing(newServeTestJournal())
		// The arbiter at 127.0.0.1:1 is unreachable: once installing is
		// cleared, both databases report feed_unavailable.
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- r.runServing(ctx) }()
		deadline := time.Now().Add(10 * time.Second)
		for {
			view := r.Serving()
			if len(view.Databases) == 2 && view.Databases[0].Reason == serve.ReasonFeedUnavailable && view.Databases[1].Reason == serve.ReasonFeedUnavailable {
				if view.NodeID != "member-b" || view.Databases[0].Database != "dex" || view.Databases[1].Database != "nft" {
					t.Fatalf("view %+v", view)
				}
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("view %+v, want dex and nft feed_unavailable", view)
			}
			time.Sleep(10 * time.Millisecond)
		}
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("runServing: %v, want the context's cancellation", err)
		}
		req := httptest.NewRequest(http.MethodGet, "/v1/serving", nil)
		req.Header.Set("Authorization", "Bearer role-test-token")
		rec := httptest.NewRecorder()
		r.ServeHandler().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"node_id":"member-b"`) {
			t.Fatalf("GET /v1/serving: %d %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("the role's sources wire every installer and start the server", func(t *testing.T) {
		files, err := filepath.Glob("*.go")
		if err != nil || len(files) == 0 {
			t.Fatalf("package sources: %v %v", files, err)
		}
		fset := token.NewFileSet()
		literals, attaches, runs := 0, 0, 0
		for _, name := range files {
			if strings.HasSuffix(name, "_test.go") || name == "serving.go" {
				continue
			}
			f, err := parser.ParseFile(fset, name, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			for _, decl := range f.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Body == nil {
					continue
				}
				// The names the function binds to r.installHooks()'s results.
				var hookBefore, hookAfter string
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					if as, ok := n.(*ast.AssignStmt); ok && len(as.Lhs) == 2 && len(as.Rhs) == 1 && serveIsMethodCall(as.Rhs[0], "installHooks") {
						hookBefore, hookAfter = serveIdentName(as.Lhs[0]), serveIdentName(as.Lhs[1])
					}
					return true
				})
				ast.Inspect(fn.Body, func(n ast.Node) bool {
					switch n := n.(type) {
					case *ast.CompositeLit:
						sel, ok := n.Type.(*ast.SelectorExpr)
						if !ok || sel.Sel.Name != "Deps" || serveIdentName(sel.X) != "install" {
							return true
						}
						literals++
						fields := map[string]string{}
						for _, e := range n.Elts {
							if kv, ok := e.(*ast.KeyValueExpr); ok {
								fields[serveIdentName(kv.Key)] = serveIdentName(kv.Value)
							}
						}
						if hookBefore == "" || hookBefore == "_" || hookAfter == "" || hookAfter == "_" ||
							fields["BeforeInstall"] != hookBefore || fields["AfterInstall"] != hookAfter {
							t.Errorf("%s: the install.Deps literal in %s sets BeforeInstall %q and AfterInstall %q, want the results %q and %q of r.installHooks()",
								fset.Position(n.Pos()), fn.Name.Name, fields["BeforeInstall"], fields["AfterInstall"], hookBefore, hookAfter)
						}
					case *ast.CallExpr:
						if serveIsMethodCall(n, "attachServing") {
							attaches++
						}
					case *ast.SelectorExpr:
						if n.Sel.Name == "runServing" {
							runs++
						}
					}
					return true
				})
			}
		}
		if literals == 0 || attaches == 0 || runs == 0 {
			t.Fatalf("%d install.Deps literals, %d attachServing calls and %d runServing uses outside serving.go; want at least one of each", literals, attaches, runs)
		}
	})
}

func serveIsMethodCall(e ast.Expr, name string) bool {
	call, ok := e.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	return ok && sel.Sel.Name == name
}

func serveIdentName(e ast.Expr) string {
	if id, ok := e.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}

// TestServedCopiesReadTheInstallJournal: the serving view's copies are the
// install journal's, per owner and database, at the owner's journal entry.
func TestServedCopiesReadTheInstallJournal(t *testing.T) {
	j := newServeTestJournal()
	c := &servedCopies{owners: []uint64{0, 2}}
	if got := c.Databases(); got != nil {
		t.Fatalf("without a journal: %v", got)
	}
	c.attach(j)
	if got, want := c.Databases(), []serve.OwnerDatabase{{Owner: 0, Database: "dex"}, {Owner: 2, Database: "nft"}}; !reflect.DeepEqual(got, want) {
		t.Fatalf("Databases %v, want %v", got, want)
	}
	got, ok := c.Database(0, "dex")
	want := serve.DatabaseCopy{SnapshotID: "0xs40", Entry: serveEntry0, AuditClean: true, Tables: []serve.TableCopy{
		{TableID: "dex.pools", IncarnationSeq: 8, CopyTable: "dex__pools__i8", SchemaHash: "0xpools", Condition: serve.ConditionInstalled},
		{TableID: "dex.swaps", IncarnationSeq: 7, CopyTable: "dex__swaps__i7", SchemaHash: "0xswaps", Condition: serve.ConditionInstalled},
	}}
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("Database(0, dex) = %+v %t, want %+v", got, ok, want)
	}
	if _, ok := c.Database(0, "nft"); ok {
		t.Fatal("owner 0 has no copy of nft")
	}
	if _, ok := c.Database(1, "dex"); ok {
		t.Fatal("owner 1 has no journal")
	}

	// Foreign wins over Diverged; a table whose journal lags the owner's is
	// excluded; the audit is the installer's.
	j.tables[0][0].Foreign, j.tables[0][0].Diverged = true, true
	j.tables[0][1].SnapshotID = "0xs39"
	j.clean = false
	c.attach(j)
	got, _ = c.Database(0, "dex")
	if got.AuditClean || got.Tables[0].Condition != serve.ConditionExcluded || got.Tables[1].Condition != serve.ConditionForeign {
		t.Fatalf("conditions %+v", got)
	}
	j.tables[0][0].Foreign = false
	got, _ = c.Database(0, "dex")
	if got.Tables[1].Condition != serve.ConditionDiverged {
		t.Fatalf("a Diverged table: %+v", got.Tables[1])
	}
}

type serveTestWatermark struct {
	w   arbiter.SafeWatermark
	err error
}

func (s serveTestWatermark) GetSafeWatermark(context.Context) (arbiter.SafeWatermark, error) {
	return s.w, s.err
}

type serveTestStore map[string]replay.SafeSnapshotManifest

func (s serveTestStore) GetSafeSnapshot(_ context.Context, id string) (replay.SafeSnapshotManifest, error) {
	if m, ok := s[id]; ok {
		return m, nil
	}
	return replay.SafeSnapshotManifest{}, errors.New("manifestfeed: manifest refused")
}

// TestPublishedFeedReadsTheWatermarksManifestWithFrontiers: the serving view
// reads the watermark's manifest, verified, with its root and frontiers.
func TestPublishedFeedReadsTheWatermarksManifestWithFrontiers(t *testing.T) {
	good := replay.SafeSnapshotManifest{SnapshotID: "0xs40", ManifestRoot: "0xs40", TrackFrontiers: []replay.TrackFrontier{serveEntry0, serveEntry2}}
	bare := replay.SafeSnapshotManifest{SnapshotID: "0xs39", ManifestRoot: "0xs39"}
	store := serveTestStore{"0xs40": good, "0xs39": bare}
	cases := []struct {
		name string
		w    serveTestWatermark
		want string
	}{
		{"the watermark's manifest", serveTestWatermark{w: arbiter.SafeWatermark{SnapshotID: "0xs40", ManifestRoot: "0xs40"}}, ""},
		{"no watermark", serveTestWatermark{err: errors.New("rpc error: code = Unavailable")}, "member serving: safe watermark: rpc error: code = Unavailable"},
		{"nothing published", serveTestWatermark{}, "member serving: the arbiter has published no manifest yet"},
		{"a refused manifest", serveTestWatermark{w: arbiter.SafeWatermark{SnapshotID: "0xs41", ManifestRoot: "0xs41"}}, "member serving: manifest 0xs41: manifestfeed: manifest refused"},
		{"another root", serveTestWatermark{w: arbiter.SafeWatermark{SnapshotID: "0xs40", ManifestRoot: "0xother"}}, "member serving: watermark names manifest_root 0xother, manifest 0xs40 has 0xs40"},
		{"no track frontiers", serveTestWatermark{w: arbiter.SafeWatermark{SnapshotID: "0xs39", ManifestRoot: "0xs39"}}, "member serving: manifest 0xs39 carries no track_frontiers"},
	}
	for _, tc := range cases {
		m, err := publishedFeed{watermark: tc.w, store: store}.Latest(t.Context())
		switch {
		case tc.want == "" && (err != nil || !reflect.DeepEqual(m, good)):
			t.Errorf("%s: %+v %v", tc.name, m, err)
		case tc.want != "" && (err == nil || err.Error() != tc.want):
			t.Errorf("%s: %v, want %s", tc.name, err, tc.want)
		}
	}
}
