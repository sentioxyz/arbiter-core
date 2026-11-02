package tableset

import (
	"context"
	"fmt"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/sentioxyz/arbiter-core/wire"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

func TestNewDefaultsTheKeeperAuditInterval(t *testing.T) {
	pinned := ddl.Pinned{UnsafeDB: "u", SafeDB: "s", PromoteDB: "p", NodeID: "n"}
	r, err := New(Config{Pinned: pinned}, Deps{Conn: nopConnT{}})
	if err != nil || r.cfg.KeeperAuditInterval != DefaultKeeperAuditInterval || DefaultKeeperAuditInterval != 10*time.Minute {
		t.Fatalf("interval = %v, %v; want %v", r.cfg.KeeperAuditInterval, err, DefaultKeeperAuditInterval)
	}
	r, err = New(Config{Pinned: pinned, KeeperAuditInterval: time.Minute}, Deps{Conn: nopConnT{}})
	if err != nil || r.cfg.KeeperAuditInterval != time.Minute {
		t.Fatalf("an explicit interval = %v, %v", r.cfg.KeeperAuditInterval, err)
	}
	if st := r.Stats(); st.KeeperFindings != nil || st.KeeperAuditRuns != 0 {
		t.Fatalf("a new reconciler's Keeper audit stats = %+v", st)
	}
}

// keeperAuditConn is a unit-only Keeper/table view. Exec records intended
// mutations; it does not model ClickHouse's active-replica refusal or writes.
type keeperAuditConn struct {
	clickhouse.Conn
	children map[string][]string
	local    [][]string
	execs    []string
	queryErr error
}

func (c *keeperAuditConn) Query(_ context.Context, q string, args ...any) (driver.Rows, error) {
	if c.queryErr != nil {
		return nil, c.queryErr
	}
	if strings.Contains(q, "FROM system.tables") {
		return &keeperAuditRows{rows: c.local}, nil
	}
	if q != `SELECT name FROM system.zookeeper WHERE path = ?` || len(args) != 1 {
		return nil, fmt.Errorf("unexpected query %q %v", q, args)
	}
	var rows [][]string
	for _, n := range c.children[args[0].(string)] {
		rows = append(rows, []string{n})
	}
	return &keeperAuditRows{rows: rows}, nil
}
func (c *keeperAuditConn) Exec(_ context.Context, q string, _ ...any) error {
	c.execs = append(c.execs, q)
	return nil
}

type keeperAuditRows struct {
	driver.Rows
	rows [][]string
	cur  []string
}

func (r *keeperAuditRows) Next() bool {
	if len(r.rows) == 0 {
		return false
	}
	r.cur, r.rows = r.rows[0], r.rows[1:]
	return true
}
func (r *keeperAuditRows) Scan(dst ...any) error {
	if len(dst) != len(r.cur) {
		return fmt.Errorf("wrong scan arity")
	}
	for i, d := range dst {
		p, ok := d.(*string)
		if !ok {
			return fmt.Errorf("wrong scan target")
		}
		*p = r.cur[i]
	}
	return nil
}
func (r *keeperAuditRows) Err() error   { return nil }
func (r *keeperAuditRows) Close() error { return nil }

// A scoped desired table does not authorize destroying a stranded legacy
// replica belonging to local tables whose marker is unknown. The create is
// already refused as drift; a later sweep must preserve that refusal.
func TestScopedSweepPreservesUnknownLocalKeeperMetadata(t *testing.T) {
	sch := schemaFor(t, "drift")
	legacy := chainInc(t, 1, sch, wire.TableStatusPurged)
	live := scopedInc(t, 2, sch, wire.TableStatusActive)
	snap := wire.TableRegistrySnapshot{Incarnations: []wire.TableIncarnation{legacy, live}}
	physical := ddl.CHTableName(sch.TableID)
	c := &keeperAuditConn{children: map[string][]string{
		"/sentio/0/unsafe": {physical}, "/sentio/0/unsafe/" + physical + "/replicas": {"n"},
	}, local: [][]string{{"u", physical, "foreign-marker"}}}
	r, err := New(Config{Pinned: ddl.Pinned{UnsafeDB: "u", SafeDB: "s", PromoteDB: "p", NodeID: "n"}, SweepDecommissioned: true}, Deps{Conn: c, Arbiter: &fakeArbiter{nodeSet: []string{"n"}}})
	if err != nil {
		t.Fatal(err)
	}
	// Check both the first drift and a backed-off pass: protection may not be
	// derived only from ensurePresent being called in this pass.
	for i := 0; i < 2; i++ {
		_ = r.reconcileRegistry(context.Background(), snap, ddl.ModeCreateAndVerify)
		if len(c.execs) != 0 {
			t.Fatalf("unknown local marker must protect Keeper metadata: %v", c.execs)
		}
	}
}

func TestRetiredKeeperSweepsProtectUnknownMarkersOnBothRoots(t *testing.T) {
	sch := schemaFor(t, "protected")
	retired := scopedInc(t, 1, sch, wire.TableStatusPurged)
	current := scopedInc(t, 2, sch, wire.TableStatusActive)
	snap := wire.TableRegistrySnapshot{Incarnations: []wire.TableIncarnation{retired, current}}
	physical := ddl.CHTableName(sch.TableID)
	for _, tc := range []struct {
		marker  string
		blocked bool
	}{
		{"foreign-marker", true}, {"hg_incarnation=999", true}, {"hg_incarnation=01", true}, {"", true},
		{"hg_incarnation=1", false}, {"hg_incarnation=2", false},
	} {
		t.Run(tc.marker, func(t *testing.T) {
			c := &keeperAuditConn{children: map[string][]string{
				"/sentio/0/unsafe": {physical}, "/sentio/0/unsafe/" + physical + "/replicas": {"n"},
				"/sentio/0/unsafe_inc": {physical}, "/sentio/0/unsafe_inc/" + physical: {"1"},
				"/sentio/0/unsafe_inc/" + physical + "/1/replicas": {"n"},
			}}
			r, err := New(Config{Pinned: ddl.Pinned{UnsafeDB: "u", SafeDB: "s", PromoteDB: "p", NodeID: "n"}, SweepDecommissioned: true}, Deps{Conn: c, Arbiter: &fakeArbiter{nodeSet: []string{"n"}}})
			if err != nil {
				t.Fatal(err)
			}
			protection := unattributableKeeperTables(snap, map[string][]string{physical: {tc.marker}})
			r.sweepPurgedKeeperPaths(context.Background(), snap, nil, nil, protection, map[string]string{physical: sch.TableID})
			want := 2
			if tc.blocked {
				want = 0
			}
			if len(c.execs) != want {
				t.Fatalf("marker %q intended mutations %v; want %d", tc.marker, c.execs, want)
			}
		})
	}
}

func TestKeeperAuditPureFakeRetriesPacesReportsAndNeverMutates(t *testing.T) {
	ctx := context.Background()
	sch := schemaFor(t, "audit")
	inc := chainInc(t, 1, sch, wire.TableStatusActive)
	physical := ddl.CHTableName(sch.TableID)
	path := "/sentio/0/unsafe/" + physical
	c := &keeperAuditConn{children: map[string][]string{
		"/sentio/0/unsafe": {physical}, path + "/replicas": {"intruder", "n", "pool"},
		"/sentio/0/unsafe_inc": {"stray"}, "/sentio/0/unsafe_inc/stray": {"bogus"},
	}}
	snap := wire.TableRegistrySnapshot{Incarnations: []wire.TableIncarnation{inc}}
	r, clock, logs := auditedOwner(t, c, ddl.Pinned{UnsafeDB: "u", SafeDB: "s", PromoteDB: "p", NodeID: "n"}, newFakeView(), &fakeArbiter{nodeSet: []string{"pool"}})
	r.auditKeeperIfDue(ctx, snap)
	st := r.Stats()
	if st.KeeperAuditRuns != 1 || len(st.KeeperFindings) != 2 || len(c.execs) != 0 {
		t.Fatalf("audit stats %+v, mutations %v", st, c.execs)
	}
	st.KeeperFindings[0].Path = "changed"
	if r.Stats().KeeperFindings[0].Path == "changed" {
		t.Fatal("Stats aliases internal findings")
	}
	r.auditKeeperIfDue(ctx, snap)
	if r.Stats().KeeperAuditRuns != 1 {
		t.Fatal("audit ignored its interval")
	}
	before := logs.String()
	clock.advance(DefaultKeeperAuditInterval)
	r.auditKeeperIfDue(ctx, snap)
	if r.Stats().KeeperAuditRuns != 2 || logs.String() != before {
		t.Fatal("repeated findings logged again")
	}
	clock.advance(DefaultKeeperAuditInterval)
	c.queryErr = fmt.Errorf("unit Keeper failure")
	r.auditKeeperIfDue(ctx, snap)
	if r.Stats().KeeperAuditRuns != 2 || len(r.Stats().KeeperFindings) != 2 {
		t.Fatal("failed audit changed completed state")
	}
	c.queryErr = nil
	r.auditKeeperIfDue(ctx, snap)
	if r.Stats().KeeperAuditRuns != 3 {
		t.Fatal("failed audit was not retried next pass")
	}
	r.cfg.SweepDecommissioned = false
	clock.advance(DefaultKeeperAuditInterval)
	r.auditKeeperIfDue(ctx, snap)
	if r.Stats().KeeperAuditRuns != 3 || len(c.execs) != 0 {
		t.Fatal("non-owner audited or audit mutated Keeper")
	}
}
