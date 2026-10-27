package copyset

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"slices"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// requireCH is opt-in via ARBITER_CH_INTEGRATION=1, like dataplane/ddl's, and
// connects through dataplane/chtest (CH_USER, CH_PASSWORD).
func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	return chtest.Open(t, addr)
}

// execRecorder passes every call through and records each Exec statement.
type execRecorder struct {
	clickhouse.Conn
	mu    sync.Mutex
	execs []string
}

func (c *execRecorder) Exec(ctx context.Context, query string, args ...any) error {
	c.mu.Lock()
	c.execs = append(c.execs, query)
	c.mu.Unlock()
	return c.Conn.Exec(ctx, query, args...)
}

func (c *execRecorder) take() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.execs
	c.execs = nil
	return out
}

func mustExec(t *testing.T, conn clickhouse.Conn, sql string) {
	t.Helper()
	if err := conn.Exec(context.Background(), sql); err != nil {
		t.Fatalf("%s: %v", sql, err)
	}
}

func tableNames(t *testing.T, conn clickhouse.Conn, database string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), `SELECT name FROM system.tables WHERE database = ? ORDER BY name`, database)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		out = append(out, name)
	}
	return out
}

// TestCopysetReconcilesAgainstClickHouse: against a real server the
// reconciler creates and verifies every peer owner's copy, drops the copy of
// a Retiring incarnation and an unknown table, leaves the host SNode's
// protocol tables and the stage database alone, and every statement it issues
// names hg_replica only. A second pass changes nothing.
func TestCopysetReconcilesAgainstClickHouse(t *testing.T) {
	conn := requireCH(t)
	sum := sha1.Sum([]byte(t.Name()))
	s := hex.EncodeToString(sum[:])[:10]
	m := ddl.MemberPinned{MemberUnsafeDB: "hg_member_unsafe_" + s, ReplicaDB: "hg_replica_" + s, StageDB: "hg_rstage_" + s, NodeID: "member-" + s, StoragePolicy: "default"}
	protocol := ddl.Pinned{UnsafeDB: "hg_unsafe_" + s, SafeDB: "hg_safe_" + s, PromoteDB: "hg_promote_" + s, NodeID: "snode-" + s}
	t.Cleanup(func() {
		for _, db := range []string{m.MemberUnsafeDB, m.ReplicaDB, m.StageDB, protocol.UnsafeDB, protocol.SafeDB, protocol.PromoteDB} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+db+"` SYNC")
		}
	})
	ctx := context.Background()
	if err := ddl.EnsureMemberDatabases(ctx, conn, m); err != nil {
		t.Fatal(err)
	}
	// The host SNode's table under the name and comment of a Retiring
	// incarnation, and this node's scratch: neither is the reconciler's.
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS `"+protocol.SafeDB+"`")
	mustExec(t, conn, "CREATE TABLE `"+protocol.SafeDB+"`.`db0__d__i4` (x UInt8) ENGINE = MergeTree ORDER BY x COMMENT 'hg_incarnation=4'")
	scratch, err := ddl.ScratchIntent(m, chainSchema("db0.c"), m.NodeID+"_0a1b2c3d_1")
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, scratch.SQL())
	// A copy whose incarnation is now Retiring, and a table nobody accounts for.
	if err := ddl.EnsureCopy(ctx, conn, m, chainSchema("db0.d"), 4); err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, "CREATE TABLE `"+m.ReplicaDB+"`.`junk` (x UInt8) ENGINE = MergeTree ORDER BY x")

	reg := newFakeRegistry(snapshot(
		genesisInc(1, "db0.g", wire.TableStatusActive),
		chainInc(2, "db0.c", 0, wire.TableStatusActive),
		chainInc(3, "db2.p", 2, wire.TableStatusPending),
		chainInc(4, "db0.d", 0, wire.TableStatusRetiring),
		chainInc(5, "db1.h", 1, wire.TableStatusActive),
	), true)
	rec := &execRecorder{Conn: conn}
	var dropped droppedCopies
	cfg := testConfig()
	cfg.Pinned, cfg.Protocol = m, protocol
	r, err := New(cfg, Deps{Conn: rec, Registry: reg, Dropped: dropped.hook})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := r.Reconcile(ctx); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if got, want := tableNames(t, conn, m.ReplicaDB), []string{"db0__c__i2", "db0__g__i1", "db2__p__i3"}; !slices.Equal(got, want) {
		t.Fatalf("%s holds %v, want %v", m.ReplicaDB, got, want)
	}
	if got := readyCopyTables(r); !slices.Equal(got, []string{"db0__c__i2", "db0__g__i1", "db2__p__i3"}) {
		t.Fatalf("ready copies = %v", got)
	}
	if got := dropped.tables(); !slices.Equal(got, []string{"db0__d__i4", "junk"}) {
		t.Fatalf("Dropped hook saw %v", got)
	}
	if st := r.Stats(); st.Copies != 3 || st.Dropped != 1 || st.Unknown != 1 || len(st.Failures) != 0 {
		t.Fatalf("Stats = %+v", st)
	}
	if got := tableNames(t, conn, protocol.SafeDB); !slices.Equal(got, []string{"db0__d__i4"}) {
		t.Fatalf("%s holds %v, want the host's table untouched", protocol.SafeDB, got)
	}
	if got := tableNames(t, conn, m.StageDB); !slices.Equal(got, []string{scratch.Table}) {
		t.Fatalf("%s holds %v, want the scratch untouched", m.StageDB, got)
	}
	first := rec.take()
	for _, stmt := range first {
		if !strings.Contains(stmt, "`"+m.ReplicaDB+"`.") {
			t.Fatalf("statement %q does not name %s", stmt, m.ReplicaDB)
		}
		for _, other := range []string{protocol.UnsafeDB, protocol.SafeDB, protocol.PromoteDB, m.StageDB, m.MemberUnsafeDB} {
			if strings.Contains(stmt, other) {
				t.Fatalf("statement %q names %s", stmt, other)
			}
		}
	}
	if err := r.Reconcile(ctx); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	for _, stmt := range rec.take() {
		if !strings.HasPrefix(stmt, "CREATE TABLE IF NOT EXISTS `"+m.ReplicaDB+"`.") {
			t.Fatalf("second pass issued %q, want only the idempotent create of each copy", stmt)
		}
	}
	if st := r.Stats(); st.Copies != 3 || st.Dropped != 1 || st.Unknown != 1 {
		t.Fatalf("Stats after the second pass = %+v", st)
	}
}
