package ddl

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// requireCH mirrors snode/ch_test.go: opt-in via ARBITER_CH_INTEGRATION=1.
func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	return openCH(t, addr)
}

func requireReplicaCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_REPLICA") != "1" {
		t.Skip("set ARBITER_CH_REPLICA=1 and CH_REPLICA_ADDR to run the two-node replication acceptance")
	}
	addr := os.Getenv("CH_REPLICA_ADDR")
	if addr == "" {
		t.Fatal("ARBITER_CH_REPLICA=1 requires CH_REPLICA_ADDR")
	}
	return openCH(t, addr)
}

func openCH(t *testing.T, addr string) clickhouse.Conn {
	t.Helper()
	conn, err := clickhouse.Open(&clickhouse.Options{Addr: []string{addr}})
	if err != nil {
		t.Fatalf("clickhouse: %v", err)
	}
	if err := conn.Ping(context.Background()); err != nil {
		t.Fatalf("clickhouse ping: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// requireKeeper skips (or fails under ARBITER_CH_KEEPER=1, which CI sets) when
// the ClickHouse has no Keeper: ReplicatedMergeTree cannot be created without one.
func requireKeeper(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	var n uint64
	err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n)
	if err == nil {
		return
	}
	if os.Getenv("ARBITER_CH_KEEPER") == "1" {
		t.Fatalf("ARBITER_CH_KEEPER=1 but ClickHouse has no Keeper (mount scripts/ci/clickhouse-keeper.xml): %v", err)
	}
	t.Skipf("ClickHouse has no Keeper configured; skipping ReplicatedMergeTree test: %v", err)
}

// uniqueSuffix keeps zk paths and databases unique per test: zk paths are
// shared across databases and across the parallel snode/verifier/ddl targets.
func uniqueSuffix(t *testing.T) string {
	t.Helper()
	sum := sha1.Sum([]byte(t.Name()))
	return hex.EncodeToString(sum[:])[:10]
}

func testPinned(t *testing.T) Pinned {
	t.Helper()
	s := uniqueSuffix(t)
	return Pinned{UnsafeDB: "hg_unsafe_" + s, SafeDB: "hg_safe_" + s, PromoteDB: "hg_promote_" + s, NodeID: "node-" + s}
}

func dropDatabasesSync(t *testing.T, conn clickhouse.Conn, p Pinned) {
	t.Helper()
	t.Cleanup(func() {
		for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
}
