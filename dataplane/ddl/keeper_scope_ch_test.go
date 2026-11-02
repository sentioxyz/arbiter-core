package ddl

import (
	"context"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"slices"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// isolatedShard is a Keeper shard of the test's own. The ddl, tableset, snode
// and verifier targets share one Keeper in CI, and these tests create
// incarnation paths and read whole roots, so they never use shard 0.
func isolatedShard(t *testing.T) uint32 {
	t.Helper()
	sum := sha1.Sum([]byte("keeper-shard/" + t.Name()))
	return 1_000_000 + binary.BigEndian.Uint32(sum[:4])%1_000_000
}

// strandReplicaAt is strandReplica on the path of scope s: it leaves this
// node's replica of sch's hg_unsafe table registered there with no attached
// table, by creating the table in the database p.UnsafeDB + "_" + label of
// the second server and detaching it. A label per path lets one test strand
// the same table on both roots. The cleanup drops that database and then the
// replica, if it is still registered, so a rerun on the same Keeper starts
// clean.
func strandReplicaAt(t *testing.T, replicaConn clickhouse.Conn, p Pinned, sch payloadexec.TableSchema, s KeeperScope, label string) {
	t.Helper()
	ctx := context.Background()
	unsafe, _, _, err := IncarnationIntentsFor(p, sch, s)
	if err != nil {
		t.Fatal(err)
	}
	unsafe.Database = p.UnsafeDB + "_" + label
	if err := replicaConn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(unsafe.Database)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx := context.Background()
		_ = replicaConn.Exec(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(unsafe.Database)+" SYNC")
		if replicas, err := keeperChildren(ctx, replicaConn, unsafe.ZooKeeperPath+"/replicas"); err == nil && slices.Contains(replicas, p.NodeID) {
			_ = dropReplicaAtPath(ctx, replicaConn, unsafe.ZooKeeperPath, p.NodeID)
		}
	})
	if err := replicaConn.Exec(ctx, unsafe.SQL()); err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, fmt.Sprintf("DETACH TABLE %s.%s", quoteIdent(unsafe.Database), quoteIdent(unsafe.Table))); err != nil {
		t.Fatal(err)
	}
}

// replicaState is one replica's registration under a Keeper path: listed
// under <path>/replicas, and active while a server holds its table attached.
// A replica becomes active shortly after its CREATE, so a wanted active state
// is polled for up to ten seconds.
func replicaState(t *testing.T, conn clickhouse.Conn, path, replica string, wantActive bool) (listed, active bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		replicas, err := keeperChildren(context.Background(), conn, path+"/replicas")
		if err != nil {
			t.Fatal(err)
		}
		children, err := keeperChildren(context.Background(), conn, path+"/replicas/"+replica)
		if err != nil {
			t.Fatal(err)
		}
		listed, active = slices.Contains(replicas, replica), slices.Contains(children, "is_active")
		if active == wantActive || time.Now().After(deadline) {
			return listed, active
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestEnsureTableForRemovesAStrandedReplicaOnlyOnItsOwnPath: the
// REPLICA_ALREADY_EXISTS recovery removes this node's stranded replica from
// the path of the create's own scope only. A scoped create leaves the same
// replica name stranded on the legacy path, and a legacy create leaves it
// stranded on an incarnation path (plan S3-E1 contract §42.1).
func TestEnsureTableForRemovesAStrandedReplicaOnlyOnItsOwnPath(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	p.KeeperShardID = isolatedShard(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	legacy, scoped4, scoped5 := KeeperScope{IncarnationSeq: 3}, KeeperScope{IncarnationSeq: 4, Scoped: true}, KeeperScope{IncarnationSeq: 5, Scoped: true}
	legacyPath := ZooKeeperPathFor(p.KeeperShardID, sch.TableID, legacy)
	scoped4Path := ZooKeeperPathFor(p.KeeperShardID, sch.TableID, scoped4)
	scoped5Path := ZooKeeperPathFor(p.KeeperShardID, sch.TableID, scoped5)
	attachedAt := func() string {
		t.Helper()
		var path string
		if err := conn.QueryRow(ctx, `SELECT zookeeper_path FROM system.replicas WHERE database = ? AND table = ?`, p.UnsafeDB, CHTableName(sch.TableID)).Scan(&path); err != nil {
			t.Fatal(err)
		}
		return path
	}

	strandReplicaAt(t, replicaConn, p, sch, legacy, "legacy")
	strandReplicaAt(t, replicaConn, p, sch, scoped4, "scoped4")
	if err := EnsureTableFor(ctx, conn, p, sch, scoped4, ModeCreateAndVerify); err != nil {
		t.Fatalf("scoped ensure over a stranded own replica: %v", err)
	}
	if path := attachedAt(); path != scoped4Path {
		t.Fatalf("hg_unsafe replicates at %q, want %q", path, scoped4Path)
	}
	if listed, active := replicaState(t, conn, scoped4Path, p.NodeID, true); !listed || !active {
		t.Fatalf("incarnation path: listed %v active %v; want this node's attached replica", listed, active)
	}
	if listed, active := replicaState(t, conn, legacyPath, p.NodeID, false); !listed || active {
		t.Fatalf("legacy path: listed %v active %v; a scoped create must leave the stranded legacy replica alone", listed, active)
	}

	if err := DropTableFor(ctx, conn, p, sch.TableID, scoped4); err != nil {
		t.Fatal(err)
	}
	strandReplicaAt(t, replicaConn, p, sch, scoped5, "scoped5")
	if err := EnsureTableFor(ctx, conn, p, sch, legacy, ModeCreateAndVerify); err != nil {
		t.Fatalf("legacy ensure over a stranded own replica: %v", err)
	}
	if path := attachedAt(); path != legacyPath {
		t.Fatalf("hg_unsafe replicates at %q, want %q", path, legacyPath)
	}
	if listed, active := replicaState(t, conn, legacyPath, p.NodeID, true); !listed || !active {
		t.Fatalf("legacy path: listed %v active %v; want this node's attached replica", listed, active)
	}
	if listed, active := replicaState(t, conn, scoped5Path, p.NodeID, false); !listed || active {
		t.Fatalf("incarnation path: listed %v active %v; a legacy create must leave the stranded scoped replica alone", listed, active)
	}
}
