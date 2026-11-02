package ddl

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// createReplicatedAt creates database.table as a ReplicatedMergeTree replica
// named replica at path, whatever path that is, and drops the database when
// the test ends.
func createReplicatedAt(t *testing.T, conn clickhouse.Conn, database, table, path, replica string) {
	t.Helper()
	ctx := context.Background()
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(database)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+quoteIdent(database)+" SYNC") })
	sql := fmt.Sprintf("CREATE TABLE %s.%s (x UInt64) ENGINE = ReplicatedMergeTree(%s, %s) ORDER BY x",
		quoteIdent(database), quoteIdent(table), quoteLiteral(path), quoteLiteral(replica))
	if err := conn.Exec(ctx, sql); err != nil {
		t.Fatal(err)
	}
}

// TestListKeeperRootsSeparatesIncarnationPathsFromOtherRoots: on a real
// Keeper, a legacy table, incarnation paths of two tables, and three
// replicated-table roots under unsafe_inc that are not incarnation paths (a
// non-decimal child, a non-canonical decimal, a table created directly at
// unsafe_inc/<t>) are told apart; dropping an incarnation's last replica
// removes its incarnation path only.
func TestListKeeperRootsSeparatesIncarnationPathsFromOtherRoots(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	p.KeeperShardID = isolatedShard(t)
	dropDatabasesSync(t, conn, p)
	legacy := ensureSchema(t)
	scoped := legacy
	scoped.TableID = legacy.TableID + "_s"
	scope := KeeperScope{IncarnationSeq: 5, Scoped: true}
	if err := EnsureTableFor(ctx, conn, p, legacy, KeeperScope{IncarnationSeq: 1}, ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	if err := EnsureTableFor(ctx, conn, p, scoped, scope, ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	root := fmt.Sprintf("/sentio/%d/unsafe_inc", p.KeeperShardID)
	stray := "hg_stray_" + uniqueSuffix(t)
	createReplicatedAt(t, conn, stray, "seq_7", root+"/db__multi/7", "r1")
	createReplicatedAt(t, conn, stray, "seq_12", root+"/db__multi/12", "r1")
	createReplicatedAt(t, conn, stray, "bogus", root+"/db__bogus/bogus", "r1")
	createReplicatedAt(t, conn, stray, "padded", root+"/db__bogus/007", "r1")
	createReplicatedAt(t, conn, stray, "direct", root+"/db__direct", "r1")

	got, err := ListKeeperRoots(ctx, conn, p.KeeperShardID)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{CHTableName(legacy.TableID)}; !slices.Equal(got.Legacy, want) {
		t.Fatalf("Legacy = %v, want %v", got.Legacy, want)
	}
	wantScoped := []ScopedKeeperPath{{"db__multi", 7}, {"db__multi", 12}, {CHTableName(scoped.TableID), 5}}
	if !reflect.DeepEqual(got.Scoped, wantScoped) {
		t.Fatalf("Scoped = %v, want %v", got.Scoped, wantScoped)
	}
	head := []string{root + "/db__bogus/007", root + "/db__bogus/bogus", root + "/db__direct"}
	if len(got.NonIncarnation) < len(head) || !slices.Equal(got.NonIncarnation[:len(head)], head) {
		t.Fatalf("NonIncarnation = %v, want it to start with %v", got.NonIncarnation, head)
	}
	for _, path := range got.NonIncarnation[len(head):] {
		if !strings.HasPrefix(path, root+"/db__direct/") {
			t.Fatalf("NonIncarnation lists %s, which is neither a stray root nor a child of the table created at %s/db__direct", path, root)
		}
	}
	if !slices.Contains(got.NonIncarnation, root+"/db__direct/replicas") {
		t.Fatalf("NonIncarnation = %v, want every non-decimal child of %s/db__direct", got.NonIncarnation, root)
	}
	if replicas, err := KeeperReplicasAt(ctx, conn, p.KeeperShardID, scoped.TableID, scope); err != nil || !slices.Equal(replicas, []string{p.NodeID}) {
		t.Fatalf("scoped replicas = %v, %v; want only %s", replicas, err, p.NodeID)
	}
	if tables, err := KeeperUnsafeTables(ctx, conn, p); err != nil || !slices.Equal(tables, []string{CHTableName(legacy.TableID)}) {
		t.Fatalf("KeeperUnsafeTables = %v, %v; the incarnation path is not on the legacy root", tables, err)
	}

	if err := DropTableFor(ctx, conn, p, scoped.TableID, scope); err != nil {
		t.Fatal(err)
	}
	after, err := ListKeeperRoots(ctx, conn, p.KeeperShardID)
	if err != nil {
		t.Fatal(err)
	}
	if want := []ScopedKeeperPath{{"db__multi", 7}, {"db__multi", 12}}; !reflect.DeepEqual(after.Scoped, want) {
		t.Fatalf("Scoped after the drop = %v, want %v", after.Scoped, want)
	}
	if !slices.Equal(after.Legacy, got.Legacy) || !slices.Equal(after.NonIncarnation, got.NonIncarnation) {
		t.Fatalf("dropping one incarnation changed the other roots: %+v, was %+v", after, got)
	}
}
