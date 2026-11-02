package ddl

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// keeperFakeConn is a clickhouse.Conn that records every Exec and Query and
// answers each system.zookeeper listing from children (path -> child names).
// Any other method is unimplemented (the embedded nil Conn panics).
type keeperFakeConn struct {
	clickhouse.Conn
	children map[string][]string
	calls    []string
}

func (c *keeperFakeConn) Exec(_ context.Context, query string, args ...any) error {
	c.calls = append(c.calls, fmt.Sprintf("exec %s %v", query, args))
	return nil
}

func (c *keeperFakeConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	c.calls = append(c.calls, fmt.Sprintf("query %s %v", query, args))
	if query != `SELECT name FROM system.zookeeper WHERE path = ?` || len(args) != 1 {
		return nil, fmt.Errorf("keeperFakeConn: unexpected query %q %v", query, args)
	}
	path, _ := args[0].(string)
	return &nameRows{names: slices.Clone(c.children[path])}, nil
}

// nameRows yields one name per row.
type nameRows struct {
	driver.Rows
	names []string
	cur   string
}

func (r *nameRows) Next() bool {
	if len(r.names) == 0 {
		return false
	}
	r.cur, r.names = r.names[0], r.names[1:]
	return true
}

func (r *nameRows) Scan(dest ...any) error {
	p, ok := dest[0].(*string)
	if len(dest) != 1 || !ok {
		return errors.New("nameRows: scan into one *string")
	}
	*p = r.cur
	return nil
}

func (r *nameRows) Err() error   { return nil }
func (r *nameRows) Close() error { return nil }

// TestZooKeeperPathForUnscopedIsByteIdentical: an unscoped scope, of any
// incarnation, is the legacy /sentio/<shard>/unsafe/<db__table> path every
// replica uses today (housegate spec 2026-10-14 §8.8), and every legacy entry
// point (ZooKeeperPath, IncarnationIntents, DropTable, KeeperReplicas,
// DropReplica, KeeperUnsafeTables) keeps its exact intents and statements.
func TestZooKeeperPathForUnscopedIsByteIdentical(t *testing.T) {
	for _, tc := range []struct {
		shard   uint32
		tableID string
		want    string
	}{
		{0, "db.t", "/sentio/0/unsafe/db__t"},
		{0, "dex.swaps", "/sentio/0/unsafe/dex__swaps"},
		{7, "a.b.c", "/sentio/7/unsafe/a__b__c"},
	} {
		for _, seq := range []uint64{0, 1, 42} {
			if got := ZooKeeperPathFor(tc.shard, tc.tableID, KeeperScope{IncarnationSeq: seq}); got != tc.want {
				t.Fatalf("ZooKeeperPathFor(%d, %q, unscoped %d) = %q, want %q", tc.shard, tc.tableID, seq, got, tc.want)
			}
		}
		p := goldenPinned()
		p.KeeperShardID = tc.shard
		if got := ZooKeeperPath(p, tc.tableID); got != tc.want {
			t.Fatalf("ZooKeeperPath(shard %d, %q) = %q, want %q", tc.shard, tc.tableID, got, tc.want)
		}
	}

	p, sch := goldenPinned(), goldenSchema()
	for _, seq := range []uint64{0, 7} {
		u1, s1, p1, err1 := IncarnationIntents(p, sch, seq)
		u2, s2, p2, err2 := IncarnationIntentsFor(p, sch, KeeperScope{IncarnationSeq: seq})
		if err1 != nil || err2 != nil {
			t.Fatalf("seq %d: %v, %v", seq, err1, err2)
		}
		if !reflect.DeepEqual([]TableIntent{u1, s1, p1}, []TableIntent{u2, s2, p2}) {
			t.Fatalf("seq %d: IncarnationIntents and the unscoped IncarnationIntentsFor differ:\n%+v\n%+v", seq, []TableIntent{u1, s1, p1}, []TableIntent{u2, s2, p2})
		}
	}
	unsafe, _, _, err := IncarnationIntentsFor(p, sch, KeeperScope{})
	if err != nil || unsafe.SQL() != goldenUnsafeDDL {
		t.Fatalf("unscoped genesis DDL = %q, %v; want the golden DDL", unsafe.SQL(), err)
	}

	// The Keeper half of DropTable, KeeperReplicas, DropReplica and
	// KeeperUnsafeTables issue exactly the statements they issued before.
	legacyPath := "/sentio/0/unsafe/db__t"
	run := func(f func(ctx context.Context, conn clickhouse.Conn) error) []string {
		t.Helper()
		conn := &keeperFakeConn{children: map[string][]string{legacyPath + "/replicas": {"node-1", "verifier-1"}}}
		if err := f(context.Background(), conn); err != nil {
			t.Fatal(err)
		}
		return conn.calls
	}
	want := []string{
		"exec DROP TABLE IF EXISTS `hg_promote`.`db__t` SYNC []",
		"exec DROP TABLE IF EXISTS `hg_safe`.`db__t` SYNC []",
		"exec DROP TABLE IF EXISTS `hg_unsafe`.`db__t` SYNC []",
		"query SELECT name FROM system.zookeeper WHERE path = ? [" + legacyPath + "/replicas]",
		"exec SYSTEM DROP REPLICA 'node-1' FROM ZKPATH '" + legacyPath + "' []",
	}
	for name, f := range map[string]func(ctx context.Context, conn clickhouse.Conn) error{
		"DropTable": func(ctx context.Context, conn clickhouse.Conn) error { return DropTable(ctx, conn, p, "db.t") },
		"DropTableFor unscoped 9": func(ctx context.Context, conn clickhouse.Conn) error {
			return DropTableFor(ctx, conn, p, "db.t", KeeperScope{IncarnationSeq: 9})
		},
		"DropTableFor zero scope": func(ctx context.Context, conn clickhouse.Conn) error {
			return DropTableFor(ctx, conn, p, "db.t", KeeperScope{})
		},
	} {
		if got := run(f); !slices.Equal(got, want) {
			t.Fatalf("%s statements:\n%s\nwant:\n%s", name, strings.Join(got, "\n"), strings.Join(want, "\n"))
		}
	}
	replicasLegacy := run(func(ctx context.Context, conn clickhouse.Conn) error {
		got, err := KeeperReplicas(ctx, conn, p, "db.t")
		if err == nil && !slices.Equal(got, []string{"node-1", "verifier-1"}) {
			err = fmt.Errorf("KeeperReplicas = %v", got)
		}
		return err
	})
	replicasAt := run(func(ctx context.Context, conn clickhouse.Conn) error {
		got, err := KeeperReplicasAt(ctx, conn, 0, "db.t", KeeperScope{IncarnationSeq: 3})
		if err == nil && !slices.Equal(got, []string{"node-1", "verifier-1"}) {
			err = fmt.Errorf("KeeperReplicasAt = %v", got)
		}
		return err
	})
	if !slices.Equal(replicasLegacy, replicasAt) {
		t.Fatalf("KeeperReplicas %q != unscoped KeeperReplicasAt %q", replicasLegacy, replicasAt)
	}
	dropLegacy := run(func(ctx context.Context, conn clickhouse.Conn) error {
		return DropReplica(ctx, conn, p, "db.t", "gone")
	})
	dropAt := run(func(ctx context.Context, conn clickhouse.Conn) error {
		return DropReplicaAt(ctx, conn, 0, "db.t", KeeperScope{IncarnationSeq: 3}, "gone")
	})
	if want := []string{"exec SYSTEM DROP REPLICA 'gone' FROM ZKPATH '" + legacyPath + "' []"}; !slices.Equal(dropLegacy, want) || !slices.Equal(dropAt, want) {
		t.Fatalf("DropReplica %q, unscoped DropReplicaAt %q; want %q", dropLegacy, dropAt, want)
	}
	tables := run(func(ctx context.Context, conn clickhouse.Conn) error {
		_, err := KeeperUnsafeTables(ctx, conn, p)
		return err
	})
	if want := []string{"query SELECT name FROM system.zookeeper WHERE path = ? [/sentio/0/unsafe]"}; !slices.Equal(tables, want) {
		t.Fatalf("KeeperUnsafeTables %q, want %q", tables, want)
	}
}

// TestScopedZooKeeperPathIsNeverUnderTheLegacyRoot: ClickHouse removes every
// child of a table's Keeper path when its last replica drops (S3-A M10), so
// no incarnation path may be a descendant of a legacy path, whatever the
// table is called (spec §8.8). A scoped path needs an incarnation.
func TestScopedZooKeeperPathIsNeverUnderTheLegacyRoot(t *testing.T) {
	ids := []string{"db.t", "unsafe.t", "unsafe_inc.t", "db.unsafe_inc", "a.b.c", "x.1"}
	for _, shard := range []uint32{0, 1, 17} {
		legacyRoot := fmt.Sprintf("/sentio/%d/unsafe", shard)
		for _, id := range ids {
			for _, seq := range []uint64{1, 2, 10, 1 << 40} {
				scoped := ZooKeeperPathFor(shard, id, KeeperScope{IncarnationSeq: seq, Scoped: true})
				if want := fmt.Sprintf("/sentio/%d/unsafe_inc/%s/%d", shard, CHTableName(id), seq); scoped != want {
					t.Fatalf("scoped path = %q, want %q", scoped, want)
				}
				if strings.HasPrefix(scoped, legacyRoot+"/") {
					t.Fatalf("scoped path %q is under the legacy root %q", scoped, legacyRoot)
				}
				for _, other := range ids {
					if legacy := ZooKeeperPathFor(shard, other, KeeperScope{}); strings.HasPrefix(scoped, legacy+"/") {
						t.Fatalf("scoped path %q is a descendant of the legacy path %q", scoped, legacy)
					}
				}
			}
		}
	}
	zero := KeeperScope{Scoped: true}
	if _, _, _, err := IncarnationIntentsFor(goldenPinned(), goldenSchema(), zero); err == nil || err.Error() != "ddl: an incarnation-scoped Keeper path needs an incarnation seq of at least 1" {
		t.Fatalf("IncarnationIntentsFor with a scoped seq 0 = %v", err)
	}
	conn := &keeperFakeConn{}
	if _, err := KeeperReplicasAt(context.Background(), conn, 0, "db.t", zero); err == nil {
		t.Fatal("KeeperReplicasAt accepted a scoped seq 0")
	}
	if err := DropReplicaAt(context.Background(), conn, 0, "db.t", zero, "r"); err == nil {
		t.Fatal("DropReplicaAt accepted a scoped seq 0")
	}
	if len(conn.calls) != 0 {
		t.Fatalf("a refused scope issued %q", conn.calls)
	}
}

// TestIncarnationIntentsForUsesTheIncarnationPath: a scoped incarnation's
// hg_unsafe replicates at its incarnation path under this node's replica
// name; the tables carry its marker and hg_safe / hg_promote are unchanged.
func TestIncarnationIntentsForUsesTheIncarnationPath(t *testing.T) {
	p, sch := goldenPinned(), goldenSchema()
	scope := KeeperScope{IncarnationSeq: 7, Scoped: true}
	unsafe, safe, promote, err := IncarnationIntentsFor(p, sch, scope)
	if err != nil {
		t.Fatal(err)
	}
	if unsafe.ZooKeeperPath != "/sentio/0/unsafe_inc/db__t/7" || unsafe.ReplicaName != p.NodeID {
		t.Fatalf("unsafe path %q replica %q", unsafe.ZooKeeperPath, unsafe.ReplicaName)
	}
	wantSQL := strings.Replace(goldenUnsafeDDL, "'/sentio/0/unsafe/db__t'", "'/sentio/0/unsafe_inc/db__t/7'", 1) + "\nCOMMENT 'hg_incarnation=7'"
	if unsafe.SQL() != wantSQL {
		t.Fatalf("scoped unsafe DDL:\n got: %s\nwant: %s", unsafe.SQL(), wantSQL)
	}
	_, legacySafe, legacyPromote, err := IncarnationIntents(p, sch, 7)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(safe, legacySafe) || !reflect.DeepEqual(promote, legacyPromote) {
		t.Fatalf("hg_safe / hg_promote intents depend on the Keeper scope:\n%+v %+v\n%+v %+v", safe, promote, legacySafe, legacyPromote)
	}

	scopedPath := "/sentio/0/unsafe_inc/db__t/7"
	conn := &keeperFakeConn{children: map[string][]string{scopedPath + "/replicas": {"node-1"}, "/sentio/0/unsafe/db__t/replicas": {"node-1"}}}
	if err := DropTableFor(context.Background(), conn, p, "db.t", scope); err != nil {
		t.Fatal(err)
	}
	want := []string{
		"exec DROP TABLE IF EXISTS `hg_promote`.`db__t` SYNC []",
		"exec DROP TABLE IF EXISTS `hg_safe`.`db__t` SYNC []",
		"exec DROP TABLE IF EXISTS `hg_unsafe`.`db__t` SYNC []",
		"query SELECT name FROM system.zookeeper WHERE path = ? [" + scopedPath + "/replicas]",
		"exec SYSTEM DROP REPLICA 'node-1' FROM ZKPATH '" + scopedPath + "' []",
	}
	if !slices.Equal(conn.calls, want) {
		t.Fatalf("scoped DropTableFor statements:\n%s\nwant:\n%s", strings.Join(conn.calls, "\n"), strings.Join(want, "\n"))
	}
}
