package ddl

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func tableComments(t *testing.T, conn clickhouse.Conn, p Pinned, tableID string) map[string]string {
	t.Helper()
	tables, err := ListProtocolTables(context.Background(), conn, p)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]string{}
	for _, lt := range tables {
		if lt.Table == CHTableName(tableID) {
			out[lt.Database] = lt.Comment
		}
	}
	return out
}

// strandReplica leaves a replica named replica under tableID's Keeper path
// with no attached table anywhere: it creates hg_unsafe on the second server
// and detaches it, which is the Keeper state a crash between Keeper
// registration and local metadata leaves behind. The cleanup drops
// p.UnsafeDB wholesale, detached table and all, so it does not depend on
// what the test did with the replica in the meantime.
func strandReplica(t *testing.T, replicaConn clickhouse.Conn, p Pinned, sch payloadexec.TableSchema, replica string) {
	t.Helper()
	ctx := context.Background()
	stranded := p
	stranded.NodeID = replica
	unsafe, _, _, err := Intents(stranded, sch)
	if err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(p.UnsafeDB)); err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, unsafe.SQL()); err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, fmt.Sprintf("DETACH TABLE %s.%s", quoteIdent(p.UnsafeDB), quoteIdent(unsafe.Table))); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = replicaConn.Exec(ctx, "DROP DATABASE IF EXISTS "+quoteIdent(p.UnsafeDB)+" SYNC")
	})
}

func TestEnsureTable_MarksVerifiesAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)

	for i := 0; i < 2; i++ {
		if err := EnsureTable(ctx, conn, p, sch, 7, ModeCreateAndVerify); err != nil {
			t.Fatalf("ensure %d: %v", i, err)
		}
	}
	comments := tableComments(t, conn, p, sch.TableID)
	for _, db := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
		if comments[db] != "hg_incarnation=7" {
			t.Fatalf("%s comment = %q, want hg_incarnation=7 (all: %v)", db, comments[db], comments)
		}
	}
	unsafe, _, _, err := IncarnationIntents(p, sch, 8)
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyProtocolTable(ctx, conn, unsafe); !errors.Is(err, ErrProtocolTableDrift) {
		t.Fatalf("verify against incarnation 8 = %v, want comment drift", err)
	}
	if err := EnsureTable(ctx, conn, p, sch, 7, ModeVerifyOnly); err != nil {
		t.Fatalf("verify-only: %v", err)
	}
}

func TestEnsureTable_VerifyOnlyReportsMissing(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	err := EnsureTable(ctx, conn, p, ensureSchema(t), 0, ModeVerifyOnly)
	if !errors.Is(err, ErrProtocolTableMissing) {
		t.Fatalf("err = %v, want ErrProtocolTableMissing", err)
	}
}

func TestEnsureTable_RecoversItsOwnStrandedReplica(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	strandReplica(t, replicaConn, p, sch, p.NodeID)

	if err := EnsureTable(ctx, conn, p, sch, 3, ModeCreateAndVerify); err != nil {
		t.Fatalf("ensure over a stranded own replica: %v", err)
	}
	replicas, err := KeeperReplicas(ctx, conn, p, sch.TableID)
	if err != nil || !slices.Equal(replicas, []string{p.NodeID}) {
		t.Fatalf("replicas = %v, %v; want only %s", replicas, err, p.NodeID)
	}
}

func TestEnsureTable_RefusesToStealAnActiveReplica(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	dropDatabasesSync(t, replicaConn, p)
	sch := ensureSchema(t)
	if err := EnsureTable(ctx, replicaConn, p, sch, 3, ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	err := EnsureTable(ctx, conn, p, sch, 3, ModeCreateAndVerify)
	if err == nil || !ReplicaActive(err) {
		t.Fatalf("err = %v, want the active-replica refusal", err)
	}
}

func TestDropTable_RemovesTablesAndKeeperPathAndIsRepeatable(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	if err := EnsureTable(ctx, conn, p, sch, 5, ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := DropTable(ctx, conn, p, sch.TableID); err != nil {
			t.Fatalf("drop %d: %v", i, err)
		}
	}
	if left := tableComments(t, conn, p, sch.TableID); len(left) != 0 {
		t.Fatalf("tables left after drop: %v", left)
	}
	paths, err := KeeperUnsafeTables(ctx, conn, p)
	if err != nil || slices.Contains(paths, CHTableName(sch.TableID)) {
		t.Fatalf("keeper paths = %v, %v; the table path must be gone", paths, err)
	}
}

func TestDropTable_RemovesItsOwnKeeperOnlyReplica(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	strandReplica(t, replicaConn, p, sch, p.NodeID)
	if err := DropTable(ctx, conn, p, sch.TableID); err != nil {
		t.Fatal(err)
	}
	paths, err := KeeperUnsafeTables(ctx, conn, p)
	if err != nil || slices.Contains(paths, CHTableName(sch.TableID)) {
		t.Fatalf("keeper paths = %v, %v; a crash-stranded own replica must be removed", paths, err)
	}
}

func TestDropReplica_DecommissionedReplicaUnblocksSameNameRecreation(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeper(t, conn)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t)
	dropDatabasesSync(t, conn, p)
	sch := ensureSchema(t)
	if err := EnsureTable(ctx, conn, p, sch, 4, ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	strandReplica(t, replicaConn, p, sch, "decommissioned")
	if err := DropTable(ctx, conn, p, sch.TableID); err != nil {
		t.Fatal(err)
	}
	replicas, err := KeeperReplicas(ctx, conn, p, sch.TableID)
	if err != nil || !slices.Equal(replicas, []string{"decommissioned"}) {
		t.Fatalf("replicas after own drop = %v, %v", replicas, err)
	}
	recreated := sch
	recreated.Columns = append(slices.Clone(sch.Columns), lthash.Column{Name: "w", Type: "String"})
	if err := EnsureTable(ctx, conn, p, recreated, 9, ModeCreateAndVerify); err == nil {
		t.Fatal("a same-name recreation with a new structure must fail while the stale path survives")
	}
	if err := DropTable(ctx, conn, p, sch.TableID); err != nil {
		t.Fatal(err)
	}
	if err := DropReplica(ctx, conn, p, sch.TableID, "decommissioned"); err != nil {
		t.Fatal(err)
	}
	if paths, err := KeeperUnsafeTables(ctx, conn, p); err != nil || slices.Contains(paths, CHTableName(sch.TableID)) {
		t.Fatalf("keeper paths = %v, %v; dropping the last replica must remove the path", paths, err)
	}
	if err := EnsureTable(ctx, conn, p, recreated, 9, ModeCreateAndVerify); err != nil {
		t.Fatalf("same-name recreation after the sweep: %v", err)
	}
}

// FR-m1: a detached local hg_unsafe table still owns its Keeper replica, so
// EnsureTable fails for an operator to resolve and leaves that replica, and
// the table's data, untouched.
func TestEnsureTable_DetachedTableKeepsItsKeeperReplica(t *testing.T) {
	for _, permanently := range []bool{true, false} {
		t.Run(fmt.Sprintf("permanently=%v", permanently), func(t *testing.T) {
			ctx := context.Background()
			conn := requireCH(t)
			requireKeeper(t, conn)
			p := testPinned(t)
			dropDatabasesSync(t, conn, p)
			sch := ensureSchema(t)
			if err := EnsureTable(ctx, conn, p, sch, 6, ModeCreateAndVerify); err != nil {
				t.Fatal(err)
			}
			unsafe := detachUnsafe(t, conn, p, sch, permanently)
			if err := EnsureTable(ctx, conn, p, sch, 6, ModeCreateAndVerify); err == nil {
				t.Fatal("EnsureTable over a detached hg_unsafe table must fail")
			}
			assertOwnReplicaAttaches(t, conn, p, sch, unsafe)
		})
	}
}

// FR-m1: the stale-replica recovery itself refuses a detached table. ClickHouse
// 25.8 answers CREATE TABLE IF NOT EXISTS over a detached table of the same
// name as a no-op rather than with REPLICA_ALREADY_EXISTS, so this drives the
// recovery step directly: it is the step that would destroy the metadata.
func TestDropStaleReplica_RefusesADetachedTable(t *testing.T) {
	for _, permanently := range []bool{true, false} {
		t.Run(fmt.Sprintf("permanently=%v", permanently), func(t *testing.T) {
			ctx := context.Background()
			conn := requireCH(t)
			requireKeeper(t, conn)
			p := testPinned(t)
			dropDatabasesSync(t, conn, p)
			sch := ensureSchema(t)
			if err := EnsureTable(ctx, conn, p, sch, 6, ModeCreateAndVerify); err != nil {
				t.Fatal(err)
			}
			unsafe := detachUnsafe(t, conn, p, sch, permanently)
			if err := dropStaleReplica(ctx, conn, p, sch.TableID, unsafe); err == nil {
				t.Fatal("the stale-replica recovery must refuse a detached local table")
			}
			assertOwnReplicaAttaches(t, conn, p, sch, unsafe)
		})
	}
}

func detachUnsafe(t *testing.T, conn clickhouse.Conn, p Pinned, sch payloadexec.TableSchema, permanently bool) TableIntent {
	t.Helper()
	unsafe, _, _, err := IncarnationIntents(p, sch, 6)
	if err != nil {
		t.Fatal(err)
	}
	q := fmt.Sprintf("DETACH TABLE %s.%s", quoteIdent(unsafe.Database), quoteIdent(unsafe.Table))
	if permanently {
		q += " PERMANENTLY"
	}
	if err := conn.Exec(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	return unsafe
}

func assertOwnReplicaAttaches(t *testing.T, conn clickhouse.Conn, p Pinned, sch payloadexec.TableSchema, unsafe TableIntent) {
	t.Helper()
	ctx := context.Background()
	replicas, err := KeeperReplicas(ctx, conn, p, sch.TableID)
	if err != nil || !slices.Equal(replicas, []string{p.NodeID}) {
		t.Fatalf("replicas = %v, %v; the detached table's replica must be untouched", replicas, err)
	}
	if err := conn.Exec(ctx, fmt.Sprintf("ATTACH TABLE %s.%s", quoteIdent(unsafe.Database), quoteIdent(unsafe.Table))); err != nil {
		t.Fatalf("re-attach the detached table: %v", err)
	}
	if err := VerifyProtocolTable(ctx, conn, unsafe); err != nil {
		t.Fatalf("the re-attached table must verify: %v", err)
	}
}
