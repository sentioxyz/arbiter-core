package consensus

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"reflect"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
)

// readinessCH opens CH_ADDR as the test's admin user (ARBITER_CH_INTEGRATION=1)
// and requires Keeper: skipped without it, failed under ARBITER_CH_KEEPER=1.
func readinessCH(t *testing.T) (clickhouse.Conn, string) {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse with Keeper on CH_ADDR or localhost:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	conn := chtest.Open(t, addr)
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but ClickHouse has no Keeper: %v", err)
		}
		t.Skipf("ClickHouse has no Keeper configured; skipping: %v", err)
	}
	return conn, addr
}

func readinessExec(t *testing.T, conn clickhouse.Conn, query string) {
	t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// readinessCleanup runs query on a fresh admin connection when the test ends.
func readinessCleanup(t *testing.T, addr, query string) {
	t.Helper()
	t.Cleanup(func() {
		conn, err := clickhouse.Open(chtest.Options(addr))
		if err != nil {
			t.Logf("cleanup %s: %v", query, err)
			return
		}
		defer func() { _ = conn.Close() }()
		if err := conn.Exec(context.Background(), query); err != nil {
			t.Logf("cleanup %s: %v", query, err)
		}
	})
}

// TestReadLocalReplicasReportsOnlyTheIncarnationsSyncedReplica (ClickHouse
// and Keeper): as a user with si_member's rights on the member unsafe
// database (SELECT, SYSTEM SYNC REPLICA) and on system.replicas, the
// member's replica at the incarnation's Keeper path is synced; the same
// table asked at another path, a missing table and another node's replica
// are not.
func TestReadLocalReplicasReportsOnlyTheIncarnationsSyncedReplica(t *testing.T) {
	admin, addr := readinessCH(t)
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	sfx := hex.EncodeToString(b)
	db := "hg_member_unsafe_" + sfx
	root := "/sentio/test_" + sfx
	readinessCleanup(t, addr, "DROP DATABASE IF EXISTS "+memberIdent(db)+" SYNC")
	readinessExec(t, admin, "CREATE DATABASE "+memberIdent(db))
	pools := root + "/unsafe_inc/dex__pools/7"
	readinessExec(t, admin, fmt.Sprintf("CREATE TABLE %s.`dex__pools` (v UInt64) ENGINE = ReplicatedMergeTree('%s', '%s') ORDER BY v", memberIdent(db), pools, fixtureNode))
	readinessExec(t, admin, fmt.Sprintf("CREATE TABLE %s.`dex__other` (v UInt64) ENGINE = ReplicatedMergeTree('%s', 'member-c') ORDER BY v", memberIdent(db), root+"/unsafe/dex__other"))
	readinessExec(t, admin, "INSERT INTO "+memberIdent(db)+".`dex__pools` VALUES (1), (2)")
	user := "readiness_" + sfx
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	password := hex.EncodeToString(b) + sfx
	readinessCleanup(t, addr, "DROP USER IF EXISTS "+memberIdent(user))
	if err := admin.Exec(context.Background(), fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY '%s'", memberIdent(user), password)); err != nil {
		t.Fatalf("create user %s: %v", user, err)
	}
	for _, grant := range []string{"SELECT ON " + memberIdent(db) + ".*", "SYSTEM SYNC REPLICA ON " + memberIdent(db) + ".*", "SELECT ON system.replicas"} {
		readinessExec(t, admin, "GRANT "+grant+" TO "+memberIdent(user))
	}
	conn := chtest.OpenAs(t, addr, user, password)
	got, err := ReadLocalReplicas(context.Background(), conn, db, fixtureNode, map[string]string{
		"dex__pools": pools, "dex__missing": root + "/unsafe/dex__missing", "dex__other": root + "/unsafe/dex__other",
	})
	if err != nil {
		t.Fatalf("ReadLocalReplicas: %v", err)
	}
	if want := (LocalReplicas{"dex__pools": true, "dex__missing": false, "dex__other": false}); !reflect.DeepEqual(got, want) {
		t.Fatalf("ReadLocalReplicas = %v, want %v", got, want)
	}
	got, err = ReadLocalReplicas(context.Background(), conn, db, fixtureNode, map[string]string{"dex__pools": root + "/unsafe/dex__pools"})
	if err != nil || got["dex__pools"] {
		t.Fatalf("the replica asked at the unscoped path = %v, %v; want not synced", got, err)
	}
}
