package sources

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// The ClickHouse-gated tests of this package use the environment of the
// member confinement suite (contract §23.3): CH_ADDR is the member's
// ClickHouse, CH_PEER_ADDR a second server (the owner, or a network
// verifier) as this test reaches it, and CH_PEER_ADDR_FROM_SERVER the same
// server as CH_ADDR's server reaches it; S3_ENDPOINT, S3_BUCKET,
// S3_ACCESS_KEY_ID and S3_SECRET name an S3 bucket that both servers reach
// with a key that may write it (the restore test makes a backup first). The
// test's admin user is chtest's (CH_USER, CH_PASSWORD); it must manage users
// and named collections. Every user, collection, database and table a test
// makes is its own and is dropped when it ends.
const (
	envIntegration        = "ARBITER_CH_INTEGRATION"
	envAddr               = "CH_ADDR"
	envPeerAddr           = "CH_PEER_ADDR"
	envPeerAddrFromServer = "CH_PEER_ADDR_FROM_SERVER"
	envS3Endpoint         = "S3_ENDPOINT"
	envS3Bucket           = "S3_BUCKET"
	envS3AccessKeyID      = "S3_ACCESS_KEY_ID"
	envS3Secret           = "S3_SECRET"
)

// memberServer opens the member's ClickHouse (CH_ADDR) as the admin user.
func memberServer(t *testing.T) (clickhouse.Conn, string) {
	t.Helper()
	if os.Getenv(envIntegration) != "1" {
		t.Skipf("set %s=1 and %s to run the ClickHouse-gated member source tests", envIntegration, envAddr)
	}
	addr := os.Getenv(envAddr)
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	return chtest.Open(t, addr), addr
}

// pair is the member's ClickHouse and a second server it reaches through
// remote().
type pair struct {
	Member     clickhouse.Conn
	MemberAddr string
	Peer       clickhouse.Conn
	PeerAddr   string
	PeerHost   string // as the member's server reaches the peer
	PeerPort   uint64
}

func requirePair(t *testing.T) pair {
	t.Helper()
	member, memberAddr := memberServer(t)
	peerAddr, fromServer := os.Getenv(envPeerAddr), os.Getenv(envPeerAddrFromServer)
	if peerAddr == "" || fromServer == "" {
		t.Skipf("set %s (the second ClickHouse as this test reaches it) and %s (the same server as %s's server reaches it) to run the two-server source tests", envPeerAddr, envPeerAddrFromServer, envAddr)
	}
	host, portText, err := net.SplitHostPort(fromServer)
	if err != nil {
		t.Fatalf("%s %q: %v", envPeerAddrFromServer, fromServer, err)
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil {
		t.Fatalf("%s %q port: %v", envPeerAddrFromServer, fromServer, err)
	}
	return pair{Member: member, MemberAddr: memberAddr, Peer: chtest.Open(t, peerAddr), PeerAddr: peerAddr, PeerHost: host, PeerPort: port}
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// suffix is unique per test run, so objects never collide on a shared server
// or with an earlier run.
func suffix(t *testing.T) string {
	t.Helper()
	return randomHex(t, 4)
}

func mustExec(t *testing.T, conn clickhouse.Conn, query string) {
	t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// cleanup runs query on a fresh admin connection to addr when the test ends.
func cleanup(t *testing.T, addr, query string) {
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

// testSchema is a partitioned SI table schema whose logical database is
// unique to the test.
func testSchema(database string) payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID: database + ".t", PartitionBy: "p",
		Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
}

// sourceDatabase creates database db on conn; a database of the test's own
// (not the shared hg_safe or hg_unsafe) is dropped when the test ends.
func sourceDatabase(t *testing.T, conn clickhouse.Conn, addr, db string) {
	t.Helper()
	mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+ident(db))
	if db != "hg_safe" && db != "hg_unsafe" {
		cleanup(t, addr, "DROP DATABASE IF EXISTS "+ident(db)+" SYNC")
	}
}

// createSourceTable creates, on conn, schema's protocol MergeTree table in
// database db (the owner's hg_safe structure; the source engine does not
// matter to a pull) and drops it when the test ends; it returns the physical
// table name.
func createSourceTable(t *testing.T, conn clickhouse.Conn, addr, db string, schema payloadexec.TableSchema) string {
	t.Helper()
	_, safe, _, err := ddl.Intents(ddl.Pinned{UnsafeDB: db + "_u", SafeDB: db, PromoteDB: db + "_p", NodeID: "sources-test"}, schema)
	if err != nil {
		t.Fatal(err)
	}
	sourceDatabase(t, conn, addr, db)
	cleanup(t, addr, "DROP TABLE IF EXISTS "+ident(db)+"."+ident(safe.Table)+" SYNC")
	mustExec(t, conn, safe.SQL())
	return safe.Table
}

// insertPart writes n rows with row ids first..first+n-1 into partition p of
// db.table as one part.
func insertPart(t *testing.T, conn clickhouse.Conn, db, table, p string, first, n uint64) {
	t.Helper()
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(NoSplitSettings(n)))
	query := fmt.Sprintf("INSERT INTO %s.%s (%s, `p`, `v`) SELECT toFixedString(leftPad(lower(hex(number)), 32, '0'), 32), %s, number FROM numbers(%d, %d)",
		ident(db), ident(table), ident(ddl.RowIDColumn), literal(p), first, n)
	if err := conn.Exec(ctx, query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

type partRow struct {
	Name, PartitionID, Hash string
	Rows                    uint64
}

// partsOf lists the active parts of db.table, by name.
func partsOf(t *testing.T, conn clickhouse.Conn, db, table string) []partRow {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT name, partition_id, hash_of_all_files, rows FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY name", db, table)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []partRow
	for rows.Next() {
		var p partRow
		if err := rows.Scan(&p.Name, &p.PartitionID, &p.Hash, &p.Rows); err != nil {
			t.Fatal(err)
		}
		out = append(out, p)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	return out
}

// scanOf is the row LtHash of every active part of db.table.
func scanOf(t *testing.T, conn clickhouse.Conn, db, table string, schema payloadexec.TableSchema) map[string]chexec.PartScanResult {
	t.Helper()
	scans, err := chexec.ScanAllParts(context.Background(), conn, db+"."+table, schema)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]chexec.PartScanResult, len(scans))
	for _, s := range scans {
		out[s.PartName] = s
	}
	return out
}

// createUser creates a password user (crypto/rand password) on conn with
// grants (each with one %s for the quoted user) and drops it when the test
// ends. settings is "" or a SETTINGS clause.
func createUser(t *testing.T, conn clickhouse.Conn, addr, name, settings string, grants ...string) string {
	t.Helper()
	password := randomHex(t, 16)
	cleanup(t, addr, "DROP USER IF EXISTS "+ident(name))
	mustExec(t, conn, fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY %s %s", ident(name), literal(password), settings))
	for _, g := range grants {
		mustExec(t, conn, fmt.Sprintf(g, ident(name)))
	}
	return password
}

// createReader creates, on the peer, a reader with spec §8.6's reader grants
// on db: SELECT on the database and SHOW COLUMNS plus si_reader's seven
// columns on system.parts. remote() describes system.parts before it reads
// it, which needs SHOW COLUMNS (measured; plan S3-C2 Appendix B).
func createReader(t *testing.T, p pair, db string) (string, string) {
	t.Helper()
	user := "src_reader_" + suffix(t)
	password := createUser(t, p.Peer, p.PeerAddr, user, "",
		"GRANT SELECT ON "+ident(db)+".* TO %s",
		"GRANT SHOW COLUMNS ON system.parts TO %s",
		"GRANT SELECT(database, table, name, active, rows, partition_id, hash_of_all_files) ON system.parts TO %s")
	return user, password
}

// createCollection creates, on the member's ClickHouse, a named collection
// reaching host:port as user, every key NOT OVERRIDABLE (contract §23.2), and
// drops it when the test ends.
func createCollection(t *testing.T, p pair, name, host string, port uint64, user, password string) {
	t.Helper()
	cleanup(t, p.MemberAddr, "DROP NAMED COLLECTION IF EXISTS "+ident(name))
	mustExec(t, p.Member, fmt.Sprintf("CREATE NAMED COLLECTION %s AS host = %s NOT OVERRIDABLE, port = %d NOT OVERRIDABLE, user = %s NOT OVERRIDABLE, password = %s NOT OVERRIDABLE",
		ident(name), literal(host), port, literal(user), literal(password)))
}

// ownerCollection creates a reader for db on the peer and a collection to it
// on the member's ClickHouse, and returns the collection's name.
func ownerCollection(t *testing.T, p pair, prefix, db string) string {
	t.Helper()
	user, password := createReader(t, p, db)
	name := prefix + suffix(t)
	createCollection(t, p, name, p.PeerHost, p.PeerPort, user, password)
	return name
}

// memberUser creates, on the member's ClickHouse, a user holding the slice of
// contract §23.2's SI_MEMBER_GRANTS that the sources use, with stage as its
// hg_rstage, plus extra grants, and returns a connection as it.
func memberUser(t *testing.T, p pair, stage, settings string, collections []string, extra ...string) clickhouse.Conn {
	t.Helper()
	mustExec(t, p.Member, "CREATE DATABASE IF NOT EXISTS "+ident(stage))
	cleanup(t, p.MemberAddr, "DROP DATABASE IF EXISTS "+ident(stage)+" SYNC")
	grants := []string{
		"GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE ON " + ident(stage) + ".* TO %s",
		"GRANT TABLE ENGINE ON MergeTree TO %s",
		"GRANT SELECT ON system.parts TO %s",
		"GRANT READ ON REMOTE TO %s",
		"GRANT CREATE TEMPORARY TABLE ON *.* TO %s",
	}
	for _, c := range collections {
		grants = append(grants, "GRANT NAMED COLLECTION ON "+ident(c)+" TO %s")
	}
	user := "src_member_" + suffix(t)
	password := createUser(t, p.Member, p.MemberAddr, user, settings, append(grants, extra...)...)
	return chtest.OpenAs(t, p.MemberAddr, user, password)
}

// newScratch creates scratch table name in stage as conn's user, with
// ddl.ScratchIntent's structure.
func newScratch(t *testing.T, conn clickhouse.Conn, stage, name string, schema payloadexec.TableSchema) Scratch {
	t.Helper()
	intent, err := ddl.ScratchIntent(ddl.MemberPinned{
		MemberUnsafeDB: "hg_member_unsafe", ReplicaDB: "hg_replica", StageDB: stage, NodeID: "member-test",
	}, schema, name)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, conn, intent.SQL())
	return Scratch{Database: stage, Table: name}
}

// tablesIn lists the tables of db.
func tablesIn(t *testing.T, conn clickhouse.Conn, db string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT name FROM system.tables WHERE database = ? ORDER BY name", db)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			t.Fatal(err)
		}
		out = append(out, n)
	}
	return out
}

// clickHouseCode is the ClickHouse error code of err, or 0.
func clickHouseCode(err error) int32 {
	var exception *clickhouse.Exception
	if !errors.As(err, &exception) {
		return 0
	}
	return exception.Code
}

// queryLog reads the finished queries of user from system.query_log after a
// flush.
func queryLog(t *testing.T, conn clickhouse.Conn, user string) []string {
	t.Helper()
	mustExec(t, conn, "SYSTEM FLUSH LOGS")
	rows, err := conn.Query(context.Background(), "SELECT query FROM system.query_log WHERE user = ? AND type IN ('QueryFinish', 'ExceptionBeforeStart', 'ExceptionWhileProcessing') ORDER BY event_time_microseconds", user)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var q string
		if err := rows.Scan(&q); err != nil {
			t.Fatal(err)
		}
		out = append(out, strings.TrimSpace(q))
	}
	return out
}
