// Package chmechanics pins, on a real ClickHouse pair, the engine behaviour
// the storage-integrity protocol relies on (housegate spec 2026-10-14 §2,
// "ClickHouse mechanics (measured)" and "Restart resurrection"; plan S3-A
// clarification K2), so that an image whose behaviour differs fails here
// before it reaches a storage-integrity ClickHouse. The package holds tests
// only. Every test is opt-in with ARBITER_CH_MECHANICS=1 and then fails,
// rather than skips, when a prerequisite is missing;
// scripts/ci/clickhouse-mechanics.sh starts the pair and runs the suite
// (README § ClickHouse mechanics).
package chmechanics

import (
	"context"
	"crypto/rand"
	"crypto/sha1"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// The suite's environment (README § ClickHouse mechanics).
const (
	envMechanics     = "ARBITER_CH_MECHANICS"
	envIntegration   = "ARBITER_CH_INTEGRATION"
	envKeeper        = "ARBITER_CH_KEEPER"
	envAddr          = "CH_ADDR"
	envReplicaAddr   = "CH_REPLICA_ADDR"
	envPeerAddr      = "CH_PEER_ADDR"
	envRestart       = "ARBITER_CH_RESTART"
	envRestartAddr   = "CH_RESTART_ADDR"
	envVersionPrefix = "ARBITER_CH_EXPECT_VERSION_PREFIX"
	envImage         = "ARBITER_CH_IMAGE"
)

// ClickHouse error codes the suite asserts.
const (
	codeBadArguments = 36  // BAD_ARGUMENTS
	codeTooManyParts = 252 // TOO_MANY_PARTS
	codeAccessDenied = 497 // ACCESS_DENIED
)

const (
	// restartTimeout bounds one restart: the process exit, Docker's restart
	// of the container and the new server answering with a later start time.
	restartTimeout = 120 * time.Second
	// settleTimeout bounds a wait for background replication.
	settleTimeout = 60 * time.Second
)

// mechanicsEnv is the pair the suite runs on. Node A hosts the shared
// Keeper. Node B is a Keeper client and, in the scripted topology, also the
// server the restart tests restart (CH_RESTART_ADDR).
type mechanicsEnv struct {
	A, B          clickhouse.Conn
	AAddr, BAddr  string
	PeerAddr      string // node A as node B reaches it, for remote() and named collections
	Image         string
	VersionPrefix string
}

// requireMechanics skips unless ARBITER_CH_MECHANICS=1 and then fails on any
// missing prerequisite: the suite is a release gate, so a half-configured run
// must not pass by skipping.
func requireMechanics(t *testing.T) mechanicsEnv {
	t.Helper()
	if os.Getenv(envMechanics) != "1" {
		t.Skip("set ARBITER_CH_MECHANICS=1 and run scripts/ci/clickhouse-mechanics.sh (README § ClickHouse mechanics) to run the ClickHouse mechanics suite")
	}
	var missing []string
	for _, flag := range []string{envIntegration, envKeeper} {
		if os.Getenv(flag) != "1" {
			missing = append(missing, flag+"=1")
		}
	}
	for _, name := range []string{envAddr, envReplicaAddr, envPeerAddr, envVersionPrefix, envImage} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("ARBITER_CH_MECHANICS=1 requires %s", strings.Join(missing, ", "))
	}
	env := mechanicsEnv{
		AAddr:         os.Getenv(envAddr),
		BAddr:         os.Getenv(envReplicaAddr),
		PeerAddr:      os.Getenv(envPeerAddr),
		Image:         os.Getenv(envImage),
		VersionPrefix: os.Getenv(envVersionPrefix),
	}
	env.A = chtest.Open(t, env.AAddr)
	env.B = chtest.Open(t, env.BAddr)
	requireKeeper(t, envAddr, env.A)
	requireKeeper(t, envReplicaAddr, env.B)
	return env
}

// requireKeeper waits until the server reaches its Keeper: node B may answer
// queries right after a restart test restarted it, before its Keeper session
// is back.
func requireKeeper(t *testing.T, name string, conn clickhouse.Conn) {
	t.Helper()
	eventually(t, settleTimeout, "the server at "+name+" reaching its Keeper (scripts/ci/clickhouse-shared-keeper-*.xml)", func() (bool, string) {
		var n uint64
		err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n)
		return err == nil, fmt.Sprintf("last error: %v", err)
	})
}

// requireRestartable returns CH_RESTART_ADDR. The restart tests are a second
// opt-in, ARBITER_CH_RESTART=1, because they need a server whose container
// Docker restarts (--restart always) and that does not host the Keeper.
func requireRestartable(t *testing.T) string {
	t.Helper()
	if os.Getenv(envRestart) != "1" {
		t.Skip("set ARBITER_CH_RESTART=1 and CH_RESTART_ADDR (a server whose container runs with --restart always and does not host the Keeper) to run the restart mechanics")
	}
	addr := os.Getenv(envRestartAddr)
	if addr == "" {
		t.Fatal("ARBITER_CH_RESTART=1 requires CH_RESTART_ADDR")
	}
	return addr
}

// suffix keeps databases, users, collections and Keeper paths unique per
// test and subtest.
func suffix(t *testing.T) string {
	sum := sha1.Sum([]byte(t.Name()))
	return hex.EncodeToString(sum[:])[:10]
}

// mechanicsPinned is a test's own protocol databases, replica name and Keeper
// shard, so ddl.KeeperUnsafeTables lists only that test's tables.
func mechanicsPinned(t *testing.T) ddl.Pinned {
	sum := sha1.Sum([]byte(t.Name()))
	s := hex.EncodeToString(sum[:])[:10]
	return ddl.Pinned{
		UnsafeDB:      "hg_unsafe_m" + s,
		SafeDB:        "hg_safe_m" + s,
		PromoteDB:     "hg_promote_m" + s,
		NodeID:        "mech-" + s,
		KeeperShardID: 1_000_000 + binary.BigEndian.Uint32(sum[10:14])%1_000_000,
	}
}

// mechanicsSchema is the shape of an SI table: a String partition key and one
// value column.
func mechanicsSchema(t *testing.T) payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "mech.t_" + suffix(t),
		PartitionBy: "p",
		Columns:     []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}},
	}
}

// ident quotes a ClickHouse identifier.
func ident(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// literal quotes a ClickHouse string literal.
func literal(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return "'" + strings.ReplaceAll(s, `'`, `\'`) + "'"
}

func qualified(db, table string) string {
	return ident(db) + "." + ident(table)
}

func mustExec(t *testing.T, conn clickhouse.Conn, query string) {
	t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("%s: %v", query, err)
	}
}

// clickHouseCode is err's server error code, or 0 when err is not a server
// exception (a transport error, or nil).
func clickHouseCode(err error) int32 {
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		return exception.Code
	}
	return 0
}

// dial opens and pings a connection without failing the test, for the
// restart loop and for cleanups that run after a restart.
func dial(ctx context.Context, addr string) (clickhouse.Conn, error) {
	opts := chtest.Options(addr)
	opts.DialTimeout = 2 * time.Second
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

// dropDatabasesAt drops databases on the server at addr when the test ends,
// through a fresh connection, so a restart in between leaves nothing behind.
func dropDatabasesAt(t *testing.T, addr string, databases ...string) {
	t.Helper()
	t.Cleanup(func() {
		conn, err := dial(context.Background(), addr)
		if err != nil {
			t.Logf("cleanup: cannot reach %s to drop %v: %v", addr, databases, err)
			return
		}
		defer func() { _ = conn.Close() }()
		for _, database := range databases {
			if err := conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+ident(database)+" SYNC"); err != nil {
				t.Logf("cleanup: drop database %s on %s: %v", database, addr, err)
			}
		}
	})
}

// createTables creates each intent's database and table on conn with the
// protocol DDL (TableIntent.SQL) and drops those databases when the test ends.
func createTables(t *testing.T, conn clickhouse.Conn, addr string, intents ...ddl.TableIntent) {
	t.Helper()
	var databases []string
	for _, intent := range intents {
		if !slices.Contains(databases, intent.Database) {
			databases = append(databases, intent.Database)
		}
	}
	dropDatabasesAt(t, addr, databases...)
	for _, database := range databases {
		mustExec(t, conn, "CREATE DATABASE IF NOT EXISTS "+ident(database))
	}
	for _, intent := range intents {
		mustExec(t, conn, intent.SQL())
	}
}

// protocolTables creates p's hg_safe and hg_promote MergeTree tables for sch
// from ddl.Intents, so every pinned engine setting applies, and returns all
// three intents; a caller that needs the hg_unsafe ReplicatedMergeTree table
// passes the first one to createTables.
func protocolTables(t *testing.T, conn clickhouse.Conn, addr string, p ddl.Pinned, sch payloadexec.TableSchema) (ddl.TableIntent, ddl.TableIntent, ddl.TableIntent) {
	t.Helper()
	unsafe, safe, promote, err := ddl.Intents(p, sch)
	if err != nil {
		t.Fatalf("protocol intents for %s: %v", sch.TableID, err)
	}
	createTables(t, conn, addr, safe, promote)
	return unsafe, safe, promote
}

// mechPart is one row of system.parts.
type mechPart struct {
	Name      string
	Partition string // system.parts.partition: the value, p0 for partition 'p0'
	PhysHash  string // system.parts.hash_of_all_files
	Rows      uint64
	MinBlock  int64
	MaxBlock  int64
	Active    bool
}

// partsOf lists every part of db.table that system.parts shows, active and
// outdated, by name.
func partsOf(t *testing.T, conn clickhouse.Conn, db, table string) []mechPart {
	t.Helper()
	rows, err := conn.Query(context.Background(), `
		SELECT name, partition, hash_of_all_files, rows, min_block_number, max_block_number, toUInt8(active)
		FROM system.parts WHERE database = ? AND table = ? ORDER BY name`, db, table)
	if err != nil {
		t.Fatalf("system.parts of %s.%s: %v", db, table, err)
	}
	defer rows.Close()
	var out []mechPart
	for rows.Next() {
		var part mechPart
		var active uint8
		if err := rows.Scan(&part.Name, &part.Partition, &part.PhysHash, &part.Rows, &part.MinBlock, &part.MaxBlock, &active); err != nil {
			t.Fatalf("scan system.parts of %s.%s: %v", db, table, err)
		}
		part.Active = active == 1
		out = append(out, part)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("system.parts of %s.%s: %v", db, table, err)
	}
	return out
}

// activeParts is partsOf restricted to the active parts of one partition.
func activeParts(t *testing.T, conn clickhouse.Conn, db, table, partition string) []mechPart {
	t.Helper()
	var out []mechPart
	for _, part := range partsOf(t, conn, db, table) {
		if part.Active && part.Partition == partition {
			out = append(out, part)
		}
	}
	return out
}

func names(parts []mechPart) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		out = append(out, part.Name)
	}
	slices.Sort(out)
	return out
}

func physHashes(parts []mechPart) []string {
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		out = append(out, part.PhysHash)
	}
	slices.Sort(out)
	return out
}

func totalRows(parts []mechPart) uint64 {
	var n uint64
	for _, part := range parts {
		n += part.Rows
	}
	return n
}

func withRows(parts []mechPart) []mechPart {
	var out []mechPart
	for _, part := range parts {
		if part.Rows > 0 {
			out = append(out, part)
		}
	}
	return out
}

func zeroRowParts(parts []mechPart) []mechPart {
	var out []mechPart
	for _, part := range parts {
		if part.Rows == 0 {
			out = append(out, part)
		}
	}
	return out
}

func partNamed(parts []mechPart, name string) (mechPart, bool) {
	for _, part := range parts {
		if part.Name == name {
			return part, true
		}
	}
	return mechPart{}, false
}

func partWithHash(parts []mechPart, physHash string) (mechPart, bool) {
	for _, part := range parts {
		if part.PhysHash == physHash {
			return part, true
		}
	}
	return mechPart{}, false
}

// rowHashes scans parts of db.table with chexec.ScanParts, the SNode's row
// identity, and returns each part's row LtHash by name. Every part must hold
// rows: ScanParts refuses an empty one.
func rowHashes(t *testing.T, conn clickhouse.Conn, db, table string, sch payloadexec.TableSchema, parts []mechPart) map[string]string {
	t.Helper()
	scans, err := chexec.ScanParts(context.Background(), conn, db+"."+table, sch, names(parts))
	if err != nil {
		t.Fatalf("scan %s.%s parts %v: %v", db, table, names(parts), err)
	}
	out := make(map[string]string, len(scans))
	for _, scan := range scans {
		out[scan.PartName] = scan.RowLtHash
	}
	return out
}

// rowHashList is rowHashes as a sorted multiset.
func rowHashList(t *testing.T, conn clickhouse.Conn, db, table string, sch payloadexec.TableSchema, parts []mechPart) []string {
	t.Helper()
	var out []string
	for _, h := range rowHashes(t, conn, db, table, sch, parts) {
		out = append(out, h)
	}
	slices.Sort(out)
	return out
}

// sumRowHashes adds row LtHash accumulators: the content root of the parts.
func sumRowHashes(t *testing.T, hashes ...string) string {
	t.Helper()
	acc := lthash.New()
	for _, h := range hashes {
		raw, err := hex.DecodeString(strings.TrimPrefix(h, "0x"))
		if err != nil {
			t.Fatalf("row LtHash %s: %v", abbreviate(h), err)
		}
		part, err := lthash.FromBytes(raw)
		if err != nil {
			t.Fatalf("row LtHash %s: %v", abbreviate(h), err)
		}
		acc.AddHash(part)
	}
	return "0x" + hex.EncodeToString(acc.Bytes())
}

// abbreviate shortens a 4098-character accumulator for messages.
func abbreviate(h string) string {
	if len(h) <= 26 {
		return h
	}
	return h[:18] + "…" + h[len(h)-8:]
}

// insertRows writes n rows of partition into db.table with one
// single-threaded INSERT ... SELECT. v runs from `from` and _hg_row_id is the
// SHA-256 of the partition and v, so equal (partition, from, n) writes equal
// rows, with equal row LtHash, into any table of mechanicsSchema. Without
// extra settings one call writes one part for n up to the default
// min_insert_block_size_rows.
func insertRows(conn clickhouse.Conn, db, table, partition string, from, n uint64, extra clickhouse.Settings) error {
	settings := clickhouse.Settings{"max_threads": 1, "max_insert_threads": 1}
	for name, value := range extra {
		settings[name] = value
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(settings))
	return conn.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s (%s, `p`, `v`) SELECT SHA256(concat('mech:', %s, ':', toString(number))), %s, number FROM numbers(%d, %d)",
		qualified(db, table), ident(ddl.RowIDColumn), literal(partition), literal(partition), from, n))
}

func mustInsert(t *testing.T, conn clickhouse.Conn, db, table, partition string, from, n uint64, extra clickhouse.Settings) {
	t.Helper()
	if err := insertRows(conn, db, table, partition, from, n, extra); err != nil {
		t.Fatalf("insert %d rows into %s.%s partition %s: %v", n, db, table, partition, err)
	}
}

// countRows is SELECT count() FROM source, where source is a table or a table
// function.
func countRows(conn clickhouse.Conn, source string) (uint64, error) {
	var n uint64
	err := conn.QueryRow(context.Background(), "SELECT count() FROM "+source).Scan(&n)
	return n, err
}

func mustCountRows(t *testing.T, conn clickhouse.Conn, source string) uint64 {
	t.Helper()
	n, err := countRows(conn, source)
	if err != nil {
		t.Fatalf("count %s: %v", source, err)
	}
	return n
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// eventually polls cond every 500 ms until it holds or timeout passes; cond
// reports the state it saw for the failure message.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, state := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: not reached within %s: %s", what, timeout, state)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// restartKind is the SQL statement that ends the server process. Docker
// restarts the container (--restart always), so the tests need no Docker
// CLI.
type restartKind string

const (
	// restartAbrupt exits at once without shutting tables down, as a SIGKILL
	// or an OOM kill does (SYSTEM KILL calls _exit).
	restartAbrupt restartKind = "SYSTEM KILL"
	// restartGraceful shuts the server down through its SIGTERM handler, as a
	// pod deletion does.
	restartGraceful restartKind = "SYSTEM SHUTDOWN"
)

func startedAt(t *testing.T, conn clickhouse.Conn) time.Time {
	t.Helper()
	at, err := chrestart.StartedAt(context.Background(), conn, time.Now())
	if err != nil {
		t.Fatalf("ClickHouse start time: %v", err)
	}
	return at
}

// ensureUptime waits until the server has been up for at least atLeast, so a
// restart moves its start time by more than chrestart.DefaultRestartSkew and
// Docker's restart back-off stays at its first step.
func ensureUptime(t *testing.T, conn clickhouse.Conn, atLeast time.Duration) {
	t.Helper()
	if up := time.Since(startedAt(t, conn)); up < atLeast {
		time.Sleep(atLeast - up)
	}
}

// restartServer ends the server at addr with kind and returns a connection to
// the server Docker started in its place: the one whose start time
// (chrestart.StartedAt, the owner audit's restart detector) is later than the
// old one by more than chrestart.DefaultRestartSkew.
func restartServer(t *testing.T, addr string, kind restartKind) clickhouse.Conn {
	t.Helper()
	ctx := context.Background()
	old, err := dial(ctx, addr)
	if err != nil {
		t.Fatalf("connect to %s before %s: %v", addr, kind, err)
	}
	ensureUptime(t, old, 2*chrestart.DefaultRestartSkew)
	before := startedAt(t, old)
	execCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	err = old.Exec(execCtx, string(kind))
	cancel()
	_ = old.Close()
	t.Logf("%s on %s returned %v", kind, addr, err)
	deadline := time.Now().Add(restartTimeout)
	last := errors.New("no reconnect attempt")
	for time.Now().Before(deadline) {
		time.Sleep(500 * time.Millisecond)
		conn, err := dial(ctx, addr)
		if err != nil {
			last = err
			continue
		}
		at, err := chrestart.StartedAt(ctx, conn, time.Now())
		if err == nil && at.After(before.Add(chrestart.DefaultRestartSkew)) {
			t.Cleanup(func() { _ = conn.Close() })
			t.Logf("%s restarted %s: start time %s, then %s", kind, addr, before.UTC().Format(time.RFC3339), at.UTC().Format(time.RFC3339))
			return conn
		}
		if err == nil {
			err = fmt.Errorf("the server still reports start time %s", at.UTC().Format(time.RFC3339))
		}
		last = err
		_ = conn.Close()
	}
	t.Fatalf("%s did not restart %s within %s (does its container run with --restart always?): %v", kind, addr, restartTimeout, last)
	return nil
}

// waitLoaded waits until a restarted server has loaded db.table's parts:
// SYSTEM WAIT LOADING PARTS covers parts loaded asynchronously, and the count
// forces a table that loads lazily at startup.
func waitLoaded(t *testing.T, conn clickhouse.Conn, db, table string) {
	t.Helper()
	mustExec(t, conn, "SYSTEM WAIT LOADING PARTS "+qualified(db, table))
	mustCountRows(t, conn, qualified(db, table))
}

// TestMechanics_ServerIsTheDeployedImage (M0) records which server the suite
// measured: every server runs a ClickHouse whose version has the expected
// prefix, all the same one, and the log names the image, so a green run on
// upstream 26.3.37.3 is never mistaken for one on the deployed
// 26.3-lts-decimal512 image.
func TestMechanics_ServerIsTheDeployedImage(t *testing.T) {
	env := requireMechanics(t)
	type server struct {
		name string
		conn clickhouse.Conn
	}
	servers := []server{{envAddr, env.A}, {envReplicaAddr, env.B}}
	if addr := os.Getenv(envRestartAddr); os.Getenv(envRestart) == "1" && addr != "" {
		servers = append(servers, server{envRestartAddr, chtest.Open(t, addr)})
	}
	var first string
	for _, s := range servers {
		var version string
		var uptime uint32
		if err := s.conn.QueryRow(context.Background(), "SELECT version(), uptime()").Scan(&version, &uptime); err != nil {
			t.Fatalf("%s: SELECT version(): %v", s.name, err)
		}
		t.Logf("mechanics server %s: image %s, ClickHouse %s, up %ds", s.name, env.Image, version, uptime)
		if !strings.HasPrefix(version, env.VersionPrefix) {
			t.Fatalf("%s runs ClickHouse %s, want the %s prefix of image %s", s.name, version, env.VersionPrefix, env.Image)
		}
		if first == "" {
			first = version
		} else if version != first {
			t.Fatalf("%s runs ClickHouse %s but %s runs %s: the pair must run one image", s.name, version, servers[0].name, first)
		}
	}
}
