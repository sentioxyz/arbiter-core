package confinement

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
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

// The suite's environment (contract §23.3). CH_ADDR is the fenced server
// (access_control_improvements.table_engines_require_grant, the
// replicated_merge_tree default of scripts/ci/clickhouse-member-access-control.xml
// and an embedded Keeper); CH_PEER_ADDR is a second, unfenced server as the
// test reaches it, CH_PEER_ADDR_FROM_SERVER the same server as CH_ADDR's
// server reaches it; S3_* name a bucket both servers reach, with a key that
// may write it. The admin user of both servers is chtest's (CH_USER,
// CH_PASSWORD; the passwordless default in the script) and manages users and
// named collections.
const (
	envOptIn          = "ARBITER_CH_MEMBER_CONFINEMENT"
	envAddr           = "CH_ADDR"
	envPeerAddr       = "CH_PEER_ADDR"
	envPeerFromServer = "CH_PEER_ADDR_FROM_SERVER"
	envVersionPrefix  = "ARBITER_CH_EXPECT_VERSION_PREFIX"
	envImage          = "ARBITER_CH_IMAGE"
	envS3Endpoint     = "S3_ENDPOINT"
	envS3Bucket       = "S3_BUCKET"
	envS3AccessKeyID  = "S3_ACCESS_KEY_ID"
	envS3Secret       = "S3_SECRET"
)

// The users, collections and node id the suite runs as (contract §23.2, K29).
const (
	memberUser      = "si_member"
	memberDDLUser   = "si_member_ddl"
	readerUser      = "si_reader"
	ownerCollection = "si_owner_0"
	poolCollection  = "si_peer_verifier_1"
	memberNodeID    = "member-conf"
)

// siMemberGrants is SI_MEMBER_GRANTS of contract §23.2 in its order, with
// the collection grants of this suite's two collections in their place.
var siMemberGrants = []string{
	"GRANT SELECT ON hg_member_unsafe.*",
	"GRANT SYSTEM SYNC REPLICA ON hg_member_unsafe.*",
	"GRANT SELECT, INSERT, ALTER DELETE, CREATE TABLE, DROP TABLE ON hg_replica.*",
	"GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE, TRUNCATE, OPTIMIZE ON hg_rstage.*",
	"GRANT CREATE DATABASE ON hg_replica.*",
	"GRANT CREATE DATABASE ON hg_rstage.*",
	"GRANT READ ON S3",
	"GRANT TABLE ENGINE ON MergeTree",
	"GRANT TABLE ENGINE ON Memory",
	"GRANT SELECT ON system.parts",
	"GRANT SELECT ON system.tables",
	"GRANT SELECT ON system.detached_tables",
	"GRANT SELECT ON system.replicas",
	"GRANT SELECT ON system.zookeeper",
	"GRANT NAMED COLLECTION ON " + ownerCollection,
	"GRANT NAMED COLLECTION ON " + poolCollection,
	"GRANT READ ON REMOTE",
	"GRANT CREATE TEMPORARY TABLE ON *.*",
}

// siMemberDDLGrants is SI_MEMBER_DDL_GRANTS of contract §23.2, and
// siMemberDDLProfile its settings profile's one setting.
var siMemberDDLGrants = []string{
	"GRANT CREATE DATABASE ON hg_member_unsafe.*",
	"GRANT CREATE TABLE, DROP TABLE ON hg_member_unsafe.*",
	"GRANT TABLE ENGINE ON ReplicatedMergeTree",
}

const siMemberDDLProfile = "merge_tree_max_bytes_to_merge_at_max_space_in_pool = 0 CONST"

// siReaderGrants is si_reader's login on an owner or verifier ClickHouse:
// Plan S3-B contract §11's three grants and SHOW COLUMNS ON system.parts,
// without which remote() cannot describe the owner's system.parts for the
// owner pull's resolve step (measured on 26.3: 497, "SHOW COLUMNS ON
// system.parts"; Plan S3-C2 Appendix B, Task 31).
var siReaderGrants = []string{
	"GRANT SELECT ON hg_safe.*",
	"GRANT SELECT ON hg_unsafe.*",
	"GRANT SELECT(database, table, name, active, rows, partition_id, hash_of_all_files) ON system.parts",
	"GRANT SHOW COLUMNS ON system.parts",
}

// pinned is the member's database set; the protocol set is the host SNode's.
var (
	pinned   = ddl.DefaultMemberPinned(memberNodeID, 0, "")
	protocol = ddl.Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "snode-conf"}
)

// suite is one test's servers and users.
type suite struct {
	t                    *testing.T
	addr, peerAddr       string
	peerHost             string
	peerPort             uint64
	prefix, image        string
	s3Endpoint, s3Bucket string
	s3Key, s3Secret      string
	admin, peer          clickhouse.Conn // the admin users of CH_ADDR and CH_PEER_ADDR
	member               clickhouse.Conn // si_member on CH_ADDR
	memberPassword       string
	readerPassword       string
	sfx                  string
}

// setup opens both servers and makes si_reader on the peer, the two
// collections and si_member on the fenced server, and the member databases
// as si_member.
func setup(t *testing.T) *suite {
	t.Helper()
	if os.Getenv(envOptIn) != "1" {
		t.Skipf("set %s=1 and run scripts/ci/clickhouse-member-confinement.sh <image> <version prefix>", envOptIn)
	}
	var missing []string
	get := func(name string) string {
		v := os.Getenv(name)
		if v == "" {
			missing = append(missing, name)
		}
		return v
	}
	s := &suite{t: t}
	s.addr, s.peerAddr = get(envAddr), get(envPeerAddr)
	fromServer := get(envPeerFromServer)
	s.prefix, s.image = get(envVersionPrefix), get(envImage)
	s.s3Endpoint, s.s3Bucket = strings.TrimRight(get(envS3Endpoint), "/"), get(envS3Bucket)
	s.s3Key, s.s3Secret = get(envS3AccessKeyID), get(envS3Secret)
	if len(missing) > 0 {
		t.Fatalf("%s=1 requires %s", envOptIn, strings.Join(missing, ", "))
	}
	host, port, err := net.SplitHostPort(fromServer)
	if err != nil {
		t.Fatalf("%s %q: %v", envPeerFromServer, fromServer, err)
	}
	if s.peerPort, err = strconv.ParseUint(port, 10, 16); err != nil {
		t.Fatalf("%s %q: %v", envPeerFromServer, fromServer, err)
	}
	s.peerHost = host
	s.sfx = randomHex(t, 4)
	s.admin, s.peer = chtest.Open(t, s.addr), chtest.Open(t, s.peerAddr)

	for _, db := range []string{"hg_safe", "hg_unsafe"} {
		s.exec(s.peer, "CREATE DATABASE IF NOT EXISTS "+db)
	}
	s.readerPassword = s.createUser(s.peer, readerUser, "", siReaderGrants)
	for _, c := range []string{ownerCollection, poolCollection} {
		s.exec(s.admin, "DROP NAMED COLLECTION IF EXISTS "+c)
		s.exec(s.admin, fmt.Sprintf("CREATE NAMED COLLECTION %s AS host = %s NOT OVERRIDABLE, port = %d NOT OVERRIDABLE, user = %s NOT OVERRIDABLE, password = %s NOT OVERRIDABLE",
			c, literal(s.peerHost), s.peerPort, literal(readerUser), literal(s.readerPassword)))
	}
	s.exec(s.admin, "CREATE DATABASE IF NOT EXISTS hg_member_unsafe")
	s.memberPassword = s.createUser(s.admin, memberUser, "", siMemberGrants)
	s.member = chtest.OpenAs(t, s.addr, memberUser, s.memberPassword)
	if err := ddl.EnsureMemberDatabases(context.Background(), s.member, pinned); err != nil {
		t.Fatalf("ddl.EnsureMemberDatabases as %s: %v", memberUser, err)
	}
	return s
}

// createUser drops and creates user with a fresh password from crypto/rand,
// the settings profile profile when not empty, and each grant; it returns
// the password, which goes only into the CREATE USER statement.
func (s *suite) createUser(conn clickhouse.Conn, user, profile string, grants []string) string {
	s.t.Helper()
	password := randomHex(s.t, 16)
	s.exec(conn, "DROP USER IF EXISTS "+user)
	q := "CREATE USER " + user + " IDENTIFIED WITH sha256_password BY " + literal(password)
	if profile != "" {
		q += " SETTINGS PROFILE " + literal(profile)
	}
	s.exec(conn, q)
	for _, g := range grants {
		s.exec(conn, g+" TO "+user)
	}
	return password
}

// ddlUser makes si_member_ddl with its settings profile.
func (s *suite) ddlUser() clickhouse.Conn {
	s.t.Helper()
	s.exec(s.admin, "DROP USER IF EXISTS "+memberDDLUser)
	s.exec(s.admin, "DROP SETTINGS PROFILE IF EXISTS "+memberDDLUser)
	s.exec(s.admin, "CREATE SETTINGS PROFILE "+memberDDLUser+" SETTINGS "+siMemberDDLProfile)
	password := s.createUser(s.admin, memberDDLUser, memberDDLUser, siMemberDDLGrants)
	return chtest.OpenAs(s.t, s.addr, memberDDLUser, password)
}

// name returns a table name unique to this test.
func (s *suite) name(base string) string { return "conf_" + base + "_" + s.sfx }

func (s *suite) exec(conn clickhouse.Conn, query string) {
	s.t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		s.t.Fatalf("%s: %v", redact(query), err)
	}
}

// refused requires query to fail with ClickHouse error code and a message
// containing want: an access refusal, never a parse error.
func (s *suite) refused(conn clickhouse.Conn, code int32, want, query string) {
	s.t.Helper()
	err := conn.Exec(context.Background(), query)
	var ex *clickhouse.Exception
	switch {
	case err == nil:
		s.t.Fatalf("%s was accepted, want code %d (%s)", redact(query), code, want)
	case !errors.As(err, &ex) || ex.Code != code || !strings.Contains(ex.Message, want):
		s.t.Fatalf("%s: %v, want code %d with %q", redact(query), err, code, want)
	}
}

// refusedSelect requires the same 497 SELECT denial on every supported
// build. 26.3.12.1 says "SELECT for at least one column on <table>" for
// count(), whereas 26.3.37.3 says "SELECT ON <table>". Both the privilege
// and the exact table must appear; a syntax error or another denial fails.
func (s *suite) refusedSelect(conn clickhouse.Conn, table, query string) {
	s.t.Helper()
	err := conn.Exec(context.Background(), query)
	var ex *clickhouse.Exception
	if !errors.As(err, &ex) || ex.Code != 497 || !strings.Contains(ex.Message, "SELECT") || !strings.Contains(ex.Message, table) {
		s.t.Fatalf("%s: %v, want code 497 with SELECT and %q", redact(query), err, table)
	}
}

// measure runs query, logs whether it was accepted, for the run record, and
// returns whether it was.
func (s *suite) measure(conn clickhouse.Conn, what, query string) bool {
	s.t.Helper()
	err := conn.Exec(context.Background(), query)
	var ex *clickhouse.Exception
	switch {
	case err == nil:
		s.t.Logf("confinement: %s: accepted", what)
		return true
	case errors.As(err, &ex):
		s.t.Logf("confinement: %s: refused (%d)", what, ex.Code)
		return false
	default:
		s.t.Fatalf("%s: %v", redact(query), err)
		return false
	}
}

func (s *suite) count(conn clickhouse.Conn, query string, args ...any) uint64 {
	s.t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), query, args...).Scan(&n); err != nil {
		s.t.Fatalf("%s: %v", query, err)
	}
	return n
}

func (s *suite) tableExists(db, table string) bool {
	s.t.Helper()
	return s.count(s.admin, "SELECT count() FROM system.tables WHERE database = ? AND name = ?", db, table) == 1
}

// s3URL is an object URL under this test's own prefix of the bucket.
func (s *suite) s3URL(key string) string {
	return s.s3Endpoint + "/" + s.s3Bucket + "/confinement-" + s.sfx + "/" + key
}

func (s *suite) s3Args(key string) string {
	return literal(s.s3URL(key)) + ", " + literal(s.s3Key) + ", " + literal(s.s3Secret)
}

func randomHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

// qualified is db.table with both identifiers quoted, as ddl renders them:
// a scratch name carries the node id's hyphen.
func qualified(db, table string) string { return ident(db) + "." + ident(table) }

func ident(s string) string { return "`" + strings.ReplaceAll(s, "`", "``") + "`" }

func literal(s string) string {
	return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
}

// redact hides every quoted literal of a failing statement, so a password or
// key never reaches a test log.
func redact(query string) string {
	var b strings.Builder
	in := false
	for i := 0; i < len(query); i++ {
		c := query[i]
		switch {
		case c == '\\' && in && i+1 < len(query):
			i++
		case c == '\'':
			if !in {
				b.WriteString("'…")
			} else {
				b.WriteByte('\'')
			}
			in = !in
		case !in:
			b.WriteByte(c)
		}
	}
	return b.String()
}

// testSchema is the declared schema of the suite's owner tables.
func testSchema(id string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: "devuser1." + id, PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}
}

// rowID is a 32-byte _hg_row_id.
func rowID(n int) string { return fmt.Sprintf("conf%028d", n) }

// replicatedEngines are the eight Replicated*MergeTree engines of contract
// §23.3, each with the extra columns and engine arguments it needs.
var replicatedEngines = []struct{ engine, columns, args string }{
	{"ReplicatedMergeTree", "", ""},
	{"ReplicatedReplacingMergeTree", "", ""},
	{"ReplicatedSummingMergeTree", "", ""},
	{"ReplicatedAggregatingMergeTree", "", ""},
	{"ReplicatedCollapsingMergeTree", ", sign Int8", ", sign"},
	{"ReplicatedVersionedCollapsingMergeTree", ", sign Int8, ver UInt64", ", sign, ver"},
	{"ReplicatedGraphiteMergeTree", ", Path String, Time DateTime, Value Float64, Timestamp UInt32", ", 'graphite_rollup'"},
	{"ReplicatedCoalescingMergeTree", "", ""},
}

var memberDatabases = []string{ddl.StageDatabase, ddl.ReplicaDatabase}

func TestConfinement_ServerIsTheExpectedImage(t *testing.T) {
	s := setup(t)
	versions := map[string]string{}
	for _, srv := range []struct {
		name string
		conn clickhouse.Conn
	}{{envAddr, s.admin}, {envPeerAddr, s.peer}} {
		var version string
		var uptime uint64
		if err := srv.conn.QueryRow(context.Background(), "SELECT version(), toUInt64(uptime())").Scan(&version, &uptime); err != nil {
			t.Fatal(err)
		}
		if !strings.HasPrefix(version, s.prefix) {
			t.Fatalf("%s runs ClickHouse %s, want the %s prefix of image %s", srv.name, version, s.prefix, s.image)
		}
		versions[srv.name] = version
		t.Logf("confinement server %s: image %s, ClickHouse %s, up %ds", srv.name, s.image, version, uptime)
	}
	if versions[envAddr] != versions[envPeerAddr] {
		t.Fatalf("the servers run %v, want one version", versions)
	}
	// The fenced server loaded the member access-control file; the peer did
	// not.
	merges := func(conn clickhouse.Conn) (string, uint8) {
		var value string
		var changed uint8
		if err := conn.QueryRow(context.Background(), "SELECT value, changed FROM system.replicated_merge_tree_settings WHERE name = 'max_bytes_to_merge_at_max_space_in_pool'").Scan(&value, &changed); err != nil {
			t.Fatal(err)
		}
		return value, changed
	}
	if v, c := merges(s.admin); v != "0" || c != 1 {
		t.Fatalf("%s: replicated_merge_tree max_bytes_to_merge_at_max_space_in_pool = %s (changed %d), want 0 from scripts/ci/clickhouse-member-access-control.xml", envAddr, v, c)
	}
	if v, c := merges(s.peer); v == "0" || c != 0 {
		t.Fatalf("%s: replicated_merge_tree max_bytes_to_merge_at_max_space_in_pool = %s (changed %d), want the image default on the unfenced peer", envPeerAddr, v, c)
	}
	// The engine fence: a user with CREATE TABLE and no TABLE ENGINE grant
	// creates no table at all on the fenced server.
	control := "si_conf_control"
	password := s.createUser(s.admin, control, "", []string{"GRANT CREATE TABLE ON hg_rstage.*"})
	conn := chtest.OpenAs(t, s.addr, control, password)
	s.refused(conn, 497, "TABLE ENGINE ON MergeTree", "CREATE TABLE hg_rstage."+s.name("fence")+" (x UInt8) ENGINE = MergeTree ORDER BY x")
	s.exec(s.admin, "DROP USER IF EXISTS "+control)
	// The member databases were created without ENGINE: Atomic.
	for _, db := range memberDatabases {
		var engine string
		if err := s.admin.QueryRow(context.Background(), "SELECT engine FROM system.databases WHERE name = ?", db).Scan(&engine); err != nil || engine != "Atomic" {
			t.Fatalf("database %s has engine %q (%v), want Atomic", db, engine, err)
		}
	}
}

func TestConfinement_SIMemberCreatesNoReplicatedEngine(t *testing.T) {
	s := setup(t)
	// Nowhere: in hg_member_unsafe it may create no table at all.
	unsafe := s.name("member_unsafe_rmt")
	s.refused(s.member, 497, "CREATE TABLE ON hg_member_unsafe."+unsafe, fmt.Sprintf(
		"CREATE TABLE hg_member_unsafe.%s (_hg_row_id String, v UInt64) ENGINE = ReplicatedMergeTree('/sentio/confinement/%s/hg_member_unsafe/%s', %s) ORDER BY _hg_row_id",
		unsafe, s.sfx, unsafe, literal(memberNodeID)))
	for _, db := range memberDatabases {
		for _, e := range replicatedEngines {
			table := s.name(strings.ToLower(e.engine))
			s.refused(s.member, 497, "TABLE ENGINE ON "+e.engine, fmt.Sprintf(
				"CREATE TABLE %s.%s (_hg_row_id String, v UInt64%s) ENGINE = %s('/sentio/confinement/%s/%s/%s', %s%s) ORDER BY _hg_row_id",
				db, table, e.columns, e.engine, s.sfx, db, table, literal(memberNodeID), e.args))
			if s.tableExists(db, table) {
				t.Fatalf("%s.%s exists after a refused CREATE", db, table)
			}
		}
	}
}

func TestConfinement_SIMemberCreatesNoBufferDistributedMergeOrAlias(t *testing.T) {
	s := setup(t)
	for _, db := range memberDatabases {
		src := s.name("src")
		s.exec(s.member, fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id String, v UInt64) ENGINE = MergeTree ORDER BY _hg_row_id", db, src))
		for _, tc := range []struct{ engine, query string }{
			{"Buffer", fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id String, v UInt64) ENGINE = Buffer(%s, %s, 1, 10, 100, 10000, 1000000, 10000000, 100000000)", db, s.name("buffer"), literal(db), literal(src))},
			{"Distributed", fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id String, v UInt64) ENGINE = Distributed('default', %s, %s)", db, s.name("distributed"), literal(db), literal(src))},
			{"Merge", fmt.Sprintf("CREATE TABLE %s.%s (_hg_row_id String, v UInt64) ENGINE = Merge(%s, %s)", db, s.name("merge"), literal(db), literal("^"+src+"$"))},
			{"Alias", fmt.Sprintf("CREATE TABLE %s.%s ENGINE = Alias(%s, %s)", db, s.name("alias"), literal(db), literal(src))},
		} {
			s.refused(s.member, 497, "TABLE ENGINE ON "+tc.engine, tc.query)
		}
		s.exec(s.member, fmt.Sprintf("DROP TABLE %s.%s SYNC", db, src))
	}
}

func TestConfinement_SIMemberCreatesNoReplicatedDatabase(t *testing.T) {
	s := setup(t)
	defer func() {
		// Leave both databases as the role creates them.
		for _, db := range memberDatabases {
			s.exec(s.admin, "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
		if err := ddl.EnsureMemberDatabases(context.Background(), s.member, pinned); err != nil {
			t.Errorf("ddl.EnsureMemberDatabases as %s: %v", memberUser, err)
		}
	}()
	for _, db := range memberDatabases {
		s.exec(s.admin, "DROP DATABASE IF EXISTS "+db+" SYNC")
		s.refused(s.member, 497, "TABLE ENGINE ON Replicated",
			fmt.Sprintf("CREATE DATABASE %s ENGINE = Replicated('/sentio/confinement/%s/db/%s', 's1', %s)", db, s.sfx, db, literal(memberNodeID)))
		if n := s.count(s.admin, "SELECT count() FROM system.databases WHERE name = ?", db); n != 0 {
			t.Fatalf("database %s exists after a refused CREATE DATABASE", db)
		}
		// ClickHouse 26.3 checks a database ENGINE as TABLE ENGINE ON
		// <engine>: Memory is granted for temporary tables, so it is
		// measured for the run record; the role never names an engine.
		if s.measure(s.member, "CREATE DATABASE "+db+" ENGINE = Memory as si_member", "CREATE DATABASE "+db+" ENGINE = Memory") {
			s.exec(s.admin, "DROP DATABASE "+db+" SYNC")
		}
	}
	s.refused(s.member, 497, "CREATE DATABASE ON "+s.name("other")+".*", "CREATE DATABASE "+s.name("other"))
}

func TestConfinement_SIMemberCannotMoveDataIntoMemberUnsafe(t *testing.T) {
	s := setup(t)
	src, target, rtarget, mv := s.name("src"), s.name("target"), s.name("rtarget"), s.name("mv")
	s.exec(s.admin, "CREATE TABLE hg_member_unsafe."+target+" (_hg_row_id String, v UInt64) ENGINE = MergeTree ORDER BY _hg_row_id")
	s.exec(s.admin, "CREATE TABLE hg_member_unsafe."+rtarget+" (_hg_row_id String, v UInt64) ENGINE = ReplicatedMergeTree('/sentio/0/unsafe/"+rtarget+"', 'admin') ORDER BY _hg_row_id")
	t.Cleanup(func() {
		_ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS hg_member_unsafe."+target+" SYNC")
		_ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS hg_member_unsafe."+rtarget+" SYNC")
	})
	s.exec(s.member, "CREATE TABLE hg_rstage."+src+" (_hg_row_id String, v UInt64) ENGINE = MergeTree ORDER BY _hg_row_id")
	s.exec(s.member, "INSERT INTO hg_rstage."+src+" VALUES ('a', 1), ('b', 2)")
	for _, table := range []string{target, rtarget} {
		s.refused(s.member, 497, "INSERT, ALTER DELETE ON hg_member_unsafe."+table, "ALTER TABLE hg_member_unsafe."+table+" ATTACH PARTITION tuple() FROM hg_rstage."+src)
		s.refused(s.member, 497, "INSERT ON hg_member_unsafe."+table, "ALTER TABLE hg_rstage."+src+" MOVE PARTITION tuple() TO TABLE hg_member_unsafe."+table)
		s.refused(s.member, 497, "INSERT ON hg_member_unsafe."+table, "CREATE MATERIALIZED VIEW hg_rstage."+mv+" TO hg_member_unsafe."+table+" AS SELECT _hg_row_id, v FROM hg_rstage."+src)
		s.refused(s.member, 497, "ON hg_member_unsafe."+table, "INSERT INTO hg_member_unsafe."+table+" VALUES ('c', 3)")
		// Nor does it change a replicated table there in any other way (spec
		// §8.2: no member user writes, drops partitions of, truncates or
		// optimizes a replicated table).
		s.refused(s.member, 497, "ALTER DELETE ON hg_member_unsafe."+table, "ALTER TABLE hg_member_unsafe."+table+" DROP PARTITION tuple()")
		s.refused(s.member, 497, "ALTER DELETE ON hg_member_unsafe."+table, "ALTER TABLE hg_member_unsafe."+table+" DELETE WHERE 1")
		s.refused(s.member, 497, "TRUNCATE ON hg_member_unsafe."+table, "TRUNCATE TABLE hg_member_unsafe."+table)
		s.refused(s.member, 497, "OPTIMIZE ON hg_member_unsafe."+table, "OPTIMIZE TABLE hg_member_unsafe."+table+" FINAL")
		s.refused(s.member, 497, "DROP TABLE ON hg_member_unsafe."+table, "DROP TABLE hg_member_unsafe."+table)
		if n := s.count(s.admin, "SELECT count() FROM hg_member_unsafe."+table); n != 0 {
			t.Fatalf("hg_member_unsafe.%s holds %d rows after the refused moves", table, n)
		}
	}
	if s.tableExists("hg_rstage", mv) {
		t.Fatalf("the refused materialized view hg_rstage.%s exists", mv)
	}
	if n := s.count(s.member, "SELECT count() FROM hg_rstage."+src); n != 2 {
		t.Fatalf("hg_rstage.%s holds %d rows, want its 2: nothing moved", src, n)
	}
	s.exec(s.member, "DROP TABLE hg_rstage."+src+" SYNC")
}

func TestConfinement_SIMemberCannotWriteThroughRemote(t *testing.T) {
	s := setup(t)
	table := s.name("remote")
	for _, db := range []string{"hg_safe", "hg_unsafe"} {
		s.exec(s.peer, "CREATE TABLE "+db+"."+table+" (_hg_row_id String, v UInt64) ENGINE = MergeTree ORDER BY _hg_row_id")
		s.exec(s.peer, "INSERT INTO "+db+"."+table+" VALUES ('a', 1)")
	}
	t.Cleanup(func() {
		for _, db := range []string{"hg_safe", "hg_unsafe"} {
			_ = s.peer.Exec(context.Background(), "DROP TABLE IF EXISTS "+db+"."+table+" SYNC")
		}
	})
	for _, tc := range []struct{ collection, db string }{{ownerCollection, "hg_safe"}, {poolCollection, "hg_unsafe"}} {
		s.refused(s.member, 497, "WRITE ON REMOTE", fmt.Sprintf("INSERT INTO FUNCTION remote(%s, database = %s, table = %s) SELECT 'z', 9", tc.collection, literal(tc.db), literal(table)))
		if err := sources.CheckRemoteIsReadOnly(context.Background(), s.member, tc.collection); err != nil {
			t.Fatalf("sources.CheckRemoteIsReadOnly(%s) as %s: %v", tc.collection, memberUser, err)
		}
		if n := s.count(s.member, fmt.Sprintf("SELECT count() FROM remote(%s, database = %s, table = %s)", tc.collection, literal(tc.db), literal(table))); n != 1 {
			t.Fatalf("%s.%s through %s holds %d rows, want its 1", tc.db, table, tc.collection, n)
		}
	}
	// system.grants, read as the admin: READ ON REMOTE and no write source.
	rows, err := s.admin.Query(context.Background(), "SELECT toString(access_type), access_object FROM system.grants WHERE user_name = ? ORDER BY access_type, access_object", memberUser)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	readRemote := 0
	for rows.Next() {
		var accessType, object string
		if err := rows.Scan(&accessType, &object); err != nil {
			t.Fatal(err)
		}
		switch {
		case accessType == "READ" && object == "REMOTE":
			readRemote++
		case accessType == "WRITE", accessType == "REMOTE", accessType == "SOURCES", accessType == "ALL":
			t.Fatalf("system.grants shows %s %s for %s", accessType, object, memberUser)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if readRemote != 1 {
		t.Fatalf("system.grants shows %d READ ON REMOTE rows for %s, want 1", readRemote, memberUser)
	}
	// As si_member itself, as the role's startup probe reads it.
	switch grants, err := sources.ReadOwnGrants(context.Background(), s.member); {
	case errors.Is(err, sources.ErrGrantsUnreadable):
		t.Logf("confinement: system.grants as si_member: unreadable")
	case err != nil:
		t.Fatalf("sources.ReadOwnGrants as %s: %v", memberUser, err)
	default:
		t.Logf("confinement: system.grants as si_member: readable (%d rows)", len(grants))
		if err := sources.CheckRemoteGrants(grants); err != nil {
			t.Fatalf("sources.CheckRemoteGrants of %s's own rows: %v", memberUser, err)
		}
	}
}

func TestConfinement_SIMemberCannotOverrideACollection(t *testing.T) {
	s := setup(t)
	table := s.name("override")
	s.exec(s.peer, "CREATE TABLE hg_safe."+table+" (_hg_row_id String, v UInt64) ENGINE = MergeTree ORDER BY _hg_row_id")
	t.Cleanup(func() { _ = s.peer.Exec(context.Background(), "DROP TABLE IF EXISTS hg_safe."+table+" SYNC") })
	read := fmt.Sprintf("SELECT count() FROM remote(%s, database = 'hg_safe', table = %s", ownerCollection, literal(table))
	if n := s.count(s.member, read+")"); n != 0 {
		t.Fatalf("hg_safe.%s through %s holds %d rows, want 0", table, ownerCollection, n)
	}
	// A NOT OVERRIDABLE key refuses an override with BAD_ARGUMENTS (36), as
	// Plan S3-A's M8 measured; the access check does not run first.
	for _, kv := range []struct{ key, value string }{
		{"host", literal("127.0.0.1")}, {"port", "9440"}, {"user", literal("default")}, {"password", literal("")},
	} {
		s.refused(s.member, 36, "Override not allowed for '"+kv.key+"'", read+", "+kv.key+" = "+kv.value+")")
	}
	// READ ON REMOTE also admits remote() with an explicit address and login
	// (the role never writes one, Global Constraints): measured for the run
	// record.
	s.measure(s.member, "remote() with an explicit address and login as si_member",
		fmt.Sprintf("SELECT count() FROM remote(%s, 'hg_safe', %s, %s, %s)", literal(net.JoinHostPort(s.peerHost, strconv.FormatUint(s.peerPort, 10))), literal(table), literal(readerUser), literal(s.readerPassword)))
}

func TestConfinement_SIMemberCannotDropAReplica(t *testing.T) {
	s := setup(t)
	table := s.name("replica")
	path := "/sentio/0/unsafe/" + table
	s.exec(s.admin, "CREATE TABLE hg_member_unsafe."+table+" (_hg_row_id String, v UInt64) ENGINE = ReplicatedMergeTree("+literal(path)+", 'admin') ORDER BY _hg_row_id")
	t.Cleanup(func() { _ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS hg_member_unsafe."+table+" SYNC") })
	s.refused(s.member, 497, "SYSTEM DROP REPLICA ON *.*", "SYSTEM DROP REPLICA 'other' FROM ZKPATH "+literal(path))
	s.refused(s.member, 497, "SYSTEM DROP REPLICA ON hg_member_unsafe."+table, "SYSTEM DROP REPLICA 'other' FROM TABLE hg_member_unsafe."+table)
	s.refused(s.member, 497, "SYSTEM DROP REPLICA ON hg_member_unsafe.*", "SYSTEM DROP REPLICA 'other' FROM DATABASE hg_member_unsafe")
	s.refused(s.member, 497, "SYSTEM DROP REPLICA", "SYSTEM DROP REPLICA 'other'")
	// Its one SYSTEM right.
	s.exec(s.member, "SYSTEM SYNC REPLICA hg_member_unsafe."+table)
	rows, err := s.admin.Query(context.Background(), "SELECT toString(access_type), ifNull(database, '') FROM system.grants WHERE user_name = ? AND startsWith(toString(access_type), 'SYSTEM')", memberUser)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var system []string
	for rows.Next() {
		var accessType, db string
		if err := rows.Scan(&accessType, &db); err != nil {
			t.Fatal(err)
		}
		system = append(system, accessType+" ON "+db)
	}
	if strings.Join(system, ";") != "SYSTEM SYNC REPLICA ON hg_member_unsafe" {
		t.Fatalf("%s's SYSTEM rights are %v, want only SYSTEM SYNC REPLICA ON hg_member_unsafe", memberUser, system)
	}
}

func TestConfinement_SIMemberHasNoDictionarySystemOrDefinerRight(t *testing.T) {
	s := setup(t)
	for _, db := range memberDatabases {
		s.refused(s.member, 497, "CREATE DICTIONARY ON "+db+"."+s.name("dict"),
			"CREATE DICTIONARY "+db+"."+s.name("dict")+" (k UInt64, v UInt64) PRIMARY KEY k SOURCE(NULL()) LAYOUT(FLAT()) LIFETIME(0)")
		s.refused(s.member, 497, "SET DEFINER ON default", "CREATE VIEW "+db+"."+s.name("vdef")+" DEFINER = default SQL SECURITY DEFINER AS SELECT 1")
		s.refused(s.member, 497, "ALLOW SQL SECURITY NONE", "CREATE VIEW "+db+"."+s.name("vnone")+" SQL SECURITY NONE AS SELECT 1")
		mt := s.name("mt")
		s.exec(s.member, "CREATE TABLE "+db+"."+mt+" (x UInt8) ENGINE = MergeTree ORDER BY x")
		s.refused(s.member, 497, "SYSTEM MERGES ON "+db+"."+mt, "SYSTEM STOP MERGES "+db+"."+mt)
		s.exec(s.member, "DROP TABLE "+db+"."+mt+" SYNC")
		// ClickHouse 26.3 grants CREATE VIEW implicitly with CREATE TABLE
		// (contract §23.2): measured for the run record.
		view := s.name("view")
		if s.measure(s.member, "CREATE VIEW in "+db+" as si_member", "CREATE VIEW "+db+"."+view+" AS SELECT 1") {
			s.exec(s.admin, "DROP VIEW "+db+"."+view+" SYNC")
		}
	}
}

func TestConfinement_SIMemberHasNothingOnTheProtocolDatabases(t *testing.T) {
	s := setup(t)
	table := s.name("protocol")
	for _, db := range []string{protocol.UnsafeDB, protocol.SafeDB, protocol.PromoteDB} {
		s.exec(s.admin, "CREATE DATABASE IF NOT EXISTS "+db)
		s.exec(s.admin, "CREATE TABLE "+db+"."+table+" (_hg_row_id String, v UInt64) ENGINE = MergeTree ORDER BY _hg_row_id")
		t.Cleanup(func() { _ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS "+db+"."+table+" SYNC") })
		s.refusedSelect(s.member, db+"."+table, "SELECT count() FROM "+db+"."+table)
		s.refused(s.member, 497, "ON "+db+"."+table, "INSERT INTO "+db+"."+table+" VALUES ('a', 1)")
		s.refused(s.member, 497, "ON "+db+"."+table, "ALTER TABLE "+db+"."+table+" DROP PARTITION tuple()")
		s.refused(s.member, 497, "ON "+db+"."+table, "DROP TABLE "+db+"."+table)
		s.refused(s.member, 497, "ON "+db+"."+s.name("new"), "CREATE TABLE "+db+"."+s.name("new")+" (x UInt8) ENGINE = MergeTree ORDER BY x")
	}
	if err := ddl.ValidateMemberPinned(pinned, protocol); err != nil {
		t.Fatalf("ddl.ValidateMemberPinned(%+v, %+v): %v", pinned, protocol, err)
	}
}

func TestConfinement_SIMemberHoldsEveryRightItsRoleNeeds(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	// Startup steps (3)-(5) of the role.
	if err := ddl.EnsureMemberDatabases(ctx, s.member, pinned); err != nil {
		t.Fatalf("ddl.EnsureMemberDatabases: %v", err)
	}
	if err := sources.CheckEngineFence(ctx, s.member, pinned.StageDB, memberNodeID+"_"+s.sfx+"_engine_probe"); err != nil {
		t.Fatalf("sources.CheckEngineFence: %v", err)
	}
	for _, c := range []string{ownerCollection, poolCollection} {
		if err := sources.CheckRemoteIsReadOnly(ctx, s.member, c); err != nil {
			t.Fatalf("sources.CheckRemoteIsReadOnly(%s): %v", c, err)
		}
	}
	if _, err := chrestart.StartedAt(ctx, s.member, time.Now()); err != nil {
		t.Fatalf("chrestart.StartedAt: %v", err)
	}

	// The owner's tables: two parts of partition 'a' in hg_safe, one in
	// hg_unsafe (a network verifier's copy of the owner's unsafe table).
	schema := testSchema(s.name("t"))
	physical := ddl.CHTableName(schema.TableID)
	_, safe, _, err := ddl.Intents(protocol, schema)
	if err != nil {
		t.Fatal(err)
	}
	s.exec(s.peer, safe.SQL())
	unsafe := safe
	unsafe.Database = protocol.UnsafeDB
	s.exec(s.peer, unsafe.SQL())
	t.Cleanup(func() {
		_ = s.peer.Exec(ctx, "DROP TABLE IF EXISTS hg_safe."+physical+" SYNC")
		_ = s.peer.Exec(ctx, "DROP TABLE IF EXISTS hg_unsafe."+physical+" SYNC")
	})
	s.exec(s.peer, fmt.Sprintf("INSERT INTO hg_safe.%s VALUES ('%s', 'a', 1), ('%s', 'a', 2)", physical, rowID(1), rowID(2)))
	s.exec(s.peer, fmt.Sprintf("INSERT INTO hg_safe.%s VALUES ('%s', 'a', 3)", physical, rowID(3)))
	s.exec(s.peer, fmt.Sprintf("INSERT INTO hg_unsafe.%s VALUES ('%s', 'a', 4)", physical, rowID(4)))
	type part struct {
		name, hash string
		rows       uint64
	}
	partsOf := func(conn clickhouse.Conn, db, table string) []part {
		t.Helper()
		rows, err := conn.Query(ctx, "SELECT name, hash_of_all_files, rows FROM system.parts WHERE database = ? AND table = ? AND active ORDER BY name", db, table)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var out []part
		for rows.Next() {
			var p part
			if err := rows.Scan(&p.name, &p.hash, &p.rows); err != nil {
				t.Fatal(err)
			}
			out = append(out, p)
		}
		return out
	}
	need := func(db string, p part) sources.Need {
		t.Helper()
		scans, err := chexec.ScanParts(ctx, s.peer, db+"."+physical, schema, []string{p.name})
		if err != nil || len(scans) != 1 {
			t.Fatalf("scan %s.%s %s: %v %v", db, physical, p.name, scans, err)
		}
		return sources.Need{TableKey: schema.TableID, Table: physical, PartitionID: "p_a", PartName: p.name, PartPhysHash: p.hash, PartRowLtHash: scans[0].RowLtHash, RowCount: p.rows}
	}
	safeParts, unsafeParts := partsOf(s.peer, "hg_safe", physical), partsOf(s.peer, "hg_unsafe", physical)
	if len(safeParts) != 2 || len(unsafeParts) != 1 {
		t.Fatalf("owner parts: hg_safe %v, hg_unsafe %v", safeParts, unsafeParts)
	}

	// The copy and two scratch tables, as the copyset and the installer
	// create them.
	if err := ddl.EnsureCopy(ctx, s.member, pinned, schema, 1); err != nil {
		t.Fatalf("ddl.EnsureCopy: %v", err)
	}
	copyTable := ddl.CopyTableName(schema.TableID, 1)
	t.Cleanup(func() { _ = ddl.DropCopy(ctx, s.member, pinned, schema.TableID, 1) })
	scratch := func(n int) sources.Scratch {
		t.Helper()
		in, err := ddl.ScratchIntent(pinned, schema, fmt.Sprintf("%s_%s_%d", memberNodeID, s.sfx, n))
		if err != nil {
			t.Fatal(err)
		}
		s.exec(s.member, in.SQL())
		t.Cleanup(func() { _ = s.member.Exec(ctx, "DROP TABLE IF EXISTS "+qualified(in.Database, in.Table)+" SYNC") })
		return sources.Scratch{Database: in.Database, Table: in.Table}
	}

	// The owner pull and the peer unsafe pull, each into its own scratch.
	s1, s2 := scratch(1), scratch(2)
	for _, tc := range []struct {
		source  sources.Source
		need    sources.Need
		scratch sources.Scratch
	}{
		{sources.NewOwnerPull(s.member, ownerCollection, protocol.SafeDB), need("hg_safe", safeParts[0]), s1},
		{sources.NewPeerUnsafePull(s.member, poolCollection), need("hg_unsafe", unsafeParts[0]), s2},
	} {
		res := tc.source.Fetch(ctx, tc.need, schema, tc.scratch)
		if res.Outcome != sources.OutcomeFetched || res.Err != nil || len(res.Parts) != 1 {
			t.Fatalf("%s.Fetch(%s) = %+v, want one fetched part", tc.source.Name(), tc.need.PartName, res)
		}
	}
	partitionID := func(db, table string) string {
		t.Helper()
		var id string
		if err := s.member.QueryRow(ctx, "SELECT DISTINCT partition_id FROM system.parts WHERE database = ? AND table = ? AND active", db, table).Scan(&id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	pid := partitionID(s1.Database, s1.Table)

	// The installer's statements: ATTACH PARTITION FROM into the copy, the
	// Diverged repair's DROP PARTITION, the audit's DROP PART, and DROP PART
	// inside a scratch.
	copyQualified := qualified(pinned.ReplicaDB, copyTable)
	s.exec(s.member, "ALTER TABLE "+copyQualified+" ATTACH PARTITION ID "+literal(pid)+" FROM "+qualified(s1.Database, s1.Table))
	s.exec(s.member, "ALTER TABLE "+copyQualified+" ATTACH PARTITION ID "+literal(pid)+" FROM "+qualified(s2.Database, s2.Table))
	if got := partsOf(s.member, pinned.ReplicaDB, copyTable); len(got) != 2 {
		t.Fatalf("the copy holds %v after two attaches", got)
	}
	s.exec(s.member, "ALTER TABLE "+copyQualified+" DROP PARTITION ID "+literal(pid))
	s.exec(s.member, "ALTER TABLE "+copyQualified+" ATTACH PARTITION ID "+literal(pid)+" FROM "+qualified(s1.Database, s1.Table))
	s.exec(s.member, "ALTER TABLE "+copyQualified+" ATTACH PARTITION ID "+literal(pid)+" FROM "+qualified(s2.Database, s2.Table))
	copyParts := partsOf(s.member, pinned.ReplicaDB, copyTable)
	if len(copyParts) != 2 {
		t.Fatalf("the copy holds %v after the repair", copyParts)
	}
	s.exec(s.member, "ALTER TABLE "+copyQualified+" DROP PART "+literal(copyParts[1].name))
	s.exec(s.member, "ALTER TABLE "+qualified(s2.Database, s2.Table)+" DROP PART "+literal(partsOf(s.member, s2.Database, s2.Table)[0].name))
	if got := partsOf(s.member, pinned.ReplicaDB, copyTable); len(got) != 1 || got[0].hash != safeParts[0].hash {
		t.Fatalf("the copy holds %v, want the owner's part %s with its hash_of_all_files", got, safeParts[0].name)
	}

	// The five system tables, the disk budget, the listings and a temporary
	// Memory table.
	for _, q := range []string{
		"SELECT count() FROM system.parts WHERE database = 'hg_replica'",
		"SELECT count() FROM system.tables WHERE database = 'hg_rstage'",
		"SELECT count() FROM system.detached_tables",
		"SELECT count() FROM system.replicas",
		"SELECT count() FROM system.zookeeper WHERE path = '/'",
		"SELECT sum(bytes_on_disk) FROM system.parts WHERE database IN ('hg_replica', 'hg_rstage')",
	} {
		s.count(s.member, q)
	}
	if tables, err := ddl.ListMemberTables(ctx, s.member, pinned); err != nil || len(tables) == 0 {
		t.Fatalf("ddl.ListMemberTables = %v, %v", tables, err)
	}
	if names, err := ddl.ListStageTables(ctx, s.member, pinned, memberNodeID+"_"+s.sfx+"_"); err != nil || len(names) != 2 {
		t.Fatalf("ddl.ListStageTables = %v, %v, want the two scratch tables", names, err)
	}
	s.exec(s.member, "CREATE TEMPORARY TABLE "+s.name("tmp")+" (x UInt8) ENGINE = Memory")
	if err := ddl.DropCopy(ctx, s.member, pinned, schema.TableID, 1); err != nil {
		t.Fatalf("ddl.DropCopy: %v", err)
	}
	if s.tableExists(pinned.ReplicaDB, copyTable) {
		t.Fatalf("%s survived ddl.DropCopy", copyQualified)
	}
}

func TestConfinement_SIMemberRestoreNeedsReadOnS3(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	table := s.name("backup")
	s.exec(s.peer, "CREATE TABLE hg_safe."+table+" (_hg_row_id String, p String, v UInt64) ENGINE = MergeTree PARTITION BY p ORDER BY _hg_row_id")
	t.Cleanup(func() { _ = s.peer.Exec(ctx, "DROP TABLE IF EXISTS hg_safe."+table+" SYNC") })
	s.exec(s.peer, "INSERT INTO hg_safe."+table+" VALUES ('a', 'x', 1), ('b', 'y', 2)")
	// The owner's backup and its index object, as Plan S3-C1's backup job
	// writes them.
	s.exec(s.peer, "BACKUP TABLE hg_safe."+table+" TO S3("+s.s3Args("backup")+")")
	s.exec(s.peer, "INSERT INTO FUNCTION s3("+s.s3Args("index.json")+", 'RawBLOB') SELECT "+literal(`{"name":"backup"}`))

	restore := func(scratch string) string {
		return "RESTORE TABLE hg_safe." + table + " AS hg_rstage." + scratch + " FROM S3(" + s.s3Args("backup") + ")"
	}
	readIndex := "SELECT count() FROM s3(" + s.s3Args("index.json") + ", 'RawBLOB')"
	// si_member reads the index object and restores into hg_rstage.
	if n := s.count(s.member, readIndex); n != 1 {
		t.Fatalf("the index object reads as %d rows, want 1", n)
	}
	scratch := s.name("restored")
	s.exec(s.member, restore(scratch))
	t.Cleanup(func() { _ = s.member.Exec(ctx, "DROP TABLE IF EXISTS hg_rstage."+scratch+" SYNC") })
	if n := s.count(s.member, "SELECT count() FROM hg_rstage."+scratch); n != 2 {
		t.Fatalf("the restored scratch holds %d rows, want 2", n)
	}
	// It cannot write to object storage nor make a backup.
	s.refused(s.member, 497, "WRITE ON S3", "INSERT INTO FUNCTION s3("+s.s3Args("member.json")+", 'RawBLOB') SELECT 'x'")
	s.refused(s.member, 497, "BACKUP ON hg_rstage."+scratch, "BACKUP TABLE hg_rstage."+scratch+" TO S3("+s.s3Args("member-backup")+")")

	// The same user without READ ON S3 cannot read the index object.
	nos3 := "si_member_nos3"
	var grants []string
	for _, g := range siMemberGrants {
		if g != "GRANT READ ON S3" {
			grants = append(grants, g)
		}
	}
	password := s.createUser(s.admin, nos3, "", grants)
	t.Cleanup(func() { _ = s.admin.Exec(ctx, "DROP USER IF EXISTS "+nos3) })
	conn := chtest.OpenAs(t, s.addr, nos3, password)
	s.refused(conn, 497, "READ ON S3", readIndex)
	// RESTORE ... FROM S3 itself: measured for the run record (ClickHouse
	// 26.3.51.7 checks no source grant for it, so the index read is the gate).
	scratch2 := s.name("restored_nos3")
	if s.measure(conn, "RESTORE FROM S3 without READ ON S3", restore(scratch2)) {
		s.exec(s.admin, "DROP TABLE hg_rstage."+scratch2+" SYNC")
	}
}

func TestConfinement_SIMemberDDLProfileRefusesAMergesAllowedReplica(t *testing.T) {
	s := setup(t)
	ddlConn := s.ddlUser()
	create := func(table, settings string) string {
		q := "CREATE TABLE hg_member_unsafe." + table + " (_hg_row_id String, v UInt64) ENGINE = ReplicatedMergeTree('/sentio/0/unsafe_inc/" + table + "/1', " + literal(memberNodeID) + ") ORDER BY _hg_row_id"
		if settings != "" {
			q += " SETTINGS " + settings
		}
		return q
	}
	honest, pinnedOff := s.name("ddl_honest"), s.name("ddl_pinned")
	t.Cleanup(func() {
		for _, table := range []string{honest, pinnedOff} {
			_ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS hg_member_unsafe."+table+" SYNC")
		}
	})
	s.exec(ddlConn, "CREATE DATABASE IF NOT EXISTS hg_member_unsafe")
	s.exec(ddlConn, create(honest, ""))
	s.exec(ddlConn, create(pinnedOff, "max_bytes_to_merge_at_max_space_in_pool = 0"))
	s.refused(ddlConn, 452, "max_bytes_to_merge_at_max_space_in_pool should not be changed", create(s.name("ddl_merges"), "max_bytes_to_merge_at_max_space_in_pool = 1073741824"))
	s.refused(ddlConn, 452, "merge_tree_max_bytes_to_merge_at_max_space_in_pool should not be changed", "SET merge_tree_max_bytes_to_merge_at_max_space_in_pool = 1")
	if s.tableExists("hg_member_unsafe", s.name("ddl_merges")) {
		t.Fatal("the merges-allowed replica exists")
	}
}

func TestConfinement_SIMemberDDLCannotWriteAlterOrOptimize(t *testing.T) {
	s := setup(t)
	ddlConn := s.ddlUser()
	table := s.name("ddl_replica")
	qualified := "hg_member_unsafe." + table
	t.Cleanup(func() { _ = s.admin.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualified+" SYNC") })
	s.exec(ddlConn, "CREATE TABLE "+qualified+" (_hg_row_id String, v UInt64) ENGINE = ReplicatedMergeTree('/sentio/0/unsafe_inc/"+table+"/1', "+literal(memberNodeID)+") ORDER BY _hg_row_id")
	for _, tc := range []struct{ want, query string }{
		{"INSERT ON " + qualified, "INSERT INTO " + qualified + " VALUES ('a', 1)"},
		{"ALTER DELETE ON " + qualified, "ALTER TABLE " + qualified + " DELETE WHERE 1"},
		{"ALTER DELETE ON " + qualified, "ALTER TABLE " + qualified + " DROP PARTITION tuple()"},
		{"ALTER SETTINGS ON " + qualified, "ALTER TABLE " + qualified + " MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1073741824"},
		{"OPTIMIZE ON " + qualified, "OPTIMIZE TABLE " + qualified + " FINAL"},
		{"TRUNCATE ON " + qualified, "TRUNCATE TABLE " + qualified},
		{"SYSTEM DROP REPLICA ON " + qualified, "SYSTEM DROP REPLICA 'other' FROM TABLE " + qualified},
		{"TABLE ENGINE ON MergeTree", "CREATE TABLE hg_member_unsafe." + s.name("ddl_mt") + " (x UInt8) ENGINE = MergeTree ORDER BY x"},
		{"TABLE ENGINE ON ReplicatedReplacingMergeTree", "CREATE TABLE hg_member_unsafe." + s.name("ddl_rrmt") + " (_hg_row_id String) ENGINE = ReplicatedReplacingMergeTree('/sentio/0/unsafe_inc/" + s.name("ddl_rrmt") + "/1', 'r') ORDER BY _hg_row_id"},
		{"CREATE TABLE ON hg_rstage." + s.name("ddl_stage"), "CREATE TABLE hg_rstage." + s.name("ddl_stage") + " (x UInt8) ENGINE = MergeTree ORDER BY x"},
	} {
		s.refused(ddlConn, 497, tc.want, tc.query)
	}
	s.refusedSelect(ddlConn, qualified, "SELECT count() FROM "+qualified)
	s.exec(ddlConn, "DROP TABLE "+qualified+" SYNC")
}
