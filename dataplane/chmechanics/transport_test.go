package chmechanics

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// pullRows is above the default min_insert_block_size_rows (1,048,449), so an
// INSERT ... SELECT with default settings splits a part this large.
const pullRows = 1_200_000

// noSplitSettings are spec §2's measured settings under which INSERT ...
// SELECT writes a source part of rows rows as one part.
func noSplitSettings(rows uint64) clickhouse.Settings {
	return clickhouse.Settings{
		"min_insert_block_size_rows":  rows,
		"min_insert_block_size_bytes": 0,
		"max_threads":                 1,
		"max_insert_threads":          1,
	}
}

// remoteTable is the remote() table function over db.table on peer.
func remoteTable(peer, db, table, user, password string) string {
	return fmt.Sprintf("remote(%s, %s, %s, %s, %s)", literal(peer), literal(db), literal(table), literal(user), literal(password))
}

// pullPart copies the part named name out of source (a remote() table
// function) into db.table with INSERT ... SELECT ... WHERE _part = name, the
// member's owner pull (spec §8.6): single-threaded, plus extra settings.
func pullPart(t *testing.T, conn clickhouse.Conn, db, table, source, name string, extra clickhouse.Settings) {
	t.Helper()
	settings := clickhouse.Settings{"max_threads": 1, "max_insert_threads": 1}
	for key, value := range extra {
		settings[key] = value
	}
	ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(settings))
	rowID := ident(ddl.RowIDColumn)
	query := fmt.Sprintf("INSERT INTO %s (%s, `p`, `v`) SELECT %s, `p`, `v` FROM %s WHERE _part = %s",
		qualified(db, table), rowID, rowID, source, literal(name))
	if err := conn.Exec(ctx, query); err != nil {
		t.Fatalf("pull part %s into %s.%s: %v", name, db, table, err)
	}
}

// writeThroughRemote inserts one row into target, an INSERT INTO FUNCTION
// remote(...) target.
func writeThroughRemote(conn clickhouse.Conn, target string) error {
	return conn.Exec(context.Background(), fmt.Sprintf(
		"INSERT INTO FUNCTION %s (%s, `p`, `v`) VALUES (%s, 'p0', 1000)",
		target, ident(ddl.RowIDColumn), literal(strings.Repeat("0", 31)+"1")))
}

// createUser creates a password user on conn, applies each grant (a format
// string whose one %s is the user's quoted name), and drops the user when the
// test ends.
func createUser(t *testing.T, conn clickhouse.Conn, addr, name string, grants ...string) string {
	t.Helper()
	password := randomHex(t, 16)
	t.Cleanup(func() {
		c, err := dial(context.Background(), addr)
		if err != nil {
			t.Logf("cleanup: cannot reach %s to drop user %s: %v", addr, name, err)
			return
		}
		defer func() { _ = c.Close() }()
		if err := c.Exec(context.Background(), "DROP USER IF EXISTS "+ident(name)); err != nil {
			t.Logf("cleanup: drop user %s: %v", name, err)
		}
	})
	mustExec(t, conn, fmt.Sprintf("CREATE USER %s IDENTIFIED WITH sha256_password BY %s", ident(name), literal(password)))
	for _, grant := range grants {
		mustExec(t, conn, fmt.Sprintf(grant, ident(name)))
	}
	return password
}

// accessTypes lists the user's system.grants access types.
func accessTypes(t *testing.T, conn clickhouse.Conn, user string) []string {
	t.Helper()
	rows, err := conn.Query(context.Background(), "SELECT toString(access_type) FROM system.grants WHERE user_name = ?", user)
	if err != nil {
		t.Fatalf("system.grants of %s: %v", user, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var accessType string
		if err := rows.Scan(&accessType); err != nil {
			t.Fatalf("scan system.grants: %v", err)
		}
		out = append(out, accessType)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("system.grants of %s: %v", user, err)
	}
	slices.Sort(out)
	return out
}

// createCollection creates a named collection on conn holding the owner
// connection of spec §8.6 (host, port, user and password; database and table
// are passed per call), each key NOT OVERRIDABLE when locked, and drops it
// when the test ends. name must be a plain identifier.
func createCollection(t *testing.T, conn clickhouse.Conn, addr, name string, locked bool, host string, port uint64) {
	t.Helper()
	modifier := ""
	if locked {
		modifier = " NOT OVERRIDABLE"
	}
	t.Cleanup(func() {
		c, err := dial(context.Background(), addr)
		if err != nil {
			t.Logf("cleanup: cannot reach %s to drop named collection %s: %v", addr, name, err)
			return
		}
		defer func() { _ = c.Close() }()
		if err := c.Exec(context.Background(), "DROP NAMED COLLECTION IF EXISTS "+name); err != nil {
			t.Logf("cleanup: drop named collection %s: %v", name, err)
		}
	})
	mustExec(t, conn, fmt.Sprintf("CREATE NAMED COLLECTION %s AS host = %s%s, port = %d%s, user = 'default'%s, password = ''%s",
		name, literal(host), modifier, port, modifier, modifier, modifier))
}

func splitPeer(t *testing.T, peer string) (string, uint64) {
	t.Helper()
	host, port, err := net.SplitHostPort(peer)
	if err != nil {
		t.Fatalf("%s %q: %v", envPeerAddr, peer, err)
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil {
		t.Fatalf("%s %q port: %v", envPeerAddr, peer, err)
	}
	return host, n
}

// TestMechanics_NoSplitPullYieldsOnePart (M6) pins the owner pull of spec
// §8.6: node B copies one 1.2M-row part of node A with INSERT ... SELECT FROM
// remote(...) WHERE _part = <name>. With default settings the copy splits into
// parts whose row LtHashes sum to the source's; with the no-split settings it
// is one part with the source's row LtHash; and the name resolves by
// hash_of_all_files under the reader grant of §8.6.
func TestMechanics_NoSplitPullYieldsOnePart(t *testing.T) {
	env := requireMechanics(t)
	src := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, promote := protocolTables(t, env.A, env.AAddr, src, sch)
	mustInsert(t, env.A, src.SafeDB, safe.Table, "p0", 0, pullRows, noSplitSettings(pullRows))
	sourceParts := activeParts(t, env.A, src.SafeDB, safe.Table, "p0")
	if len(sourceParts) != 1 || sourceParts[0].Rows != pullRows {
		t.Fatalf("source %+v, want one part of %d rows (the no-split settings hold for a local INSERT ... SELECT too)", sourceParts, pullRows)
	}
	source := sourceParts[0]
	sourceRowHash := rowHashes(t, env.A, src.SafeDB, safe.Table, sch, sourceParts)[source.Name]
	// scratch is an empty table of the same structure on node B, the member's
	// hg_rstage scratch (spec §8.5 step 3).
	scratch := func(t *testing.T) (string, string) {
		t.Helper()
		intent := promote
		intent.Database = mechanicsPinned(t).PromoteDB
		createTables(t, env.B, env.BAddr, intent)
		return intent.Database, intent.Table
	}
	asDefault := remoteTable(env.PeerAddr, src.SafeDB, safe.Table, "default", "")

	t.Run("default_settings_split_the_part", func(t *testing.T) {
		db, table := scratch(t)
		pullPart(t, env.B, db, table, asDefault, source.Name, nil)
		parts := activeParts(t, env.B, db, table, "p0")
		if len(parts) < 2 {
			t.Fatalf("control: the default settings wrote %d part(s), want a split: %+v", len(parts), parts)
		}
		if rows := totalRows(parts); rows != pullRows {
			t.Fatalf("control: %d rows, want %d", rows, pullRows)
		}
		var hashes []string
		for _, h := range rowHashes(t, env.B, db, table, sch, parts) {
			hashes = append(hashes, h)
		}
		if got := sumRowHashes(t, hashes...); got != sourceRowHash {
			t.Fatalf("control: the split parts sum to %s, the source part is %s", abbreviate(got), abbreviate(sourceRowHash))
		}
	})
	t.Run("no_split_settings_keep_one_part", func(t *testing.T) {
		db, table := scratch(t)
		pullPart(t, env.B, db, table, asDefault, source.Name, noSplitSettings(pullRows))
		parts := activeParts(t, env.B, db, table, "p0")
		if len(parts) != 1 || parts[0].Rows != pullRows {
			t.Fatalf("no-split pull wrote %+v, want one part of %d rows", parts, pullRows)
		}
		if got := rowHashes(t, env.B, db, table, sch, parts)[parts[0].Name]; got != sourceRowHash {
			t.Fatalf("no-split pull row LtHash %s, source %s", abbreviate(got), abbreviate(sourceRowHash))
		}
	})
	t.Run("reader_grant_resolves_the_part_by_hash_of_all_files", func(t *testing.T) {
		reader := "mech_reader_" + suffix(t)
		password := createUser(t, env.A, env.AAddr, reader,
			"GRANT SELECT ON "+ident(src.SafeDB)+".* TO %s",
			"GRANT SHOW COLUMNS ON system.parts TO %s",
			"GRANT SELECT(database, table, name, active, rows, partition_id, hash_of_all_files) ON system.parts TO %s")
		parts := fmt.Sprintf("remote(%s, 'system', 'parts', %s, %s)", literal(env.PeerAddr), literal(reader), literal(password))
		var resolved string
		if err := env.B.QueryRow(context.Background(), fmt.Sprintf(
			"SELECT name FROM %s WHERE database = %s AND table = %s AND active AND hash_of_all_files = %s",
			parts, literal(src.SafeDB), literal(safe.Table), literal(source.PhysHash))).Scan(&resolved); err != nil {
			t.Fatalf("resolve the part by hash_of_all_files as %s: %v", reader, err)
		}
		if resolved != source.Name {
			t.Fatalf("hash_of_all_files %s resolved to %s, want %s", source.PhysHash, resolved, source.Name)
		}
		db, table := scratch(t)
		pullPart(t, env.B, db, table, remoteTable(env.PeerAddr, src.SafeDB, safe.Table, reader, password), resolved, noSplitSettings(pullRows))
		pulled := activeParts(t, env.B, db, table, "p0")
		if len(pulled) != 1 || rowHashes(t, env.B, db, table, sch, pulled)[pulled[0].Name] != sourceRowHash {
			t.Fatalf("the reader's no-split pull wrote %+v, want one part with the source's row LtHash", pulled)
		}
	})
}

// TestMechanics_ReadOnRemoteRefusesWrites (M7) pins the member's transport
// grant (spec §8.2, §8.6): a user with READ ON REMOTE reads through remote()
// but its INSERT INTO FUNCTION remote(...) is refused with ACCESS_DENIED and
// writes nothing, and system.grants shows READ and no WRITE; a user granted
// REMOTE, with the same target user, writes.
func TestMechanics_ReadOnRemoteRefusesWrites(t *testing.T) {
	env := requireMechanics(t)
	p := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, _ := protocolTables(t, env.A, env.AAddr, p, sch)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 0, 3, nil)
	target := remoteTable(env.PeerAddr, p.SafeDB, safe.Table, "default", "")
	onA := qualified(p.SafeDB, safe.Table)

	t.Run("read_on_remote", func(t *testing.T) {
		user := "mech_member_" + suffix(t)
		password := createUser(t, env.B, env.BAddr, user, "GRANT READ ON REMOTE TO %s", "GRANT CREATE TEMPORARY TABLE ON *.* TO %s")
		member := chtest.OpenAs(t, env.BAddr, user, password)
		if n := mustCountRows(t, member, target); n != 3 {
			t.Fatalf("read through remote(): %d rows, want 3", n)
		}
		err := writeThroughRemote(member, target)
		if clickHouseCode(err) != codeAccessDenied || !strings.Contains(err.Error(), "WRITE ON REMOTE") {
			t.Fatalf("a READ ON REMOTE user wrote through remote(): err = %v, want ACCESS_DENIED (497) naming WRITE ON REMOTE", err)
		}
		t.Logf("READ ON REMOTE refusal: %v", err)
		if n := mustCountRows(t, env.A, onA); n != 3 {
			t.Fatalf("the refused write reached node A: %d rows, want 3", n)
		}
		grants := accessTypes(t, env.B, user)
		if !slices.Contains(grants, "READ") || slices.Contains(grants, "WRITE") {
			t.Fatalf("system.grants of %s: %v, want READ and no WRITE", user, grants)
		}
	})
	t.Run("remote_grant_allows_writes", func(t *testing.T) {
		user := "mech_remote_" + suffix(t)
		password := createUser(t, env.B, env.BAddr, user, "GRANT REMOTE ON *.* TO %s", "GRANT CREATE TEMPORARY TABLE ON *.* TO %s")
		if err := writeThroughRemote(chtest.OpenAs(t, env.BAddr, user, password), target); err != nil {
			t.Fatalf("a REMOTE user's write through remote(): %v", err)
		}
		if n := mustCountRows(t, env.A, onA); n != 4 {
			t.Fatalf("node A holds %d rows after the REMOTE user's write, want 4", n)
		}
	})
}

// TestMechanics_NotOverridableCollectionRefusesOverride (M8) pins the owner
// collection of spec §8.6: with host, port, user and password NOT OVERRIDABLE
// a call that overrides user is refused with BAD_ARGUMENTS (even with the
// same value), while the same override of a control collection without the
// clause works; and a user without GRANT NAMED COLLECTION cannot use the
// collection until granted.
func TestMechanics_NotOverridableCollectionRefusesOverride(t *testing.T) {
	env := requireMechanics(t)
	p := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, _ := protocolTables(t, env.A, env.AAddr, p, sch)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 0, 3, nil)
	host, port := splitPeer(t, env.PeerAddr)
	locked := "mech_nc_locked_" + suffix(t)
	open := "mech_nc_open_" + suffix(t)
	createCollection(t, env.B, env.BAddr, locked, true, host, port)
	createCollection(t, env.B, env.BAddr, open, false, host, port)
	through := func(collection, overrides string) string {
		return fmt.Sprintf("remote(%s, database = %s, table = %s%s)", collection, literal(p.SafeDB), literal(safe.Table), overrides)
	}
	if n := mustCountRows(t, env.B, through(locked, "")); n != 3 {
		t.Fatalf("read through %s: %d rows, want 3", locked, n)
	}
	_, err := countRows(env.B, through(locked, ", user = 'default'"))
	var exception *clickhouse.Exception
	if !errors.As(err, &exception) || exception.Code != codeBadArguments || !strings.Contains(exception.Message, "Override not allowed for 'user'") {
		t.Fatalf("overriding a NOT OVERRIDABLE key: err = %v, want BAD_ARGUMENTS (36) Override not allowed for 'user'", err)
	}
	t.Logf("NOT OVERRIDABLE refusal: code %d: %s", exception.Code, exception.Message)
	if n := mustCountRows(t, env.B, through(open, ", user = 'default'")); n != 3 {
		t.Fatalf("the control collection with the same override read %d rows, want 3", n)
	}
	user := "mech_nc_user_" + suffix(t)
	password := createUser(t, env.B, env.BAddr, user, "GRANT READ ON REMOTE TO %s", "GRANT CREATE TEMPORARY TABLE ON *.* TO %s")
	if _, err := countRows(chtest.OpenAs(t, env.BAddr, user, password), through(locked, "")); clickHouseCode(err) != codeAccessDenied {
		t.Fatalf("a user without GRANT NAMED COLLECTION used %s: err = %v, want ACCESS_DENIED (497)", locked, err)
	}
	mustExec(t, env.B, fmt.Sprintf("GRANT NAMED COLLECTION ON %s TO %s", locked, ident(user)))
	if n := mustCountRows(t, chtest.OpenAs(t, env.BAddr, user, password), through(locked, "")); n != 3 {
		t.Fatalf("the granted user read %d rows through %s, want 3", n, locked)
	}
}
