package sources

import (
	"context"
	"errors"
	"os"
	"slices"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
)

// TestCheckRemoteIsReadOnlyRefusesWriteOnRemote: a READ ON REMOTE user's
// probe is refused locally for WRITE ON REMOTE, before any connection (the
// collection's server is down), so the check passes; a user with WRITE ON
// REMOTE, or the legacy REMOTE, is writable. One server suffices.
func TestCheckRemoteIsReadOnlyRefusesWriteOnRemote(t *testing.T) {
	admin, addr := memberServer(t)
	sfx := suffix(t)
	collection := "si_owner_" + sfx
	cleanup(t, addr, "DROP NAMED COLLECTION IF EXISTS "+ident(collection))
	mustExec(t, admin, "CREATE NAMED COLLECTION "+ident(collection)+" AS host = '127.0.0.1' NOT OVERRIDABLE, port = 1 NOT OVERRIDABLE, user = 'si_reader' NOT OVERRIDABLE, password = "+literal(randomHex(t, 16))+" NOT OVERRIDABLE")
	user := func(name string, grants ...string) clickhouse.Conn {
		password := createUser(t, admin, addr, name+sfx, "", append([]string{
			"GRANT NAMED COLLECTION ON " + ident(collection) + " TO %s",
			"GRANT CREATE TEMPORARY TABLE ON *.* TO %s",
		}, grants...)...)
		return chtest.OpenAs(t, addr, name+sfx, password)
	}
	if err := CheckRemoteIsReadOnly(context.Background(), user("src_read_", "GRANT READ ON REMOTE TO %s"), collection); err != nil {
		t.Fatalf("CheckRemoteIsReadOnly as a READ ON REMOTE user: %v", err)
	}
	for _, tc := range []struct{ name, grant string }{
		{"src_write_", "GRANT READ, WRITE ON REMOTE TO %s"},
		{"src_legacy_", "GRANT REMOTE ON *.* TO %s"},
	} {
		err := CheckRemoteIsReadOnly(context.Background(), user(tc.name, tc.grant), collection)
		if !errors.Is(err, ErrRemoteWritable) || !strings.HasPrefix(err.Error(), "sources: INSERT INTO FUNCTION remote("+collection+") was not refused with ACCESS_DENIED (got ") || !strings.HasSuffix(err.Error(), "): WRITE ON REMOTE must be absent") {
			t.Fatalf("CheckRemoteIsReadOnly with %q = %v, want ErrRemoteWritable", tc.grant, err)
		}
		t.Logf("%s: %v", tc.grant, err)
	}
	if err := CheckRemoteIsReadOnly(context.Background(), admin, collection+", user = 'default'"); err == nil || errors.Is(err, ErrRemoteWritable) {
		t.Fatalf("CheckRemoteIsReadOnly of a collection that is not one identifier = %v, want a refusal before any statement", err)
	}
}

// TestCheckEngineFenceRefusesAServerWithoutTheFence: on a ClickHouse without
// access_control_improvements.table_engines_require_grant a user granted only
// TABLE ENGINE ON MergeTree creates ENGINE = Log, so CheckEngineFence
// reports ErrEngineFenceOff and drops its probe; on a fenced server it
// passes. Each server in CH_ADDR and CH_PEER_ADDR is classified first by a
// control user without any engine grant; at least one of them must lack the
// fence (the peer of the confinement suite and both CI servers do).
func TestCheckEngineFenceRefusesAServerWithoutTheFence(t *testing.T) {
	_, memberAddr := memberServer(t)
	addrs := []string{memberAddr}
	if peer := os.Getenv(envPeerAddr); peer != "" {
		addrs = append(addrs, peer)
	}
	unfenced := 0
	for _, addr := range addrs {
		admin := chtest.Open(t, addr)
		sfx := suffix(t)
		db := "fence_" + sfx
		mustExec(t, admin, "CREATE DATABASE IF NOT EXISTS "+ident(db))
		cleanup(t, addr, "DROP DATABASE IF EXISTS "+ident(db)+" SYNC")
		controlPassword := createUser(t, admin, addr, "src_control_"+sfx, "", "GRANT CREATE TABLE, DROP TABLE ON "+ident(db)+".* TO %s")
		err := chtest.OpenAs(t, addr, "src_control_"+sfx, controlPassword).Exec(context.Background(), "CREATE TABLE "+ident(db)+".`control` (x UInt8) ENGINE = MergeTree ORDER BY x")
		fenced := refusedFor(err, "TABLE ENGINE ON MergeTree")
		if err != nil && !fenced {
			t.Fatalf("%s: the control CREATE TABLE failed without the engine refusal: %v", addr, err)
		}
		proberPassword := createUser(t, admin, addr, "src_prober_"+sfx, "",
			"GRANT CREATE TABLE, DROP TABLE ON "+ident(db)+".* TO %s", "GRANT TABLE ENGINE ON MergeTree TO %s")
		probeTable := "member-test_engine_probe"
		err = CheckEngineFence(context.Background(), chtest.OpenAs(t, addr, "src_prober_"+sfx, proberPassword), db, probeTable)
		switch {
		case fenced && err != nil:
			t.Fatalf("%s is fenced, CheckEngineFence = %v, want nil", addr, err)
		case !fenced && (!errors.Is(err, ErrEngineFenceOff) || err.Error() != "sources: ClickHouse accepted ENGINE = Log for this user: access_control_improvements.table_engines_require_grant is off or the user holds every engine"):
			t.Fatalf("%s is not fenced, CheckEngineFence = %v, want ErrEngineFenceOff", addr, err)
		}
		if slices.Contains(tablesIn(t, admin, db), probeTable) {
			t.Fatalf("%s: the probe table %s.%s is left behind", addr, db, probeTable)
		}
		if !fenced {
			unfenced++
		}
		t.Logf("%s: fenced %v, CheckEngineFence = %v", addr, fenced, err)
	}
	if unfenced == 0 {
		t.Fatalf("every server of %v enforces the engine fence; set %s to a server without it", addrs, envPeerAddr)
	}
}

// TestReadOwnGrantsNeverRunsShowGrants: ReadOwnGrants reads its own rows of
// system.grants with one SELECT (never SHOW GRANTS); a user without SELECT on
// system.grants gets ErrGrantsUnreadable (the stock server configuration
// requires that grant); a user with it sees READ ON REMOTE as (READ, REMOTE)
// and, once granted, WRITE ON REMOTE, which CheckRemoteGrants refuses.
func TestReadOwnGrantsNeverRunsShowGrants(t *testing.T) {
	fake := &fakeConn{rows: func(query string, _ []any) ([][]any, error) {
		return [][]any{
			{"READ", (*string)(nil), (*string)(nil), "REMOTE", uint8(0)},
			{"SELECT", ptr("system"), ptr("parts"), "", uint8(0)},
		}, nil
	}}
	grants, err := ReadOwnGrants(context.Background(), fake)
	if err != nil || !slices.Equal(grants, []Grant{{AccessType: "READ", AccessObject: "REMOTE"}, {AccessType: "SELECT", Database: "system", Table: "parts"}}) {
		t.Fatalf("ReadOwnGrants = %+v, %v", grants, err)
	}
	if got := fake.all(); !slices.Equal(got, []string{"query " + readOwnGrantsQuery}) {
		t.Fatalf("statements %q, want the one system.grants SELECT", got)
	}
	denied := &fakeConn{rows: func(string, []any) ([][]any, error) {
		return nil, &clickhouse.Exception{Code: 497, Message: "si_member: Not enough privileges. To execute this query, it's necessary to have the grant SELECT ON system.grants"}
	}}
	if _, err := ReadOwnGrants(context.Background(), denied); !errors.Is(err, ErrGrantsUnreadable) {
		t.Fatalf("ReadOwnGrants refused with 497 = %v, want ErrGrantsUnreadable", err)
	}

	admin, addr := memberServer(t)
	sfx := suffix(t)
	plain, reader := "src_plain_"+sfx, "src_grants_"+sfx
	plainConn := chtest.OpenAs(t, addr, plain, createUser(t, admin, addr, plain, "", "GRANT READ ON REMOTE TO %s"))
	readerConn := chtest.OpenAs(t, addr, reader, createUser(t, admin, addr, reader, "", "GRANT READ ON REMOTE TO %s", "GRANT SELECT ON system.grants TO %s"))
	if _, err := ReadOwnGrants(context.Background(), plainConn); !errors.Is(err, ErrGrantsUnreadable) {
		t.Fatalf("ReadOwnGrants without SELECT on system.grants = %v, want ErrGrantsUnreadable", err)
	}
	grants, err = ReadOwnGrants(context.Background(), readerConn)
	if err != nil || !slices.Contains(grants, Grant{AccessType: "READ", AccessObject: "REMOTE"}) {
		t.Fatalf("ReadOwnGrants = %+v, %v; want a (READ, REMOTE) row", grants, err)
	}
	if err := CheckRemoteGrants(grants); err != nil {
		t.Fatalf("CheckRemoteGrants of a READ ON REMOTE user: %v", err)
	}
	mustExec(t, admin, "GRANT WRITE ON REMOTE TO "+ident(reader))
	grants, err = ReadOwnGrants(context.Background(), readerConn)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckRemoteGrants(grants); err == nil || err.Error() != "sources: system.grants shows WRITE ON REMOTE for this user" {
		t.Fatalf("CheckRemoteGrants after GRANT WRITE ON REMOTE = %v (rows %+v)", err, grants)
	}
	for _, user := range []string{plain, reader} {
		queries := queryLog(t, admin, user)
		if !slices.Contains(queries, readOwnGrantsQuery) {
			t.Fatalf("%s ran %q, want the system.grants SELECT", user, queries)
		}
		for _, q := range queries {
			if strings.HasPrefix(strings.ToUpper(q), "SHOW GRANTS") {
				t.Fatalf("%s ran %q", user, q)
			}
		}
	}
}

func ptr(s string) *string { return &s }
