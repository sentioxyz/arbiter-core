package chmechanics

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
)

// The interserver mechanics (M16; plan S3-D clarification K56) run on a
// second pair that scripts/ci/clickhouse-mechanics.sh starts beside the
// first: ch-iscred has interserver_http_credentials (user si_interserver,
// the CI-only password of scripts/ci/clickhouse-mechanics-interserver.xml,
// allow_empty true), ch-isplain has none, and both use node A's Keeper.
const (
	envInterserver  = "ARBITER_CH_INTERSERVER"
	envIsCredAddr   = "CH_ISCRED_ADDR"
	envIsPlainAddr  = "CH_ISPLAIN_ADDR"
	interserverWait = 60 * time.Second
)

// interserverRefusals are the interserver handler's answers (HTTP 401) to a
// fetcher that sends credentials to a server that has none: the text plan
// S3-D clarification K56 quotes, and the one upstream 25.8 to 26.7 answer
// (measured while drafting plan S3-D on 25.8.28.1, 26.2, 26.3.51.7 and
// 26.7.5.10: the uncredentialed server checks the fetcher's user against an
// empty credential set).
var interserverRefusals = []string{
	"Client requires HTTP Basic authentication, but server doesn't provide it",
	"Incorrect user or password in HTTP basic authentication: si_interserver",
}

type interserverEnv struct {
	Cred, Plain         clickhouse.Conn
	CredAddr, PlainAddr string
}

// requireInterserver is a second opt-in, ARBITER_CH_INTERSERVER=1, after the
// suite's; once set, a missing address fails.
func requireInterserver(t *testing.T) interserverEnv {
	t.Helper()
	requireMechanics(t)
	if os.Getenv(envInterserver) != "1" {
		t.Skip("set ARBITER_CH_INTERSERVER=1, CH_ISCRED_ADDR and CH_ISPLAIN_ADDR (scripts/ci/clickhouse-mechanics.sh starts the pair) to run the interserver mechanics")
	}
	var missing []string
	for _, name := range []string{envIsCredAddr, envIsPlainAddr} {
		if os.Getenv(name) == "" {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		t.Fatalf("ARBITER_CH_INTERSERVER=1 requires %s", strings.Join(missing, ", "))
	}
	env := interserverEnv{CredAddr: os.Getenv(envIsCredAddr), PlainAddr: os.Getenv(envIsPlainAddr)}
	env.Cred, env.Plain = chtest.Open(t, env.CredAddr), chtest.Open(t, env.PlainAddr)
	requireKeeper(t, envIsCredAddr, env.Cred)
	requireKeeper(t, envIsPlainAddr, env.Plain)
	return env
}

// interserverTable creates one ReplicatedMergeTree table, under a Keeper path
// of the test's own, on both servers (replicas iscred and isplain) and returns
// its database.
func interserverTable(t *testing.T, env interserverEnv) string {
	t.Helper()
	db := "mech_is_" + suffix(t)
	path := "/mechanics/interserver/" + suffix(t)
	for _, s := range []struct {
		conn          clickhouse.Conn
		addr, replica string
	}{{env.Cred, env.CredAddr, "iscred"}, {env.Plain, env.PlainAddr, "isplain"}} {
		dropDatabasesAt(t, s.addr, db)
		mustExec(t, s.conn, "CREATE DATABASE IF NOT EXISTS "+ident(db))
		mustExec(t, s.conn, fmt.Sprintf("CREATE TABLE %s (v UInt64) ENGINE = ReplicatedMergeTree(%s, %s) ORDER BY v",
			qualified(db, "t"), literal(path), literal(s.replica)))
	}
	return db
}

// TestMechanics_InterserverAllowEmptyAcceptsAnUncredentialedFetcher (M16): a
// part inserted on the credentialed server reaches the uncredentialed one:
// with allow_empty a credentialed server serves a fetcher that sends no
// credentials, so in interserver step 1 every source may be credentialed
// before its fetchers are (K56), and the fetched part keeps its
// hash_of_all_files.
func TestMechanics_InterserverAllowEmptyAcceptsAnUncredentialedFetcher(t *testing.T) {
	env := requireInterserver(t)
	db := interserverTable(t, env)
	mustExec(t, env.Cred, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(10)", qualified(db, "t")))
	eventually(t, interserverWait, "ch-isplain fetching the part inserted on ch-iscred", func() (bool, string) {
		n, err := countRows(env.Plain, qualified(db, "t"))
		return err == nil && n == 10, fmt.Sprintf("ch-isplain holds %d rows (%v)", n, err)
	})
	if got, want := physHashes(activeParts(t, env.Plain, db, "t", "tuple()")), physHashes(activeParts(t, env.Cred, db, "t", "tuple()")); strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("the fetched part's hash_of_all_files %v, the source's %v", got, want)
	}
}

// TestMechanics_InterserverUncredentialedServerRefusesACredentialedFetcher
// (M16): a part inserted on the uncredentialed server does not reach the
// credentialed one within 60 s, whose replication queue records the
// interserver handler's refusal: a fetcher that sends credentials is refused
// by a server that has none, which is why interserver step 1 credentials the
// sources before the fetchers (K56).
func TestMechanics_InterserverUncredentialedServerRefusesACredentialedFetcher(t *testing.T) {
	env := requireInterserver(t)
	db := interserverTable(t, env)
	start := time.Now()
	mustExec(t, env.Plain, fmt.Sprintf("INSERT INTO %s SELECT number FROM numbers(10)", qualified(db, "t")))
	var refusal string
	eventually(t, interserverWait, "ch-iscred's replication queue recording the refused fetch", func() (bool, string) {
		var exception string
		err := env.Cred.QueryRow(context.Background(),
			"SELECT any(last_exception) FROM system.replication_queue WHERE database = ? AND table = 't' AND last_exception != ''", db).Scan(&exception)
		for _, r := range interserverRefusals {
			if err == nil && strings.Contains(exception, "HTTP status code: 401") && strings.Contains(exception, r) {
				refusal = r
				return true, ""
			}
		}
		first, _, _ := strings.Cut(exception, "Stack trace")
		return false, fmt.Sprintf("last_exception %q (%v)", first, err)
	})
	t.Logf("M16 interserver refusal: %s", refusal)
	for time.Since(start) < interserverWait {
		if n := mustCountRows(t, env.Cred, qualified(db, "t")); n != 0 {
			t.Fatalf("ch-iscred holds %d rows %s after the insert: an uncredentialed server served a credentialed fetcher", n, time.Since(start).Round(time.Second))
		}
		time.Sleep(time.Second)
	}
}
