package sources

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/sentioxyz/arbiter-core/dataplane/objstore"
)

const credentialsIndex = `{"format":"si-backup-index-v1","owner_indexer_id":7,"name":"20261015T000000Z-full","kind":"full","chain_length":1,` +
	`"tables":[{"physical_table":"dex__swaps","incarnation_seq":0,"parts":[{"name":"p_1_1_0","partition_id":"p_2026","phys_hash":"4adff322bb8fd41ea3d90f10b04424fb","rows":3}]}]}`

// indexOnlyConn answers the index read with credentialsIndex and every other
// query with no rows, so a Fetch restores and then misses the part.
func indexOnlyConn() *fakeConn {
	return &fakeConn{rows: func(query string, _ []any) ([][]any, error) {
		if strings.HasPrefix(query, "SELECT _file, * FROM s3(") {
			return [][]any{{"20261015T000000Z-full.index.json", credentialsIndex}}, nil
		}
		return nil, nil
	}}
}

func credentialsNeed() Need {
	return Need{TableKey: "dex.swaps", Table: "dex__swaps", PartitionID: "p_2026", PartPhysHash: "4adff322bb8fd41ea3d90f10b04424fb", RowCount: 3}
}

// TestRestoreCredentialsAreAskedForOnEveryFetch: with
// RestoreConfig.Credentials set, every Fetch asks it once, with the source's
// FetchTimeout as the need, and uses the bucket and key it answers: the
// index read takes the five-argument s3() and the RESTORE goes through a
// temporary named collection that is dropped afterwards; no rendering or
// error carries the secret or the token.
func TestRestoreCredentialsAreAskedForOnEveryFetch(t *testing.T) {
	// Given
	const secret, token = "broker-secret-0001", "broker-token-0001"
	var needs []time.Duration
	cfg := RestoreConfig{Credentials: func(_ context.Context, need time.Duration) (RestoreCredentials, error) {
		needs = append(needs, need)
		return RestoreCredentials{
			BucketURL:   "https://acct.r2.cloudflarestorage.com/sentio-si-devnet2-si-v2-backup",
			Credentials: objstore.Credentials{AccessKeyID: fmt.Sprintf("broker-key-%d", len(needs)), SecretAccessKey: secret, SessionToken: token},
		}, nil
	}}
	conn := indexOnlyConn()
	source := NewRestore(conn, cfg, 7)
	source.FetchTimeout = 20 * time.Minute

	// When
	first := source.Fetch(context.Background(), credentialsNeed(), pullSchema(), Scratch{Database: "hg_rstage", Table: "member-a_0011aabb_1"})
	second := source.Fetch(context.Background(), credentialsNeed(), pullSchema(), Scratch{Database: "hg_rstage", Table: "member-a_0011aabb_2"})

	// Then
	if len(needs) != 2 || needs[0] != 20*time.Minute || needs[1] != 20*time.Minute {
		t.Fatalf("the credentials were asked for with %v, want twice with 20m", needs)
	}
	if first.Outcome != OutcomeMissed || second.Outcome != OutcomeMissed {
		t.Fatalf("Fetch = %+v, %+v; want missed (the fake has no restored part)", first, second)
	}
	reads := conn.matching("query SELECT _file, * FROM s3('https://acct.r2.cloudflarestorage.com/sentio-si-devnet2-si-v2-backup/owner-7/*.index.json', 'broker-key-")
	if len(reads) != 2 || !strings.Contains(reads[0], "'"+token+"', 'RawBLOB')") || !strings.Contains(reads[1], "'broker-key-2'") {
		t.Fatalf("index reads = %q, want two five-argument s3() reads with each fetch's key", reads)
	}
	creates := conn.matching("exec CREATE NAMED COLLECTION si_backup_")
	restores := conn.matching("exec RESTORE TABLE hg_safe.`dex__swaps` AS `hg_rstage`.`member-a_0011aabb_1_r` PARTITIONS '2026' FROM S3(si_backup_")
	drops := conn.matching("exec DROP NAMED COLLECTION IF EXISTS si_backup_")
	if len(creates) != 2 || len(restores) != 1 || len(drops) != 2 {
		t.Fatalf("statements = %q, want a collection created, used and dropped per fetch", conn.all())
	}
	for _, s := range append(conn.matching("exec RESTORE"), fmt.Sprint(cfg), fmt.Sprintf("%#v", cfg), fmt.Sprint(source)) {
		if strings.Contains(s, secret) || strings.Contains(s, token) {
			t.Fatalf("the key reached a RESTORE statement or a rendering: %s", s)
		}
	}
}

// TestRestoreCredentialsErrorFailsTheFetch: an error of
// RestoreConfig.Credentials fails the Fetch before any statement, and the
// error carries it.
func TestRestoreCredentialsErrorFailsTheFetch(t *testing.T) {
	// Given
	cause := errors.New("broker: rate_limited")
	cfg := RestoreConfig{Credentials: func(ctx context.Context, need time.Duration) (RestoreCredentials, error) {
		return RestoreCredentials{}, cause
	}}
	conn := indexOnlyConn()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	// When
	res := NewRestore(conn, cfg, 7).Fetch(ctx, credentialsNeed(), pullSchema(), Scratch{Database: "hg_rstage", Table: "member-a_0011aabb_3"})

	// Then
	if res.Outcome != OutcomeRefused || res.Err == nil || !errors.Is(res.Err, cause) || !strings.Contains(res.Err.Error(), "the restore credentials") {
		t.Fatalf("Fetch = %+v, want refused with the credentials error", res)
	}
	if len(conn.all()) != 0 {
		t.Fatalf("statements = %q, want none", conn.all())
	}
}

// TestRestoreWithoutCredentialsIsUnchanged: without RestoreConfig.Credentials
// the static key pair renders exactly objstore.RestoreSQL and opens no
// named collection, and the config prints as before.
func TestRestoreWithoutCredentialsIsUnchanged(t *testing.T) {
	// Given
	cfg := RestoreConfig{Endpoint: "http://si-minio:9000", Bucket: "si-replica", AccessKeyID: "AKIDSTATIC0001", Secret: "static-secret-0001"}
	conn := indexOnlyConn()

	// When
	res := NewRestore(conn, cfg, 7).Fetch(context.Background(), credentialsNeed(), pullSchema(), Scratch{Database: "hg_rstage", Table: "member-a_0011aabb_4"})

	// Then
	if res.Outcome != OutcomeMissed {
		t.Fatalf("Fetch = %+v, want missed", res)
	}
	bucket, err := objstore.ParseBucket("http://si-minio:9000/si-replica")
	if err != nil {
		t.Fatal(err)
	}
	creds := objstore.Credentials{AccessKeyID: cfg.AccessKeyID, SecretAccessKey: cfg.Secret}
	want := "exec " + objstore.RestoreSQL(bucket, 7, "20261015T000000Z-full", "dex__swaps", "PARTITIONS '2026'", objstore.Scratch{Database: "hg_rstage", Table: "member-a_0011aabb_4_r"}, creds)
	if got := conn.matching("exec RESTORE "); len(got) != 1 || got[0] != want {
		t.Fatalf("RESTORE = %q, want [%q]", got, want)
	}
	if n := len(conn.matching("exec CREATE NAMED COLLECTION")) + len(conn.matching("exec DROP NAMED COLLECTION")); n != 0 {
		t.Fatalf("a static key pair opened a named collection: %q", conn.all())
	}
	if got := cfg.String(); got != `RestoreConfig{Endpoint: "http://si-minio:9000", Bucket: "si-replica", AccessKeyID: [redacted], Secret: [redacted]}` {
		t.Fatalf("String = %s", got)
	}
}
