package sources

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"math/big"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/dataplane/objstore"
)

// s3Env is the bucket of the restore test (contract §23.3's names).
type s3Env struct {
	Endpoint, Bucket, AccessKeyID, Secret string
}

func requireS3(t *testing.T) s3Env {
	t.Helper()
	env := s3Env{Endpoint: os.Getenv(envS3Endpoint), Bucket: os.Getenv(envS3Bucket), AccessKeyID: os.Getenv(envS3AccessKeyID), Secret: os.Getenv(envS3Secret)}
	if env.Endpoint == "" {
		t.Skipf("set %s, %s, %s and %s (a bucket both servers reach, with a key that may write it) to run the restore test", envS3Endpoint, envS3Bucket, envS3AccessKeyID, envS3Secret)
	}
	if env.Bucket == "" || env.AccessKeyID == "" || env.Secret == "" {
		t.Fatalf("%s is set, so %s, %s and %s are required", envS3Endpoint, envS3Bucket, envS3AccessKeyID, envS3Secret)
	}
	return env
}

// TestRestoreSourceKeepsOnlyTheNeededPart: the owner's backup (Plan S3-C1's
// BACKUP DATABASE hg_safe TO S3, with its index object) holds a partition of
// two parts and another partition; restoring the need copies exactly the
// needed part into the scratch (with its row hash, under the scratch's own
// structure), drops <scratch>_r, and leaves every other part out; a need the
// index does not list is missed; an endpoint that refuses connections is a
// refused fetch at the index read's deadline, with no part of the key in its
// error and nothing in the scratch.
func TestRestoreSourceKeepsOnlyTheNeededPart(t *testing.T) {
	p := requirePair(t)
	s3 := requireS3(t)
	sfx := suffix(t)
	schema := testSchema("rst_" + sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, "hg_safe", schema)
	insertPart(t, p.Peer, "hg_safe", table, "a", 0, 3)
	insertPart(t, p.Peer, "hg_safe", table, "a", 100, 2)
	insertPart(t, p.Peer, "hg_safe", table, "b", 200, 4)
	parts := partsOf(t, p.Peer, "hg_safe", table)
	scans := scanOf(t, p.Peer, "hg_safe", table, schema)
	n, err := rand.Int(rand.Reader, big.NewInt(1_000_000_000))
	if err != nil {
		t.Fatal(err)
	}
	owner := 1_000_000_000 + n.Uint64() // a fresh owner-<id>/ prefix per run
	bucket, err := objstore.ParseBucket(strings.TrimRight(s3.Endpoint, "/") + "/" + s3.Bucket)
	if err != nil {
		t.Fatal(err)
	}
	creds := objstore.Credentials{AccessKeyID: s3.AccessKeyID, SecretAccessKey: s3.Secret}
	started := time.Now().UTC()
	name := objstore.BackupName(started, objstore.BackupFull)
	mustExec(t, p.Peer, objstore.BackupSQL(bucket, owner, name, "", creds))
	index := objstore.BackupIndex{
		Format: objstore.BackupIndexFormat, NetworkID: "sources-test", OwnerIndexerID: owner, Name: name, Kind: objstore.BackupFull,
		ChainLength: 1, StartedAt: started.Format(time.RFC3339), FinishedAt: time.Now().UTC().Format(time.RFC3339),
		SnapshotID: "0x" + strings.Repeat("ab", 32), Entry: replay.TrackFrontier{IndexerID: owner, SafeBlockSeq: 1, AppliedPromotionSeq: 1},
		Tables: []objstore.BackupIndexTable{{PhysicalTable: table, TableID: schema.TableID}},
	}
	byRows := map[uint64]partRow{}
	for _, part := range parts {
		partition := "p_a"
		if part.Rows == 4 {
			partition = "p_b"
		}
		byRows[part.Rows] = part
		index.Tables[0].Parts = append(index.Tables[0].Parts, objstore.BackupIndexPart{Name: part.Name, PartitionID: partition, PhysHash: part.Hash, Rows: part.Rows})
	}
	body, err := json.Marshal(index)
	if err != nil {
		t.Fatal(err)
	}
	mustExec(t, p.Peer, "INSERT INTO FUNCTION s3("+literal(bucket.IndexURL(owner, name))+", "+literal(s3.AccessKeyID)+", "+literal(s3.Secret)+", 'RawBLOB') SELECT "+literal(string(body)))

	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", nil, "GRANT READ ON S3 TO %s")
	cfg := RestoreConfig{Endpoint: s3.Endpoint, Bucket: s3.Bucket, AccessKeyID: s3.AccessKeyID, Secret: s3.Secret}
	source := NewRestore(member, cfg, owner)
	needed := byRows[3]
	need := Need{TableKey: schema.TableID, Table: table, PartitionID: "p_a", PartName: needed.Name, PartPhysHash: needed.Hash, PartRowLtHash: scans[needed.Name].RowLtHash, RowCount: 3}

	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	res := source.Fetch(context.Background(), need, schema, scratch)
	if res.Outcome != OutcomeFetched || res.Err != nil || res.Source != SourceRestore {
		t.Fatalf("Fetch = %+v, want fetched from restore", res)
	}
	requireOnePart(t, member, res, scratch, schema, need.PartRowLtHash, 3)
	if got := tablesIn(t, p.Member, stage); !slices.Equal(got, []string{scratch.Table}) {
		t.Fatalf("%s holds %v after the restore, want only the scratch (no %s_r)", stage, got, scratch.Table)
	}

	unlisted := need
	unlisted.PartPhysHash = strings.Repeat("0", 32)
	scratch2 := newScratch(t, member, stage, "member-test_"+sfx+"_2", schema)
	if res := source.Fetch(context.Background(), unlisted, schema, scratch2); res.Outcome != OutcomeMissed || res.Err != nil {
		t.Fatalf("Fetch of a need no backup lists = %+v, want missed", res)
	}
	if got := partsOf(t, p.Member, stage, scratch2.Table); len(got) != 0 {
		t.Fatalf("a missed restore wrote %+v", got)
	}

	// An object store that refuses connections: the server would retry the
	// index read 500 times; the read's deadline (IndexTimeout) ends it, a
	// refused fetch.
	down := NewRestore(member, RestoreConfig{Endpoint: "http://" + p.PeerHost + ":1", Bucket: s3.Bucket, AccessKeyID: s3.AccessKeyID, Secret: s3.Secret}, owner)
	down.IndexTimeout = 5 * time.Second
	scratch3 := newScratch(t, member, stage, "member-test_"+sfx+"_3", schema)
	began := time.Now()
	res = down.Fetch(context.Background(), need, schema, scratch3)
	took := time.Since(began)
	if res.Outcome != OutcomeRefused || res.Err == nil || took > down.IndexTimeout+20*time.Second {
		t.Fatalf("Fetch from an endpoint that refuses connections = %+v after %s, want refused within the index read's deadline of %s", res, took, down.IndexTimeout)
	}
	if !strings.Contains(res.Err.Error(), "read the backup indexes of indexer") {
		t.Fatalf("the refusal %v does not name the index read", res.Err)
	}
	if strings.Contains(res.Err.Error(), s3.Secret) || strings.Contains(res.Err.Error(), s3.AccessKeyID) {
		t.Fatalf("the restore error carries the key: %v", res.Err)
	}
	if parts := partsOf(t, p.Member, stage, scratch3.Table); len(parts) != 0 {
		t.Fatalf("a refused restore wrote %+v", parts)
	}
	t.Logf("endpoint refusing connections: %s after %s: %v", res.Outcome, took.Round(time.Millisecond), res.Err)
}
