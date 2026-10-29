package rebuild

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/objstore"
	"github.com/sentioxyz/arbiter-core/wire"
)

// untouchedConn fails the test on any ClickHouse call: a refusal must come
// before the rebuild reads or writes anything.
type untouchedConn struct {
	clickhouse.Conn
	t *testing.T
}

func (c untouchedConn) Exec(context.Context, string, ...any) error {
	c.t.Fatal("the rebuild ran a statement before its refusal")
	return nil
}

func (c untouchedConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	c.t.Fatal("the rebuild queried ClickHouse before its refusal")
	return nil, nil
}

func (c untouchedConn) QueryRow(context.Context, string, ...any) driver.Row {
	c.t.Fatal("the rebuild queried ClickHouse before its refusal")
	return nil
}

func unitConfig(t *testing.T) Config {
	return Config{NetworkID: "devnet2", IndexerID: 2, StateDir: filepath.Join(t.TempDir(), "snode"),
		Pinned: ddl.Pinned{UnsafeDB: "hg_unsafe", SafeDB: "hg_safe", PromoteDB: "hg_promote", NodeID: "snode-3"}}
}

func TestRebuildRefusesALiveTrack(t *testing.T) {
	cfg := unitConfig(t)
	in := Inputs{Registry: wire.TableRegistrySnapshot{QuarantinedIndexers: []uint64{1}}}
	report, err := Rebuild(context.Background(), cfg, untouchedConn{t: t}, in, nil)
	if !errors.Is(err, ErrRebuildNotQuarantined) || err.Error() != "rebuild: indexer 2 is not quarantined; a live track's hg_safe is never rebuilt" {
		t.Fatalf("err = %v", err)
	}
	if !reflect.DeepEqual(report, Report{}) {
		t.Fatalf("report = %+v", report)
	}
	if _, err := os.Stat(cfg.StateDir); !os.IsNotExist(err) {
		t.Fatalf("the refusal touched the state directory: %v", err)
	}
	cfg.DryRun = true
	if _, err := Rebuild(context.Background(), cfg, untouchedConn{t: t}, in, nil); !errors.Is(err, ErrRebuildNotQuarantined) {
		t.Fatalf("a dry run of a live track: err = %v", err)
	}
}

func TestRebuildRefusesUnacknowledgedPromotions(t *testing.T) {
	in := Inputs{
		Registry: wire.TableRegistrySnapshot{QuarantinedIndexers: []uint64{2}},
		Unacked:  []wire.PromotionOutcome{{PromotionSeq: 42}, {PromotionSeq: 40, Acked: true}, {PromotionSeq: 41}},
	}
	_, err := Rebuild(context.Background(), unitConfig(t), untouchedConn{t: t}, in, nil)
	if !errors.Is(err, ErrRebuildUnacked) ||
		err.Error() != "rebuild: indexer 2 has issued, unacknowledged promotions [41 42]; settle them first (arbiter-admin indexer settle)" {
		t.Fatalf("err = %v", err)
	}
}

// TestReportJSONLinesCarryAPhaseOnEveryLine pins the report the runbook keeps
// (rebuild-report.jsonl, dry-run.jsonl): one fence line per fence result, one
// plan or restore line per partition, a done line with the totals, a phase on
// every line, an empty missing list as [].
func TestReportJSONLinesCarryAPhaseOnEveryLine(t *testing.T) {
	r := Report{
		Fence: []FenceResult{{TableID: "db.t", Path: "/sentio/0/unsafe/db__t", Present: true, Dropped: true}},
		Partitions: []PartitionReport{
			{TableID: "db.t", PartitionID: "p_a", PublishSeq: 7, Parts: 2, FromOwnUnsafe: 1, FromObjectStore: 1, Root: "0xroot"},
			{TableID: "db.u", PartitionID: "all", PublishSeq: 3, Parts: 1, Missing: []string{"all_1_1_0"}, Root: "0xroot2"},
		},
	}
	got := r.JSONLines()
	want := []string{
		`{"phase":"fence","table_id":"db.t","path":"/sentio/0/unsafe/db__t","present":true,"dropped":true}`,
		`{"phase":"restore","table_id":"db.t","partition_id":"p_a","publish_seq":7,"parts":2,"from_own_unsafe":1,"from_object_store":1,"missing":[],"root":"0xroot"}`,
		`{"phase":"restore","table_id":"db.u","partition_id":"all","publish_seq":3,"parts":1,"from_own_unsafe":0,"from_object_store":0,"missing":["all_1_1_0"],"root":"0xroot2"}`,
		`{"phase":"done","dry_run":false,"already_rebuilt":false,"tables":2,"partitions":2,"parts":3,"from_own_unsafe":1,"from_object_store":1,"missing":1}`,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JSONLines =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	r.DryRun, r.Fence = true, nil
	for _, line := range r.JSONLines() {
		var v map[string]any
		if err := json.Unmarshal([]byte(line), &v); err != nil || v["phase"] == nil || v["phase"] == "restore" {
			t.Fatalf("dry-run line %s: %v", line, err)
		}
	}
}

// TestIndexStoreReadsTheBackupIndex: NewObjectStore's store lists a backup
// index's parts by table and logical partition, and its partition clause is
// objstore.PartitionClause's text for one partition and lists several.
func TestIndexStoreReadsTheBackupIndex(t *testing.T) {
	bucket, err := objstore.ParseBucket("https://storage.googleapis.com/si-backups")
	if err != nil {
		t.Fatal(err)
	}
	idx := objstore.BackupIndex{Name: "20261015T120000Z-full", Tables: []objstore.BackupIndexTable{{PhysicalTable: "db__t", TableID: "db.t", Parts: []objstore.BackupIndexPart{
		{Name: "a_1_1_0", PartitionID: "p_a", PhysHash: "h1", Rows: 3}, {Name: "b_2_2_0", PartitionID: "p_b", PhysHash: "h2", Rows: 4}}}}}
	s := newIndexStore(objstore.BackupRef{Bucket: bucket, Owner: 2, Name: idx.Name}, idx)
	if got := s.Parts("db.t", "p_a"); !reflect.DeepEqual(got, []BackupPart{{Name: "a_1_1_0", HashOfAllFiles: "h1", Rows: 3}}) {
		t.Fatalf("Parts(p_a) = %+v", got)
	}
	if got := s.Parts("db.other", "p_a"); got != nil {
		t.Fatalf("Parts of another table = %+v", got)
	}
	for ids, want := range map[string]string{"all": "", "p_a": "PARTITIONS 'a'", "p_a,p_it's": `PARTITIONS 'a', 'it\'s'`} {
		got, err := partitionsClause(strings.Split(ids, ","))
		if err != nil || got != want {
			t.Fatalf("partitionsClause(%s) = %q, %v; want %q", ids, got, err, want)
		}
	}
	for id, sch := range map[string]payloadexec.TableSchema{"all": {TableID: "db.u"}, "p_it's": {TableID: "db.t", PartitionBy: "p"}} {
		want, err := objstore.PartitionClause(sch, id)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := partitionsClause([]string{id}); err != nil || got != want {
			t.Fatalf("partitionsClause(%s) = %q, %v; objstore.PartitionClause = %q", id, got, err, want)
		}
	}
	if _, err := partitionsClause([]string{"202607"}); err == nil {
		t.Fatal("a partition id without p_ must be refused")
	}
}

// echoConn fails every statement with an error that echoes it, as a server
// error may echo a fragment of the statement that failed.
type echoConn struct {
	clickhouse.Conn
	statements []string
}

func (c *echoConn) Exec(_ context.Context, query string, _ ...any) error {
	c.statements = append(c.statements, query)
	return errors.New("code: 499, message: The AWS Access Key Id you provided does not exist in our records, while executing " + query)
}

// TestIndexStoreRestoreRedactsTheSecretFromItsError: the store restores one
// partition of the named backup into hg_promote.<scratch> with
// objstore.RestoreSQL, and a failure never carries the secret, raw or
// SQL-escaped.
func TestIndexStoreRestoreRedactsTheSecretFromItsError(t *testing.T) {
	bucket, err := objstore.ParseBucket("https://storage.googleapis.com/si-backups")
	if err != nil {
		t.Fatal(err)
	}
	creds := objstore.Credentials{AccessKeyID: "GOOGTESTACCESSKEY", SecretAccessKey: "s3cr3t'value/with+chars"}
	s := newIndexStore(objstore.BackupRef{Bucket: bucket, Owner: 2, Name: "20261015T120000Z-full", Credentials: creds}, objstore.BackupIndex{})
	conn := &echoConn{}

	err = s.RestorePartitions(context.Background(), conn, "db.t", []string{"p_a"}, "db__t__rebuild_1")

	want := objstore.RestoreSQL(bucket, 2, "20261015T120000Z-full", "db__t", "PARTITIONS 'a'", objstore.Scratch{Database: "hg_promote", Table: "db__t__rebuild_1"}, creds)
	if len(conn.statements) != 1 || conn.statements[0] != want {
		t.Fatalf("statements = %q, want [%q]", conn.statements, want)
	}
	if err == nil || strings.Contains(err.Error(), "s3cr3t") || !strings.Contains(err.Error(), "[redacted]") ||
		!strings.HasPrefix(err.Error(), "rebuild: restore db.t partitions [p_a] of backup 20261015T120000Z-full into hg_promote.db__t__rebuild_1: code: 499") {
		t.Fatalf("err = %v", err)
	}
}

type failedScratchRows struct {
	driver.Rows
	read    bool
	failure error
}

func (r *failedScratchRows) Next() bool {
	if r.read {
		return false
	}
	r.read = true
	return true
}
func (r *failedScratchRows) Scan(dest ...any) error {
	*(dest[0].(*string)) = "db__t__rebuild_1"
	return nil
}
func (r *failedScratchRows) Close() error { return nil }
func (r *failedScratchRows) Err() error   { return r.failure }

type failedScratchConn struct {
	clickhouse.Conn
	rows  driver.Rows
	drops int
}

func (c *failedScratchConn) Query(context.Context, string, ...any) (driver.Rows, error) {
	return c.rows, nil
}
func (c *failedScratchConn) Exec(context.Context, string, ...any) error { c.drops++; return nil }

// A truncated enumeration is not a complete scratch inventory. Do not drop
// even the partial result or proceed to restoration after the stream fails.
func TestDropScratchesRefusesATruncatedInventoryBeforeDropping(t *testing.T) {
	failure := errors.New("scratch inventory stream interrupted")
	c := &failedScratchConn{rows: &failedScratchRows{failure: failure}}
	err := dropScratches(context.Background(), c, "hg_promote")
	if !errors.Is(err, failure) || c.drops != 0 {
		t.Fatalf("err=%v, drops=%d; want inventory error before any DROP", err, c.drops)
	}
}
