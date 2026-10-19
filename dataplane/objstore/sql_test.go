package objstore

import (
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func testBucket(t *testing.T) Bucket {
	t.Helper()
	b, err := ParseBucket("http://arbiter-core-minio:9000/si-mechanics")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return b
}

func TestBackupSQLNamesTheBaseOnlyForAnIncrementalBackup(t *testing.T) {
	// Given
	b := testBucket(t)
	c := Credentials{AccessKeyID: "mech-nodelete", SecretAccessKey: `se'cret`}

	// When
	full := BackupSQL(b, 1, "20261015T031700Z-full", "", c)
	incr := BackupSQL(b, 1, "20261016T031700Z-incr", "20261015T031700Z-full", c)

	// Then
	if want := `BACKUP DATABASE hg_safe TO S3('http://arbiter-core-minio:9000/si-mechanics/owner-1/20261015T031700Z-full/', 'mech-nodelete', 'se\'cret')`; full != want {
		t.Errorf("full:\n got %s\nwant %s", full, want)
	}
	if want := `BACKUP DATABASE hg_safe TO S3('http://arbiter-core-minio:9000/si-mechanics/owner-1/20261016T031700Z-incr/', 'mech-nodelete', 'se\'cret')` +
		` SETTINGS base_backup = S3('http://arbiter-core-minio:9000/si-mechanics/owner-1/20261015T031700Z-full/', 'mech-nodelete', 'se\'cret')`; incr != want {
		t.Errorf("incremental:\n got %s\nwant %s", incr, want)
	}
}

func TestRestoreSQLRestoresOnePartitionIntoTheScratch(t *testing.T) {
	// Given
	b := testBucket(t)
	c := Credentials{AccessKeyID: "mech-readonly", SecretAccessKey: "s"}
	sch := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}}}
	clause, err := PartitionClause(sch, "p_2026'10")
	if err != nil {
		t.Fatalf("partition clause: %v", err)
	}
	namer, err := NewScratchNamer("p1drill_b_")
	if err != nil {
		t.Fatalf("namer: %v", err)
	}

	// When
	got := RestoreSQL(b, 1, "20261016T031700Z-incr", "db__t", clause, namer.Next(), c)

	// Then
	want := "RESTORE TABLE hg_safe.`db__t` AS `hg_rstage`.`p1drill_b_1` PARTITIONS '2026\\'10' FROM S3('http://arbiter-core-minio:9000/si-mechanics/owner-1/20261016T031700Z-incr/', 'mech-readonly', 's')"
	if got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
	if next := namer.Next(); next != (Scratch{Database: "hg_rstage", Table: "p1drill_b_2"}) || next.Qualified() != "`hg_rstage`.`p1drill_b_2`" {
		t.Errorf("second scratch %+v (%s)", next, next.Qualified())
	}
	if _, err := NewScratchNamer("bad prefix"); err == nil || err.Error() != `objstore: scratch prefix "bad prefix" must match [A-Za-z0-9_-]{1,64}` {
		t.Errorf("a bad prefix: %v", err)
	}
	if _, err := PartitionClause(sch, "all"); err == nil {
		t.Error("a partitioned table refuses partition all")
	}
}

func TestPartitionClauseOfAnUnpartitionedTableIsEmpty(t *testing.T) {
	// Given
	sch := payloadexec.TableSchema{TableID: "db.u", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}

	// When
	clause, err := PartitionClause(sch, "all")
	_, other := PartitionClause(sch, "p_x")

	// Then
	if err != nil || clause != "" {
		t.Fatalf("PartitionClause(all) = %q, %v; want empty", clause, err)
	}
	if other == nil || other.Error() != "unpartitioned table wants partition all, got p_x" {
		t.Fatalf("PartitionClause(p_x) error = %v", other)
	}
	got := RestoreSQL(testBucket(t), 0, "20261015T031700Z-full", "db__u", clause, Scratch{Database: StageDatabase, Table: "s1"}, Credentials{AccessKeyID: "k", SecretAccessKey: "s"})
	if want := "RESTORE TABLE hg_safe.`db__u` AS `hg_rstage`.`s1` FROM S3('http://arbiter-core-minio:9000/si-mechanics/owner-0/20261015T031700Z-full/', 'k', 's')"; got != want {
		t.Errorf("\n got %s\nwant %s", got, want)
	}
}

func TestIndexStatementsNameTheOwnersObjects(t *testing.T) {
	// Given
	b := testBucket(t)
	c := Credentials{AccessKeyID: "k", SecretAccessKey: "s"}

	// When, Then
	for _, x := range []struct{ got, want string }{
		{indexWriteSQL(b, 3, "20261015T031700Z-full", c), "INSERT INTO FUNCTION s3('http://arbiter-core-minio:9000/si-mechanics/owner-3/20261015T031700Z-full.index.json', 'k', 's', 'RawBLOB') SELECT {index:String}"},
		{indexReadSQL(b, 3, c), "SELECT _file, * FROM s3('http://arbiter-core-minio:9000/si-mechanics/owner-3/*.index.json', 'k', 's', 'RawBLOB')"},
		{backupObjectsSQL(b, 3, "20261015T031700Z-full", c), "SELECT count() FROM s3('http://arbiter-core-minio:9000/si-mechanics/owner-3/20261015T031700Z-full/**', 'k', 's', 'One')"},
	} {
		if x.got != x.want {
			t.Errorf("\n got %s\nwant %s", x.got, x.want)
		}
	}
	if indexWriteSettings["s3_truncate_on_insert"] != 0 || indexWriteSettings["s3_create_new_file_on_insert"] != 0 {
		t.Errorf("an index is written once, never overwritten: %v", indexWriteSettings)
	}
	if listSettings["s3_throw_on_zero_files_match"] != 0 {
		t.Errorf("a glob that matches nothing is an empty answer: %v", listSettings)
	}
}

func TestBackupErrorTextLosesItsStackTrace(t *testing.T) {
	// Given
	text := "Code: 499. DB::Exception: Access Denied. (Code: 15) while removing object with path owner-1/x/.lock from S3. (S3_ERROR), Stack trace (when copying this message, always include the lines below):\n\n0. DB::Exception::Exception(...)"

	// When
	got := firstLine(text)

	// Then
	if want := "Code: 499. DB::Exception: Access Denied. (Code: 15) while removing object with path owner-1/x/.lock from S3. (S3_ERROR)"; got != want {
		t.Errorf("got %q, want %q", got, want)
	}
	if got := firstLine("one line\nsecond line"); got != "one line" {
		t.Errorf("got %q", got)
	}
}
