package objstore

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
)

// literal single-quotes s as a ClickHouse string literal. Only URLs, paths
// and the key strings are interpolated this way; every other value is a
// query parameter and every identifier is backquoted or generated.
func literal(s string) string { return "'" + chparts.EscapeSQLString(s) + "'" }

// ident backquotes a ClickHouse identifier.
func ident(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// s3Endpoint is a BACKUP or RESTORE destination with a static key pair.
func s3Endpoint(url string, c Credentials) string {
	return "S3(" + literal(url) + ", " + literal(c.AccessKeyID) + ", " + literal(c.SecretAccessKey) + ")"
}

// s3Function is the s3() table function over url in format: the four-argument
// form for a static key pair, the five-argument form with the session token
// when c has one.
func s3Function(url string, c Credentials, format string) string {
	args := literal(url) + ", " + literal(c.AccessKeyID) + ", " + literal(c.SecretAccessKey)
	if c.SessionToken != "" {
		args += ", " + literal(c.SessionToken)
	}
	return "s3(" + args + ", " + literal(format) + ")"
}

// Endpoint is how BACKUP and RESTORE address one bucket with one key. For a
// static key pair it is S3('<url>', '<key>', '<secret>'), exactly as before
// session tokens existed. BACKUP and RESTORE take no positional session token
// (ClickHouse's S3 backup engine accepts one or three arguments), so for
// credentials with one it is S3(<collection>, '<path>') over a temporary
// named collection that holds the bucket URL, the key pair and the token,
// the path relative to the bucket. OpenEndpoint makes one.
type Endpoint struct {
	bucket     Bucket
	creds      Credentials
	collection string
}

// staticEndpoint is the Endpoint of a static key pair; c's session token, if
// any, is not used.
func staticEndpoint(b Bucket, c Credentials) Endpoint { return Endpoint{bucket: b, creds: c} }

// Collection is the name of the endpoint's temporary named collection, empty
// for a static key pair.
func (e Endpoint) Collection() string { return e.collection }

// Backup is the S3(...) of backup name's directory under owner's prefix.
func (e Endpoint) Backup(owner uint64, name string) string {
	if e.collection == "" {
		return s3Endpoint(e.bucket.BackupURL(owner, name), e.creds)
	}
	return "S3(" + e.collection + ", " + literal(OwnerPrefix(owner)+name+"/") + ")"
}

// BackupSQL is the BACKUP of owner's hg_safe into backup name, incremental on
// base when base is set, with the static key pair of c. Credentials with a
// session token need BackupSQLAt with an OpenEndpoint endpoint.
func BackupSQL(b Bucket, owner uint64, name, base string, c Credentials) string {
	return BackupSQLAt(staticEndpoint(b, c), owner, name, base)
}

// BackupSQLAt is BackupSQL through e.
func BackupSQLAt(e Endpoint, owner uint64, name, base string) string {
	sql := "BACKUP DATABASE " + SafeDatabase + " TO " + e.Backup(owner, name)
	if base != "" {
		sql += " SETTINGS base_backup = " + e.Backup(owner, base)
	}
	return sql
}

// PartitionClause restores one logical partition: nothing for an
// unpartitioned table (its one partition, all, is the whole table), else
// PARTITIONS with the quoted value.
func PartitionClause(sch payloadexec.TableSchema, partitionID string) (string, error) {
	quoted, err := chparts.QuotePartition(sch, partitionID)
	if err != nil {
		return "", err
	}
	if sch.PartitionBy == "" {
		return "", nil
	}
	return "PARTITIONS " + quoted, nil
}

// RestoreSQL restores physicalTable of backup name, as hg_safe holds it, into
// the scratch s, limited by partitionClause (PartitionClause; empty restores
// the whole table), with the static key pair of c. Credentials with a session
// token need RestoreSQLAt with an OpenEndpoint endpoint.
func RestoreSQL(b Bucket, owner uint64, name, physicalTable, partitionClause string, s Scratch, c Credentials) string {
	return RestoreSQLAt(staticEndpoint(b, c), owner, name, physicalTable, partitionClause, s)
}

// RestoreSQLAt is RestoreSQL through e.
func RestoreSQLAt(e Endpoint, owner uint64, name, physicalTable, partitionClause string, s Scratch) string {
	sql := "RESTORE TABLE " + SafeDatabase + "." + ident(physicalTable) + " AS " + s.Qualified()
	if partitionClause != "" {
		sql += " " + partitionClause
	}
	return sql + " FROM " + e.Backup(owner, name)
}

// indexWriteSQL writes one index object; its JSON is the query parameter
// index. With indexWriteSettings ClickHouse refuses an existing object
// instead of overwriting it or writing a second one beside it.
func indexWriteSQL(b Bucket, owner uint64, name string, c Credentials) string {
	return "INSERT INTO FUNCTION " + s3Function(b.IndexURL(owner, name), c, "RawBLOB") + " SELECT {index:String}"
}

var indexWriteSettings = clickhouse.Settings{"s3_truncate_on_insert": 0, "s3_create_new_file_on_insert": 0}

// listSettings make a glob that matches no object an empty answer, whatever
// the user's profile says: an owner without a backup has no index yet.
var listSettings = clickhouse.Settings{"s3_throw_on_zero_files_match": 0}

// indexReadSQL reads every index object of owner: one row per object, its
// file name and its bytes.
func indexReadSQL(b Bucket, owner uint64, c Credentials) string {
	return "SELECT _file, * FROM " + s3Function(b.IndexGlob(owner), c, "RawBLOB")
}

// backupObjectsSQL counts the objects below one backup's directory; the One
// format reads none of them.
func backupObjectsSQL(b Bucket, owner uint64, name string, c Credentials) string {
	return "SELECT count() FROM " + s3Function(b.backupObjectsGlob(owner, name), c, "One")
}

// backupDescriptorSQL counts the .backup objects directly in one backup's
// directory: 1 once ClickHouse has written the backup's descriptor, which it
// does before it removes the .lock. A * stays inside one path segment.
func backupDescriptorSQL(b Bucket, owner uint64, name string, c Credentials) string {
	return "SELECT countIf(_file = '.backup') FROM " + s3Function(b.BackupURL(owner, name)+"*", c, "One")
}

// Scratch is one scratch table: hg_rstage for every restore of plan S3-C1
// and of plan S3-C2, hg_promote for plan S3-D's rebuild scratch.
type Scratch struct {
	Database, Table string
}

// Qualified is the backquoted `<Database>`.`<Table>`.
func (s Scratch) Qualified() string { return ident(s.Database) + "." + ident(s.Table) }

// ScratchNamer hands out hg_rstage.<prefix><n>, n = 1, 2, ...
type ScratchNamer struct {
	prefix string
	mu     sync.Mutex
	n      uint64
}

var scratchPrefix = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)

// NewScratchNamer names scratch tables under prefix, which keeps one
// process's scratch tables apart from every other's.
func NewScratchNamer(prefix string) (*ScratchNamer, error) {
	if !scratchPrefix.MatchString(prefix) {
		return nil, fmt.Errorf("objstore: scratch prefix %q must match [A-Za-z0-9_-]{1,64}", prefix)
	}
	return &ScratchNamer{prefix: prefix}, nil
}

// Next is the next scratch table.
func (n *ScratchNamer) Next() Scratch {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.n++
	return Scratch{Database: StageDatabase, Table: n.prefix + strconv.FormatUint(n.n, 10)}
}

// pollInterval is how often a BACKUP or RESTORE started with ASYNC is read
// back from system.backups.
const pollInterval = 500 * time.Millisecond

// runAsync runs a BACKUP or RESTORE statement with ASYNC, so no client read
// timeout bounds it, and waits through system.backups until it reaches done
// or ends otherwise, which it returns as a *backupStatusError. The statement
// carries c, so every error is redacted.
func runAsync(ctx context.Context, conn clickhouse.Conn, statement string, c Credentials, done string) error {
	var id, status string
	if err := conn.QueryRow(ctx, statement+" ASYNC").Scan(&id, &status); err != nil {
		return c.redact(err)
	}
	for {
		var errText string
		if err := conn.QueryRow(ctx, "SELECT toString(status), error FROM system.backups WHERE id = ?", id).Scan(&status, &errText); err != nil {
			return fmt.Errorf("system.backups row %s: %w", id, err)
		}
		switch status {
		case done:
			return nil
		case "CREATING_BACKUP", "RESTORING":
		default:
			return c.redact(&backupStatusError{Status: status, ID: id, Text: firstLine(errText)})
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// backupStatusError is a BACKUP or RESTORE that system.backups shows ended in
// Status (BACKUP_FAILED, RESTORE_FAILED, ...) with the server's error Text,
// its first line.
type backupStatusError struct {
	Status, ID, Text string
}

func (e *backupStatusError) Error() string {
	return fmt.Sprintf("%s (operation %s): %s", e.Status, e.ID, e.Text)
}

// firstLine is a system.backups error without its stack trace.
func firstLine(errText string) string {
	if i := strings.Index(errText, ", Stack trace"); i >= 0 {
		errText = errText[:i]
	}
	if i := strings.IndexByte(errText, '\n'); i >= 0 {
		errText = errText[:i]
	}
	return errText
}
