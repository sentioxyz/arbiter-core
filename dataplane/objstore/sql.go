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

// literal single-quotes s as a ClickHouse string literal. Only URLs and the
// two key strings are interpolated this way; every other value is a query
// parameter and every identifier is backquoted.
func literal(s string) string { return "'" + chparts.EscapeSQLString(s) + "'" }

// ident backquotes a ClickHouse identifier.
func ident(name string) string { return "`" + strings.ReplaceAll(name, "`", "``") + "`" }

// s3Endpoint is a BACKUP or RESTORE destination.
func s3Endpoint(url string, c Credentials) string {
	return "S3(" + literal(url) + ", " + literal(c.AccessKeyID) + ", " + literal(c.SecretAccessKey) + ")"
}

// s3Function is the s3() table function over url in format.
func s3Function(url string, c Credentials, format string) string {
	return "s3(" + literal(url) + ", " + literal(c.AccessKeyID) + ", " + literal(c.SecretAccessKey) + ", " + literal(format) + ")"
}

// BackupSQL is the BACKUP of owner's hg_safe into backup name, incremental on
// base when base is set.
func BackupSQL(b Bucket, owner uint64, name, base string, c Credentials) string {
	sql := "BACKUP DATABASE " + SafeDatabase + " TO " + s3Endpoint(b.BackupURL(owner, name), c)
	if base != "" {
		sql += " SETTINGS base_backup = " + s3Endpoint(b.BackupURL(owner, base), c)
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
// the whole table).
func RestoreSQL(b Bucket, owner uint64, name, physicalTable, partitionClause string, s Scratch, c Credentials) string {
	sql := "RESTORE TABLE " + SafeDatabase + "." + ident(physicalTable) + " AS " + s.Qualified()
	if partitionClause != "" {
		sql += " " + partitionClause
	}
	return sql + " FROM " + s3Endpoint(b.BackupURL(owner, name), c)
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
// or ends otherwise. The statement carries c, so every error is redacted.
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
			return c.redact(fmt.Errorf("%s (operation %s): %s", status, id, firstLine(errText)))
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
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
