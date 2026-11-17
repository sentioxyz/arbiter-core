package objstore

import (
	"context"
	"errors"
	"regexp"
	"strconv"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// s3ErrorCode is ClickHouse's S3_ERROR.
const s3ErrorCode = 499

var (
	// serverErrorCode finds the code of a server error in its text: the
	// system.backups form "Code: 499. DB::Exception: ..." and the client's
	// "Received exception from server ...: Code: 499. DB::Exception: ...".
	serverErrorCode = regexp.MustCompile(`(?:^|\s)Code: ([0-9]+)\. DB::Exception: `)
	// removingObject is the S3 client's context of a failed DeleteObject.
	removingObject = regexp.MustCompile(`while removing object with path (\S+) from S3`)
)

// lockKey is the object key of backup name's .lock, as the S3 client names
// it: relative to the bucket.
func lockKey(owner uint64, name string) string {
	return OwnerPrefix(owner) + name + "/.lock"
}

// lockDeleteRefused reports whether err, a failed BACKUP, is exactly the
// refused delete of the backup's own .lock that a bucket lock (R2
// ObjectLockedByBucketPolicy) or a key without DeleteObject (AccessDenied)
// causes after ClickHouse has written the backup's .backup (D3 of the
// si-backup R2 design): a BACKUP_FAILED row or a server exception with code
// S3_ERROR whose text names one removed object, key, and before it the
// refusal. Anything else, including a refused delete of another object, is
// not.
func lockDeleteRefused(err error, key string) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var code int64
	var text string
	var st *backupStatusError
	var ex *clickhouse.Exception
	switch {
	case errors.As(err, &st):
		if st.Status != "BACKUP_FAILED" {
			return false
		}
		m := serverErrorCode.FindStringSubmatchIndex(st.Text)
		if m == nil {
			return false
		}
		n, perr := strconv.ParseInt(st.Text[m[2]:m[3]], 10, 32)
		if perr != nil {
			return false
		}
		code, text = n, st.Text[m[1]:]
	case errors.As(err, &ex):
		code, text = int64(ex.Code), ex.Message
		// The message of a forwarded exception carries its own code.
		if m := serverErrorCode.FindStringSubmatchIndex(text); m != nil {
			if n, perr := strconv.ParseInt(text[m[2]:m[3]], 10, 32); perr != nil || n != code {
				return false
			}
			text = text[m[1]:]
		}
	default:
		return false
	}
	if code != s3ErrorCode {
		return false
	}
	paths := removingObject.FindAllStringSubmatchIndex(text, -1)
	if len(paths) != 1 || text[paths[0][2]:paths[0][3]] != key {
		return false
	}
	cause := text[:paths[0][0]]
	return strings.Contains(cause, "ObjectLockedByBucketPolicy") ||
		strings.Contains(cause, "Access Denied") || strings.Contains(cause, "AccessDenied")
}

// backupWritten reports whether backup name's .backup exists, read through
// c: ClickHouse writes it before it removes the .lock, so with it the backup
// is complete.
func backupWritten(ctx context.Context, conn clickhouse.Conn, b Bucket, owner uint64, name string, c Credentials) (bool, error) {
	var n uint64
	if err := conn.QueryRow(clickhouse.Context(ctx, clickhouse.WithSettings(listSettings)), backupDescriptorSQL(b, owner, name, c)).Scan(&n); err != nil {
		return false, c.statementError("objstore: read the .backup of backup %s of indexer %d", err, name, owner)
	}
	return n == 1, nil
}
