// Package objstore is the object-store side of the storage-integrity
// backups (housegate spec 2026-10-14 §16 P1, §8.4, §8.5, §8.6; plan S3-C1
// contract §17): the names under an owner's prefix, the BACKUP and RESTORE
// statements, the backup index object, the chained full and incremental
// Backuper, RestoreAndGate (a restore into a scratch table gated against a
// published manifest) and the restore Drill.
//
// Every object-store access is a ClickHouse statement that names the bucket
// with S3(...) or s3(...) (plan S3-C1 clarification K23): this package is the
// only code that renders them, and it holds no Go S3 or GCS client. The key
// pair travels inside those statements, which ClickHouse masks in
// system.query_log, system.processes, system.backups and its server log
// (mechanics M14); an error of such a statement is redacted here before it
// leaves the package, so no error carries the secret either.
package objstore

import (
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"time"
)

// Names and limits of plan S3-C1 contract §17.
const (
	// SafeDatabase is the owner database every backup holds.
	SafeDatabase = "hg_safe"
	// StageDatabase holds every restore scratch of plan S3-C1 and of plan
	// S3-C2 (spec §8.2).
	StageDatabase = "hg_rstage"
	// BackupIndexFormat is the format of the index object beside a backup.
	BackupIndexFormat = "si-backup-index-v1"
	// DrillVerdictFormat is the format of a restore drill's verdict.
	DrillVerdictFormat = "si-restore-drill-v1"
	// IndexSuffix ends the name of every index object.
	IndexSuffix = ".index.json"
	// DefaultMaxChainLinks is the chain length when none is configured: one
	// full backup and six incremental ones, a weekly full backup.
	DefaultMaxChainLinks = 7
	// MaxChainLinksLimit bounds the configured chain length.
	MaxChainLinksLimit = 30
)

// BackupKind is the kind of a backup: full, or incremental on the previous
// backup of its chain.
type BackupKind string

const (
	BackupFull        BackupKind = "full"
	BackupIncremental BackupKind = "incr"
)

// Bucket is one bucket of an S3-compatible endpoint: GCS's XML API
// (https://storage.googleapis.com/<bucket>) or MinIO
// (http://<host>:9000/<bucket>).
type Bucket struct {
	url string
}

var (
	bucketHost = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$`)
	bucketName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{1,220}[a-z0-9]$`)
	backupName = regexp.MustCompile(`^[0-9]{8}T[0-9]{6}Z-(full|incr)$`)
)

// ParseBucket accepts http(s)://<host>[:port]/<bucket> only: no user, no
// further path, query, fragment or trailing slash, and no character that an
// s3() glob or a SQL string literal treats specially.
func ParseBucket(raw string) (Bucket, error) {
	refuse := fmt.Errorf("objstore: bucket %q must be http(s)://<host>[:port]/<bucket> with no further path, query or trailing slash", raw)
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.User != nil || u.Opaque != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" || u.RawPath != "" {
		return Bucket{}, refuse
	}
	if !bucketHost.MatchString(u.Hostname()) {
		return Bucket{}, refuse
	}
	if port := u.Port(); port != "" {
		if n, err := strconv.ParseUint(port, 10, 16); err != nil || n == 0 || strconv.FormatUint(n, 10) != port {
			return Bucket{}, refuse
		}
	} else if u.Host != u.Hostname() {
		return Bucket{}, refuse
	}
	if len(u.Path) < 2 || u.Path[0] != '/' || !bucketName.MatchString(u.Path[1:]) {
		return Bucket{}, refuse
	}
	return Bucket{url: u.Scheme + "://" + u.Host + u.Path}, nil
}

// URL is the bucket's URL as ParseBucket accepted it.
func (b Bucket) URL() string { return b.url }

// OwnerPrefix is the prefix of every object of SI indexer owner: its
// contract-assigned id, which no signer change alters (K26).
func OwnerPrefix(owner uint64) string { return "owner-" + strconv.FormatUint(owner, 10) + "/" }

// BackupURL is the directory of one backup.
func (b Bucket) BackupURL(owner uint64, name string) string {
	return b.url + "/" + OwnerPrefix(owner) + name + "/"
}

// IndexURL is the index object of one backup, beside its directory.
func (b Bucket) IndexURL(owner uint64, name string) string {
	return b.url + "/" + OwnerPrefix(owner) + name + IndexSuffix
}

// IndexGlob matches every index object of owner. An s3() * stays inside one
// path segment, so no object inside a backup directory matches.
func (b Bucket) IndexGlob(owner uint64) string {
	return b.url + "/" + OwnerPrefix(owner) + "*" + IndexSuffix
}

// backupObjectsGlob matches every object below one backup's directory.
func (b Bucket) backupObjectsGlob(owner uint64, name string) string {
	return b.BackupURL(owner, name) + "**"
}

// BackupName is <UTC yyyymmddThhmmssZ>-<kind>, so names sort in time order.
func BackupName(at time.Time, kind BackupKind) string {
	return at.UTC().Format("20060102T150405Z") + "-" + string(kind)
}

// checkBackupName refuses a name BackupName cannot produce, so no name read
// from an index or given by an operator reaches a URL or a glob unchecked.
func checkBackupName(name string) error {
	if !backupName.MatchString(name) {
		return fmt.Errorf("objstore: backup name %q must be <yyyymmddThhmmssZ>-<full|incr>", name)
	}
	return nil
}
