package objstore

import (
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestBucketURLsAndBackupNames(t *testing.T) {
	// Given
	b, err := ParseBucket("https://storage.googleapis.com/si-backups")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	at := time.Date(2026, 10, 15, 3, 17, 4, 999, time.FixedZone("UTC+2", 2*3600))

	// When
	full := BackupName(at, BackupFull)
	incr := BackupName(at.Add(24*time.Hour), BackupIncremental)

	// Then
	for _, c := range []struct{ got, want string }{
		{b.URL(), "https://storage.googleapis.com/si-backups"},
		{OwnerPrefix(0), "owner-0/"},
		{OwnerPrefix(7), "owner-7/"},
		{full, "20261015T011704Z-full"},
		{incr, "20261016T011704Z-incr"},
		{b.BackupURL(7, full), "https://storage.googleapis.com/si-backups/owner-7/20261015T011704Z-full/"},
		{b.IndexURL(7, full), "https://storage.googleapis.com/si-backups/owner-7/20261015T011704Z-full.index.json"},
		{b.IndexGlob(7), "https://storage.googleapis.com/si-backups/owner-7/*.index.json"},
		{b.backupObjectsGlob(7, full), "https://storage.googleapis.com/si-backups/owner-7/20261015T011704Z-full/**"},
	} {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}
	if full >= incr {
		t.Errorf("names must sort in time order: %s >= %s", full, incr)
	}
	minio, err := ParseBucket("http://arbiter-core-minio:9000/si-mechanics")
	if err != nil || minio.URL() != "http://arbiter-core-minio:9000/si-mechanics" {
		t.Errorf("the mechanics bucket: %q, %v", minio.URL(), err)
	}
	for _, name := range []string{full, incr} {
		if err := checkBackupName(name); err != nil {
			t.Errorf("checkBackupName(%s): %v", name, err)
		}
	}
	for _, name := range []string{"", "20261015T011704Z-diff", "20261015T011704Z-full/x", "2026101T011704Z-full", "*-full"} {
		if err := checkBackupName(name); err == nil || !strings.Contains(err.Error(), "must be <yyyymmddThhmmssZ>-<full|incr>") {
			t.Errorf("checkBackupName(%q) = %v, want a refusal", name, err)
		}
	}
}

func TestParseBucketRefusesAPathQueryOrTrailingSlash(t *testing.T) {
	for _, raw := range []string{
		"",
		"storage.googleapis.com/si-backups",
		"ftp://storage.googleapis.com/si-backups",
		"https://storage.googleapis.com",
		"https://storage.googleapis.com/",
		"https://storage.googleapis.com/si-backups/",
		"https://storage.googleapis.com/si-backups/owner-0",
		"https://storage.googleapis.com/si-backups?x=1",
		"https://storage.googleapis.com/si-backups?",
		"https://storage.googleapis.com/si-backups#f",
		"https://user:pass@storage.googleapis.com/si-backups",
		"https://storage.googleapis.com:0/si-backups",
		"https://storage.googleapis.com:x/si-backups",
		"https://storage.googleapis.com:/si-backups",
		"https://storage.googleapis.com/SI-Backups",
		"https://storage.googleapis.com/si*backups",
		"https://storage.googleapis.com/si%2Fbackups",
		"https://storage'.googleapis.com/si-backups",
	} {
		_, err := ParseBucket(raw)
		want := fmt.Sprintf("objstore: bucket %q must be http(s)://<host>[:port]/<bucket> with no further path, query or trailing slash", raw)
		if err == nil || err.Error() != want {
			t.Errorf("ParseBucket(%q) = %v, want %q", raw, err, want)
		}
	}
}
