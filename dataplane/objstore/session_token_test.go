package objstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

const testToken = "tok'en\\value-of-the-session"

// tokenMarker is a part of testToken that no redacted text carries.
const tokenMarker = "value-of-the-session"

func tokenCredentials() Credentials {
	return Credentials{AccessKeyID: "r2-access", SecretAccessKey: testSecret, SessionToken: testToken}
}

// The two error texts R1-2 measured on R2 with ClickHouse 26.8.37.168
// (spec 2026-11-16 §13), as system.backups holds them.
const (
	measuredLocked    = "Code: 499. DB::Exception: Unable to parse ExceptionName: ObjectLockedByBucketPolicy Message: The object is locked by the bucket policy. (Code: 100) while removing object with path r1/20261116T140737Z-3319fa/ch268371/p2/l/full/.lock from S3. (S3_ERROR)"
	measuredDenied    = "Code: 499. DB::Exception: Access Denied (Code: 15) while removing object with path r1/20261116T140737Z-3319fa/ch268371/p2/n/full/.lock from S3. (S3_ERROR)"
	measuredLockedKey = "r1/20261116T140737Z-3319fa/ch268371/p2/l/full/.lock"
	measuredDeniedKey = "r1/20261116T140737Z-3319fa/ch268371/p2/n/full/.lock"
)

func TestStatementsWithASessionTokenUseTheFiveArgumentS3AndANamedCollection(t *testing.T) {
	// Given
	b := testBucket(t)
	c := Credentials{AccessKeyID: "k", SecretAccessKey: "s", SessionToken: "t'k"}
	e := Endpoint{bucket: b, creds: c, collection: "si_backup_1794882072_0123456789abcdef"}
	sch := Scratch{Database: StageDatabase, Table: "s1"}

	// When, Then
	for _, x := range []struct{ got, want string }{
		{indexWriteSQL(b, 3, "20261015T031700Z-full", c), `INSERT INTO FUNCTION s3('http://arbiter-core-minio:9000/si-mechanics/owner-3/20261015T031700Z-full.index.json', 'k', 's', 't\'k', 'RawBLOB') SELECT {index:String}`},
		{indexReadSQL(b, 3, c), `SELECT _file, * FROM s3('http://arbiter-core-minio:9000/si-mechanics/owner-3/*.index.json', 'k', 's', 't\'k', 'RawBLOB')`},
		{backupObjectsSQL(b, 3, "20261015T031700Z-full", c), `SELECT count() FROM s3('http://arbiter-core-minio:9000/si-mechanics/owner-3/20261015T031700Z-full/**', 'k', 's', 't\'k', 'One')`},
		{backupDescriptorSQL(b, 3, "20261015T031700Z-full", c), `SELECT countIf(_file = '.backup') FROM s3('http://arbiter-core-minio:9000/si-mechanics/owner-3/20261015T031700Z-full/*', 'k', 's', 't\'k', 'One')`},
		{BackupSQLAt(e, 2, "20261116T031700Z-full", ""), `BACKUP DATABASE hg_safe TO S3(si_backup_1794882072_0123456789abcdef, 'owner-2/20261116T031700Z-full/')`},
		{BackupSQLAt(e, 2, "20261117T031700Z-incr", "20261116T031700Z-full"), `BACKUP DATABASE hg_safe TO S3(si_backup_1794882072_0123456789abcdef, 'owner-2/20261117T031700Z-incr/')` +
			` SETTINGS base_backup = S3(si_backup_1794882072_0123456789abcdef, 'owner-2/20261116T031700Z-full/')`},
		{RestoreSQLAt(e, 2, "20261116T031700Z-full", "", "db__u", "", sch), "RESTORE TABLE hg_safe.`db__u` AS `hg_rstage`.`s1` FROM S3(si_backup_1794882072_0123456789abcdef, 'owner-2/20261116T031700Z-full/')"},
		{RestoreSQLAt(e, 2, "20261118T031700Z-incr", "20261116T031700Z-full", "db__u", "PARTITIONS 'a'", sch), "RESTORE TABLE hg_safe.`db__u` AS `hg_rstage`.`s1` PARTITIONS 'a' FROM S3(si_backup_1794882072_0123456789abcdef, 'owner-2/20261118T031700Z-incr/')" +
			" SETTINGS base_backup = S3(si_backup_1794882072_0123456789abcdef, 'owner-2/20261116T031700Z-full/')"},
		{createCollectionSQL(e.collection, b, c), `CREATE NAMED COLLECTION si_backup_1794882072_0123456789abcdef AS url = 'http://arbiter-core-minio:9000/si-mechanics/', access_key_id = 'k', secret_access_key = 's', session_token = 't\'k'`},
		{dropCollectionSQL(e.collection), `DROP NAMED COLLECTION IF EXISTS si_backup_1794882072_0123456789abcdef`},
	} {
		if x.got != x.want {
			t.Errorf("\n got %s\nwant %s", x.got, x.want)
		}
	}
}

func TestStatementsWithoutASessionTokenAreUnchanged(t *testing.T) {
	// Given
	b := testBucket(t)
	c := Credentials{AccessKeyID: "k", SecretAccessKey: "s"}
	e := staticEndpoint(b, c)
	sch := Scratch{Database: StageDatabase, Table: "s1"}

	// When, Then
	if got, want := BackupSQLAt(e, 1, "20261016T031700Z-incr", "20261015T031700Z-full"), BackupSQL(b, 1, "20261016T031700Z-incr", "20261015T031700Z-full", c); got != want {
		t.Errorf("BackupSQLAt of a static endpoint:\n got %s\nwant %s", got, want)
	}
	if got, want := RestoreSQLAt(e, 1, "20261015T031700Z-full", "20261014T031700Z-full", "db__u", "", sch), RestoreSQL(b, 1, "20261015T031700Z-full", "db__u", "", sch, c); got != want {
		t.Errorf("RestoreSQLAt of a static endpoint:\n got %s\nwant %s", got, want)
	}
	if want := `SELECT countIf(_file = '.backup') FROM s3('http://arbiter-core-minio:9000/si-mechanics/owner-1/20261015T031700Z-full/*', 'k', 's', 'One')`; backupDescriptorSQL(b, 1, "20261015T031700Z-full", c) != want {
		t.Errorf("descriptor read: %s", backupDescriptorSQL(b, 1, "20261015T031700Z-full", c))
	}
	conn := &scriptConn{t: t}
	endpoint, closeEndpoint, err := OpenEndpoint(context.Background(), conn, b, c)
	if err != nil || endpoint.Collection() != "" || closeEndpoint() != nil || len(conn.statements) != 0 {
		t.Fatalf("a static key pair opens no collection: %+v, %v, %q", endpoint, err, conn.statements)
	}
}

func TestCredentialsRedactTheSessionToken(t *testing.T) {
	// Given
	c := tokenCredentials()
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("backup", "credentials", c)
	asJSON, err := json.Marshal(struct{ C Credentials }{c})
	if err != nil {
		t.Fatal(err)
	}

	// When
	printed := []string{c.String(), fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), logged.String(), string(asJSON)}

	// Then
	for _, p := range printed {
		if strings.Contains(p, tokenMarker) || strings.Contains(p, "s3cr3t") {
			t.Errorf("a secret was printed: %s", p)
		}
		if !strings.Contains(p, "r2-access") || strings.Count(p, redacted) != 2 {
			t.Errorf("want the access key id and two %s in %s", redacted, p)
		}
	}
	plain := Credentials{AccessKeyID: "k", SecretAccessKey: "s"}
	if got := plain.String(); got != "objstore.Credentials{AccessKeyID: k, SecretAccessKey: [redacted]}" {
		t.Errorf("a static key pair prints as before: %s", got)
	}
	if got, _ := json.Marshal(plain); string(got) != `{"access_key_id":"k","secret_access_key":"[redacted]"}` {
		t.Errorf("a static key pair marshals as before: %s", got)
	}
	if err := (Credentials{AccessKeyID: "k", SecretAccessKey: "s", SessionToken: "t"}).validate(); err != nil {
		t.Errorf("a token is optional and allowed: %v", err)
	}
}

func TestStatementErrorsNeverCarryTheSessionToken(t *testing.T) {
	// Given
	c := tokenCredentials()
	escaped := `tok\'en\\value-of-the-session`
	errs := []error{
		&clickhouse.Exception{Code: 62, Name: "DB::Exception", Message: "Syntax error near '" + escaped + "'", StackTrace: "at " + testToken},
		errors.New("dial tcp: " + testToken + " " + escaped),
		&backupStatusError{Status: "BACKUP_FAILED", ID: "x", Text: "Code: 499. DB::Exception: " + testToken},
		fmt.Errorf("polling %s: %w", testToken, context.Canceled),
	}

	// When, Then
	for _, in := range errs {
		got := c.statementError("objstore: backup %s", in, "x")
		for e := got; e != nil; e = errors.Unwrap(e) {
			if strings.Contains(e.Error(), tokenMarker) {
				t.Errorf("the token survived: %v", got)
			}
		}
	}
	var st *backupStatusError
	if got := c.redact(errs[2]); !errors.As(got, &st) || st.Status != "BACKUP_FAILED" || st.Text != "Code: 499. DB::Exception: [redacted]" {
		t.Errorf("a failed BACKUP stays a *backupStatusError: %#v", got)
	}
	if got := c.redact(errs[3]); !errors.Is(got, context.Canceled) {
		t.Errorf("cancellation survives redaction: %v", got)
	}
}

func TestLockDeleteRefusedAcceptsOnlyTheBackupsOwnLock(t *testing.T) {
	status := func(text string) error { return &backupStatusError{Status: "BACKUP_FAILED", ID: "id", Text: text} }
	exception := func(code int32, message string) error {
		return fmt.Errorf("objstore: backup x: %w", &clickhouse.Exception{Code: code, Name: "DB::Exception", Message: message})
	}
	lockedMsg := strings.TrimPrefix(measuredLocked, "Code: 499. DB::Exception: ")
	deniedMsg := strings.TrimPrefix(measuredDenied, "Code: 499. DB::Exception: ")
	for _, tc := range []struct {
		name string
		err  error
		key  string
		want bool
	}{
		{"measured R2 bucket lock", status(measuredLocked), measuredLockedKey, true},
		{"measured R2 access denied", status(measuredDenied), measuredDeniedKey, true},
		{"MinIO access denied", status("Code: 499. DB::Exception: Access Denied. (Code: 15) while removing object with path owner-1/20261015T031700Z-full/.lock from S3. (S3_ERROR)"), "owner-1/20261015T031700Z-full/.lock", true},
		{"AccessDenied code name", status("Code: 499. DB::Exception: AccessDenied: denied while removing object with path owner-1/x/.lock from S3. (S3_ERROR)"), "owner-1/x/.lock", true},
		{"server exception", exception(499, lockedMsg), measuredLockedKey, true},
		{"client-wrapped server exception", exception(499, "Received from localhost:9000. DB::Exception: "+deniedMsg), measuredDeniedKey, true},
		{"client display text", status("Received exception from server (version 26.8.37): " + measuredDenied), measuredDeniedKey, true},

		{"another backup's lock", status(measuredLocked), measuredDeniedKey, false},
		{"the lock of another destination", status(measuredLocked), "r1/20261116T140737Z-3319fa/ch268371/p2/l/incr/.lock", false},
		{"a longer key ending in the lock", status(strings.Replace(measuredLocked, "path r1/", "path x/r1/", 1)), measuredLockedKey, false},
		{"another object of the backup", status(strings.Replace(measuredLocked, "full/.lock", "full/.backup", 1)), measuredLockedKey, false},
		{"the lock key as a prefix", status(strings.Replace(measuredLocked, "full/.lock", "full/.lock.tmp", 1)), measuredLockedKey, false},
		{"two removed objects", status(strings.Replace(measuredDenied, " from S3.", " from S3 while removing object with path "+measuredDeniedKey+" from S3.", 1)), measuredDeniedKey, false},
		{"another error code", status(strings.Replace(measuredLocked, "Code: 499.", "Code: 1000.", 1)), measuredLockedKey, false},
		{"another exception code", exception(1000, lockedMsg), measuredLockedKey, false},
		{"a forwarded code that disagrees", exception(499, "Code: 1000. DB::Exception: "+lockedMsg), measuredLockedKey, false},
		{"another cause", status("Code: 499. DB::Exception: Connection reset by peer while removing object with path " + measuredLockedKey + " from S3. (S3_ERROR)"), measuredLockedKey, false},
		{"the cause after the path", status("Code: 499. DB::Exception: Timeout while removing object with path " + measuredLockedKey + " from S3: AccessDenied. (S3_ERROR)"), measuredLockedKey, false},
		{"a failed restore", &backupStatusError{Status: "RESTORE_FAILED", ID: "id", Text: measuredLocked}, measuredLockedKey, false},
		{"no code", status("DB::Exception: Access Denied while removing object with path " + measuredDeniedKey + " from S3."), measuredDeniedKey, false},
		{"plain text", errors.New(measuredLocked), measuredLockedKey, false},
		{"cancellation", fmt.Errorf("%w: %v", context.Canceled, status(measuredLocked)), measuredLockedKey, false},
		{"nil", nil, measuredLockedKey, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := lockDeleteRefused(tc.err, tc.key); got != tc.want {
				t.Errorf("lockDeleteRefused = %v, want %v", got, tc.want)
			}
		})
	}
	if got := lockKey(2, "20261116T031700Z-full"); got != "owner-2/20261116T031700Z-full/.lock" {
		t.Errorf("lockKey = %s", got)
	}
}

func TestNextDifferentialBackupAlwaysBasesOnTheChainsFull(t *testing.T) {
	// Given: a four-link chain, run nine times.
	var indexes []BackupIndex
	at := s3T0

	// When
	for i := 0; i < 9; i++ {
		var latest *BackupIndex
		if len(indexes) > 0 {
			latest = &indexes[len(indexes)-1]
		}
		kind, base, chain := NextDifferentialBackup(latest, indexes, 4)
		indexes = append(indexes, BackupIndex{Name: BackupName(at, kind), Kind: kind, Base: base, ChainLength: chain})
		at = at.Add(time.Hour)
	}

	// Then: full, three differentials on it, full, three on it, full.
	kinds := ""
	for i, idx := range indexes {
		kinds += string(idx.Kind) + " "
		if idx.Kind == BackupFull {
			if idx.Base != "" || idx.ChainLength != 1 {
				t.Errorf("full %d: %+v", i, idx)
			}
			continue
		}
		var base *BackupIndex
		for j := range indexes[:i] {
			if indexes[j].Name == idx.Base {
				base = &indexes[j]
			}
		}
		if base == nil || base.Kind != BackupFull {
			t.Errorf("backup %d is based on %q, which is not a full backup", i, idx.Base)
		}
	}
	if kinds != "full incr incr incr full incr incr incr full " {
		t.Errorf("kinds: %s", kinds)
	}
	if got := indexes[3].ChainLength; got != 4 {
		t.Errorf("chain length of the third differential: %d", got)
	}
	// A static key pair's incremental chain continued with a token: its
	// latest Base is an incremental backup, so a new full starts.
	static := []BackupIndex{{Name: "a", Kind: BackupFull, ChainLength: 1}, {Name: "b", Kind: BackupIncremental, Base: "a", ChainLength: 2}, {Name: "c", Kind: BackupIncremental, Base: "b", ChainLength: 3}}
	if kind, base, chain := NextDifferentialBackup(&static[2], static, 7); kind != BackupFull || base != "" || chain != 1 {
		t.Errorf("after a static chain: %s %q %d", kind, base, chain)
	}
	if kind, base, chain := NextDifferentialBackup(&static[1], static, 7); kind != BackupIncremental || base != "a" || chain != 3 {
		t.Errorf("after one static link on a full: %s %q %d", kind, base, chain)
	}
	if kind, _, _ := NextDifferentialBackup(&BackupIndex{Name: "x", Kind: BackupIncremental, Base: "gone", ChainLength: 2}, nil, 7); kind != BackupFull {
		t.Errorf("a Base that names no index starts a full backup, got %s", kind)
	}
}

func TestBackuperWithASessionTokenBasesOnTheFull(t *testing.T) {
	// Given: a full and one differential on it; the next run's BACKUP.
	at := s3T0.Add(48 * time.Hour)
	full := BackupIndex{Format: BackupIndexFormat, NetworkID: "current", OwnerIndexerID: 1, Name: BackupName(s3T0, BackupFull), Kind: BackupFull, ChainLength: 1}
	diff := full
	diff.Name, diff.Kind, diff.Base, diff.ChainLength = BackupName(s3T0.Add(time.Hour), BackupIncremental), BackupIncremental, full.Name, 2
	for _, tc := range []struct {
		name  string
		creds Credentials
		want  string
	}{
		{"session token", tokenCredentials(), "'owner-1/" + full.Name + "/')"},
		{"static key pair", Credentials{AccessKeyID: "k", SecretAccessKey: "s"}, "/owner-1/" + diff.Name + "/', 'k', 's')"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := &scriptConn{t: t, indexes: []BackupIndex{full, diff}}
			b := Backuper{
				Conn: conn, Manifests: newFakeManifests(testManifest(t, "")), Bucket: testBucket(t), Owner: 1, NetworkID: "current",
				Credentials: tc.creds, Now: func() time.Time { return at },
			}

			// When
			idx, err := b.Run(context.Background())

			// Then
			if err != nil {
				t.Fatal(err)
			}
			var backup string
			for _, s := range conn.statements {
				if strings.HasPrefix(s, "BACKUP ") {
					backup = s
				}
			}
			if !strings.Contains(backup, " SETTINGS base_backup = ") {
				t.Fatalf("no base: %q", backup)
			}
			settings := backup[strings.Index(backup, "SETTINGS base_backup = "):]
			if !strings.Contains(settings, tc.want) {
				t.Errorf("BACKUP %s\nwant its base_backup to end in %s", backup, tc.want)
			}
			if idx.Kind != BackupIncremental || idx.ChainLength != 3 {
				t.Errorf("index %+v", idx)
			}
			if tc.creds.SessionToken != "" && idx.Base != full.Name {
				t.Errorf("a differential's Base names its full: %q", idx.Base)
			}
		})
	}
}

func TestCollectionNamesCarryTheirCreationTime(t *testing.T) {
	// Given
	at := time.Unix(1794882072, 0)

	// When
	name, err := newCollectionName(at)
	again, _ := newCollectionName(at)

	// Then
	if err != nil || !collectionName.MatchString(name) || !strings.HasPrefix(name, "si_backup_1794882072_") || name == again {
		t.Fatalf("names %q, %q, %v", name, again, err)
	}
	if created, ok := collectionCreated(name); !ok || !created.Equal(at) {
		t.Errorf("created %v, %v", created, ok)
	}
	for _, other := range []string{"si_backup_x", "si_backup_1794882072_0123", "si_backup_1794882072_0123456789ABCDEF", "si_backup_99999999999999999999_0123456789abcdef"} {
		if _, ok := collectionCreated(other); ok {
			t.Errorf("%s is not a name OpenEndpoint makes", other)
		}
	}
	defer func(old func([]byte) (int, error)) { randRead = old }(randRead)
	randRead = func([]byte) (int, error) { return 0, errors.New("no entropy") }
	if _, err := newCollectionName(at); err == nil {
		t.Error("a nonce failure must refuse")
	}
}

func TestOpenEndpointDropsItsCollectionEvenAfterCancellation(t *testing.T) {
	// Given
	b := testBucket(t)
	c := tokenCredentials()
	ctx, cancel := context.WithCancel(context.Background())
	conn := &scriptConn{t: t}

	// When
	e, closeEndpoint, err := openEndpoint(ctx, conn, b, c, time.Unix(1794882072, 0))
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	dropErr := closeEndpoint()

	// Then
	if dropErr != nil || len(conn.statements) != 2 || !strings.HasPrefix(conn.statements[0], "CREATE NAMED COLLECTION "+e.Collection()+" AS ") ||
		conn.statements[1] != "DROP NAMED COLLECTION IF EXISTS "+e.Collection() {
		t.Fatalf("statements %q, drop %v", conn.statements, dropErr)
	}
	if conn.ctxErrs[1] != nil {
		t.Errorf("the drop ran on the cancelled context: %v", conn.ctxErrs[1])
	}
}

func TestOpenEndpointDropsAfterAFailedCreateAndRedacts(t *testing.T) {
	// Given
	c := tokenCredentials()
	conn := &scriptConn{t: t, execErr: map[string]error{
		"CREATE NAMED COLLECTION ": &clickhouse.Exception{Code: 1, Name: "DB::Exception", Message: "bad " + testToken + " " + testSecret},
	}}

	// When
	_, _, err := OpenEndpoint(context.Background(), conn, testBucket(t), c)

	// Then
	if err == nil || strings.Contains(err.Error(), tokenMarker) || strings.Contains(err.Error(), "s3cr3t") {
		t.Fatalf("err %v", err)
	}
	if len(conn.statements) != 2 || !strings.HasPrefix(conn.statements[1], "DROP NAMED COLLECTION IF EXISTS si_backup_") {
		t.Fatalf("statements %q", conn.statements)
	}
}

func TestDropStaleCollectionsDropsOnlyOldNamesItMade(t *testing.T) {
	// Given
	now := time.Unix(1794882072, 0)
	old := fmt.Sprintf("si_backup_%d_0123456789abcdef", now.Add(-13*time.Hour).Unix())
	fresh := fmt.Sprintf("si_backup_%d_fedcba9876543210", now.Add(-time.Hour).Unix())
	conn := &scriptConn{t: t, names: []string{old, fresh, "si_backup_manual"}}

	// When
	dropped, err := dropStaleCollections(context.Background(), conn, 12*time.Hour, now)

	// Then
	if err != nil || len(dropped) != 1 || dropped[0] != old {
		t.Fatalf("dropped %v, %v", dropped, err)
	}
	if len(conn.statements) != 2 || conn.statements[1] != "DROP NAMED COLLECTION IF EXISTS "+old {
		t.Fatalf("statements %q", conn.statements)
	}
	if _, err := dropStaleCollections(context.Background(), conn, 0, now); err == nil {
		t.Error("a zero age must refuse")
	}
}

// TestBackuperRunWithASessionToken drives Run to its BACKUP with a fake
// ClickHouse and checks the named collection, the refused-.lock acceptance
// and the logs.
func TestBackuperRunWithASessionToken(t *testing.T) {
	at := s3T0.Add(48 * time.Hour)
	name := BackupName(at, BackupFull)
	ownLock := "Code: 499. DB::Exception: Unable to parse ExceptionName: ObjectLockedByBucketPolicy Message: The object is locked by the bucket policy. (Code: 100) while removing object with path owner-1/" + name + "/.lock from S3. (S3_ERROR)"
	for _, tc := range []struct {
		name        string
		backupError string
		descriptors uint64
		cancel      bool
		wantErr     string
		wantIndex   bool
		wantRead    bool
	}{
		{name: "created", wantIndex: true},
		{name: "own lock refused, backup written", backupError: strings.Replace(ownLock, "(Code: 100)", "(Code: 100) "+testToken, 1) + ", Stack trace: x", descriptors: 1, wantIndex: true, wantRead: true},
		{name: "own lock refused, no backup", backupError: ownLock, descriptors: 0, wantErr: "its .backup does not exist", wantRead: true},
		{name: "another backup's lock", backupError: strings.Replace(ownLock, name, "20261015T031700Z-full", 1), descriptors: 1, wantErr: "BACKUP_FAILED"},
		{name: "another failure", backupError: "Code: 243. DB::Exception: Not enough space. (NOT_ENOUGH_SPACE)", descriptors: 1, wantErr: "NOT_ENOUGH_SPACE"},
		{name: "cancelled while polling", cancel: true, wantErr: "context canceled"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Given
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			conn := &scriptConn{t: t, backupError: tc.backupError, descriptors: tc.descriptors}
			if tc.cancel {
				conn.onPoll = cancel
			}
			var logged bytes.Buffer
			b := Backuper{
				Conn: conn, Manifests: newFakeManifests(testManifest(t, "")), Bucket: testBucket(t), Owner: 1, NetworkID: "current",
				Credentials: tokenCredentials(), Now: func() time.Time { return at },
				Logger: slog.New(slog.NewTextHandler(&logged, nil)),
			}

			// When
			idx, err := b.Run(ctx)

			// Then
			if tc.wantErr == "" {
				if err != nil || idx.Name != name {
					t.Fatalf("Run: %+v, %v", idx, err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Run error %v, want %q", err, tc.wantErr)
			}
			var collection string
			var created, dropped, backups, reads, indexes int
			for i, s := range conn.statements {
				switch {
				case strings.HasPrefix(s, "CREATE NAMED COLLECTION si_backup_"):
					created++
					collection = strings.Fields(s)[3]
				case s == "DROP NAMED COLLECTION IF EXISTS "+collection:
					dropped++
					if conn.ctxErrs[i] != nil {
						t.Errorf("the drop ran on an ended context")
					}
				case strings.HasPrefix(s, "BACKUP DATABASE hg_safe TO S3("):
					backups++
					if want := "BACKUP DATABASE hg_safe TO S3(" + collection + ", 'owner-1/" + name + "/') ASYNC"; s != want {
						t.Errorf("BACKUP\n got %s\nwant %s", s, want)
					}
				case strings.HasPrefix(s, "SELECT countIf(_file = '.backup') FROM s3("):
					reads++
					if !strings.Contains(s, ", 'r2-access', '"+sqlEscaped(testSecret)+"', '"+sqlEscaped(testToken)+"', 'One')") {
						t.Errorf("the .backup is read through the run's credentials: %s", s)
					}
				case strings.HasPrefix(s, "INSERT INTO FUNCTION s3("):
					indexes++
				}
			}
			if created != 1 || dropped != 1 || backups != 1 {
				t.Errorf("created %d, dropped %d, backups %d: %q", created, dropped, backups, conn.statements)
			}
			if (reads == 1) != tc.wantRead || (indexes == 1) != tc.wantIndex {
				t.Errorf(".backup reads %d, index writes %d: %q", reads, indexes, conn.statements)
			}
			warned := strings.Contains(logged.String(), "accepting the backup")
			if warned != (tc.name == "own lock refused, backup written") {
				t.Errorf("warning logged %v: %s", warned, logged.String())
			}
			for _, text := range []string{logged.String(), fmt.Sprint(err)} {
				if strings.Contains(text, tokenMarker) || strings.Contains(text, "s3cr3t") {
					t.Errorf("a secret reached a log or an error: %s", text)
				}
			}
		})
	}
}

// sqlEscaped is the SQL-escaped form literal() writes.
func sqlEscaped(s string) string { return strings.TrimSuffix(strings.TrimPrefix(literal(s), "'"), "'") }

// scriptConn is a fake ClickHouse for the statements Run, OpenEndpoint and
// DropStaleCollections send. It records every Exec and QueryRow statement
// and the state of its context.
type scriptConn struct {
	driver.Conn
	t           *testing.T
	statements  []string
	ctxErrs     []error
	execErr     map[string]error
	names       []string
	backupError string
	descriptors uint64
	onPoll      func()
	indexes     []BackupIndex
}

func (c *scriptConn) record(ctx context.Context, query string) {
	c.statements = append(c.statements, query)
	c.ctxErrs = append(c.ctxErrs, ctx.Err())
}

func (c *scriptConn) Exec(ctx context.Context, query string, _ ...any) error {
	c.record(ctx, query)
	for prefix, err := range c.execErr {
		if strings.HasPrefix(query, prefix) {
			return err
		}
	}
	switch {
	case strings.HasPrefix(query, "CREATE NAMED COLLECTION "), strings.HasPrefix(query, "DROP NAMED COLLECTION IF EXISTS "),
		strings.HasPrefix(query, "INSERT INTO FUNCTION s3("):
		return nil
	}
	return fmt.Errorf("unexpected exec %s", query)
}

func (c *scriptConn) Query(ctx context.Context, query string, _ ...any) (driver.Rows, error) {
	c.record(ctx, query)
	switch {
	case strings.HasPrefix(query, "SELECT _file, * FROM s3("):
		return &indexRows{indexes: c.indexes}, nil
	case strings.HasPrefix(query, "SELECT name, comment, partition_key FROM system.tables"):
		return &stringRows{}, nil
	case strings.HasPrefix(query, "SELECT name FROM system.named_collections"):
		return &stringRows{values: c.names}, nil
	}
	return nil, fmt.Errorf("unexpected query %s", query)
}

func (c *scriptConn) QueryRow(ctx context.Context, query string, _ ...any) driver.Row {
	c.record(ctx, query)
	switch {
	case strings.HasPrefix(query, "SELECT count() FROM s3("):
		return valueRow{values: []any{uint64(0)}}
	case strings.HasPrefix(query, "BACKUP DATABASE "):
		return valueRow{values: []any{"backup-id", "CREATING_BACKUP"}}
	case strings.HasPrefix(query, "SELECT toString(status), error FROM system.backups"):
		if c.onPoll != nil {
			c.onPoll()
			return valueRow{values: []any{"CREATING_BACKUP", ""}}
		}
		if c.backupError == "" {
			return valueRow{values: []any{"BACKUP_CREATED", ""}}
		}
		return valueRow{values: []any{"BACKUP_FAILED", c.backupError}}
	case strings.HasPrefix(query, "SELECT countIf(_file = '.backup') FROM s3("):
		return valueRow{values: []any{c.descriptors}}
	}
	return errorRow{err: fmt.Errorf("unexpected query row %s", query)}
}

type valueRow struct {
	driver.Row
	values []any
}

func (r valueRow) Scan(dest ...any) error {
	for i, v := range r.values {
		switch d := dest[i].(type) {
		case *string:
			*d = v.(string)
		case *uint64:
			*d = v.(uint64)
		default:
			return fmt.Errorf("unexpected scan type %T", dest[i])
		}
	}
	return nil
}

type stringRows struct {
	driver.Rows
	values []string
	n      int
}

func (r *stringRows) Next() bool { return r.n < len(r.values) }
func (r *stringRows) Scan(dest ...any) error {
	*dest[0].(*string) = r.values[r.n]
	r.n++
	return nil
}
func (r *stringRows) Close() error { return nil }
func (r *stringRows) Err() error   { return nil }
