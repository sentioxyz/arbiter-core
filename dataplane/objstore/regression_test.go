package objstore

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// indexRestoreConn supplies real index JSON at the ClickHouse boundary and
// injects a restore transport failure. Unexpected SQL fails the test path.
type indexRestoreConn struct {
	driver.Conn
	indexes    []BackupIndex
	restoreErr error
}

func (c indexRestoreConn) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	if !strings.HasPrefix(query, "SELECT _file, * FROM s3(") {
		return nil, errors.New("unexpected query after index read")
	}
	return &indexRows{indexes: c.indexes}, nil
}

func (c indexRestoreConn) QueryRow(_ context.Context, query string, _ ...any) driver.Row {
	if strings.HasPrefix(query, "RESTORE TABLE ") {
		return errorRow{err: c.restoreErr}
	}
	return errorRow{err: errors.New("unexpected query after index read")}
}

func (c indexRestoreConn) Exec(_ context.Context, query string, _ ...any) error {
	if strings.HasPrefix(query, "CREATE DATABASE IF NOT EXISTS ") {
		return nil
	}
	return errors.New("unexpected exec after index read")
}

type errorRow struct {
	driver.Row
	err error
}

func (r errorRow) Scan(...any) error { return r.err }

type indexRows struct {
	driver.Rows
	indexes []BackupIndex
	n       int
}

func (r *indexRows) Next() bool { return r.n < len(r.indexes) }
func (r *indexRows) Scan(dest ...any) error {
	idx := r.indexes[r.n]
	r.n++
	blob, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	*dest[0].(*string), *dest[1].(*string) = idx.Name+IndexSuffix, string(blob)
	return nil
}
func (r *indexRows) Close() error { return nil }
func (r *indexRows) Err() error   { return nil }

func TestBackuperRefusesIndexesFromAnotherNetwork(t *testing.T) {
	foreign := BackupIndex{Format: BackupIndexFormat, NetworkID: "foreign", OwnerIndexerID: 1, Name: "20261015T031700Z-full", Kind: BackupFull, ChainLength: 1}
	current := foreign
	current.NetworkID, current.Name = "current", "20261016T031700Z-incr"
	for _, tc := range []struct {
		name    string
		indexes []BackupIndex
	}{
		{"foreign predecessor", []BackupIndex{foreign}},
		{"older foreign index before current predecessor", []BackupIndex{foreign, current}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := Backuper{
				Conn: indexRestoreConn{indexes: tc.indexes}, Manifests: newFakeManifests(testManifest(t, "")),
				Bucket: testBucket(t), Owner: 1, NetworkID: "current",
				Credentials: Credentials{AccessKeyID: "test", SecretAccessKey: testSecret},
				Now:         func() time.Time { return s3T0.Add(48 * time.Hour) },
			}
			_, err := b.Run(context.Background())
			want := `objstore: backup 20261015T031700Z-full of indexer 1 belongs to network "foreign", not "current"`
			if err == nil || err.Error() != want {
				t.Fatalf("foreign index must stop the chain before further SQL: got %v, want %s", err, want)
			}
		})
	}
}

func TestDrillPropagatesRestoreCancellation(t *testing.T) {
	schema := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}}}
	m := testManifest(t, "")
	m.Tables[0].SchemaHash = payloadexec.TableSchemaHash("current", schema)
	m.SnapshotID = ""
	m, err := m.Seal()
	if err != nil {
		t.Fatal(err)
	}
	idx := BackupIndex{
		Format: BackupIndexFormat, NetworkID: "current", OwnerIndexerID: 1,
		Name: "20261015T031700Z-full", SnapshotID: m.SnapshotID, Entry: m.TrackFrontiers[1],
		Tables: []BackupIndexTable{{PhysicalTable: "db__t"}},
	}
	for _, tc := range []struct {
		name  string
		cause error
	}{
		{"canceled", context.Canceled},
		{"deadline", context.DeadlineExceeded},
		{"ordinary restore failure", errors.New("restore unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			namer, err := NewScratchNamer("cancellation_")
			if err != nil {
				t.Fatal(err)
			}
			d := Drill{
				Conn:      indexRestoreConn{indexes: []BackupIndex{idx}, restoreErr: fmt.Errorf("backend %s: %w", testSecret, tc.cause)},
				Manifests: newFakeManifests(m), Schemas: fakeSchemas{schema.TableID: schema},
				Bucket: testBucket(t), Owner: 1, NetworkID: "current", Scratch: namer,
				Credentials: Credentials{AccessKeyID: "test", SecretAccessKey: testSecret},
			}
			verdict, err := d.Run(context.Background())
			if tc.name == "ordinary restore failure" {
				if err != nil || verdict.Pass || len(verdict.Tables) != 1 || verdict.Tables[0].Reason != DrillRestoreFailed {
					t.Fatalf("ordinary restore failure must remain a failed verdict: %+v, %v", verdict, err)
				}
				return
			}
			if !errors.Is(err, tc.cause) {
				t.Fatalf("cancellation must remain an error through redaction and drill wrapping: got %v, want %v", err, tc.cause)
			}
			if strings.Contains(err.Error(), testSecret) {
				t.Fatal("cancellation error retained the secret")
			}
			for cause := err; cause != nil; cause = errors.Unwrap(cause) {
				if strings.Contains(cause.Error(), testSecret) {
					t.Fatal("cancellation unwrap chain retained the secret")
				}
			}
		})
	}
}
