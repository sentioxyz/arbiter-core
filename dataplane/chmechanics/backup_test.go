package chmechanics

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// backupStats is a BACKUP or RESTORE result with its system.backups sizes.
type backupStats struct {
	ID               string
	Status           string
	NumFiles         uint64
	NumEntries       uint64
	TotalSize        uint64
	UncompressedSize uint64
	CompressedSize   uint64
}

// runBackup runs one synchronous BACKUP or RESTORE statement, requires the
// status it answers, and returns its system.backups row.
func runBackup(t *testing.T, conn clickhouse.Conn, statement, wantStatus string) backupStats {
	t.Helper()
	ctx := context.Background()
	var s backupStats
	if err := conn.QueryRow(ctx, statement).Scan(&s.ID, &s.Status); err != nil {
		t.Fatalf("%s: %v", statement, err)
	}
	if s.Status != wantStatus {
		t.Fatalf("%s: status %s, want %s", statement, s.Status, wantStatus)
	}
	if err := conn.QueryRow(ctx,
		"SELECT num_files, num_entries, total_size, uncompressed_size, compressed_size FROM system.backups WHERE id = ?", s.ID).
		Scan(&s.NumFiles, &s.NumEntries, &s.TotalSize, &s.UncompressedSize, &s.CompressedSize); err != nil {
		t.Fatalf("system.backups row %s: %v", s.ID, err)
	}
	return s
}

// TestMechanics_BackupRestorePartitionsKeepsHashOfAllFiles (M5) pins the
// object-store transport of P1 and the restore source of spec §8.6: BACKUP
// ... PARTITIONS with base_backup stores only what its base lacks (fewer
// bytes, and fewer stored entries than the files it describes), and
// RESTORE ... AS <staging> PARTITIONS from it restores every part of the
// partition, the base's included, with its hash_of_all_files and rows, and no
// other partition.
func TestMechanics_BackupRestorePartitionsKeepsHashOfAllFiles(t *testing.T) {
	env := requireMechanics(t)
	p := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, _ := protocolTables(t, env.A, env.AAddr, p, sch)
	source := qualified(p.SafeDB, safe.Table)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 0, 100_000, nil)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p1", 0, 10, nil)
	name := fmt.Sprintf("mech_%s_%d", suffix(t), time.Now().UnixNano())
	fullDisk := fmt.Sprintf("Disk('backups', %s)", literal(name+"_full"))
	incDisk := fmt.Sprintf("Disk('backups', %s)", literal(name+"_inc"))
	full := runBackup(t, env.A, fmt.Sprintf("BACKUP TABLE %s PARTITIONS 'p0' TO %s", source, fullDisk), "BACKUP_CREATED")
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 100_000, 10, nil)
	inc := runBackup(t, env.A, fmt.Sprintf("BACKUP TABLE %s PARTITIONS 'p0' TO %s SETTINGS base_backup = %s", source, incDisk, fullDisk), "BACKUP_CREATED")
	t.Logf("full backup %+v; incremental backup %+v", full, inc)
	if inc.UncompressedSize >= full.UncompressedSize {
		t.Fatalf("the incremental backup stores %d bytes, not less than the full backup's %d: base_backup was not used", inc.UncompressedSize, full.UncompressedSize)
	}
	// It describes every file of the partition but stores only the new ones.
	if inc.NumEntries >= inc.NumFiles {
		t.Fatalf("the incremental backup stores %d of the %d files it describes: base_backup was not used", inc.NumEntries, inc.NumFiles)
	}
	stage := "hg_rstage_m" + suffix(t)
	dropDatabasesAt(t, env.AAddr, stage)
	mustExec(t, env.A, "CREATE DATABASE IF NOT EXISTS "+ident(stage))
	runBackup(t, env.A, fmt.Sprintf("RESTORE TABLE %s AS %s PARTITIONS 'p0' FROM %s", source, qualified(stage, safe.Table), incDisk), "RESTORED")
	want := activeParts(t, env.A, p.SafeDB, safe.Table, "p0")
	got := activeParts(t, env.A, stage, safe.Table, "p0")
	if len(want) != 2 {
		t.Fatalf("source p0 holds %d parts, want 2: %v", len(want), names(want))
	}
	if gotHashes, wantHashes := physHashes(got), physHashes(want); !slices.Equal(gotHashes, wantHashes) {
		t.Fatalf("RESTORE from the incremental backup: hash_of_all_files %v, source %v", gotHashes, wantHashes)
	}
	if rows := totalRows(got); rows != 100_010 {
		t.Fatalf("the staging p0 holds %d rows, want 100010", rows)
	}
	if !slices.Equal(rowHashList(t, env.A, stage, safe.Table, sch, got), rowHashList(t, env.A, p.SafeDB, safe.Table, sch, want)) {
		t.Fatal("the restored rows differ from the source's")
	}
	if other := activeParts(t, env.A, stage, safe.Table, "p1"); len(other) != 0 {
		t.Fatalf("RESTORE ... PARTITIONS 'p0' restored partition p1: %v", names(other))
	}
}
