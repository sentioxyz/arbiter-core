package chmechanics

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// partHashes is every active part of db.table as (row LtHash, hash_of_all_files),
// sorted: the identity the restore gate and the arbiter compare, whatever the
// part names. Row LtHashes come from chexec.ScanAllParts, the rebuild's scan.
func partHashes(t *testing.T, conn clickhouse.Conn, db, table string, sch payloadexec.TableSchema) []string {
	t.Helper()
	scans, err := chexec.ScanAllParts(context.Background(), conn, db+"."+table, sch)
	if err != nil {
		t.Fatalf("scan %s.%s: %v", db, table, err)
	}
	phys := map[string]string{}
	for _, part := range partsOf(t, conn, db, table) {
		if part.Active {
			phys[part.Name] = part.PhysHash
		}
	}
	out := make([]string, 0, len(scans))
	for _, s := range scans {
		out = append(out, s.RowLtHash+"/"+phys[s.PartName])
	}
	slices.Sort(out)
	return out
}

// TestMechanics_RestoreAndAttachIntoAFreshTableKeepsEveryPartHash (M15) pins
// the restore path of the same-identity owner rebuild (housegate spec
// 2026-10-14 §16 P4; plan S3-D clarification K52): an hg_safe table with parts
// in two partitions, one of them after a REPLACE PARTITION, is backed up to a
// disk as in M5, restored with RESTORE ... AS <scratch> PARTITIONS, and
// attached with ATTACH PARTITION ... FROM the scratch into a fresh table of
// the same definition. Every part keeps its row LtHash and hash_of_all_files,
// which is what the arbiter's SafeParts and the owner audit's rebuilt rule
// compare; the names the fresh table gives the parts are logged, not
// asserted, because the rebuild never relies on them.
func TestMechanics_RestoreAndAttachIntoAFreshTableKeepsEveryPartHash(t *testing.T) {
	env := requireMechanics(t)
	p := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, promote := protocolTables(t, env.A, env.AAddr, p, sch)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 0, 1_000, nil)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 1_000, 1_000, nil)
	mustInsert(t, env.A, p.SafeDB, safe.Table, "p1", 0, 10, nil)
	// A promotion of p0: its shadow holds the base parts and one candidate,
	// and REPLACE PARTITION publishes them under new names.
	source, shadow := qualified(p.SafeDB, safe.Table), qualified(p.PromoteDB, promote.Table)
	mustExec(t, env.A, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION 'p0' FROM %s", shadow, source))
	mustInsert(t, env.A, p.PromoteDB, promote.Table, "p0", 2_000, 500, nil)
	mustExec(t, env.A, fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION 'p0' FROM %s", source, shadow))
	mustExec(t, env.A, fmt.Sprintf("ALTER TABLE %s DROP PARTITION 'p0'", shadow))
	want := partHashes(t, env.A, p.SafeDB, safe.Table, sch)
	if len(want) != 4 {
		t.Fatalf("the source holds %d parts, want 4 (three in p0 after the REPLACE, one in p1)", len(want))
	}
	disk := fmt.Sprintf("Disk('backups', %s)", literal(fmt.Sprintf("mech_%s_%d_rebuild", suffix(t), time.Now().UnixNano())))
	runBackup(t, env.A, fmt.Sprintf("BACKUP TABLE %s TO %s", source, disk), "BACKUP_CREATED")

	restoreDB, freshDB := "hg_promote_r"+suffix(t), "hg_safe_r"+suffix(t)
	dropDatabasesAt(t, env.AAddr, restoreDB, freshDB)
	mustExec(t, env.A, "CREATE DATABASE IF NOT EXISTS "+ident(restoreDB))
	mustExec(t, env.A, "CREATE DATABASE IF NOT EXISTS "+ident(freshDB))
	scratch := qualified(restoreDB, safe.Table+"__rebuild_1")
	fresh := qualified(freshDB, safe.Table)
	runBackup(t, env.A, fmt.Sprintf("RESTORE TABLE %s AS %s PARTITIONS 'p0', 'p1' FROM %s", source, scratch, disk), "RESTORED")
	mustExec(t, env.A, fmt.Sprintf("CREATE TABLE %s AS %s", fresh, source))
	for _, partition := range []string{"p0", "p1"} {
		mustExec(t, env.A, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", fresh, literal(partition), scratch))
	}

	got := partHashes(t, env.A, freshDB, safe.Table, sch)
	if !slices.Equal(got, want) {
		t.Fatalf("after RESTORE and ATTACH PARTITION FROM the fresh table holds (row LtHash/hash_of_all_files)\n%v\nthe source held\n%v", got, want)
	}
	t.Logf("part names: source %v, fresh table %v", names(activeParts(t, env.A, p.SafeDB, safe.Table, "p0")), names(activeParts(t, env.A, freshDB, safe.Table, "p0")))
}
