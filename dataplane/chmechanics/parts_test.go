package chmechanics

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chrestart"
	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// The promotion replaceAPartition performs: two base parts and one candidate,
// so REPLACE publishes 9 rows and a restart that brings the two base parts
// back shows 14 (spec §2 measured 3 rows becoming 5).
const (
	baseRowsA     = 2
	baseRowsB     = 3
	candidateRows = 4
)

// replaceFixture is one promotion of partition p0 as the SNode performs it.
type replaceFixture struct {
	p                ddl.Pinned
	sch              payloadexec.TableSchema
	table            string     // the physical table name in hg_safe and hg_promote
	retired          []mechPart // hg_safe's active parts before REPLACE
	shadow           []mechPart // hg_promote's parts that REPLACE copied
	replacements     []mechPart // hg_safe's active parts after REPLACE
	retiredRowHashes map[string]string
	shadowRowHashes  map[string]string
}

// replaceAPartition performs the SNode's promotion of p0 on the server at
// addr with the protocol DDL (snode/promote_replace.go buildAndReplace): two
// base parts in hg_safe, the shadow built with ATTACH PARTITION FROM hg_safe
// plus one candidate part, REPLACE PARTITION, and the shadow partition
// dropped.
func replaceAPartition(t *testing.T, conn clickhouse.Conn, addr string) replaceFixture {
	t.Helper()
	fx := replaceFixture{p: mechanicsPinned(t), sch: mechanicsSchema(t)}
	_, safe, promote := protocolTables(t, conn, addr, fx.p, fx.sch)
	fx.table = safe.Table
	safeTable := qualified(fx.p.SafeDB, fx.table)
	promoteTable := qualified(fx.p.PromoteDB, promote.Table)
	// A restart test restarts right after REPLACE: wait for the uptime first.
	ensureUptime(t, conn, 2*chrestart.DefaultRestartSkew)
	mustInsert(t, conn, fx.p.SafeDB, fx.table, "p0", 0, baseRowsA, nil)
	mustInsert(t, conn, fx.p.SafeDB, fx.table, "p0", baseRowsA, baseRowsB, nil)
	fx.retired = activeParts(t, conn, fx.p.SafeDB, fx.table, "p0")
	if len(fx.retired) != 2 {
		t.Fatalf("base: hg_safe holds %d active parts, want 2: %+v", len(fx.retired), fx.retired)
	}
	fx.retiredRowHashes = rowHashes(t, conn, fx.p.SafeDB, fx.table, fx.sch, fx.retired)
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION 'p0' FROM %s", promoteTable, safeTable))
	mustInsert(t, conn, fx.p.PromoteDB, promote.Table, "p0", baseRowsA+baseRowsB, candidateRows, nil)
	fx.shadow = activeParts(t, conn, fx.p.PromoteDB, promote.Table, "p0")
	if len(fx.shadow) != 3 {
		t.Fatalf("shadow: hg_promote holds %d active parts, want 3: %+v", len(fx.shadow), fx.shadow)
	}
	fx.shadowRowHashes = rowHashes(t, conn, fx.p.PromoteDB, promote.Table, fx.sch, fx.shadow)
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION 'p0' FROM %s", safeTable, promoteTable))
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s DROP PARTITION 'p0'", promoteTable))
	fx.replacements = activeParts(t, conn, fx.p.SafeDB, fx.table, "p0")
	return fx
}

// TestMechanics_ReplacePartitionKeepsHashOfAllFiles (M1) pins what the SNode's
// promotion and the owner audit rely on: REPLACE PARTITION publishes a copy
// of every shadow part with its hash_of_all_files and row LtHash, under a new
// name whose block range lies above every part it replaced, so the
// replacements cover none of the parts they replace.
func TestMechanics_ReplacePartitionKeepsHashOfAllFiles(t *testing.T) {
	env := requireMechanics(t)
	fx := replaceAPartition(t, env.A, env.AAddr)
	if got, want := totalRows(fx.replacements), uint64(baseRowsA+baseRowsB+candidateRows); got != want {
		t.Fatalf("hg_safe holds %d rows after REPLACE, want %d: %+v", got, want, fx.replacements)
	}
	if got, want := physHashes(fx.replacements), physHashes(fx.shadow); !slices.Equal(got, want) {
		t.Fatalf("REPLACE PARTITION changed hash_of_all_files: hg_safe %v, shadow %v", got, want)
	}
	published := physHashes(fx.replacements)
	var maxRetiredBlock int64
	for _, retired := range fx.retired {
		if !slices.Contains(published, retired.PhysHash) {
			t.Fatalf("base part %s (%s) has no copy with its hash_of_all_files after REPLACE: %+v", retired.Name, retired.PhysHash, fx.replacements)
		}
		maxRetiredBlock = max(maxRetiredBlock, retired.MaxBlock)
	}
	safeRowHashes := rowHashes(t, env.A, fx.p.SafeDB, fx.table, fx.sch, fx.replacements)
	for _, part := range fx.replacements {
		if _, reused := partNamed(fx.retired, part.Name); reused {
			t.Fatalf("replacement %s reuses a retired part's name", part.Name)
		}
		if part.MinBlock <= maxRetiredBlock {
			t.Fatalf("replacement %s starts at block %d, at or below the retired parts' last block %d", part.Name, part.MinBlock, maxRetiredBlock)
		}
		source, ok := partWithHash(fx.shadow, part.PhysHash)
		if !ok {
			t.Fatalf("replacement %s has no shadow part with hash_of_all_files %s", part.Name, part.PhysHash)
		}
		if got, want := safeRowHashes[part.Name], fx.shadowRowHashes[source.Name]; got != want {
			t.Fatalf("replacement %s row LtHash %s differs from its shadow part %s (%s)", part.Name, abbreviate(got), source.Name, abbreviate(want))
		}
	}
}

// TestMechanics_AttachPartitionFromKeepsHashOfAllFiles (M2) pins the shadow
// build and the install path (snode/promote_replace.go prepareShadow and
// attachCandidateParts; spec §8.5 step 5): ATTACH PARTITION FROM copies every
// part with its hash_of_all_files and row LtHash, across databases and from
// the hg_unsafe ReplicatedMergeTree into a MergeTree, and leaves its source
// unchanged.
func TestMechanics_AttachPartitionFromKeepsHashOfAllFiles(t *testing.T) {
	env := requireMechanics(t)
	t.Run("across_databases", func(t *testing.T) {
		p := mechanicsPinned(t)
		sch := mechanicsSchema(t)
		_, safe, promote := protocolTables(t, env.A, env.AAddr, p, sch)
		mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 0, 2, nil)
		mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 2, 3, nil)
		assertAttachCopies(t, env.A, sch, safe, promote)
	})
	t.Run("from_replicated_merge_tree", func(t *testing.T) {
		p := mechanicsPinned(t)
		sch := mechanicsSchema(t)
		unsafe, _, promote := protocolTables(t, env.A, env.AAddr, p, sch)
		createTables(t, env.A, env.AAddr, unsafe)
		mustInsert(t, env.A, p.UnsafeDB, unsafe.Table, "p0", 0, 2, nil)
		mustInsert(t, env.A, p.UnsafeDB, unsafe.Table, "p0", 2, 3, nil)
		assertAttachCopies(t, env.A, sch, unsafe, promote)
	})
}

// assertAttachCopies runs ATTACH PARTITION 'p0' FROM source into target and
// checks that target's p0 holds a copy of every source part: the same
// multisets of hash_of_all_files and of row LtHash, with the source unchanged.
func assertAttachCopies(t *testing.T, conn clickhouse.Conn, sch payloadexec.TableSchema, source, target ddl.TableIntent) {
	t.Helper()
	before := activeParts(t, conn, source.Database, source.Table, "p0")
	sourceRows := rowHashList(t, conn, source.Database, source.Table, sch, before)
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION 'p0' FROM %s",
		qualified(target.Database, target.Table), qualified(source.Database, source.Table)))
	copied := activeParts(t, conn, target.Database, target.Table, "p0")
	if got, want := physHashes(copied), physHashes(before); !slices.Equal(got, want) {
		t.Fatalf("ATTACH PARTITION FROM %s.%s changed hash_of_all_files: %v, source %v", source.Database, source.Table, got, want)
	}
	if got := rowHashList(t, conn, target.Database, target.Table, sch, copied); !slices.Equal(got, sourceRows) {
		t.Fatalf("ATTACH PARTITION FROM %s.%s changed the rows", source.Database, source.Table)
	}
	if after := activeParts(t, conn, source.Database, source.Table, "p0"); !slices.Equal(names(after), names(before)) {
		t.Fatalf("ATTACH PARTITION FROM changed its source: %v, was %v", names(after), names(before))
	}
}

// TestMechanics_AttachPartitionFromIsNotThrottledByPartsToThrowInsert (M2)
// pins why an install may attach more parts than an INSERT could write
// (spec §2, §8.5 step 5): ATTACH PARTITION FROM into a table whose
// parts_to_throw_insert is 3 attaches 5 parts, while an INSERT into that
// partition is refused with TOO_MANY_PARTS.
func TestMechanics_AttachPartitionFromIsNotThrottledByPartsToThrowInsert(t *testing.T) {
	env := requireMechanics(t)
	p := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, promote := protocolTables(t, env.A, env.AAddr, p, sch)
	throttled := promote
	throttled.Table = promote.Table + "_throttled"
	throttled.Settings = append(slices.Clone(promote.Settings),
		ddl.PinnedSetting{Name: "parts_to_delay_insert", Value: "2"},
		ddl.PinnedSetting{Name: "parts_to_throw_insert", Value: "3"})
	mustExec(t, env.A, throttled.SQL())
	for i := uint64(0); i < 5; i++ {
		mustInsert(t, env.A, p.SafeDB, safe.Table, "p0", 10*i, 2, nil)
	}
	source := activeParts(t, env.A, p.SafeDB, safe.Table, "p0")
	if len(source) != 5 {
		t.Fatalf("source holds %d parts, want 5: %v", len(source), names(source))
	}
	mustExec(t, env.A, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION 'p0' FROM %s",
		qualified(throttled.Database, throttled.Table), qualified(safe.Database, safe.Table)))
	attached := activeParts(t, env.A, throttled.Database, throttled.Table, "p0")
	if got, want := physHashes(attached), physHashes(source); !slices.Equal(got, want) {
		t.Fatalf("attached %v, want the 5 source parts %v", got, want)
	}
	err := insertRows(env.A, throttled.Database, throttled.Table, "p0", 1000, 1, nil)
	if clickHouseCode(err) != codeTooManyParts {
		t.Fatalf("INSERT into a partition of 5 parts with parts_to_throw_insert = 3: err = %v, want TOO_MANY_PARTS (252)", err)
	}
}

// assertOldPartsLifetime pins the window in which a restart resurrects the
// parts a REPLACE PARTITION retired: the server's old_parts_lifetime is 480 s.
func assertOldPartsLifetime(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	var value string
	var changed uint8
	if err := conn.QueryRow(context.Background(),
		"SELECT value, toUInt8(changed) FROM system.merge_tree_settings WHERE name = 'old_parts_lifetime'").Scan(&value, &changed); err != nil {
		t.Fatalf("system.merge_tree_settings: %v", err)
	}
	if value != "480" {
		t.Fatalf("old_parts_lifetime = %s (changed by the server config: %d), want 480", value, changed)
	}
}

// assertProtocolDDLKeepsOldPartsLifetime pins that the protocol DDL does not
// override old_parts_lifetime, so its tables inherit the server's 480 s.
func assertProtocolDDLKeepsOldPartsLifetime(t *testing.T, conn clickhouse.Conn, fx replaceFixture) {
	t.Helper()
	unsafe, safe, promote, err := ddl.Intents(fx.p, fx.sch)
	if err != nil {
		t.Fatal(err)
	}
	for _, intent := range []ddl.TableIntent{unsafe, safe, promote} {
		for _, setting := range intent.Settings {
			if setting.Name == "old_parts_lifetime" {
				t.Fatalf("the protocol DDL of %s.%s pins old_parts_lifetime = %s", intent.Database, intent.Table, setting.Value)
			}
		}
	}
	for _, intent := range []ddl.TableIntent{safe, promote} {
		var create string
		if err := conn.QueryRow(context.Background(),
			"SELECT create_table_query FROM system.tables WHERE database = ? AND name = ?", intent.Database, intent.Table).Scan(&create); err != nil {
			t.Fatalf("create_table_query of %s.%s: %v", intent.Database, intent.Table, err)
		}
		if strings.Contains(create, "old_parts_lifetime") {
			t.Fatalf("%s.%s sets old_parts_lifetime: %s", intent.Database, intent.Table, create)
		}
	}
}

// assertRetiredPartsAreOutdated pins the window M3 depends on: right after
// REPLACE the retired parts are still on disk, listed as outdated, not
// removed.
func assertRetiredPartsAreOutdated(t *testing.T, conn clickhouse.Conn, fx replaceFixture) {
	t.Helper()
	all := partsOf(t, conn, fx.p.SafeDB, fx.table)
	for _, retired := range fx.retired {
		part, ok := partNamed(all, retired.Name)
		if !ok {
			t.Fatalf("REPLACE PARTITION removed retired part %s at once; system.parts lists %v", retired.Name, names(all))
		}
		if part.Active {
			t.Fatalf("retired part %s is still active after REPLACE PARTITION", retired.Name)
		}
	}
}

// assertResurrected checks the state a restart inside old_parts_lifetime
// leaves after replaceAPartition: every retired part is active again under its
// own name and hash_of_all_files, beside its replacement twin with the same
// hash_of_all_files and row LtHash, so p0 holds the retired rows twice. This
// is the state the owner audit repairs (spec §14).
func assertResurrected(t *testing.T, conn clickhouse.Conn, fx replaceFixture) {
	t.Helper()
	active := activeParts(t, conn, fx.p.SafeDB, fx.table, "p0")
	wantNames := append(names(fx.replacements), names(fx.retired)...)
	slices.Sort(wantNames)
	if got := names(active); !slices.Equal(got, wantNames) {
		t.Fatalf("active p0 parts %v, want the replacements and the resurrected retired parts %v", got, wantNames)
	}
	if got, want := totalRows(active), totalRows(fx.replacements)+totalRows(fx.retired); got != want {
		t.Fatalf("p0 holds %d rows, want %d: the replacements' and the resurrected parts'", got, want)
	}
	now := rowHashes(t, conn, fx.p.SafeDB, fx.table, fx.sch, active)
	for _, retired := range fx.retired {
		part, _ := partNamed(active, retired.Name)
		if part.PhysHash != retired.PhysHash {
			t.Fatalf("resurrected %s has hash_of_all_files %s, it had %s", retired.Name, part.PhysHash, retired.PhysHash)
		}
		twin, ok := partWithHash(fx.replacements, retired.PhysHash)
		if !ok {
			t.Fatalf("resurrected %s has no replacement twin with hash_of_all_files %s", retired.Name, retired.PhysHash)
		}
		if now[retired.Name] != now[twin.Name] || now[retired.Name] != fx.retiredRowHashes[retired.Name] {
			t.Fatalf("resurrected %s row LtHash %s; its twin %s has %s; before REPLACE it had %s",
				retired.Name, abbreviate(now[retired.Name]), twin.Name, abbreviate(now[twin.Name]), abbreviate(fx.retiredRowHashes[retired.Name]))
		}
	}
}

// assertExactlyTheReplacements checks that p0's active parts are exactly what
// REPLACE published: the replacement parts by name and hash_of_all_files,
// with their rows, and no zero-row part (clarification K2).
func assertExactlyTheReplacements(t *testing.T, conn clickhouse.Conn, fx replaceFixture, when string) {
	t.Helper()
	active := activeParts(t, conn, fx.p.SafeDB, fx.table, "p0")
	if empty := zeroRowParts(active); len(empty) > 0 {
		t.Fatalf("%s: active zero-row parts %v in p0; clarification K2 says none stays active", when, names(empty))
	}
	if got, want := names(active), names(fx.replacements); !slices.Equal(got, want) {
		t.Fatalf("%s: active p0 parts %v, want exactly the replacements %v", when, got, want)
	}
	if got, want := physHashes(active), physHashes(fx.replacements); !slices.Equal(got, want) {
		t.Fatalf("%s: hash_of_all_files %v, want %v", when, got, want)
	}
	if got, want := totalRows(active), totalRows(fx.replacements); got != want {
		t.Fatalf("%s: p0 holds %d rows, want %d", when, got, want)
	}
}

// assertNotDetached checks that no retired part is left in detached/.
func assertNotDetached(t *testing.T, conn clickhouse.Conn, fx replaceFixture) {
	t.Helper()
	rows, err := conn.Query(context.Background(),
		"SELECT name FROM system.detached_parts WHERE database = ? AND table = ?", fx.p.SafeDB, fx.table)
	if err != nil {
		t.Fatalf("system.detached_parts: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan system.detached_parts: %v", err)
		}
		if _, retired := partNamed(fx.retired, name); retired {
			t.Fatalf("retired part %s is left in detached/", name)
		}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("system.detached_parts: %v", err)
	}
}

// TestMechanics_ReplacePartitionResurrectsReplacedPartsAfterARestart (M3)
// pins the hazard the owner audit exists for (spec §2 "Restart resurrection",
// §14): REPLACE PARTITION leaves the parts it retired on disk as outdated for
// old_parts_lifetime (480 s, which the protocol DDL does not override), and a
// restart inside that window makes them active again beside their equal-hash
// replacements, because the replacements' block ranges do not cover them. It
// pins both restart kinds production sees: an abrupt exit (SIGKILL, OOM kill)
// and a graceful shutdown (a pod deletion's SIGTERM). A row whose outcome
// changes on an image is a measured fact to record in Appendix B, not a test
// to weaken.
func TestMechanics_ReplacePartitionResurrectsReplacedPartsAfterARestart(t *testing.T) {
	requireMechanics(t)
	addr := requireRestartable(t)
	for _, tc := range []struct {
		name            string
		kind            restartKind
		wantResurrected bool
	}{
		{name: "abrupt", kind: restartAbrupt, wantResurrected: true},
		{name: "graceful", kind: restartGraceful, wantResurrected: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			conn := chtest.Open(t, addr)
			assertOldPartsLifetime(t, conn)
			fx := replaceAPartition(t, conn, addr)
			assertProtocolDDLKeepsOldPartsLifetime(t, conn, fx)
			assertRetiredPartsAreOutdated(t, conn, fx)
			restarted := restartServer(t, addr, tc.kind)
			waitLoaded(t, restarted, fx.p.SafeDB, fx.table)
			if tc.wantResurrected {
				assertResurrected(t, restarted, fx)
			} else {
				assertExactlyTheReplacements(t, restarted, fx, "after a "+tc.name+" restart")
			}
		})
	}
}

// TestMechanics_DropPartitionThenAttachDoesNotResurrect (M4) pins the repair
// the spec uses instead of REPLACE PARTITION for a copy (§2, §8.5 step 2):
// DROP PARTITION followed by ATTACH PARTITION FROM a rebuilt scratch leaves
// exactly the attached rows active, and an abrupt restart inside the outdated
// window brings none of the dropped parts back.
func TestMechanics_DropPartitionThenAttachDoesNotResurrect(t *testing.T) {
	requireMechanics(t)
	addr := requireRestartable(t)
	conn := chtest.Open(t, addr)
	p := mechanicsPinned(t)
	sch := mechanicsSchema(t)
	_, safe, promote := protocolTables(t, conn, addr, p, sch)
	copyTable := qualified(p.SafeDB, safe.Table)
	scratchTable := qualified(p.PromoteDB, promote.Table)
	ensureUptime(t, conn, 2*chrestart.DefaultRestartSkew)
	// The copy holds two verified parts and a foreign one.
	mustInsert(t, conn, p.SafeDB, safe.Table, "p0", 0, 2, nil)
	mustInsert(t, conn, p.SafeDB, safe.Table, "p0", 2, 3, nil)
	mustInsert(t, conn, p.SafeDB, safe.Table, "p0", 100, 4, nil)
	dropped := activeParts(t, conn, p.SafeDB, safe.Table, "p0")
	if len(dropped) != 3 {
		t.Fatalf("copy holds %d parts, want 3: %v", len(dropped), names(dropped))
	}
	// The rebuilt partition holds the verified rows only, written afresh.
	mustInsert(t, conn, p.PromoteDB, promote.Table, "p0", 0, 2, nil)
	mustInsert(t, conn, p.PromoteDB, promote.Table, "p0", 2, 3, nil)
	rebuilt := activeParts(t, conn, p.PromoteDB, promote.Table, "p0")
	rebuiltRows := rowHashList(t, conn, p.PromoteDB, promote.Table, sch, rebuilt)
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s DROP PARTITION 'p0'", copyTable))
	mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION 'p0' FROM %s", copyTable, scratchTable))
	check := func(c clickhouse.Conn, when string) {
		t.Helper()
		active := activeParts(t, c, p.SafeDB, safe.Table, "p0")
		for _, part := range dropped {
			if _, back := partNamed(active, part.Name); back {
				t.Fatalf("%s: dropped part %s is active: %v", when, part.Name, names(active))
			}
		}
		if empty := zeroRowParts(active); len(empty) > 0 {
			t.Logf("%s: zero-row parts %v (DROP PARTITION's covering parts)", when, names(empty))
		}
		data := withRows(active)
		if got, want := physHashes(data), physHashes(rebuilt); !slices.Equal(got, want) {
			t.Fatalf("%s: parts with rows %v, want the attached rebuild %v", when, got, want)
		}
		if got := totalRows(data); got != 5 {
			t.Fatalf("%s: p0 holds %d rows, want the 5 verified rows", when, got)
		}
		if got := rowHashList(t, c, p.SafeDB, safe.Table, sch, data); !slices.Equal(got, rebuiltRows) {
			t.Fatalf("%s: the copy's rows differ from the rebuild's", when)
		}
	}
	check(conn, "after the repair")
	restarted := restartServer(t, addr, restartAbrupt)
	waitLoaded(t, restarted, p.SafeDB, safe.Table)
	check(restarted, "after an abrupt restart")
}

// TestMechanics_DropPartOfAResurrectedTwinLeavesNoActivePartAndStaysDropped
// (M11) pins clarification K2, on which the owner audit's repair mode rests:
// after an abrupt restart resurrects the parts a REPLACE PARTITION retired,
// removing each resurrected twin leaves exactly the replacement parts active,
// leaves no active zero-row part, and the removed parts stay removed across a
// second abrupt restart. The audit uses ALTER TABLE ... DROP PART; DETACH
// PART followed by DROP DETACHED PART is the alternative K2 measured as
// equivalent. The zero-row check runs right after the drop, because the audit
// lists system.parts right after its drops.
func TestMechanics_DropPartOfAResurrectedTwinLeavesNoActivePartAndStaysDropped(t *testing.T) {
	requireMechanics(t)
	addr := requireRestartable(t)
	for _, tc := range []struct {
		name   string
		remove func(t *testing.T, conn clickhouse.Conn, table, part string)
	}{
		{name: "drop_part", remove: func(t *testing.T, conn clickhouse.Conn, table, part string) {
			mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s DROP PART %s", table, literal(part)))
		}},
		{name: "detach_part_then_drop_detached", remove: func(t *testing.T, conn clickhouse.Conn, table, part string) {
			mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s DETACH PART %s", table, literal(part)))
			ctx := clickhouse.Context(context.Background(), clickhouse.WithSettings(clickhouse.Settings{"allow_drop_detached": 1}))
			if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP DETACHED PART %s", table, literal(part))); err != nil {
				t.Fatalf("drop detached part %s: %v", part, err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fx := replaceAPartition(t, chtest.Open(t, addr), addr)
			restarted := restartServer(t, addr, restartAbrupt)
			waitLoaded(t, restarted, fx.p.SafeDB, fx.table)
			assertResurrected(t, restarted, fx)
			safeTable := qualified(fx.p.SafeDB, fx.table)
			for _, retired := range fx.retired {
				tc.remove(t, restarted, safeTable, retired.Name)
			}
			assertExactlyTheReplacements(t, restarted, fx, "right after removing the resurrected twins")
			assertNotDetached(t, restarted, fx)
			again := restartServer(t, addr, restartAbrupt)
			waitLoaded(t, again, fx.p.SafeDB, fx.table)
			assertExactlyTheReplacements(t, again, fx, "after a second abrupt restart")
			assertNotDetached(t, again, fx)
		})
	}
}
