package sources

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// largeRows is above the default min_insert_block_size_rows (1,048,449), so
// an INSERT ... SELECT with default settings splits a part this large.
const largeRows = 1_200_000

// requireOnePart asserts that scratch holds exactly one part, of rows rows
// whose row LtHash under schema is rowHash ("" skips the scan), and that res
// names it.
func requireOnePart(t *testing.T, conn clickhouse.Conn, res Result, scratch Scratch, schema payloadexec.TableSchema, rowHash string, rows uint64) {
	t.Helper()
	parts := partsOf(t, conn, scratch.Database, scratch.Table)
	if len(parts) != 1 || parts[0].Rows != rows {
		t.Fatalf("scratch holds %+v, want one part of %d rows", parts, rows)
	}
	if !slices.Equal(res.Parts, []string{parts[0].Name}) {
		t.Fatalf("Result.Parts = %v, scratch holds %s", res.Parts, parts[0].Name)
	}
	if rowHash == "" {
		return
	}
	scan := scanOf(t, conn, scratch.Database, scratch.Table, schema)
	if got := scan[parts[0].Name].RowLtHash; got != rowHash {
		t.Fatalf("the scratch part's row LtHash is %s, want %s", got, rowHash)
	}
}

// TestOwnerPullResolvesTheCurrentNameByHashOfAllFiles: the owner's REPLACE
// PARTITION renamed the manifest's part and kept its hash_of_all_files; the
// pull resolves the current name by that hash and copies that part alone,
// not the other part of its partition.
func TestOwnerPullResolvesTheCurrentNameByHashOfAllFiles(t *testing.T) {
	p := requirePair(t)
	sfx := suffix(t)
	db, schema := "src_"+sfx, testSchema("dex_"+sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, db, schema)
	insertPart(t, p.Peer, db, table, "a", 0, 3)
	insertPart(t, p.Peer, db, table, "a", 100, 2)
	before := partsOf(t, p.Peer, db, table)
	scans := scanOf(t, p.Peer, db, table, schema)
	var manifest partRow
	for _, part := range before {
		if part.Rows == 3 {
			manifest = part
		}
	}
	// A promotion's REPLACE PARTITION: the shadow takes the partition with
	// ATTACH PARTITION FROM, and REPLACE brings it back under new names.
	shadow := table + "_shadow"
	mustExec(t, p.Peer, "CREATE TABLE "+ident(db)+"."+ident(shadow)+" AS "+ident(db)+"."+ident(table))
	mustExec(t, p.Peer, "ALTER TABLE "+ident(db)+"."+ident(shadow)+" ATTACH PARTITION 'a' FROM "+ident(db)+"."+ident(table))
	mustExec(t, p.Peer, "ALTER TABLE "+ident(db)+"."+ident(table)+" REPLACE PARTITION 'a' FROM "+ident(db)+"."+ident(shadow))
	var renamed partRow
	for _, part := range partsOf(t, p.Peer, db, table) {
		if part.Hash == manifest.Hash {
			renamed = part
		}
	}
	if renamed.Name == "" || renamed.Name == manifest.Name {
		t.Fatalf("REPLACE PARTITION left %+v for the manifest's part %+v; want a new name with its hash", renamed, manifest)
	}
	collection := ownerCollection(t, p, "si_owner_", db)
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", []string{collection})
	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	need := Need{
		TableKey: schema.TableID, Table: table, PartitionID: "p_a", PartName: manifest.Name,
		PartPhysHash: manifest.Hash, PartRowLtHash: scans[manifest.Name].RowLtHash, RowCount: 3,
	}
	res := NewOwnerPull(member, collection, db).Fetch(context.Background(), need, schema, scratch)
	if res.Outcome != OutcomeFetched || res.Err != nil || res.Source != SourceOwnerPull {
		t.Fatalf("Fetch = %+v, want fetched", res)
	}
	requireOnePart(t, member, res, scratch, schema, need.PartRowLtHash, 3)
	t.Logf("manifest part %s, current name %s, hash_of_all_files %s", manifest.Name, renamed.Name, manifest.Hash)
}

// TestOwnerPullFallsBackToTheRowHashWhenTheOwnerRebuiltItsParts: no active
// part has the manifest's hash_of_all_files (the owner rebuilt its parts), so
// the pull lists the parts of the needed partition and row count, pulls each
// into its own fresh <scratch>_c<k>, keeps the one whose scan is the needed
// row hash and drops every candidate table; the decoy of another partition,
// whose part sorts before the needed one, is never pulled; a bound below that
// candidate's position misses, and the next fetch of the same need continues
// after the last candidate it pulled.
func TestOwnerPullFallsBackToTheRowHashWhenTheOwnerRebuiltItsParts(t *testing.T) {
	p := requirePair(t)
	sfx := suffix(t)
	db, schema := "src_"+sfx, testSchema("dex_"+sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, db, schema)
	insertPart(t, p.Peer, db, table, "a", 200, 3) // a decoy of the same row count, an earlier name
	insertPart(t, p.Peer, db, table, "a", 0, 3)   // the needed rows
	// A decoy of the row count in partition c, whose partition_id sorts before
	// a's (partitionId('c') = 0b8b7d0f…, partitionId('a') = 51fbadfb…,
	// measured), so a listing not scoped to the partition would pull it first.
	insertPart(t, p.Peer, db, table, "c", 300, 3)
	scans := scanOf(t, p.Peer, db, table, schema)
	parts := partsOf(t, p.Peer, db, table)
	needed := parts[0]
	for _, part := range parts {
		if strings.HasSuffix(part.Name, "_2_2_0") {
			needed = part
		}
	}
	// The candidates are the needed partition's parts of the row count, in
	// name order; the decoy of partition c is none of them.
	var candidates []partRow
	var decoy partRow
	for _, part := range parts {
		if part.PartitionID == needed.PartitionID {
			candidates = append(candidates, part)
		} else {
			decoy = part
		}
	}
	position := slices.IndexFunc(candidates, func(r partRow) bool { return r.Name == needed.Name })
	if position < 1 || decoy.Name == "" || decoy.Name > needed.Name {
		t.Fatalf("the needed part %s is first among its partition's %+v, or partition c holds no part that sorts before it (%+v); both decoys must sort before it", needed.Name, candidates, parts)
	}
	need := Need{
		TableKey: schema.TableID, Table: table, PartitionID: "p_a", PartName: needed.Name,
		PartPhysHash: strings.Repeat("0", 32), PartRowLtHash: scans[needed.Name].RowLtHash, RowCount: 3,
	}
	collection := ownerCollection(t, p, "si_owner_", db)
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", []string{collection})

	t.Run("keeps_the_candidate_with_the_row_hash", func(t *testing.T) {
		scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
		rec := &recordingConn{Conn: member}
		res := NewOwnerPull(rec, collection, db).Fetch(context.Background(), need, schema, scratch)
		if res.Outcome != OutcomeFetched || res.Err != nil {
			t.Fatalf("Fetch = %+v, want fetched", res)
		}
		requireOnePart(t, member, res, scratch, schema, need.PartRowLtHash, 3)
		pulls := rec.matching("exec INSERT INTO")
		if len(pulls) != position+1 {
			t.Fatalf("%d candidate pulls %q, want %d (every candidate up to the needed one)", len(pulls), pulls, position+1)
		}
		for k, pull := range pulls {
			if want := fmt.Sprintf("exec INSERT INTO %s.%s ", ident(stage), ident(fmt.Sprintf("%s_c%d", scratch.Table, k+1))); !strings.HasPrefix(pull, want) {
				t.Fatalf("candidate pull %d is %s, want its own scratch (%s...)", k+1, pull, want)
			}
			if strings.Contains(pull, decoy.Name) {
				t.Fatalf("candidate pull %d reads %s of another partition: %s", k+1, decoy.Name, pull)
			}
		}
		if got := tablesIn(t, p.Member, stage); !slices.Equal(got, []string{scratch.Table}) {
			t.Fatalf("%s holds %v after the fetch, want only the scratch", stage, got)
		}
	})
	t.Run("a_bound_below_the_candidate_misses_and_the_next_fetch_continues", func(t *testing.T) {
		scratch := newScratch(t, member, stage, "member-test_"+sfx+"_2", schema)
		rec := &recordingConn{Conn: member}
		pull := NewOwnerPull(rec, collection, db)
		pull.MaxFallbackCandidates = position
		res := pull.Fetch(context.Background(), need, schema, scratch)
		if res.Outcome != OutcomeMissed || res.Err != nil {
			t.Fatalf("Fetch with %d candidates = %+v, want missed", position, res)
		}
		if parts := partsOf(t, member, stage, scratch.Table); len(parts) != 0 {
			t.Fatalf("a missed fetch wrote %+v", parts)
		}
		before := len(rec.matching("exec INSERT INTO"))
		next := newScratch(t, member, stage, "member-test_"+sfx+"_3", schema)
		res = pull.Fetch(context.Background(), need, schema, next)
		if res.Outcome != OutcomeFetched || res.Err != nil {
			t.Fatalf("the next Fetch of the same need = %+v, want fetched after the cursor", res)
		}
		requireOnePart(t, member, res, next, schema, need.PartRowLtHash, 3)
		if got := len(rec.matching("exec INSERT INTO")) - before; got != 1 {
			t.Fatalf("the next Fetch pulled %d candidates, want 1: it lists after the last candidate the first one pulled", got)
		}
	})
}

// TestOwnerPullFallbackScopesTheNeededPartition: nine other partitions each
// hold one part of the needed row count whose name sorts before the needed
// one, so a listing bounded before it is scoped would never reach the needed
// part (housegate spec 2026-10-14 §8.6: candidates of the same partition and
// row count; Plan S3-C2 review fix 245-3). The member's server computes
// ClickHouse's partition_id of every partition as the owner's server assigns
// it, and the fallback lists the needed partition alone and pulls its part
// first.
func TestOwnerPullFallbackScopesTheNeededPartition(t *testing.T) {
	p := requirePair(t)
	sfx := suffix(t)
	db, schema := "src_"+sfx, testSchema("dex_"+sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, db, schema)
	values := []string{"p0", "p1", "p2", "p3", "p4", "p5", "p6", "p7", "p8", "p9"}
	ids := map[string]string{}
	for k, v := range values {
		insertPart(t, p.Peer, db, table, v, uint64(100*k), 3)
		var id string
		if err := p.Member.QueryRow(context.Background(), partitionIDQuery(literal(v))).Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids[v] = id
	}
	parts := partsOf(t, p.Peer, db, table)
	for _, v := range values {
		if !slices.ContainsFunc(parts, func(r partRow) bool { return r.PartitionID == ids[v] }) {
			t.Fatalf("partitionId(%q) is %s on the member's server, and no part of the owner has it: %+v", v, ids[v], parts)
		}
	}
	// The needed partition is the one whose id sorts last, so the part of
	// every other partition sorts before its part.
	needed := slices.MaxFunc(values, func(a, b string) int { return strings.Compare(ids[a], ids[b]) })
	position := slices.IndexFunc(parts, func(r partRow) bool { return r.PartitionID == ids[needed] })
	if position < DefaultMaxFallbackCandidates {
		t.Fatalf("the needed partition's part sorts at %d of %+v, want at least %d", position, parts, DefaultMaxFallbackCandidates)
	}
	target := parts[position]
	scans := scanOf(t, p.Peer, db, table, schema)
	need := Need{
		TableKey: schema.TableID, Table: table, PartitionID: "p_" + needed, PartName: target.Name,
		PartPhysHash: strings.Repeat("0", 32), PartRowLtHash: scans[target.Name].RowLtHash, RowCount: 3,
	}
	collection := ownerCollection(t, p, "si_owner_", db)
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", []string{collection})
	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	rec := &recordingConn{Conn: member}
	res := NewOwnerPull(rec, collection, db).Fetch(context.Background(), need, schema, scratch)
	if res.Outcome != OutcomeFetched || res.Err != nil {
		t.Fatalf("Fetch = %+v, want fetched from the needed partition", res)
	}
	requireOnePart(t, member, res, scratch, schema, need.PartRowLtHash, 3)
	if pulls := rec.matching("exec INSERT INTO"); len(pulls) != 1 {
		t.Fatalf("%d candidate pulls %q, want 1: the listing holds the needed partition only", len(pulls), pulls)
	}
	t.Logf("needed partition %s (partition_id %s) sorts at %d of %d parts of 3 rows", needed, ids[needed], position, len(parts))
}

// TestOwnerPullUsesTheNoSplitSettings: the pull runs under NoSplitSettings of
// the resolved part's row count (system.query_log records the query's
// settings).
func TestOwnerPullUsesTheNoSplitSettings(t *testing.T) {
	want := clickhouse.Settings{"min_insert_block_size_rows": uint64(3), "min_insert_block_size_bytes": 0, "max_threads": 1, "max_insert_threads": 1}
	if got := NoSplitSettings(3); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("NoSplitSettings(3) = %v, want %v", got, want)
	}
	if got := NoSplitSettings(0)["min_insert_block_size_rows"]; got != uint64(1) {
		t.Fatalf("NoSplitSettings(0) min_insert_block_size_rows = %v, want 1", got)
	}
	p := requirePair(t)
	sfx := suffix(t)
	db, schema := "src_"+sfx, testSchema("dex_"+sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, db, schema)
	insertPart(t, p.Peer, db, table, "a", 0, 3)
	part := partsOf(t, p.Peer, db, table)[0]
	collection := ownerCollection(t, p, "si_owner_", db)
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", []string{collection})
	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	need := Need{TableKey: schema.TableID, Table: table, PartitionID: "p_a", PartName: part.Name, PartPhysHash: part.Hash, RowCount: 3}
	if res := NewOwnerPull(member, collection, db).Fetch(context.Background(), need, schema, scratch); res.Outcome != OutcomeFetched {
		t.Fatalf("Fetch = %+v, want fetched", res)
	}
	mustExec(t, p.Member, "SYSTEM FLUSH LOGS")
	var rows, bytes, threads, insertThreads string
	if err := p.Member.QueryRow(context.Background(), `
		SELECT Settings['min_insert_block_size_rows'], Settings['min_insert_block_size_bytes'], Settings['max_threads'], Settings['max_insert_threads']
		FROM system.query_log
		WHERE type = 'QueryFinish' AND startsWith(query, ?)
		ORDER BY event_time_microseconds DESC LIMIT 1`, "INSERT INTO "+scratch.qualified()+" ").Scan(&rows, &bytes, &threads, &insertThreads); err != nil {
		t.Fatalf("the pull's query_log row: %v", err)
	}
	if rows != "3" || bytes != "0" || threads != "1" || insertThreads != "1" {
		t.Fatalf("the pull ran with min_insert_block_size_rows=%q min_insert_block_size_bytes=%q max_threads=%q max_insert_threads=%q, want 3 0 1 1", rows, bytes, threads, insertThreads)
	}
}

// TestOwnerPullOfALargePartYieldsOnePart: a part above the default insert
// block size arrives as one part with the same rows (mechanics M6 for the
// member's own pull).
func TestOwnerPullOfALargePartYieldsOnePart(t *testing.T) {
	p := requirePair(t)
	sfx := suffix(t)
	db, schema := "src_"+sfx, testSchema("dex_"+sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, db, schema)
	insertPart(t, p.Peer, db, table, "a", 0, largeRows)
	parts := partsOf(t, p.Peer, db, table)
	if len(parts) != 1 || parts[0].Rows != largeRows {
		t.Fatalf("source %+v, want one part of %d rows", parts, largeRows)
	}
	collection := ownerCollection(t, p, "si_owner_", db)
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", []string{collection})
	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	need := Need{TableKey: schema.TableID, Table: table, PartitionID: "p_a", PartName: parts[0].Name, PartPhysHash: parts[0].Hash, RowCount: largeRows}
	res := NewOwnerPull(member, collection, db).Fetch(context.Background(), need, schema, scratch)
	if res.Outcome != OutcomeFetched || res.Err != nil {
		t.Fatalf("Fetch = %+v, want fetched", res)
	}
	requireOnePart(t, member, res, scratch, schema, "", largeRows)
	checksum := func(conn clickhouse.Conn, db, table string) string {
		var n, sum uint64
		if err := conn.QueryRow(context.Background(), fmt.Sprintf("SELECT count(), sum(cityHash64(%s, `p`, `v`)) FROM %s.%s", ident("_hg_row_id"), ident(db), ident(table))).Scan(&n, &sum); err != nil {
			t.Fatal(err)
		}
		return fmt.Sprintf("%d/%d", n, sum)
	}
	if got, want := checksum(member, stage, scratch.Table), checksum(p.Peer, db, table); got != want {
		t.Fatalf("the pulled part's rows %s differ from the source's %s", got, want)
	}
}

// TestOwnerPullMemoryLimitIsRefusedNotRetried: the member profile's
// max_memory_usage bounds the largest pullable part (K35); a no-split pull
// above it is refused (MEMORY_LIMIT_EXCEEDED, 241), issued once, and leaves
// the scratch empty.
func TestOwnerPullMemoryLimitIsRefusedNotRetried(t *testing.T) {
	p := requirePair(t)
	sfx := suffix(t)
	db, schema := "src_"+sfx, testSchema("dex_"+sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, db, schema)
	insertPart(t, p.Peer, db, table, "a", 0, largeRows)
	part := partsOf(t, p.Peer, db, table)[0]
	collection := ownerCollection(t, p, "si_owner_", db)
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "SETTINGS max_memory_usage = 10000000", []string{collection})
	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	rec := &recordingConn{Conn: member}
	need := Need{TableKey: schema.TableID, Table: table, PartitionID: "p_a", PartName: part.Name, PartPhysHash: part.Hash, PartRowLtHash: "0x00", RowCount: largeRows}
	res := NewOwnerPull(rec, collection, db).Fetch(context.Background(), need, schema, scratch)
	if res.Outcome != OutcomeRefused || clickHouseCode(res.Err) != 241 {
		t.Fatalf("Fetch = %+v, want refused with MEMORY_LIMIT_EXCEEDED (241)", res)
	}
	if pulls := rec.matching("exec "); len(pulls) != 1 {
		t.Fatalf("statements %q, want exactly one pull", pulls)
	}
	if parts := partsOf(t, p.Member, stage, scratch.Table); len(parts) != 0 {
		t.Fatalf("a refused pull wrote %+v", parts)
	}
	t.Logf("memory refusal: %v", res.Err)
}

// TestUnsafePullOfACleanedPartIsMissed: the owner's cleanup dropped the
// candidate part from hg_unsafe; the unsafe pull misses it after one lookup,
// pulls nothing (no fallback over the other part of the same row count) and
// leaves the scratch empty.
func TestUnsafePullOfACleanedPartIsMissed(t *testing.T) {
	p := requirePair(t)
	sfx := suffix(t)
	db, schema := "unsafe_"+sfx, testSchema("dex_"+sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, db, schema)
	insertPart(t, p.Peer, db, table, "a", 0, 3)
	insertPart(t, p.Peer, db, table, "a", 100, 3)
	parts := partsOf(t, p.Peer, db, table)
	scans := scanOf(t, p.Peer, db, table, schema)
	cleaned := parts[0]
	mustExec(t, p.Peer, "ALTER TABLE "+ident(db)+"."+ident(table)+" DROP PART "+literal(cleaned.Name))
	collection := ownerCollection(t, p, "si_owner_", db)
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", []string{collection})
	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	rec := &recordingConn{Conn: member}
	need := Need{TableKey: schema.TableID, Table: table, PartitionID: "p_a", PartName: cleaned.Name, PartPhysHash: cleaned.Hash, PartRowLtHash: scans[cleaned.Name].RowLtHash, RowCount: 3}
	res := NewOwnerUnsafePull(rec, collection, db).Fetch(context.Background(), need, schema, scratch)
	if res.Outcome != OutcomeMissed || res.Err != nil || res.Source != SourceOwnerUnsafePull {
		t.Fatalf("Fetch = %+v, want missed from owner_unsafe_pull", res)
	}
	if got := rec.all(); len(got) != 1 || !strings.Contains(got[0], "hash_of_all_files") {
		t.Fatalf("statements %q, want the hash lookup alone", got)
	}
	if got := tablesIn(t, p.Member, stage); !slices.Equal(got, []string{scratch.Table}) || len(partsOf(t, p.Member, stage, scratch.Table)) != 0 {
		t.Fatalf("%s holds %v (scratch parts %+v), want the empty scratch alone", stage, got, partsOf(t, p.Member, stage, scratch.Table))
	}
}

// TestPullFromAnUnreachableOwnerIsUnreachable: a collection whose server
// refuses connections makes every pull unreachable (remote() answers 519
// around 279 and 210), with nothing written.
func TestPullFromAnUnreachableOwnerIsUnreachable(t *testing.T) {
	p := requirePair(t)
	sfx := suffix(t)
	schema := testSchema("dex_" + sfx)
	collection := "si_owner_" + sfx
	createCollection(t, p, collection, p.PeerHost, 1, "si_reader", randomHex(t, 16))
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", []string{collection})
	for i, source := range []*RemotePull{NewOwnerPull(member, collection, "hg_safe"), NewPeerUnsafePull(member, collection)} {
		scratch := newScratch(t, member, stage, fmt.Sprintf("member-test_%s_%d", sfx, i+1), schema)
		need := Need{TableKey: schema.TableID, Table: "dex_" + sfx + "__t", PartitionID: "p_a", PartName: "x_1_1_0", PartPhysHash: strings.Repeat("a", 32), PartRowLtHash: "0x00", RowCount: 3}
		res := source.Fetch(context.Background(), need, schema, scratch)
		code := clickHouseCode(res.Err)
		if res.Outcome != OutcomeUnreachable || (code != 519 && code != 279 && code != 210 && code != 209) {
			t.Fatalf("%s: Fetch = %+v (code %d), want unreachable", source.Name(), res, code)
		}
		if parts := partsOf(t, p.Member, stage, scratch.Table); len(parts) != 0 {
			t.Fatalf("%s: an unreachable pull wrote %+v", source.Name(), parts)
		}
		t.Logf("%s unreachable: code %d", source.Name(), code)
	}
}

// TestPeerUnsafePullReadsTheNamedPoolCollection: the peer unsafe pull reads a
// network verifier's hg_unsafe through its pool collection
// (si_peer_verifier_<n>, contract §23.2).
func TestPeerUnsafePullReadsTheNamedPoolCollection(t *testing.T) {
	p := requirePair(t)
	sfx := suffix(t)
	schema := testSchema("pool_" + sfx)
	table := createSourceTable(t, p.Peer, p.PeerAddr, "hg_unsafe", schema)
	insertPart(t, p.Peer, "hg_unsafe", table, "a", 0, 3)
	part := partsOf(t, p.Peer, "hg_unsafe", table)[0]
	scans := scanOf(t, p.Peer, "hg_unsafe", table, schema)
	user, password := createReader(t, p, "hg_unsafe")
	n, err := strconv.ParseUint(sfx, 16, 64)
	if err != nil {
		t.Fatal(err)
	}
	collection := fmt.Sprintf("si_peer_verifier_%d", n)
	createCollection(t, p, collection, p.PeerHost, p.PeerPort, user, password)
	stage := "hg_rstage_" + sfx
	member := memberUser(t, p, stage, "", []string{collection})
	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	source := NewPeerUnsafePull(member, collection)
	if source.Name() != SourcePeerUnsafePull || source.Database != "hg_unsafe" {
		t.Fatalf("NewPeerUnsafePull = %+v, want peer_unsafe_pull on hg_unsafe", source)
	}
	need := Need{TableKey: schema.TableID, Table: table, PartitionID: "p_a", PartName: part.Name, PartPhysHash: part.Hash, PartRowLtHash: scans[part.Name].RowLtHash, RowCount: 3}
	res := source.Fetch(context.Background(), need, schema, scratch)
	if res.Outcome != OutcomeFetched || res.Err != nil {
		t.Fatalf("Fetch = %+v, want fetched", res)
	}
	requireOnePart(t, member, res, scratch, schema, need.PartRowLtHash, 3)
}
