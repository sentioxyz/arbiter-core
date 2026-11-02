package sources

import (
	"context"
	"os"
	"slices"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// requireHardlinkKeeper skips the hard-link test on a ClickHouse without
// Keeper (ReplicatedMergeTree needs one) and fails it under
// ARBITER_CH_KEEPER=1, which CI sets.
func requireHardlinkKeeper(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but ClickHouse has no Keeper: %v", err)
		}
		t.Skipf("ClickHouse has no Keeper configured; skipping: %v", err)
	}
}

// TestHardlinkSourceAttachesFromMemberUnsafeAndKeepsHashOfAllFiles
// (ClickHouse and Keeper): the member's hg_member_unsafe replica, made with
// ddl.MemberUnsafeIntent at an incarnation-scoped Keeper path, holds two parts
// of partition a and one of b. As a user with si_member's rights on
// hg_rstage and SELECT on hg_member_unsafe, the hard-link source fills a
// fresh scratch with exactly the needed part, whose hash_of_all_files and
// row LtHash are the replica's; the replica keeps every part; a need the
// replica does not hold, or a table it does not have, is missed and writes
// nothing.
func TestHardlinkSourceAttachesFromMemberUnsafeAndKeepsHashOfAllFiles(t *testing.T) {
	admin, addr := memberServer(t)
	requireHardlinkKeeper(t, admin)
	sfx := suffix(t)
	schema := testSchema("dex_" + sfx)
	unsafeDB, stage := "hg_member_unsafe_"+sfx, "hg_rstage_"+sfx
	pinned := ddl.MemberPinned{MemberUnsafeDB: unsafeDB, ReplicaDB: "hg_replica_" + sfx, StageDB: stage, NodeID: "member-test"}
	intent, err := ddl.MemberUnsafeIntent(pinned, schema, 7, ddl.ZooKeeperPathFor(0, schema.TableID, ddl.KeeperScope{IncarnationSeq: 7, Scoped: true}))
	if err != nil {
		t.Fatal(err)
	}
	sourceDatabase(t, admin, addr, unsafeDB)
	cleanup(t, addr, "DROP TABLE IF EXISTS "+ident(unsafeDB)+"."+ident(intent.Table)+" SYNC")
	mustExec(t, admin, intent.SQL())
	insertPart(t, admin, unsafeDB, intent.Table, "a", 0, 3)
	insertPart(t, admin, unsafeDB, intent.Table, "a", 100, 2)
	insertPart(t, admin, unsafeDB, intent.Table, "b", 200, 4)
	before := partsOf(t, admin, unsafeDB, intent.Table)
	scans := scanOf(t, admin, unsafeDB, intent.Table, schema)
	var needed partRow
	for _, p := range before {
		if p.Rows == 3 {
			needed = p
		}
	}
	if len(before) != 3 || needed.Name == "" {
		t.Fatalf("hg_member_unsafe holds %+v, want three parts with one of 3 rows", before)
	}

	mustExec(t, admin, "CREATE DATABASE IF NOT EXISTS "+ident(stage))
	cleanup(t, addr, "DROP DATABASE IF EXISTS "+ident(stage)+" SYNC")
	user := "src_hardlink_" + sfx
	password := createUser(t, admin, addr, user, "",
		"GRANT SELECT, INSERT, ALTER, CREATE TABLE, DROP TABLE ON "+ident(stage)+".* TO %s",
		"GRANT TABLE ENGINE ON MergeTree TO %s",
		"GRANT SELECT ON system.parts TO %s",
		"GRANT SELECT ON "+ident(unsafeDB)+".* TO %s")
	member := chtest.OpenAs(t, addr, user, password)
	src := NewMemberUnsafeHardlink(member, unsafeDB)
	if src.Name() != SourceMemberUnsafeHardlink || SourceMemberUnsafeHardlink != "member_unsafe_hardlink" {
		t.Fatalf("Name = %s", src.Name())
	}
	need := Need{
		TableKey: schema.TableID, Table: intent.Table, PartitionID: "p_a", PartName: needed.Name,
		PartPhysHash: needed.Hash, PartRowLtHash: scans[needed.Name].RowLtHash, RowCount: 3,
	}
	scratch := newScratch(t, member, stage, "member-test_"+sfx+"_1", schema)
	res := src.Fetch(context.Background(), need, schema, scratch)
	if res.Outcome != OutcomeFetched || res.Err != nil || res.Source != SourceMemberUnsafeHardlink {
		t.Fatalf("Fetch = %+v, want fetched", res)
	}
	requireOnePart(t, member, res, scratch, schema, need.PartRowLtHash, 3)
	if got := partsOf(t, admin, stage, scratch.Table); got[0].Hash != needed.Hash {
		t.Fatalf("the scratch part's hash_of_all_files is %s, the replica's is %s", got[0].Hash, needed.Hash)
	}
	if after := partsOf(t, admin, unsafeDB, intent.Table); !slices.Equal(after, before) {
		t.Fatalf("hg_member_unsafe changed: %+v, was %+v", after, before)
	}

	missed := need
	missed.PartPhysHash = "0000000000000000000000000000000000000000000000000000000000000000"
	empty := newScratch(t, member, stage, "member-test_"+sfx+"_2", schema)
	if res := src.Fetch(context.Background(), missed, schema, empty); res.Outcome != OutcomeMissed || res.Err != nil {
		t.Fatalf("Fetch of a hash the replica does not hold = %+v, want missed", res)
	}
	noTable := need
	noTable.Table = "dex_" + sfx + "__absent"
	if res := src.Fetch(context.Background(), noTable, schema, empty); res.Outcome != OutcomeMissed || res.Err != nil {
		t.Fatalf("Fetch from a table hg_member_unsafe does not have = %+v, want missed", res)
	}
	if got := partsOf(t, admin, stage, empty.Table); len(got) != 0 {
		t.Fatalf("a missed fetch wrote %+v", got)
	}
}
