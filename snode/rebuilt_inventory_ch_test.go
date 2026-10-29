package snode

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/housegate/housegate/pkg/replay/chexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// TestRebuildThenOwnerAuditIsCleanAndTheNextPromotionSucceeds is the SNode's
// side of a same-identity owner rebuild (plan S3-D contract §34,
// clarification K52), in repair mode against the arbiter's SafeParts: the
// source ClickHouse's hg_safe is lost and restored the way package
// snode/rebuild restores it (a fresh table, the own hg_unsafe parts attached
// through a __rebuild_ scratch in hg_promote, so every part has a new name),
// the journal records the restored inventory with
// RecordRebuiltSafeInventories while no SNode runs, and the restarted SNode
// then finds hg_safe clean, drops nothing, and applies the next promotion on
// the exact base; that promotion's inventory replaces the rebuilt one.
// (snode/rebuild imports this package, so the restore is spelled out here and
// snode/rebuild's own tests prove Rebuild produces this state.)
func TestRebuildThenOwnerAuditIsCleanAndTheNextPromotionSucceeds(t *testing.T) {
	ctx := context.Background()
	fsm := &fsmPartitionsFakeS{}
	f := newOwnerAuditFixtureS(t, requireCH(t), OwnerAuditRepair, fsm)
	for seq := uint64(1); seq <= 2; seq++ {
		part := f.submit(ctx, seq)
		if err := f.promote(ctx, seq, part); err != nil {
			t.Fatalf("promotion %d: %v", seq, err)
		}
	}
	acked := f.lastAck()
	fsm.set(fsmStateOfAckS(acked))
	var acknowledged []string
	for _, m := range acked.SafePartitionParts {
		acknowledged = append(acknowledged, m.SafePartName)
	}

	// The loss: hg_safe is gone; the journal and hg_unsafe's promoted parts
	// (their cleanups never ran) survive.
	table := f.table()
	_, safeIntent, _, err := ddl.Intents(f.role.pinned(), f.schema)
	if err != nil {
		t.Fatal(err)
	}
	mustExecIntake(t, f.conn, fmt.Sprintf("DROP TABLE %s SYNC", f.safe()))
	mustExecIntake(t, f.conn, safeIntent.SQL())
	// The restore, as snode/rebuild runs it from the own hg_unsafe replica.
	partition, err := quotePartition(f.schema, auditPartitionS)
	if err != nil {
		t.Fatal(err)
	}
	scratch := f.role.cfg.PromoteDatabase + "." + table + "__rebuild_1"
	mustExecIntake(t, f.conn, fmt.Sprintf("CREATE TABLE %s AS %s", scratch, f.safe()))
	mustExecIntake(t, f.conn, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s.%s", scratch, partition, f.role.cfg.UnsafeDatabase, table))
	mustExecIntake(t, f.conn, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", f.safe(), partition, scratch))
	mustExecIntake(t, f.conn, fmt.Sprintf("DROP TABLE %s SYNC", scratch))
	restored := f.safeParts(ctx)
	scans, err := chexec.ScanAllParts(ctx, f.conn, f.safe(), f.schema)
	if err != nil {
		t.Fatal(err)
	}
	rows := map[string]string{}
	for _, s := range scans {
		rows[s.PartName] = s.RowLtHash
	}
	inv := RebuiltSafeInventory{TableID: f.schema.TableID, PartitionID: auditPartitionS, PublishSeq: acked.PromotionSeq}
	var names []string
	for _, p := range restored {
		inv.Parts = append(inv.Parts, arbiter.SafePartMapping{PartRowLtHash: rows[p.Name], SafePartName: p.Name, PartPhysHash: p.PhysHash})
		names = append(names, p.Name)
	}
	slices.Sort(acknowledged)
	slices.Sort(names)
	if slices.Equal(names, acknowledged) {
		t.Fatalf("the restore kept the acknowledged names %v: the fixture must rename", names)
	}
	if err := RecordRebuiltSafeInventories(f.role.cfg.StateDir, []RebuiltSafeInventory{inv}); err != nil {
		t.Fatal(err)
	}
	restarted, err := New(f.role.cfg, f.role.d)
	if err != nil {
		t.Fatal(err)
	}
	f.role = restarted

	f.role.runOwnerAuditPass(ctx)

	if stats := f.role.OwnerAuditStats(); len(stats.Unclean) != 0 || stats.DroppedTwins != 0 || !f.role.OwnerAuditClean(f.schema.TableID) {
		t.Fatalf("the rebuilt hg_safe is not clean: %+v", stats)
	}
	if got := f.safeParts(ctx); !slices.Equal(got, restored) {
		t.Fatalf("the audit changed hg_safe: %+v -> %+v", restored, got)
	}
	third := f.submit(ctx, 3)
	if err := f.promote(ctx, 3, third); err != nil {
		t.Fatalf("the first promotion after the rebuild: %v", err)
	}
	ack := f.lastAck()
	if !ack.Applied || ack.PromotionSeq != 3 || len(ack.SafePartitionParts) != 3 {
		t.Fatalf("ack = %+v, want promotion 3 applied over the three rebuilt and promoted parts", ack)
	}
	assertCompleteSafeInventory(t, ctx, f.role, f.schema, auditPartitionS, ack.SafePartitionParts)
	k := partitionKey{Table: f.schema.TableID, Partition: auditPartitionS}
	if got, ok := f.role.state.AckedSafeInventory(k); !ok || got.Rebuilt || got.PromotionSeq != 3 {
		t.Fatalf("acknowledged inventory = %+v, %v; the promotion must replace the rebuilt one", got, ok)
	}
	fsm.set(fsmStateOfAckS(ack))
	f.role.runOwnerAuditPass(ctx)
	if stats := f.role.OwnerAuditStats(); len(stats.Unclean) != 0 {
		t.Fatalf("after the promotion the audit is unclean: %+v", stats)
	}
}
