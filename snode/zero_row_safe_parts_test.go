package snode

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

// The hg_safe side of zero-row parts (arbiter-core#88). ClickHouse 26.8's
// REPLACE PARTITION leaves an empty part covering the replaced block range; a
// restart loads it as active until remove_empty_parts retires it, and an
// ATTACH PARTITION FROM in that window copies it into the shadow. These tests
// reuse zeroPartConn: its p0_73_73_1 is such a covering part.

func TestSNodeSafePartitionPartsIgnoreEmptyCoveringParts(t *testing.T) {
	r, c, _, schema := zeroPartsFixture(t)
	got, err := r.safePartitionParts(t.Context(), schema, CHTableName(schema.TableID), "p_p0")
	if err != nil {
		t.Fatal(err)
	}
	if names := activePartNames(got); !slices.Equal(names, []string{"p0_77_77_0"}) {
		t.Fatalf("audited parts %v, want only the part with rows", names)
	}
	// Against an inventory that acknowledges the part with rows, the audit is
	// clean: the covering part is neither an unacknowledged part nor a twin.
	ref := ownerAuditReference{Source: "local", Seq: 1, Parts: []ownerAuditPart{{Name: "p0_77_77_0", PhysHash: c.parts[1].PhysHash}}}
	if plan := planOwnerAudit(ref, true, got); plan.Reason != "" || len(plan.Drop) != 0 {
		t.Fatalf("plan %+v, want a clean partition", plan)
	}
}

func TestSNodeSafeMappingsLeaveEmptyCoveringPartsOutOfTheInventory(t *testing.T) {
	r, c, _, schema := zeroPartsFixture(t)
	table := CHTableName(schema.TableID)
	safe := r.cfg.SafeDatabase + "." + table
	post, err := r.partitionContentRoot(t.Context(), r.cfg.SafeDatabase, table, schema, "p_p0")
	if err != nil {
		t.Fatal(err)
	}
	c.scanned = nil
	cmd := arbiter.PromoteSafePartition{TableID: schema.TableID, PartitionID: "p_p0", PromotionSeq: 1}
	got, err := r.safeMappings(t.Context(), safe, table, schema, cmd, post)
	if err != nil {
		t.Fatalf("safeMappings with an active empty covering part: %v", err)
	}
	if len(got.Partition) != 1 || got.Partition[0].SafePartName != "p0_77_77_0" {
		t.Fatalf("inventory %+v, want only the part with rows", got.Partition)
	}
	if !slices.Equal(c.scanned, []string{"p0_77_77_0"}) {
		t.Fatalf("scanned %v, want only the part with rows", c.scanned)
	}
}

func TestSNodeShadowScanLeavesEmptyCoveringPartsOut(t *testing.T) {
	r, c, _, schema := zeroPartsFixture(t)
	// Two covering parts: both have the empty row LtHash, which the shadow's
	// duplicate check must not read as the same rows twice.
	c.parts = append([]partInfo{{Name: "p0_70_70_1", PartitionValue: "p0", Rows: 0}}, c.parts...)
	_, parts, err := r.partitionContentScan(t.Context(), r.cfg.PromoteDatabase, CHTableName(schema.TableID), schema, "p_p0")
	if err != nil {
		t.Fatal(err)
	}
	if len(parts) != 1 || parts[0].Name != "p0_77_77_0" {
		t.Fatalf("shadow parts %+v, want only the part with rows", parts)
	}
	if err := duplicateShadowPartHash(arbiter.PromoteSafePartition{}, parts); err != nil {
		t.Fatal(err)
	}
}

func TestSNodePublishShadow(t *testing.T) {
	t.Run("a shadow with parts is published with REPLACE PARTITION", func(t *testing.T) {
		r, c, _, schema := zeroPartsFixture(t)
		table := CHTableName(schema.TableID)
		safe, promote := r.cfg.SafeDatabase+"."+table, r.cfg.PromoteDatabase+"."+table
		if err := r.publishShadow(t.Context(), schema, table, safe, promote, "p_p0", "'p0'", []shadowPart{{Name: "p0_77_77_0"}}); err != nil {
			t.Fatal(err)
		}
		want := fmt.Sprintf("ALTER TABLE %s REPLACE PARTITION 'p0' FROM %s", safe, promote)
		if !slices.Equal(c.dropped, []string{want}) {
			t.Fatalf("statements %v, want %q", c.dropped, want)
		}
	})
	t.Run("an empty shadow drops the safe partition instead", func(t *testing.T) {
		// ClickHouse 26.6+ refuses REPLACE PARTITION from a source with no
		// parts (code 36); 25.8 and 26.3 cleared the destination.
		r, c, _, schema := zeroPartsFixture(t)
		table := CHTableName(schema.TableID)
		safe, promote := r.cfg.SafeDatabase+"."+table, r.cfg.PromoteDatabase+"."+table
		if err := r.publishShadow(t.Context(), schema, table, safe, promote, "p_p0", "'p0'", nil); err != nil {
			t.Fatal(err)
		}
		if len(c.dropped) != 1 || !strings.Contains(c.dropped[0], "DROP PARTITION 'p0'") || !strings.Contains(c.dropped[0], r.cfg.SafeDatabase+".") {
			t.Fatalf("statements %v, want one DROP PARTITION of the safe table and no REPLACE", c.dropped)
		}
	})
}

// TestPromotion_EmptyShadowClearsTheSafePartitionOnEveryServer runs
// publishShadow against the server CI starts (25.8 and 26.8): an empty shadow
// leaves the safe partition empty without an error on both, where REPLACE
// PARTITION from it fails on 26.8 with code 36.
func TestPromotion_EmptyShadowClearsTheSafePartitionOnEveryServer(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	schema := promoteSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	setUniqueDatabases(t, &cfg)
	role, _ := newIntakeHarness(t, conn, cfg)
	createSNodeTables(t, conn, role.cfg, schema)
	table := CHTableName(schema.TableID)
	safe, promote := role.cfg.SafeDatabase+"."+table, role.cfg.PromoteDatabase+"."+table
	mustExecIntake(t, conn, "INSERT INTO "+safe+" (p, v) VALUES ('p0', 1), ('p1', 2)")
	partition, err := quotePartition(schema, "p_p0")
	if err != nil {
		t.Fatal(err)
	}
	_, shadow, err := role.partitionContentScan(ctx, role.cfg.PromoteDatabase, table, schema, "p_p0")
	if err != nil || len(shadow) != 0 {
		t.Fatalf("shadow %v, err %v: want an empty shadow", shadow, err)
	}
	if err := role.publishShadow(ctx, schema, table, safe, promote, "p_p0", partition, shadow); err != nil {
		t.Fatalf("publish an empty shadow: %v", err)
	}
	for id, want := range map[string]int{"p_p0": 0, "p_p1": 1} {
		parts, err := role.safePartitionParts(ctx, schema, table, id)
		if err != nil {
			t.Fatal(err)
		}
		if len(parts) != want {
			t.Fatalf("safe partition %s holds %d parts with rows, want %d", id, len(parts), want)
		}
	}
}
