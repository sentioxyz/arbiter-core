package tableset

import (
	"context"
	"fmt"
	"slices"
	"testing"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// setSnapshot installs s, with its owners, as the next version.
func (v *fakeView) setSnapshot(s wire.TableRegistrySnapshot) {
	v.mu.Lock()
	defer v.mu.Unlock()
	s.Version = v.snap.Version + 1
	v.snap, v.enabled = s, true
	close(v.changed)
	v.changed = make(chan struct{})
}

func TestReconciler_OwnerFilterMaterialisesAndPurgesOnlyOwnedKeys(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view, arb := newFakeView(), &fakeArbiter{}
	one, two := uint64(1), uint64(2)
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.Owner = &one })
	mine, theirs, theirsPurging := schemaFor(t, "mine"), schemaFor(t, "theirs"), schemaFor(t, "gone")
	incMine := chainInc(t, 1, mine, wire.TableStatusActive)
	incMine.OwnerIndexerID = &one
	incTheirs := chainInc(t, 2, theirs, wire.TableStatusActive)
	incTheirs.OwnerIndexerID = &two
	incGone := chainInc(t, 3, theirsPurging, wire.TableStatusPurging)
	incGone.OwnerIndexerID = &two
	view.setSnapshot(wire.TableRegistrySnapshot{Seeded: true, Incarnations: []wire.TableIncarnation{incMine, incTheirs, incGone}})
	mustReconcile(t, r)
	if !r.Ready(mine.TableID) || len(localComments(t, conn, p, mine.TableID)) != 3 {
		t.Fatal("the owned table must be created and verified")
	}
	if r.Ready(theirs.TableID) || len(localComments(t, conn, p, theirs.TableID)) != 0 {
		t.Fatal("another indexer's table must never be created on this SNode")
	}
	if got := arb.reported(); len(got) != 0 {
		t.Fatalf("reported %v: another indexer's purge is never this SNode's to report", got)
	}
	if st := r.Stats(); stateCount(st) != 1 || st.States[StateReady] != 1 {
		t.Fatalf("stats = %+v: only the owned key has a state", st)
	}
}

// TestReconciler_OwnerFilterNeverSweepsAnotherOwnersKeeperPath: the Keeper
// root is shared by every owner (spec D11), so a source's sweep of
// decommissioned replicas must stay on its own keys.
func TestReconciler_OwnerFilterNeverSweepsAnotherOwnersKeeperPath(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	replicaConn := requireReplicaCH(t)
	p := testPinned(t, conn, replicaConn)
	view, arb := newFakeView(), &fakeArbiter{}
	one, two := uint64(1), uint64(2)
	r := newReconciler(t, conn, p, view, arb, func(c *Config, _ *Deps) { c.SweepDecommissioned, c.Owner = true, &one })
	theirs := schemaFor(t, "theirs")
	other := p
	other.UnsafeDB, other.SafeDB, other.PromoteDB, other.NodeID = p.UnsafeDB+"_o", p.SafeDB+"_o", p.PromoteDB+"_o", "snode-other"
	t.Cleanup(func() {
		for _, db := range []string{other.UnsafeDB, other.SafeDB, other.PromoteDB} {
			_ = replicaConn.Exec(ctx, "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	if err := ddl.EnsureTable(ctx, replicaConn, other, theirs, 1, ddl.ModeCreateAndVerify); err != nil {
		t.Fatal(err)
	}
	if err := replicaConn.Exec(ctx, fmt.Sprintf("DETACH TABLE %s.%s", other.UnsafeDB, ddl.CHTableName(theirs.TableID))); err != nil {
		t.Fatal(err)
	}
	inc := chainInc(t, 1, theirs, wire.TableStatusPurged)
	inc.OwnerIndexerID = &two
	view.setSnapshot(wire.TableRegistrySnapshot{Seeded: true, Incarnations: []wire.TableIncarnation{inc}})
	mustReconcile(t, r)
	replicas, err := ddl.KeeperReplicas(ctx, conn, p, theirs.TableID)
	if err != nil || !slices.Equal(replicas, []string{"snode-other"}) {
		t.Fatalf("replicas = %v, %v: another owner's Keeper path must be left alone", replicas, err)
	}
}
