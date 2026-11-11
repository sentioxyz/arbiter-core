package tableset

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// arbiter-core#76: a chain table whose protocol DDL this binary cannot build
// is locally unservable. It is never Ready, it is reported in Stats, and the
// pass that meets it returns no error, so the role keeps serving every other
// table instead of ending Run or failing its startup pass.

func unservableSchemas(t *testing.T) map[string]payloadexec.TableSchema {
	freeze := schemaFor(t, "freeze")
	freeze.PartitionBy = "intDiv(v, 1)" // a divisor below 2: no admission rules version admits it, so the partition freeze refuses it
	decimal := schemaFor(t, "decimal", lthash.Column{Name: "d", Type: "Decimal(18, 4)"})
	return map[string]payloadexec.TableSchema{
		UnservablePartitionFreeze:       freeze,
		UnservableUnsupportedColumnType: decimal,
	}
}

func TestReconcile_UnbuildableChainTableIsUnservableAndNotFatal(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view := newFakeView()
	r := newReconciler(t, conn, p, view, &fakeArbiter{}, nil)
	ok := schemaFor(t, "ok")
	bad := unservableSchemas(t)
	view.set(chainInc(t, 1, ok, wire.TableStatusActive),
		chainInc(t, 2, bad[UnservablePartitionFreeze], wire.TableStatusActive),
		chainInc(t, 3, bad[UnservableUnsupportedColumnType], wire.TableStatusActive))

	for pass := 0; pass < 2; pass++ {
		if err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
			t.Fatalf("pass %d: Reconcile = %v, want nil (an unservable table is not fatal)", pass, err)
		}
	}
	if !r.Ready(ok.TableID) {
		t.Fatal("the servable table is not Ready")
	}
	st := r.Stats()
	if st.States[StateUnservable] != 2 || st.States[StateReady] != 1 {
		t.Fatalf("states = %v, want 2 unservable and 1 ready", st.States)
	}
	got := map[string]UnservableTable{}
	for _, u := range st.Unservable {
		got[u.TableID] = u
	}
	for reason, s := range bad {
		if r.Ready(s.TableID) {
			t.Fatalf("%s: unservable table is Ready", reason)
		}
		u, found := got[s.TableID]
		if !found || u.Reason != reason {
			t.Fatalf("Stats().Unservable = %+v, want %s for %s", st.Unservable, reason, s.TableID)
		}
		if len(localComments(t, conn, p, s.TableID)) != 0 {
			t.Fatalf("%s: tables were created for an unservable table", reason)
		}
	}
	if !ddl.UnservableTableError(ddl.ErrPartitionFreeze) || !ddl.UnservableTableError(payloadexec.ErrUnsupportedColumnType) || ddl.UnservableTableError(ddl.ErrProtocolTableDrift) {
		t.Fatal("UnservableTableError classifies the wrong errors")
	}
}

// An upgrade that makes the table buildable is picked up by a later pass:
// the binary's DDL is stood in for by ensure, which refuses until upgraded.
func TestReconcile_UnservableTableBecomesReadyOnceBuildable(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view := newFakeView()
	r := newReconciler(t, conn, p, view, &fakeArbiter{}, nil)
	clock := pinClock(r)
	s := schemaFor(t, "later")
	var upgraded atomic.Bool
	real := r.ensure
	r.ensure = func(ctx context.Context, schema payloadexec.TableSchema, scope ddl.KeeperScope, mode ddl.Mode) error {
		if schema.TableID == s.TableID && !upgraded.Load() {
			return ddl.ErrPartitionFreeze
		}
		return real(ctx, schema, scope, mode)
	}
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	if err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
		t.Fatal(err)
	}
	if r.Ready(s.TableID) || len(r.Stats().Unservable) != 1 {
		t.Fatalf("before the upgrade: ready=%v unservable=%v", r.Ready(s.TableID), r.Stats().Unservable)
	}
	upgraded.Store(true)
	clock.advance(time.Hour) // past the backoff
	if err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
		t.Fatal(err)
	}
	if !r.Ready(s.TableID) || len(r.Stats().Unservable) != 0 || r.Stats().States[StateUnservable] != 0 {
		t.Fatalf("after the upgrade: ready=%v stats=%+v", r.Ready(s.TableID), r.Stats())
	}
}

// Node-wide and integrity errors keep their behaviour: drift on a table this
// node holds is still returned.
func TestReconcile_DriftIsStillFatal(t *testing.T) {
	conn := requireCH(t)
	p := testPinned(t, conn)
	view := newFakeView()
	r := newReconciler(t, conn, p, view, &fakeArbiter{}, nil)
	s := schemaFor(t, "drift")
	createWithComment(t, conn, p, s, "not-a-marker")
	view.set(chainInc(t, 1, s, wire.TableStatusActive))
	err := r.Reconcile(context.Background(), ddl.ModeVerifyOnly)
	if !errors.Is(err, ddl.ErrProtocolTableDrift) || !ddl.FatalReconcileError(err) {
		t.Fatalf("Reconcile = %v, want fatal drift", err)
	}
	if len(r.Stats().Unservable) != 0 {
		t.Fatalf("drift reported as unservable: %+v", r.Stats().Unservable)
	}
}
