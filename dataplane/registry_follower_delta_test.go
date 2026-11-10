package dataplane

import (
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/sentioxyz/arbiter-core/wire"
)

func chainIncPB(seq uint64, table string, st pb.TableIncarnationStatus) *pb.TableIncarnation {
	return &pb.TableIncarnation{Seq: seq, DatabaseId: "db", TableId: table, SchemaHash: "0x" + table,
		Origin: pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_CHAIN, Status: st}
}

const (
	statusActive   = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_ACTIVE
	statusRetiring = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_RETIRING
	statusPending  = pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PENDING
)

// registryDeltaPB is a delta over base: version, the listed incarnations
// and the total count.
func registryDeltaPB(base, version, count uint64, incs ...*pb.TableIncarnation) *pb.TableRegistrySnapshot {
	return &pb.TableRegistrySnapshot{Version: version, Seeded: true, DeltaBaseVersion: base, IncarnationCount: count, Incarnations: incs}
}

func TestApplyRegistryDelta(t *testing.T) {
	base := &pb.TableRegistrySnapshot{Version: 4, Seeded: true, Incarnations: []*pb.TableIncarnation{
		chainIncPB(1, "a", statusActive), chainIncPB(2, "b", statusActive), chainIncPB(3, "c", statusActive)}}
	got, err := applyRegistryDelta(base, registryDeltaPB(4, 5, 4, chainIncPB(2, "b", statusRetiring), chainIncPB(4, "d", statusPending)))
	if err != nil {
		t.Fatal(err)
	}
	if got.GetVersion() != 5 || got.GetDeltaBaseVersion() != 0 || got.GetIncarnationCount() != 0 || len(got.GetIncarnations()) != 4 {
		t.Fatalf("merged = %+v", got)
	}
	for i, want := range []pb.TableIncarnationStatus{statusActive, statusRetiring, statusActive, statusPending} {
		if inc := got.GetIncarnations()[i]; inc.GetSeq() != uint64(i)+1 || inc.GetStatus() != want {
			t.Fatalf("incarnation %d = %+v, want status %v", i+1, inc, want)
		}
	}
	if got.GetIncarnations()[0] != base.GetIncarnations()[0] || base.GetIncarnations()[1].GetStatus() != statusActive || len(base.GetIncarnations()) != 3 {
		t.Fatal("an unchanged incarnation must be shared and the base left as it was")
	}
	for name, tc := range map[string]struct {
		base  *pb.TableRegistrySnapshot
		delta *pb.TableRegistrySnapshot
		want  string
	}{
		"first message":      {nil, registryDeltaPB(4, 5, 3), "first message"},
		"other base":         {base, registryDeltaPB(3, 5, 3), "follows version 4"},
		"fewer incarnations": {base, registryDeltaPB(4, 5, 2), "has 2 incarnations"},
		"seq past the count": {base, registryDeltaPB(4, 5, 3, chainIncPB(4, "d", statusPending)), "seq 4"},
		"seq out of order":   {base, registryDeltaPB(4, 5, 3, chainIncPB(2, "b", statusActive), chainIncPB(1, "a", statusActive)), "seq 1 after 2"},
		"missing new one":    {base, registryDeltaPB(4, 5, 5, chainIncPB(4, "d", statusPending)), "incarnation 5 of 5 unset"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := applyRegistryDelta(tc.base, tc.delta); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestRegistryFollower_AppliesDeltasAndResyncsOnAMismatch: the watch asks for
// deltas, a delta over the stream's previous message updates the view, and a
// delta that does not apply ends the stream, which reopens from the accepted
// version and accepts the full snapshot it starts with.
func TestRegistryFollower_AppliesDeltasAndResyncsOnAMismatch(t *testing.T) {
	r := newFakeRegistry()
	r.getSnap = registrySnapshotPB(3)
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), discardLogger())
	runFollower(t, f)
	waitChanged(t, f.Ready(), "ready")

	changed := f.Changed()
	r.sends <- registrySnapshotPB(4)
	waitChanged(t, changed, "full version 4")
	changed = f.Changed()
	retired := registrySnapshotPB(5).GetIncarnations()[0]
	retired.Status = statusRetiring
	r.sends <- registryDeltaPB(4, 5, 2, retired, chainIncPB(2, "n", statusPending))
	waitChanged(t, changed, "delta version 5")
	snap, _ := f.View()
	if snap.Version != 5 || len(snap.Incarnations) != 2 || snap.Incarnations[0].Status != wire.TableStatusRetiring || snap.Incarnations[1].Status != wire.TableStatusPending {
		t.Fatalf("view after the delta = %+v", snap)
	}

	// A delta over a version the stream never sent: resync.
	r.sends <- registryDeltaPB(9, 10, 2)
	waitWatchRequests(t, r, 2)
	if snap, _ := f.View(); snap.Version != 5 {
		t.Fatalf("a delta that does not apply moved the view to %d", snap.Version)
	}
	changed = f.Changed()
	full := registrySnapshotPB(10)
	full.Incarnations = append(full.Incarnations, chainIncPB(2, "n", statusActive))
	r.sends <- full
	waitChanged(t, changed, "full version 10 after the resync")
	if snap, _ := f.View(); snap.Version != 10 || len(snap.Incarnations) != 2 {
		t.Fatalf("view after the resync = %+v", snap)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if got := r.watchReqs; len(got) != 2 || got[0] != 3 || got[1] != 5 {
		t.Fatalf("watch since_version = %v, want [3 5]", got)
	}
	for i, d := range r.watchDelt {
		if !d {
			t.Fatalf("watch %d did not ask for deltas", i+1)
		}
	}
}

// TestRegistryFollower_ResyncErrorIsRetryable: the resync ends only the
// stream, never Run.
func TestRegistryFollower_ResyncErrorIsRetryable(t *testing.T) {
	if !retryableStatus(status.Code(errRegistryResync)) || status.Code(errRegistryResync) != codes.Unavailable {
		t.Fatalf("resync error %v must be retryable", errRegistryResync)
	}
}
