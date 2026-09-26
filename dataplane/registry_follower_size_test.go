package dataplane

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// grpcDefaultMaxRecvMsgSize is gRPC-Go's client receive default, the limit
// the data-plane client used before it set its own.
const grpcDefaultMaxRecvMsgSize = 4 << 20

func sizeTestEventRef(block uint64) *pb.L2EventRef {
	return &pb.L2EventRef{BlockNumber: block, BlockHash: "0x" + strings.Repeat("ab", 32), LogIndex: 7, TxHash: "0x" + strings.Repeat("cd", 32)}
}

// largeRegistrySnapshotPB returns a registry snapshot of at least minBytes
// encoded bytes: the genesis incarnation of registrySnapshotPB followed by
// Purged chain incarnations shaped like production ones (three event refs,
// 20-column schema_json, five purgers; about 1.6 KiB each), which the arbiter
// retains forever.
func largeRegistrySnapshotPB(t *testing.T, version uint64, minBytes int) *pb.TableRegistrySnapshot {
	t.Helper()
	cols := make([]string, 20)
	for i := range cols {
		cols[i] = fmt.Sprintf(`{"name":"column_name_%02d","type":"DateTime64(3, 'UTC')"}`, i)
	}
	snap := registrySnapshotPB(version)
	for proto.Size(snap) < minBytes {
		seq := uint64(len(snap.Incarnations)) + 1
		table := fmt.Sprintf("events_%06d", seq)
		snap.Incarnations = append(snap.Incarnations, &pb.TableIncarnation{
			Seq: seq, DatabaseId: "processor_db_0001", TableId: table,
			Origin:  pb.TableIncarnationOrigin_TABLE_INCARNATION_ORIGIN_CHAIN,
			Status:  pb.TableIncarnationStatus_TABLE_INCARNATION_STATUS_PURGED,
			Created: sizeTestEventRef(1000 + seq), SchemaRef: sizeTestEventRef(1001 + seq), SchemaVersion: 1,
			SchemaHash: "0x" + strings.Repeat("ef", 32),
			SchemaJson: `{"table_id":"processor_db_0001.` + table + `","partition_by":"toYYYYMM(column_name_00)","columns":[` + strings.Join(cols, ",") + `]}`,
			Deleted:    sizeTestEventRef(2000 + seq), RetireReason: pb.TableRetireReason_TABLE_RETIRE_REASON_TABLE_DELETED,
			AddBlockSeq: 100 + seq, RetireBlockSeq: 200 + seq,
			PurgedBy: []string{"snode-0", "snode-1", "snode-2", "verifier-0", "verifier-1"},
		})
	}
	return snap
}

func newSizedTestClient(t *testing.T, maxRecv int, peers ...Peer) *Client {
	t.Helper()
	c, err := New(Config{Peers: peers, RetryBackoffMin: 5 * time.Millisecond, RetryBackoffMax: 20 * time.Millisecond, MaxRecvMsgSize: maxRecv})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func TestClient_DefaultMaxRecvMsgSize(t *testing.T) {
	c, err := New(Config{Peers: []Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if c.cfg.MaxRecvMsgSize != DefaultMaxRecvMsgSize {
		t.Fatalf("default MaxRecvMsgSize = %d, want %d", c.cfg.MaxRecvMsgSize, DefaultMaxRecvMsgSize)
	}
	if DefaultMaxRecvMsgSize <= grpcDefaultMaxRecvMsgSize {
		t.Fatalf("DefaultMaxRecvMsgSize %d must exceed gRPC's default %d", DefaultMaxRecvMsgSize, grpcDefaultMaxRecvMsgSize)
	}
}

// A registry past gRPC's 4 MiB default is received by Get and by Watch with
// the default client configuration.
func TestRegistryFollower_ReceivesSnapshotsLargerThanGRPCDefault(t *testing.T) {
	get := largeRegistrySnapshotPB(t, 1, grpcDefaultMaxRecvMsgSize+(1<<20))
	watch := largeRegistrySnapshotPB(t, 2, grpcDefaultMaxRecvMsgSize+(2<<20))

	r := newFakeRegistry()
	r.getSnap = get
	f := NewRegistryFollower(newTestClient(t, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), discardLogger())
	runFollower(t, f)

	waitChanged(t, f.Ready(), "ready")
	snap, enabled := f.View()
	if !enabled || snap.Version != 1 || len(snap.Incarnations) != len(get.Incarnations) {
		t.Fatalf("view after Get: version=%d incarnations=%d enabled=%v, want version 1 with %d", snap.Version, len(snap.Incarnations), enabled, len(get.Incarnations))
	}
	changed := f.Changed()
	r.sends <- watch
	waitChanged(t, changed, "oversized watch snapshot")
	snap, _ = f.View()
	if snap.Version != 2 || len(snap.Incarnations) != len(watch.Incarnations) {
		t.Fatalf("view after Watch: version=%d incarnations=%d, want version 2 with %d", snap.Version, len(snap.Incarnations), len(watch.Incarnations))
	}
	t.Logf("received snapshots of %d and %d bytes (%d and %d incarnations)", proto.Size(get), proto.Size(watch), len(get.Incarnations), len(watch.Incarnations))
}

// Pinning the old 4 MiB limit reproduces the failure at startup, where the
// follower has no view to fall back to: Run returns ResourceExhausted and
// names the knob.
func TestRegistryFollower_StartupSnapshotOverLimitFails(t *testing.T) {
	r := newFakeRegistry()
	r.getSnap = largeRegistrySnapshotPB(t, 1, grpcDefaultMaxRecvMsgSize+(1<<20))
	f := NewRegistryFollower(newSizedTestClient(t, grpcDefaultMaxRecvMsgSize, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), discardLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := f.Run(ctx)
	if status.Code(err) != codes.ResourceExhausted || !strings.Contains(err.Error(), "MaxRecvMsgSize") {
		t.Fatalf("Run = %v, want ResourceExhausted naming MaxRecvMsgSize", err)
	}
	select {
	case <-f.Ready():
		t.Fatal("a failed startup Get must not close Ready")
	default:
	}
}

// A watched snapshot over the limit does not stop the follower: it keeps the
// last accepted version, retries the watch, and accepts the next version that
// fits.
func TestRegistryFollower_OversizedWatchSnapshotKeepsViewAndRetries(t *testing.T) {
	const limit = 64 << 10
	r := newFakeRegistry()
	r.getSnap = registrySnapshotPB(1)
	f := NewRegistryFollower(newSizedTestClient(t, limit, Peer{ID: "n1", GRPCAddr: startRegistryPeer(t, r)}), discardLogger())
	f.oversizeRetry = 10 * time.Millisecond
	runFollower(t, f) // its cleanup requires Run to end with context.Canceled

	waitChanged(t, f.Ready(), "ready")
	r.sends <- largeRegistrySnapshotPB(t, 2, 2*limit)
	// The reconnect proves Run survived the ResourceExhausted Recv.
	waitWatchRequests(t, r, 2)
	if snap, enabled := f.View(); !enabled || snap.Version != 1 {
		t.Fatalf("view after oversized snapshot = version %d enabled=%v, want version 1", snap.Version, enabled)
	}
	if got := r.watchRequests(); got[1] != 1 {
		t.Fatalf("reconnect since_version = %d, want 1", got[1])
	}
	changed := f.Changed()
	r.sends <- registrySnapshotPB(3)
	waitChanged(t, changed, "version 3 after the oversized one")
	if snap, _ := f.View(); snap.Version != 3 {
		t.Fatalf("view = version %d, want 3", snap.Version)
	}
}
