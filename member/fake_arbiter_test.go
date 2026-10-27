package member

import (
	"context"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// readRPCs are the only methods a P2 member may call (contract §20.1).
var readRPCs = []string{
	"/arbiter.SafeState/GetSafeWatermark",
	"/arbiter.SafeState/GetManifest",
	"/arbiter.SafeState/GetBlockVerification",
	"/arbiter.TableRegistry/GetTableRegistry",
	"/arbiter.TableRegistry/WatchTableRegistry",
	"/arbiter.TableRegistry/GetPurgeNodeSet",
	"/arbiter.PayloadStore/FetchPayloads",
}

const (
	testNetworkID        = "s3c2-member-test"
	testSchemaSnapshotID = "schema-genesis"
	testExecutorProfile  = "housegate-replay-mvp-v0"
	testPayload          = "member-test-payload"
)

// fakeArbiter serves the arbiter's data-plane listener and the DA data
// address on one port. Its interceptors record every method and fail the
// test on any method outside readRPCs.
type fakeArbiter struct {
	pb.UnimplementedSafeStateServer
	pb.UnimplementedTableRegistryServer
	pb.UnimplementedPayloadStoreServer

	t    *testing.T
	addr string

	mu        sync.Mutex
	watermark *pb.SafeWatermark
	manifests map[string]*pb.SafeSnapshotManifest
	registry  *pb.TableRegistrySnapshot
	purge     []string
	calls     map[string]int
}

func newFakeArbiter(t *testing.T) *fakeArbiter {
	t.Helper()
	f := &fakeArbiter{t: t, manifests: map[string]*pb.SafeSnapshotManifest{}, calls: map[string]int{}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer(
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, h grpc.UnaryHandler) (any, error) {
			if err := f.record(info.FullMethod); err != nil {
				return nil, err
			}
			return h(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, h grpc.StreamHandler) error {
			if err := f.record(info.FullMethod); err != nil {
				return err
			}
			return h(srv, ss)
		}),
	)
	pb.RegisterSafeStateServer(srv, f)
	pb.RegisterTableRegistryServer(srv, f)
	pb.RegisterPayloadStoreServer(srv, f)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	f.addr = ln.Addr().String()
	return f
}

func (f *fakeArbiter) record(method string) error {
	f.mu.Lock()
	f.calls[method]++
	f.mu.Unlock()
	if !slices.Contains(readRPCs, method) {
		f.t.Errorf("the member called %s, which is not one of the read RPCs of contract §20.1", method)
		return status.Errorf(codes.PermissionDenied, "%s is not a member read", method)
	}
	return nil
}

// called returns how often method was called.
func (f *fakeArbiter) called(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}

// client is a dataplane.Client on the fake.
func (f *fakeArbiter) client(t *testing.T) *dataplane.Client {
	t.Helper()
	c, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "arbiter-1", GRPCAddr: f.addr}}, RetryBackoffMax: 100 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// publish stores m and makes it the watermark.
func (f *fakeArbiter) publish(m replay.SafeSnapshotManifest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.manifests[m.SnapshotID] = wire.ManifestToPB(m)
	f.watermark = &pb.SafeWatermark{SnapshotId: m.SnapshotID, SafeBlockSeq: m.SafeBlockSeq, ManifestRoot: m.ManifestRoot}
}

// store stores m without moving the watermark.
func (f *fakeArbiter) store(m replay.SafeSnapshotManifest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.manifests[m.SnapshotID] = wire.ManifestToPB(m)
}

func (f *fakeArbiter) setRegistry(snap wire.TableRegistrySnapshot) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.registry = wire.TableRegistrySnapshotToPB(snap)
}

func (f *fakeArbiter) setPurge(nodes ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purge = nodes
}

func (f *fakeArbiter) GetSafeWatermark(context.Context, *pb.GetSafeWatermarkRequest) (*pb.SafeWatermark, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.watermark == nil {
		return &pb.SafeWatermark{}, nil
	}
	return f.watermark, nil
}

func (f *fakeArbiter) GetManifest(_ context.Context, ref *pb.SnapshotRef) (*pb.SafeSnapshotManifest, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	m, ok := f.manifests[ref.GetSnapshotId()]
	if !ok {
		return nil, status.Error(codes.NotFound, "unknown snapshot id")
	}
	return m, nil
}

func (f *fakeArbiter) GetBlockVerification(context.Context, *pb.L3BlockRef) (*pb.BlockVerification, error) {
	return nil, status.Error(codes.NotFound, "no sealed block at that seq")
}

func (f *fakeArbiter) GetTableRegistry(context.Context, *emptypb.Empty) (*pb.TableRegistrySnapshot, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.registry == nil {
		return nil, status.Error(codes.FailedPrecondition, dataplane.TableRegistryDisabledMessage)
	}
	return f.registry, nil
}

func (f *fakeArbiter) WatchTableRegistry(_ *pb.WatchTableRegistryRequest, stream grpc.ServerStreamingServer[pb.TableRegistrySnapshot]) error {
	f.mu.Lock()
	snap := f.registry
	f.mu.Unlock()
	if snap != nil {
		if err := stream.Send(snap); err != nil {
			return err
		}
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (f *fakeArbiter) GetPurgeNodeSet(context.Context, *emptypb.Empty) (*pb.PurgeNodeSet, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return &pb.PurgeNodeSet{NodeIds: slices.Clone(f.purge)}, nil
}

// FetchPayloads serves testPayload for every ref, in one data frame.
func (f *fakeArbiter) FetchPayloads(req *pb.FetchPayloadsRequest, stream grpc.ServerStreamingServer[pb.FetchFrame]) error {
	for i, spec := range req.GetSpecs() {
		ref, idx := spec.GetPayloadRef(), uint32(i)
		for _, frame := range []*pb.FetchFrame{
			{Frame: &pb.FetchFrame_Begin{Begin: &pb.FetchBegin{SpecIndex: idx, PayloadRef: ref}}},
			{Frame: &pb.FetchFrame_Data{Data: &pb.FetchData{SpecIndex: idx, PayloadRef: ref, Chunk: []byte(testPayload)}}},
			{Frame: &pb.FetchFrame_End{End: &pb.FetchEnd{SpecIndex: idx, PayloadRef: ref, Code: pb.FetchCode_FETCH_CODE_OK, ServedLength: uint64(len(testPayload))}}},
		} {
			if err := stream.Send(frame); err != nil {
				return err
			}
		}
	}
	return nil
}

// staticRegistry is a fixed, ready RegistryView.
type staticRegistry struct {
	snap  wire.TableRegistrySnapshot
	ready chan struct{}
}

func newStaticRegistry(snap wire.TableRegistrySnapshot) *staticRegistry {
	r := &staticRegistry{snap: snap, ready: make(chan struct{})}
	close(r.ready)
	return r
}

func (r *staticRegistry) View() (wire.TableRegistrySnapshot, bool) { return r.snap, true }
func (r *staticRegistry) Changed() <-chan struct{}                 { return make(chan struct{}) }
func (r *staticRegistry) Ready() <-chan struct{}                   { return r.ready }

// testGenesis is the genesis table set of the tests.
func testGenesis() []payloadexec.TableSchema {
	return []payloadexec.TableSchema{
		{TableID: "devuser1.g_a", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}},
		{TableID: "devuser1.g_b", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}},
	}
}

func genesisManifest(t *testing.T, tables []payloadexec.TableSchema) replay.SafeSnapshotManifest {
	t.Helper()
	m, err := payloadexec.New(testNetworkID, tables...).GenesisSnapshot(0, testSchemaSnapshotID, testExecutorProfile)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// child seals a manifest on top of parent with one track frontier per
// indexer, each at blockSeq; no indexers gives a pre-tracks manifest.
func child(t *testing.T, parent replay.SafeSnapshotManifest, blockSeq uint64, indexers ...uint64) replay.SafeSnapshotManifest {
	t.Helper()
	m := parent
	m.SnapshotID, m.ParentSnapshotID, m.SafeBlockSeq = "", parent.SnapshotID, blockSeq
	m.TrackFrontiers = nil
	for _, id := range indexers {
		m.TrackFrontiers = append(m.TrackFrontiers, replay.TrackFrontier{IndexerID: id, SafeBlockSeq: blockSeq, AppliedPromotionSeq: blockSeq})
	}
	sealed, err := m.Seal()
	if err != nil {
		t.Fatal(err)
	}
	return sealed
}

// testRegistry enrols indexers with SNodes snode-<id+1>.
func testRegistry(indexers ...uint64) wire.TableRegistrySnapshot {
	snap := wire.TableRegistrySnapshot{Version: 1, Seeded: true, Params: arbiter.TableRegistryParams{
		ChainID: 1, DatabasesContract: "0x" + "11223344556677889900aabbccddeeff00112233", ActivationBlock: 1, Confirmation: arbiter.TableRegistryConfirmationFinalized,
	}}
	for _, id := range indexers {
		snap.SIIndexers = append(snap.SIIndexers, arbiter.SIIndexerEntry{
			IndexerID: id, ActivationBlock: 1, Signer: "0x20c87974e9ad8113bc6c71f3b6adb2b472a616f3",
			SNodeNodeID: "snode-" + strconv.FormatUint(id+1, 10), EnrollmentJWS: "h.p.s",
		})
	}
	return snap
}
