package verifier

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
)

func testSeedV() []byte {
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 7
	return seed
}

func testTablesV() []payloadexec.TableSchema {
	return []payloadexec.TableSchema{{
		TableID: "db.t",
		Columns: []lthash.Column{{
			Name: "v",
			Type: "UInt64",
		}},
	}}
}

func testConfigV() Config {
	tables := testTablesV()
	networkID := "testnet"
	return Config{
		ReplicaID:         "v1",
		Ed25519Seed:       testSeedV(),
		NetworkID:         networkID,
		SchemaSnapshotID:  "schema-genesis",
		ExecutorProfileID: "housegate-replay-mvp-v0",
		SchemaRoot:        payloadexec.SchemaRoot(networkID, tables),
		Tables:            tables,
	}
}

type verifierFakeServer struct {
	pb.UnimplementedVerifierGatewayServer
	pb.UnimplementedMembershipServer

	mu                  sync.Mutex
	dispatches          chan *pb.VerifierDispatch
	registrations       []*pb.NodeRegistration
	active              []string
	attestations        []*pb.ReplayAttestation
	scans               []*pb.ByteSideScanMsg
	subscriptionStarts  int
	activeSubscriptions int
}

func newVerifierFakeServer() *verifierFakeServer {
	return &verifierFakeServer{dispatches: make(chan *pb.VerifierDispatch, 16)}
}

func (s *verifierFakeServer) SubscribeVerifierDispatch(_ *pb.VerifierHello, stream grpc.ServerStreamingServer[pb.VerifierDispatch]) error {
	s.mu.Lock()
	s.subscriptionStarts++
	s.activeSubscriptions++
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeSubscriptions--
		s.mu.Unlock()
	}()
	for {
		select {
		case <-stream.Context().Done():
			return stream.Context().Err()
		case msg := <-s.dispatches:
			if msg == nil {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

func (s *verifierFakeServer) subscriptionSnapshot() (starts, active int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscriptionStarts, s.activeSubscriptions
}

func (s *verifierFakeServer) SubmitAttestation(_ context.Context, att *pb.ReplayAttestation) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attestations = append(s.attestations, att)
	return &pb.Ack{}, nil
}

func (s *verifierFakeServer) SubmitByteSideScan(_ context.Context, scan *pb.ByteSideScanMsg) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.scans = append(s.scans, scan)
	return &pb.Ack{}, nil
}

func (s *verifierFakeServer) RegisterNode(_ context.Context, reg *pb.NodeRegistration) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registrations = append(s.registrations, reg)
	return &pb.Ack{}, nil
}

func (s *verifierFakeServer) MarkActive(_ context.Context, ref *pb.NodeRef) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = append(s.active, ref.GetNodeId())
	return &pb.Ack{}, nil
}

func (s *verifierFakeServer) snapshot() ([]*pb.NodeRegistration, []string, []*pb.ReplayAttestation, []*pb.ByteSideScanMsg) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pb.NodeRegistration(nil), s.registrations...),
		append([]string(nil), s.active...),
		append([]*pb.ReplayAttestation(nil), s.attestations...),
		append([]*pb.ByteSideScanMsg(nil), s.scans...)
}

func (s *verifierFakeServer) push(msg *pb.VerifierDispatch) {
	s.dispatches <- msg
}

func startVerifierFakeServer(t *testing.T, fake *verifierFakeServer) string {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterVerifierGatewayServer(srv, fake)
	pb.RegisterMembershipServer(srv, fake)
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ln)
		close(done)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	return ln.Addr().String()
}

type fakeReplayCore struct {
	mu      sync.Mutex
	jobs    []replay.ReplayJob
	results []replay.ReplayAttestation
	errs    []error
}

func (c *fakeReplayCore) Verify(_ context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	idx := len(c.jobs)
	c.jobs = append(c.jobs, job)
	if idx < len(c.errs) && c.errs[idx] != nil {
		return replay.ReplayAttestation{}, c.errs[idx]
	}
	if idx < len(c.results) {
		return c.results[idx], nil
	}
	return replay.ReplayAttestation{ReplicaID: "v1", ReceiptHash: fmt.Sprintf("0xreceipt%d", idx+1), Signature: "sig"}, nil
}

func (c *fakeReplayCore) jobCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.jobs)
}

func (c *fakeReplayCore) jobsSnapshot() []replay.ReplayJob {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]replay.ReplayJob(nil), c.jobs...)
}

type fakeScanner struct {
	mu      sync.Mutex
	parts   [][]arbiter.PartRef
	results [][]arbiter.PartScan
	errs    []error
}

func (s *fakeScanner) Scan(_ context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := len(s.parts)
	s.parts = append(s.parts, append([]arbiter.PartRef(nil), parts...))
	if idx < len(s.errs) && s.errs[idx] != nil {
		return nil, s.errs[idx]
	}
	if idx < len(s.results) {
		return append([]arbiter.PartScan(nil), s.results[idx]...), nil
	}
	return nil, nil
}

func (s *fakeScanner) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.parts)
}

func waitVerifier(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}
