package evidence

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"slices"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
)

// gatewayFake is a VerifierGateway and Membership server that records every
// submission and registration, and rejects submissions with rejectErr when it
// is set.
type gatewayFake struct {
	pb.UnimplementedVerifierGatewayServer
	pb.UnimplementedMembershipServer

	mu           sync.Mutex
	attestations []*pb.ReplayAttestation
	scans        []*pb.ByteSideScanMsg
	regs         []*pb.NodeRegistration
	marks        []*pb.NodeRef
	rejectErr    error
}

func (f *gatewayFake) SubmitAttestation(_ context.Context, m *pb.ReplayAttestation) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.attestations = append(f.attestations, m)
	if f.rejectErr != nil {
		return nil, f.rejectErr
	}
	return &pb.Ack{}, nil
}

func (f *gatewayFake) SubmitByteSideScan(_ context.Context, m *pb.ByteSideScanMsg) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.scans = append(f.scans, m)
	if f.rejectErr != nil {
		return nil, f.rejectErr
	}
	return &pb.Ack{}, nil
}

func (f *gatewayFake) RegisterNode(_ context.Context, m *pb.NodeRegistration) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.regs = append(f.regs, m)
	return &pb.Ack{}, nil
}

func (f *gatewayFake) MarkActive(_ context.Context, m *pb.NodeRef) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.marks = append(f.marks, m)
	return &pb.Ack{}, nil
}

func (f *gatewayFake) submitted() ([]*pb.ReplayAttestation, []*pb.ByteSideScanMsg) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.attestations), slices.Clone(f.scans)
}

// startGatewayFake serves f on a loopback port and returns a client for it.
func startGatewayFake(t *testing.T, f *gatewayFake) *dataplane.Client {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterVerifierGatewayServer(srv, f)
	pb.RegisterMembershipServer(srv, f)
	done := make(chan struct{})
	go func() {
		_ = srv.Serve(ln)
		close(done)
	}()
	t.Cleanup(func() {
		srv.Stop()
		<-done
	})
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// replayFake answers every job with att (or err) and records the jobs; before,
// when set, runs first inside Verify.
type replayFake struct {
	mu     sync.Mutex
	jobs   []replay.ReplayJob
	att    replay.ReplayAttestation
	err    error
	before func(job replay.ReplayJob)
}

func (r *replayFake) Verify(_ context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error) {
	if r.before != nil {
		r.before(job)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.jobs = append(r.jobs, job)
	return r.att, r.err
}

func (r *replayFake) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.jobs)
}

// scannerFake answers every scan with scans (or err) and records the parts.
type scannerFake struct {
	mu    sync.Mutex
	parts [][]arbiter.PartRef
	scans []arbiter.PartScan
	err   error
}

func (s *scannerFake) Scan(_ context.Context, parts []arbiter.PartRef) ([]arbiter.PartScan, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.parts = append(s.parts, slices.Clone(parts))
	return slices.Clone(s.scans), s.err
}

// queryRecorder is a clickhouse.Conn whose only method is Query: it records
// the statement and answers err. Any other method panics (nil embedded Conn).
type queryRecorder struct {
	clickhouse.Conn
	mu      sync.Mutex
	queries []string
	err     error
}

func (c *queryRecorder) Query(_ context.Context, query string, _ ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, query)
	return nil, c.err
}

func (c *queryRecorder) recorded() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.queries)
}

// syncBuffer is a goroutine-safe log sink.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func textLogger(w *syncBuffer) *slog.Logger { return slog.New(slog.NewTextHandler(w, nil)) }

func marshalDeterministic(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

var errNotReadyForTest = errors.New("test: table not ready")
