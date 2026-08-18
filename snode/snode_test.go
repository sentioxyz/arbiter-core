package snode

import (
	"context"
	"net"
	"reflect"
	"strings"
	"sync"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/dastore"
	"github.com/sentioxyz/arbiter-core/dataplane/fspayload"
)

func testConfigS(t *testing.T) Config {
	t.Helper()
	tables := []payloadexec.TableSchema{{
		TableID: "db.t",
		Columns: []lthash.Column{{
			Name: "v",
			Type: "UInt64",
		}},
	}}
	return Config{
		NodeID:             "s1",
		NetworkID:          "testnet",
		SchemaSnapshotID:   "schema-genesis",
		ExecutorProfileID:  "housegate-replay-mvp-v0",
		SchemaRoot:         payloadexec.SchemaRoot("testnet", tables),
		Tables:             tables,
		StateDir:           t.TempDir(),
		AuthorityAddresses: []string{"0xabcdef0123456789abcdef0123456789abcdef01"},
	}
}

func TestNew_AssertsSchemaRoot(t *testing.T) {
	cfg := testConfigS(t)
	cfg.SchemaRoot = "0xwrong"

	_, err := New(cfg, Deps{})

	if err == nil || !strings.Contains(err.Error(), "schema_root") {
		t.Fatalf("schema_root mismatch must fail, got %v", err)
	}
}

func TestPayloadSpoolImplementations(t *testing.T) {
	var _ PayloadSpool = (*fspayload.Store)(nil)
	var _ PayloadSpool = (*dastore.Client)(nil)
}

func TestValidate_PartitionByMustBeBareStringColumn(t *testing.T) {
	cases := map[string]func(*payloadexec.TableSchema){
		"non-string column": func(s *payloadexec.TableSchema) {
			s.Columns = []lthash.Column{{Name: "p", Type: "UInt64"}}
			s.PartitionBy = "p"
		},
		"expression": func(s *payloadexec.TableSchema) {
			s.Columns = []lthash.Column{{Name: "d", Type: "String"}}
			s.PartitionBy = "toYYYYMM(d)"
		},
		"unknown column": func(s *payloadexec.TableSchema) {
			s.Columns = []lthash.Column{{Name: "p", Type: "String"}}
			s.PartitionBy = "missing"
		},
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := testConfigS(t)
			sch := payloadexec.TableSchema{TableID: "db.t"}
			mutate(&sch)
			cfg.Tables = []payloadexec.TableSchema{sch}
			cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
			if err := cfg.validate(); err == nil || !strings.Contains(err.Error(), "partition_by") {
				t.Fatalf("%s must fail with partition_by error, got %v", name, err)
			}
		})
	}

	// A bare String partition column is accepted.
	cfg := testConfigS(t)
	sch := payloadexec.TableSchema{TableID: "db.t", PartitionBy: "p", Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}
	cfg.Tables = []payloadexec.TableSchema{sch}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	if err := cfg.validate(); err != nil {
		t.Fatalf("bare String partition column must pass, got %v", err)
	}
}

func TestRegister_SendsSNodeRegistration(t *testing.T) {
	server := &snodeFakeServer{}
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	role, err := New(testConfigS(t), Deps{Client: client})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}

	if err := role.Register(context.Background()); err != nil {
		t.Fatalf("register: %v", err)
	}

	regs, active := server.snapshot()
	if len(regs) != 1 {
		t.Fatalf("registrations: %+v", regs)
	}
	if regs[0].GetNodeId() != "s1" || !reflect.DeepEqual(regs[0].GetRoles(), []pb.NodeRole{pb.NodeRole_NODE_ROLE_SNODE}) ||
		len(regs[0].GetEd25519Pubkey()) != 0 {
		t.Fatalf("registration: %+v", regs[0])
	}
	if !reflect.DeepEqual(active, []string{"s1"}) {
		t.Fatalf("active calls: %+v", active)
	}
}

type snodeFakeServer struct {
	pb.UnimplementedMembershipServer
	pb.UnimplementedPromotionGatewayServer

	mu                  sync.Mutex
	registrations       []*pb.NodeRegistration
	active              []string
	subscriptionStarts  int
	activeSubscriptions int
	subscriptionRelease <-chan struct{}
	subscriptionErr     error
}

func (s *snodeFakeServer) SubscribePromotions(_ *pb.SNodeHello, stream grpc.ServerStreamingServer[pb.PromotionCommand]) error {
	s.mu.Lock()
	s.subscriptionStarts++
	s.activeSubscriptions++
	release := s.subscriptionRelease
	subscriptionErr := s.subscriptionErr
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeSubscriptions--
		s.mu.Unlock()
	}()
	if release != nil {
		select {
		case <-release:
			return subscriptionErr
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (s *snodeFakeServer) failSubscriptionWhen(release <-chan struct{}, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subscriptionRelease = release
	s.subscriptionErr = err
}

func (s *snodeFakeServer) RegisterNode(_ context.Context, reg *pb.NodeRegistration) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registrations = append(s.registrations, reg)
	return &pb.Ack{}, nil
}

func (s *snodeFakeServer) MarkActive(_ context.Context, ref *pb.NodeRef) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active = append(s.active, ref.GetNodeId())
	return &pb.Ack{}, nil
}

func (s *snodeFakeServer) snapshot() ([]*pb.NodeRegistration, []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pb.NodeRegistration(nil), s.registrations...), append([]string(nil), s.active...)
}

func (s *snodeFakeServer) subscriptionSnapshot() (starts, active int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.subscriptionStarts, s.activeSubscriptions
}

func startSNodeFakeServer(t *testing.T, fake *snodeFakeServer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterMembershipServer(srv, fake)
	pb.RegisterPromotionGatewayServer(srv, fake)
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
