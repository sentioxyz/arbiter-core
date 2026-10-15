package dataplane

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// stage3SafeState serves the stage-3 reads and records every request. A peer
// with hint answers NotLeader naming it, one with err refuses with it, and
// otherwise it answers states and outcomes.
type stage3SafeState struct {
	pb.UnimplementedSafeStateServer

	mu          sync.Mutex
	hint        string
	err         error
	states      *pb.PartitionStates
	outcomes    *pb.PromotionOutcomes
	stateReqs   []*pb.GetPartitionStatesRequest
	outcomeReqs []*pb.GetPromotionOutcomesRequest
}

func (s *stage3SafeState) refusal() error {
	if s.hint != "" {
		st, err := status.New(codes.FailedPrecondition, "not leader").WithDetails(&pb.NotLeader{LeaderAddr: s.hint})
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		return st.Err()
	}
	return s.err
}

func (s *stage3SafeState) GetPartitionStates(_ context.Context, req *pb.GetPartitionStatesRequest) (*pb.PartitionStates, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stateReqs = append(s.stateReqs, req)
	if err := s.refusal(); err != nil {
		return nil, err
	}
	return s.states, nil
}

func (s *stage3SafeState) GetPromotionOutcomes(_ context.Context, req *pb.GetPromotionOutcomesRequest) (*pb.PromotionOutcomes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.outcomeReqs = append(s.outcomeReqs, req)
	if err := s.refusal(); err != nil {
		return nil, err
	}
	return s.outcomes, nil
}

func (s *stage3SafeState) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *stage3SafeState) requests() ([]*pb.GetPartitionStatesRequest, []*pb.GetPromotionOutcomesRequest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*pb.GetPartitionStatesRequest(nil), s.stateReqs...), append([]*pb.GetPromotionOutcomesRequest(nil), s.outcomeReqs...)
}

func startStage3Peer(t *testing.T, s *stage3SafeState) string {
	t.Helper()
	srv := grpc.NewServer()
	pb.RegisterSafeStateServer(srv, s)
	return serveGRPCForDataplaneTest(t, srv)
}

func stage3StatesFixture() wire.PartitionStates {
	return wire.PartitionStates{
		PromotionSeq:  9,
		SafeWatermark: arbiter.SafeWatermark{SnapshotID: "0xsnap", SafeBlockSeq: 7, ManifestRoot: "0xmanifest"},
		Partitions: []wire.PartitionState{
			{TableID: "db.t", PartitionID: "p_a", BasePartitionRoot: "0xroot-a", PublishSeq: 8, LastAckedPromotionSeq: 8,
				SafeParts: []replay.PartManifestEntry{{TableID: "db.t", PartitionID: "p_a", PartName: "a_8_8_0", PartPhysHash: "0xphys", PartRowLtHash: "0xrow", RowCount: 2, Bytes: 100}}},
			{TableID: "db.t", PartitionID: "p_b", PublishSeq: 3, LastAckedPromotionSeq: 3, UnackedPromotionSeq: 9},
		},
	}
}

func stage3OutcomeFixture(seq uint64, table, partition string) wire.PromotionOutcome {
	ack := arbiter.PromotionAck{NodeID: "snode-1", PromotionSeq: seq, TableID: table, PartitionID: partition, PostPartitionCommitment: "0xpost", Applied: true}
	return wire.PromotionOutcome{PromotionSeq: seq, Promote: arbiter.PromoteSafePartition{TableID: table, PartitionID: partition, PromotionSeq: seq},
		StatementSeqs: []uint64{seq * 10}, BlockSeq: seq, SourceNode: "snode-1", Acked: true, Ack: &ack}
}

// TestGetPartitionStatesFollowsTheLeaderHint: the read is leader-barriered, so
// a follower answers NotLeader and the client asks the leader it names; the
// query reaches the leader as given, and the leader is asked first next time
// (plan S3-A contract §3).
func TestGetPartitionStatesFollowsTheLeaderHint(t *testing.T) {
	want := stage3StatesFixture()
	follower := &stage3SafeState{hint: "n2"}
	leader := &stage3SafeState{states: wire.PartitionStatesToPB(want)}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startStage3Peer(t, follower)}, Peer{ID: "n2", GRPCAddr: startStage3Peer(t, leader)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	got, err := c.GetPartitionStates(ctx, wire.PartitionStatesQuery{TableID: "db.t"})

	if err != nil {
		t.Fatalf("GetPartitionStates: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("states\n got %+v\nwant %+v", got, want)
	}
	followerReqs, _ := follower.requests()
	leaderReqs, _ := leader.requests()
	if len(followerReqs) != 1 || len(leaderReqs) != 1 || leaderReqs[0].GetTableId() != "db.t" || leaderReqs[0].GetPartitionId() != "" {
		t.Fatalf("requests: follower %v, leader %v", followerReqs, leaderReqs)
	}
	if _, err := c.GetPartitionStates(ctx, wire.PartitionStatesQuery{TableID: "db.t"}); err != nil {
		t.Fatalf("second read: %v", err)
	}
	if followerReqs, _ = follower.requests(); len(followerReqs) != 1 {
		t.Fatalf("the second read asked the follower again: %d requests", len(followerReqs))
	}
}

// TestGetPromotionOutcomesFollowsTheLeaderHint: the same for the outcomes,
// with every query field on the wire.
func TestGetPromotionOutcomesFollowsTheLeaderHint(t *testing.T) {
	want := wire.PromotionOutcomes{PromotionSeq: 12, Outcomes: []wire.PromotionOutcome{stage3OutcomeFixture(5, "db.t", "p_a"), stage3OutcomeFixture(7, "db.t", "p_a")}, NextAfterPromotionSeq: 7}
	follower := &stage3SafeState{hint: "n2"}
	leader := &stage3SafeState{outcomes: wire.PromotionOutcomesToPB(want)}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startStage3Peer(t, follower)}, Peer{ID: "n2", GRPCAddr: startStage3Peer(t, leader)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	q := wire.PromotionOutcomesQuery{TableID: "db.t", PartitionID: "p_a", AfterPromotionSeq: 4, Limit: 2}

	got, err := c.GetPromotionOutcomes(ctx, q)

	if err != nil {
		t.Fatalf("GetPromotionOutcomes: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("outcomes\n got %+v\nwant %+v", got, want)
	}
	_, leaderReqs := leader.requests()
	if len(leaderReqs) != 1 || wire.PromotionOutcomesQueryFromPB(leaderReqs[0]) != q {
		t.Fatalf("leader requests = %v, want one carrying %+v", leaderReqs, q)
	}
	if _, followerReqs := follower.requests(); len(followerReqs) != 1 {
		t.Fatalf("follower requests = %v, want one", followerReqs)
	}
}

// TestStage3ReadsAnswerUnavailableOnAnArbiterWithoutThem: an arbiter v0.12.0
// serves SafeState without the stage-3 methods, and a listener without SafeState
// answers Unimplemented as well; both reads return ErrStage3ReadsUnavailable at
// once instead of retrying (clarification K3: the owner audit then falls back).
func TestStage3ReadsAnswerUnavailableOnAnArbiterWithoutThem(t *testing.T) {
	for name, start := range map[string]func(t *testing.T) string{
		"SafeState without the stage-3 methods": func(t *testing.T) string {
			return startSafeStatePeer(t, &fakeSafeState{code: codes.OK, snapshot: "safe"})
		},
		"no SafeState service": func(t *testing.T) string { return startRegistryPeer(t, newFakeRegistry()) },
	} {
		t.Run(name, func(t *testing.T) {
			c := newTestClient(t, Peer{ID: "n1", GRPCAddr: start(t)})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			began := time.Now()
			if _, err := c.GetPartitionStates(ctx, wire.PartitionStatesQuery{TableID: "db.t"}); !errors.Is(err, ErrStage3ReadsUnavailable) {
				t.Fatalf("GetPartitionStates = %v, want ErrStage3ReadsUnavailable", err)
			}
			if _, err := c.GetPromotionOutcomes(ctx, wire.PromotionOutcomesQuery{TableID: "db.t"}); !errors.Is(err, ErrStage3ReadsUnavailable) {
				t.Fatalf("GetPromotionOutcomes = %v, want ErrStage3ReadsUnavailable", err)
			}
			if elapsed := time.Since(began); elapsed > 2*time.Second {
				t.Fatalf("Unimplemented must not be retried: both reads took %v", elapsed)
			}
		})
	}
}

// TestStage3ReadsPassOtherRefusalsThrough: the server's shape refusals reach
// the caller unchanged and are not "unavailable".
func TestStage3ReadsPassOtherRefusalsThrough(t *testing.T) {
	peer := &stage3SafeState{}
	c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startStage3Peer(t, peer)})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, tc := range []struct {
		name    string
		refusal error
		read    func() error
	}{
		{"partition states", status.Error(codes.InvalidArgument, "partition states table_id is required"), func() error {
			_, err := c.GetPartitionStates(ctx, wire.PartitionStatesQuery{})
			return err
		}},
		{"promotion outcomes", status.Error(codes.InvalidArgument, "promotion outcomes limit must be at most 1024"), func() error {
			_, err := c.GetPromotionOutcomes(ctx, wire.PromotionOutcomesQuery{TableID: "db.t", Limit: 1025})
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer.setErr(tc.refusal)
			err := tc.read()
			if errors.Is(err, ErrStage3ReadsUnavailable) || status.Code(err) != status.Code(tc.refusal) || status.Convert(err).Message() != status.Convert(tc.refusal).Message() {
				t.Fatalf("read = %v, want %v unchanged", err, tc.refusal)
			}
		})
	}
}

// TestGetPartitionStatesRefusesAnAnswerOutsideTheQuery: the reads are
// unauthenticated, so an answer naming a partition the query did not ask for,
// or one partition twice, is refused rather than trusted.
func TestGetPartitionStatesRefusesAnAnswerOutsideTheQuery(t *testing.T) {
	for _, tc := range []struct {
		name   string
		query  wire.PartitionStatesQuery
		answer []wire.PartitionState
		want   string
	}{
		{"another table", wire.PartitionStatesQuery{TableID: "db.t"}, []wire.PartitionState{{TableID: "db.other", PartitionID: "all"}},
			"dataplane: GetPartitionStates for db.t answered partition db.other/all"},
		{"another partition", wire.PartitionStatesQuery{TableID: "db.t", PartitionID: "p_a"}, []wire.PartitionState{{TableID: "db.t", PartitionID: "p_b"}},
			"dataplane: GetPartitionStates for db.t/p_a answered partition db.t/p_b"},
		{"a partition twice", wire.PartitionStatesQuery{TableID: "db.t"}, []wire.PartitionState{{TableID: "db.t", PartitionID: "p_a"}, {TableID: "db.t", PartitionID: "p_a"}},
			"dataplane: GetPartitionStates answered partition db.t/p_a twice"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := &stage3SafeState{states: wire.PartitionStatesToPB(wire.PartitionStates{Partitions: tc.answer})}
			c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startStage3Peer(t, peer)})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := c.GetPartitionStates(ctx, tc.query); err == nil || err.Error() != tc.want {
				t.Fatalf("GetPartitionStates = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestGetPromotionOutcomesRefusesAnAnswerOutsideTheQuery: the same for the
// outcomes: the cursor, the ascending order, the scope, the command and stored
// acknowledgement of each outcome, and the limit.
func TestGetPromotionOutcomesRefusesAnAnswerOutsideTheQuery(t *testing.T) {
	otherCommand := stage3OutcomeFixture(3, "db.t", "p_a")
	otherCommand.Promote.PromotionSeq = 4
	otherAck := stage3OutcomeFixture(3, "db.t", "p_a")
	otherAck.Ack.PromotionSeq = 2
	for _, tc := range []struct {
		name   string
		query  wire.PromotionOutcomesQuery
		answer []wire.PromotionOutcome
		want   string
	}{
		{"at the cursor", wire.PromotionOutcomesQuery{TableID: "db.t", AfterPromotionSeq: 5}, []wire.PromotionOutcome{stage3OutcomeFixture(5, "db.t", "p_a")},
			"dataplane: GetPromotionOutcomes answered promotion 5, which is not after 5"},
		{"out of order", wire.PromotionOutcomesQuery{TableID: "db.t"}, []wire.PromotionOutcome{stage3OutcomeFixture(7, "db.t", "p_a"), stage3OutcomeFixture(6, "db.t", "p_a")},
			"dataplane: GetPromotionOutcomes answered promotion 6, which is not after 7"},
		{"another partition", wire.PromotionOutcomesQuery{TableID: "db.t", PartitionID: "p_a"}, []wire.PromotionOutcome{stage3OutcomeFixture(3, "db.t", "p_b")},
			"dataplane: GetPromotionOutcomes for db.t/p_a answered promotion 3 of db.t/p_b"},
		{"another table", wire.PromotionOutcomesQuery{TableID: "db.t"}, []wire.PromotionOutcome{stage3OutcomeFixture(3, "db.x", "all")},
			"dataplane: GetPromotionOutcomes for db.t answered promotion 3 of db.x/all"},
		{"the command of another promotion", wire.PromotionOutcomesQuery{}, []wire.PromotionOutcome{otherCommand},
			"dataplane: GetPromotionOutcomes answered promotion 3 carrying the command of promotion 4"},
		{"the acknowledgement of another promotion", wire.PromotionOutcomesQuery{TableID: "db.t"}, []wire.PromotionOutcome{otherAck},
			"dataplane: GetPromotionOutcomes answered promotion 3 with the acknowledgement of db.t/p_a promotion 2"},
		{"over the limit", wire.PromotionOutcomesQuery{TableID: "db.t", Limit: 1}, []wire.PromotionOutcome{stage3OutcomeFixture(3, "db.t", "p_a"), stage3OutcomeFixture(4, "db.t", "p_a")},
			"dataplane: GetPromotionOutcomes answered 2 outcomes for a limit of 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := &stage3SafeState{outcomes: wire.PromotionOutcomesToPB(wire.PromotionOutcomes{Outcomes: tc.answer})}
			c := newTestClient(t, Peer{ID: "n1", GRPCAddr: startStage3Peer(t, peer)})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if _, err := c.GetPromotionOutcomes(ctx, tc.query); err == nil || err.Error() != tc.want {
				t.Fatalf("GetPromotionOutcomes = %v, want %q", err, tc.want)
			}
		})
	}
}
