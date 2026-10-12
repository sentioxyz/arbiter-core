package snode

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouseproto "github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/lthash"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// errAckRefusedS is how the arbiter answers an acknowledgement its FSM
// rejects (the server maps fsm.Rejected to InvalidArgument), which
// WithLeaderRetry does not retry.
var errAckRefusedS = status.Error(codes.InvalidArgument, "cleanup acknowledgement refused")

// handleCleanupCtxS is handleCleanupS under ctx.
func handleCleanupCtxS(t *testing.T, ctx context.Context, role *Role, cleanup arbiter.UnsafeCleanup) error {
	t.Helper()
	jws, err := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex).SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	return role.handleCleanup(ctx, wire.CleanupToPB(cleanup), jws)
}

// cleanupAckS is the acknowledgement indexer 1's SNode owes for cleanup.
func cleanupAckS(cleanup arbiter.UnsafeCleanup) arbiter.CleanupAck {
	return arbiter.CleanupAck{NodeID: authoritytest.SNodeNodeID1, PromotionSeq: cleanup.PromotionSeq,
		TableID: cleanup.TableID, PartitionID: cleanup.PartitionID, RejectedBlockSeq: cleanup.RejectedBlockSeq}
}

// acceptedS decodes the cleanup acknowledgements the fake arbiter accepted,
// and requires each to carry indexer 1's signature over exactly its body.
func acceptedS(t *testing.T, fake *claimsFakeS) []arbiter.CleanupAck {
	t.Helper()
	_, _, cleanups := fake.sent()
	out := make([]arbiter.CleanupAck, 0, len(cleanups))
	for _, c := range cleanups {
		ack := wire.CleanupAckFromPB(c)
		if err := authority.VerifySNodeMessage(authority.SNodeMessageCleanupAck, authoritytest.Context(), ack, c.GetSourceJws(), authoritytest.IndexerAddr1); err != nil {
			t.Fatalf("accepted acknowledgement %+v: %v", ack, err)
		}
		out = append(out, ack)
	}
	return out
}

// purgeGateOpenS is the purge gate for db.t: tableQuiescent, which reads the
// intake journal and then stateStore.quiescent.
func purgeGateOpenS(t *testing.T, role *Role) bool {
	t.Helper()
	ok, err := role.tableQuiescent("db.t")
	if err != nil {
		t.Fatal(err)
	}
	return ok
}

// setBeforeWriteS installs fn as role's state-write observer.
func setBeforeWriteS(role *Role, fn func(next localState) error) {
	role.state.mu.Lock()
	defer role.state.mu.Unlock()
	role.state.beforeWrite = fn
}

// writeS is what a test keeps of one state write, for partition db.t/all.
type writeS struct {
	sumZero bool                 // the unpromoted sum is the zero accumulator
	owed    []arbiter.CleanupAck // every owed acknowledgement, in key order
}

// recordWritesS observes every later state write of role; the returned
// function lists them in order.
func recordWritesS(t *testing.T, role *Role) func() []writeS {
	t.Helper()
	var mu sync.Mutex
	var writes []writeS
	ks := key("db.t", "all")
	setBeforeWriteS(role, func(next localState) error {
		acc, err := parseAccumulatorHex(next.UnpromotedSums[ks])
		if err != nil {
			return err
		}
		w := writeS{sumZero: bytes.Equal(acc.Bytes(), lthash.New().Bytes())}
		for _, k := range slices.Sorted(maps.Keys(next.OwedCleanupAcks)) {
			w.owed = append(w.owed, next.OwedCleanupAcks[k])
		}
		mu.Lock()
		defer mu.Unlock()
		writes = append(writes, w)
		return nil
	})
	return func() []writeS {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(writes)
	}
}

// stateWithoutOwedS is a deep copy of role's state minus the owed
// acknowledgements.
func stateWithoutOwedS(role *Role) localState {
	role.state.mu.Lock()
	defer role.state.mu.Unlock()
	s := cloneLocalState(role.state.s)
	s.OwedCleanupAcks = nil
	return s
}

// syncBufferS is a bytes.Buffer a logger in another goroutine may write to.
type syncBufferS struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBufferS) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBufferS) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// loggedS reports whether one logged line contains every one of want.
func loggedS(logs *syncBufferS, want ...string) bool {
	for _, line := range strings.Split(logs.String(), "\n") {
		if !slices.ContainsFunc(want, func(w string) bool { return !strings.Contains(line, w) }) {
			return true
		}
	}
	return false
}

// waitForS polls cond until it holds, failing the test after timeout.
func waitForS(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("waited %v for %s", timeout, what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// subscribedGatewayS is claimsFakeS as a promotion gateway whose subscription
// stays open and delivers nothing, so a running role keeps serving.
type subscribedGatewayS struct{ *claimsFakeS }

func (subscribedGatewayS) SubscribePromotions(_ *pb.SNodeHello, stream grpc.ServerStreamingServer[pb.PromotionCommand]) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

// unreachableArbiterS is an arbiter that takes TCP connections but answers
// nothing (every call waits for its deadline) until serve is called; from then
// on it is gateway, as a subscribedGatewayS that accepts every acknowledgement.
func unreachableArbiterS(t *testing.T) (client *dataplane.Client, gateway *claimsFakeS, serve func()) {
	t.Helper()
	gateway = &claimsFakeS{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterPromotionGatewayServer(srv, subscribedGatewayS{gateway})
	t.Cleanup(func() {
		srv.Stop()
		_ = ln.Close()
	})
	client, err = dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: ln.Addr().String()}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client, gateway, func() { go func() { _ = srv.Serve(ln) }() }
}

// TestPromotionCleanupOwesItsAckUntilTheArbiterAcceptsIt: a promotion cleanup
// whose acknowledgement the arbiter has not accepted leaves the
// acknowledgement owed. The DROP ran and the exclusion is forgotten (the part
// is gone), but the purge gate and ForgetTable hold the table, so the table
// cannot be purged while the arbiter still holds the cleanup (a redelivery
// would then meet a missing table, or a part of a re-created one). A
// background re-send that the arbiter refuses keeps the entry and says so at
// warn level; the one it accepts clears the entry and the gate opens.
func TestPromotionCleanupOwesItsAckUntilTheArbiterAcceptsIt(t *testing.T) {
	for _, tc := range []struct {
		name string
		run  func(t *testing.T, role *Role, fake *claimsFakeS, cleanup arbiter.UnsafeCleanup) error
	}{
		{"the arbiter refuses it", func(t *testing.T, role *Role, fake *claimsFakeS, cleanup arbiter.UnsafeCleanup) error {
			fake.refuseCleanupAcks(errAckRefusedS)
			return handleCleanupS(t, role, cleanup)
		}},
		{"the context is canceled", func(t *testing.T, role *Role, _ *claimsFakeS, cleanup arbiter.UnsafeCleanup) error {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return handleCleanupCtxS(t, ctx, role, cleanup)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			role, fake, conn := newRecordingSNodeS(t)
			var logs syncBufferS
			role.d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
			promoted := recordedPartS(t, role, "all_9_9_0")
			applyPromotionS(t, role, 5, promoted)
			cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 5, Parts: []arbiter.PartRef{promoted}}
			want := cleanupAckS(cleanup)
			if err := tc.run(t, role, fake, cleanup); err == nil {
				t.Fatal("the cleanup reported success although the arbiter did not accept its acknowledgement")
			}
			if got, wantExec := conn.executed(), []string{"ALTER TABLE hg_unsafe.db__t DROP PART 'all_9_9_0'"}; !slices.Equal(got, wantExec) {
				t.Fatalf("executed %q, want %q", got, wantExec)
			}
			if got, err := role.PromotedUnsafeParts("db.t"); err != nil || len(got) != 0 {
				t.Fatalf("promoted unsafe parts = %v, %v: the dropped part's exclusion is forgotten", got, err)
			}
			if got := acceptedS(t, fake); len(got) != 0 {
				t.Fatalf("accepted acknowledgements = %+v, want none yet", got)
			}
			// Symptoms of one cause, each reported on its own.
			if got := role.state.OwedCleanupAcks(); !slices.Equal(got, []arbiter.CleanupAck{want}) {
				t.Errorf("owed acknowledgements = %+v, want [%+v]", got, want)
			}
			if purgeGateOpenS(t, role) {
				t.Error("the purge gate is open while the cleanup's acknowledgement is owed")
			}
			if err := role.state.ForgetTable("db.t"); err == nil || !strings.Contains(err.Error(), "owed") || !strings.Contains(err.Error(), "promotion_seq 5") {
				t.Errorf("ForgetTable while the acknowledgement is owed: err = %v, want a refusal naming it", err)
			}
			if ok, err := role.state.quiescent("db.u"); err != nil || !ok {
				t.Errorf("another table: quiescent = %v, %v; an owed acknowledgement holds only its own table", ok, err)
			}

			// A re-send the arbiter refuses keeps the entry and logs it.
			fake.refuseCleanupAcks(errAckRefusedS)
			role.resendOwedCleanupAcks(context.Background())
			if got := role.state.OwedCleanupAcks(); !slices.Equal(got, []arbiter.CleanupAck{want}) {
				t.Errorf("after a refused re-send: owed acknowledgements = %+v, want [%+v]", got, want)
			}
			if want := []string{"level=WARN", "table=db.t", "partition=all", "promotion_seq=5", "rejected_block_seq=0"}; !loggedS(&logs, want...) {
				t.Errorf("no log line has all of %q:\n%s", want, logs.String())
			}

			// The re-send the arbiter accepts clears the entry, and the gate opens.
			fake.refuseCleanupAcks(nil)
			role.resendOwedCleanupAcks(context.Background())
			if got := acceptedS(t, fake); !slices.Equal(got, []arbiter.CleanupAck{want}) {
				t.Fatalf("accepted acknowledgements = %+v, want [%+v]", got, want)
			}
			if got := role.state.OwedCleanupAcks(); len(got) != 0 {
				t.Errorf("owed acknowledgements after the accepted re-send = %+v, want none", got)
			}
			if !purgeGateOpenS(t, role) {
				t.Error("the purge gate stays closed after the arbiter accepted the acknowledgement")
			}
			if err := role.state.ForgetTable("db.t"); err != nil {
				t.Errorf("ForgetTable after the arbiter accepted the acknowledgement: %v", err)
			}
		})
	}
}

// TestRejectedPartCleanupJournalsItsAccountingAndItsAckInOneWrite: the
// accounting that opens the purge gate (the rejected parts leave
// UnpromotedSums) and the acknowledgement the SNode then owes land in one
// state write. No persisted state, a crash included, has the sum zeroed
// without the owed acknowledgement, so the gate never opens before the
// arbiter has accepted the acknowledgement.
func TestRejectedPartCleanupJournalsItsAccountingAndItsAckInOneWrite(t *testing.T) {
	role, fake, _ := newRecordingSNodeS(t)
	k := partitionKey{Table: "db.t", Partition: "all"}
	first, second := recordedPartS(t, role, "all_1_1_0"), recordedPartS(t, role, "all_2_2_0")
	writes := recordWritesS(t, role)
	fake.refuseCleanupAcks(errAckRefusedS)
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{first, second}}
	want := cleanupAckS(cleanup)
	if err := handleCleanupS(t, role, cleanup); err == nil {
		t.Fatal("the cleanup reported success although the arbiter refused its acknowledgement")
	}
	if got := fake.refused(); len(got) != 1 || wire.CleanupAckFromPB(got[0]) != want {
		t.Fatalf("refused acknowledgements = %v, want one: %+v", got, want)
	}
	// Symptoms of one cause, each reported on its own.
	got := writes()
	for i, w := range got {
		if w.sumZero && !slices.Contains(w.owed, want) {
			t.Errorf("state write %d of %d persisted the zeroed sum without the owed acknowledgement", i+1, len(got))
		}
	}
	if len(got) != 1 || !got[0].sumZero || !slices.Equal(got[0].owed, []arbiter.CleanupAck{want}) {
		t.Errorf("state writes = %+v, want exactly one, with the zeroed sum and the owed acknowledgement", got)
	}
	// The state file holds that write.
	reopened, err := openStateStore(role.cfg.StateDir)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.OwedCleanupAcks(); !slices.Equal(got, []arbiter.CleanupAck{want}) {
		t.Errorf("persisted owed acknowledgements = %+v, want [%+v]", got, want)
	}
	if ok, err := reopened.quiescent("db.t"); err != nil || ok {
		t.Errorf("persisted state: quiescent = %v, %v; the owed acknowledgement must hold the gate", ok, err)
	}
	if !unpromotedSumIsZeroS(t, role, k) {
		t.Errorf("the unpromoted sum of %s/%s is not the zero accumulator", k.Table, k.Partition)
	}
	for _, p := range []arbiter.PartRef{first, second} {
		if recorded, ok := intakePartS(role, k, p.PartName); !ok || recorded != "" {
			t.Errorf("intake record of %s: present %v, removed marker %v; want both", p.PartName, ok, ok && recorded == "")
		}
	}
	if purgeGateOpenS(t, role) {
		t.Error("the purge gate is open while the rejected-part cleanup's acknowledgement is owed")
	}
	if err := role.state.ForgetTable("db.t"); err == nil || !strings.Contains(err.Error(), "rejected_block_seq 12") {
		t.Errorf("ForgetTable while the acknowledgement is owed: err = %v, want a refusal naming it", err)
	}

	fake.refuseCleanupAcks(nil)
	role.resendOwedCleanupAcks(context.Background())
	if got := acceptedS(t, fake); !slices.Equal(got, []arbiter.CleanupAck{want}) {
		t.Fatalf("accepted acknowledgements = %+v, want [%+v]", got, want)
	}
	if got := role.state.OwedCleanupAcks(); len(got) != 0 {
		t.Errorf("owed acknowledgements after the accepted re-send = %+v, want none", got)
	}
	if !purgeGateOpenS(t, role) {
		t.Error("the purge gate stays closed after the arbiter accepted the acknowledgement")
	}
}

// TestRestartedSNodeResendsAnOwedAckWithoutBlockingStartup: the owed entry is
// durable, so a role started over the same state directory still holds the
// purge gate and re-sends the acknowledgement in the background. While the
// arbiter is unreachable (it takes connections but never answers) the role
// still becomes ready and logs each failed re-send at warn level; once the
// arbiter answers, the re-signed acknowledgement is accepted, the entry is
// cleared and the gate opens.
func TestRestartedSNodeResendsAnOwedAckWithoutBlockingStartup(t *testing.T) {
	first, fake, _ := newRecordingSNodeS(t)
	promoted := recordedPartS(t, first, "all_9_9_0")
	applyPromotionS(t, first, 5, promoted)
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 5, Parts: []arbiter.PartRef{promoted}}
	want := cleanupAckS(cleanup)
	fake.refuseCleanupAcks(errAckRefusedS)
	if err := handleCleanupS(t, first, cleanup); err == nil {
		t.Fatal("the cleanup reported success although the arbiter refused its acknowledgement")
	}

	// The arbiter after the restart answers nothing until serve runs.
	client, gateway, serve := unreachableArbiterS(t)
	cfg := first.cfg
	cfg.CleanupAckResendInterval = MinCleanupAckResendInterval
	var logs syncBufferS
	second, err := New(cfg, Deps{Client: client, Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	if got := second.state.OwedCleanupAcks(); !slices.Equal(got, []arbiter.CleanupAck{want}) {
		t.Fatalf("owed acknowledgements after the restart = %+v, want [%+v]", got, want)
	}
	if purgeGateOpenS(t, second) {
		t.Fatal("after the restart the purge gate is open while the acknowledgement is owed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan struct{})
	done := make(chan error, 1)
	go func() { done <- second.RunWithReady(ctx, func() { close(ready) }) }()
	select {
	case <-ready:
	case err := <-done:
		t.Fatalf("RunWithReady returned %v before it was ready", err)
	case <-time.After(10 * time.Second):
		t.Fatal("startup blocked while the arbiter is unreachable")
	}
	warned := []string{"level=WARN", "table=db.t", "partition=all", "promotion_seq=5", "rejected_block_seq=0"}
	waitForS(t, 10*time.Second, "a warn-level log of the failed re-send", func() bool { return loggedS(&logs, warned...) })
	if got := second.state.OwedCleanupAcks(); !slices.Equal(got, []arbiter.CleanupAck{want}) {
		t.Fatalf("owed acknowledgements while the arbiter is unreachable = %+v, want [%+v]", got, want)
	}
	if purgeGateOpenS(t, second) {
		t.Fatal("the purge gate opened while the arbiter is unreachable")
	}

	serve()
	waitForS(t, 20*time.Second, "the owed acknowledgement to clear", func() bool { return len(second.state.OwedCleanupAcks()) == 0 })
	// A re-send whose answer arrives after its pass ended is sent again; the
	// arbiter takes a repeated acknowledgement as a no-op.
	accepted := acceptedS(t, gateway)
	if len(accepted) == 0 || slices.ContainsFunc(accepted, func(a arbiter.CleanupAck) bool { return a != want }) {
		t.Fatalf("accepted acknowledgements = %+v, want %+v (at least once)", accepted, want)
	}
	if !purgeGateOpenS(t, second) {
		t.Fatal("the purge gate stays closed after the arbiter accepted the acknowledgement")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithReady exit = %v, want context canceled", err)
	}
}

// TestResendPassStopsWhenItsTimeRunsOut: one interval bounds a re-send pass.
// Against an arbiter that answers nothing, the first owed acknowledgement takes
// the whole pass and is logged as a failed re-send; the pass then stops and
// logs one line with the number of owed acknowledgements it did not attempt,
// not a failed re-send for each. All stay owed for the next pass.
func TestResendPassStopsWhenItsTimeRunsOut(t *testing.T) {
	client, gateway, _ := unreachableArbiterS(t)
	cfg := signingConfigS(t)
	cfg.CleanupAckResendInterval = MinCleanupAckResendInterval
	var logs syncBufferS
	role, err := New(cfg, Deps{Client: client, Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	var owed []arbiter.CleanupAck
	for _, seq := range []uint64{5, 6, 7} {
		cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: seq}
		if err := role.state.RecordExecutedCleanup(cleanup, cleanupAckS(cleanup)); err != nil {
			t.Fatal(err)
		}
		owed = append(owed, cleanupAckS(cleanup))
	}
	role.resendOwedCleanupAcks(context.Background())
	var failed []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "re-sending an owed cleanup acknowledgement failed") {
			failed = append(failed, line)
		}
	}
	// Symptoms of one cause, each reported on its own.
	if len(failed) != 1 || !strings.Contains(failed[0], "promotion_seq=5") {
		t.Errorf("failed re-send lines = %q, want one, for promotion_seq 5, the only acknowledgement the pass attempted", failed)
	}
	if want := []string{"level=WARN", "not_attempted=2", "owed=3"}; !loggedS(&logs, want...) {
		t.Errorf("no log line has all of %q:\n%s", want, logs.String())
	}
	if got := role.state.OwedCleanupAcks(); !slices.Equal(got, owed) {
		t.Errorf("owed acknowledgements after the pass = %+v, want all of %+v", got, owed)
	}
	if got := acceptedS(t, gateway); len(got) != 0 {
		t.Errorf("accepted acknowledgements = %+v from an arbiter that answers nothing", got)
	}
}

// TestDispatchOfAnAcknowledgedCleanupWhoseOwedEntryWasNotClearedSaysTheResenderRetries:
// the arbiter accepted the acknowledgement, so it will not redeliver the
// cleanup, but the write that clears the owed entry failed. The failure log
// must not promise a redelivery; it says the background resender retries it.
func TestDispatchOfAnAcknowledgedCleanupWhoseOwedEntryWasNotClearedSaysTheResenderRetries(t *testing.T) {
	role, fake, _ := newRecordingSNodeS(t)
	var logs syncBufferS
	role.d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 5, Parts: []arbiter.PartRef{partS(t, "all_9_9_0")}}
	want := cleanupAckS(cleanup)
	setBeforeWriteS(role, func(next localState) error {
		if !slices.Contains(slices.Collect(maps.Values(next.OwedCleanupAcks)), want) {
			return errors.New("injected: the write that clears the owed acknowledgement fails")
		}
		return nil
	})
	jws, err := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex).SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.dispatchPromotionCommand(context.Background(), &pb.PromotionCommand{
		Cmd:          &pb.PromotionCommand_Cleanup{Cleanup: wire.CleanupToPB(cleanup)},
		AuthorityJws: jws,
	}); err == nil {
		t.Fatal("the failed clearing write was not reported")
	}
	if got := acceptedS(t, fake); !slices.Equal(got, []arbiter.CleanupAck{want}) {
		t.Fatalf("accepted acknowledgements = %+v, want [%+v]", got, want)
	}
	logged := logs.String()
	if strings.Contains(logged, "the subscription redelivers it") {
		t.Errorf("log %q promises a redelivery of a cleanup the arbiter has acknowledged", logged)
	}
	if want := []string{"level=ERROR", "resender retries it", "promotion_seq=5", "rejected_block_seq=0", "table=db.t", "partition=all"}; !loggedS(&logs, want...) {
		t.Errorf("no log line has all of %q:\n%s", want, logged)
	}
}

// TestCleanupWhoseJournalWriteFailsSendsNoAck: the one state write that
// journals an executed cleanup fails (the disk is full, say). The cleanup
// fails with "journal cleanup", nothing is acknowledged or owed, the state, in
// memory and on disk, keeps what held the purge gate (the promoted part's
// exclusion, or the rejected parts' unpromoted rows), and the gate stays
// closed; the redelivered cleanup tries again.
func TestCleanupWhoseJournalWriteFailsSendsNoAck(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(t *testing.T, role *Role) arbiter.UnsafeCleanup
	}{
		{"promotion cleanup", func(t *testing.T, role *Role) arbiter.UnsafeCleanup {
			promoted := recordedPartS(t, role, "all_9_9_0")
			applyPromotionS(t, role, 5, promoted)
			return arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 5, Parts: []arbiter.PartRef{promoted}}
		}},
		{"rejected-part cleanup", func(t *testing.T, role *Role) arbiter.UnsafeCleanup {
			first, second := recordedPartS(t, role, "all_1_1_0"), recordedPartS(t, role, "all_2_2_0")
			return arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{first, second}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			role, fake, conn := newRecordingSNodeS(t)
			cleanup := tc.setup(t, role)
			before := stateWithoutOwedS(role)
			promotedBefore, err := role.PromotedUnsafeParts("db.t")
			if err != nil {
				t.Fatal(err)
			}
			sumBefore := role.state.UnpromotedSum(partitionKey{Table: "db.t", Partition: "all"})
			errDisk := errors.New("injected: the journal write fails")
			setBeforeWriteS(role, func(localState) error { return errDisk })
			err = handleCleanupS(t, role, cleanup)
			if err == nil || !strings.Contains(err.Error(), "journal cleanup") || !errors.Is(err, errDisk) {
				t.Fatalf("err = %v, want the failed journal write, as journal cleanup", err)
			}
			if got := conn.executed(); len(got) != len(cleanup.Parts) {
				t.Fatalf("executed %q, want one DROP PART per part before the journal write", got)
			}
			// Symptoms of one cause, each reported on its own.
			if got, refused := acceptedS(t, fake), fake.refused(); len(got) != 0 || len(refused) != 0 {
				t.Errorf("acknowledgements sent: accepted %+v, refused %v; want none", got, refused)
			}
			if got := role.state.OwedCleanupAcks(); len(got) != 0 {
				t.Errorf("owed acknowledgements = %+v, want none", got)
			}
			if !reflect.DeepEqual(stateWithoutOwedS(role), before) {
				t.Error("the failed journal write changed the state in memory")
			}
			if purgeGateOpenS(t, role) {
				t.Error("the purge gate opened although the cleanup was not journaled")
			}
			setBeforeWriteS(role, nil)
			reopened, err := openStateStore(role.cfg.StateDir)
			if err != nil {
				t.Fatal(err)
			}
			if got, err := reopened.PromotedUnsafeParts("db.t"); err != nil || !slices.Equal(got, promotedBefore) {
				t.Errorf("persisted promoted parts = %v, %v; want %v", got, err, promotedBefore)
			}
			if got := reopened.UnpromotedSum(partitionKey{Table: "db.t", Partition: "all"}); got != sumBefore {
				t.Error("the failed journal write changed the persisted unpromoted sum")
			}
			if got := reopened.OwedCleanupAcks(); len(got) != 0 {
				t.Errorf("persisted owed acknowledgements = %+v, want none", got)
			}
			if ok, err := reopened.quiescent("db.t"); err != nil || ok {
				t.Errorf("persisted state: quiescent = %v, %v; the cleanup's parts must still hold the gate", ok, err)
			}
		})
	}
}

// TestOwedCleanupAckHoldsOnlyItsOwnTable: the purge gate matches an owed
// acknowledgement on its table exactly. One owed for db.t does not hold db.t2,
// nor one owed for db.t2 db.t; a prefix match would hold a table for another
// table's cleanup.
func TestOwedCleanupAckHoldsOnlyItsOwnTable(t *testing.T) {
	for _, tc := range []struct{ owed, other string }{{"db.t", "db.t2"}, {"db.t2", "db.t"}} {
		t.Run("owed for "+tc.owed, func(t *testing.T) {
			st, err := openStateStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			for _, table := range []string{tc.owed, tc.other} {
				// Ledger entries that ForgetTable forgets.
				k := partitionKey{Table: table, Partition: "all"}
				ack := arbiter.PromotionAck{NodeID: "s1", PromotionSeq: 1, TableID: table, PartitionID: "all", Applied: true}
				if err := st.RecordAck(k, 1, ack, "0xbase", "snap"); err != nil {
					t.Fatal(err)
				}
			}
			cleanup := arbiter.UnsafeCleanup{TableID: tc.owed, PartitionID: "all", PromotionSeq: 2}
			owed := arbiter.CleanupAck{NodeID: "s1", PromotionSeq: 2, TableID: tc.owed, PartitionID: "all"}
			if err := st.RecordExecutedCleanup(cleanup, owed); err != nil {
				t.Fatal(err)
			}
			// Symptoms of one cause, each reported on its own.
			if ok, err := st.quiescent(tc.other); err != nil || !ok {
				t.Errorf("quiescent(%s) = %v, %v; an acknowledgement owed for %s must not hold it", tc.other, ok, err, tc.owed)
			}
			if err := st.ForgetTable(tc.other); err != nil {
				t.Errorf("ForgetTable(%s): %v; an acknowledgement owed for %s must not hold it", tc.other, err, tc.owed)
			}
			if root, _ := st.BaseRoot(partitionKey{Table: tc.other, Partition: "all"}); root != "" {
				t.Errorf("%s's base root %q survived ForgetTable", tc.other, root)
			}
			if ok, err := st.quiescent(tc.owed); err != nil || ok {
				t.Errorf("quiescent(%s) = %v, %v; its owed acknowledgement must hold it", tc.owed, ok, err)
			}
			if err := st.ForgetTable(tc.owed); err == nil || !strings.Contains(err.Error(), "forget table "+tc.owed+":") {
				t.Errorf("ForgetTable(%s): err = %v, want a refusal naming its owed acknowledgement", tc.owed, err)
			}
			if root, _ := st.BaseRoot(partitionKey{Table: tc.owed, Partition: "all"}); root != "0xbase" {
				t.Errorf("%s's base root = %q, want it kept", tc.owed, root)
			}
			if got := st.OwedCleanupAcks(); !slices.Equal(got, []arbiter.CleanupAck{owed}) {
				t.Errorf("owed acknowledgements = %+v, want [%+v]", got, owed)
			}
		})
	}
}

// TestRedeliveredCleanupResendsAnAcceptedButUnclearedAck: the arbiter accepted
// the acknowledgement, but the write that clears the owed entry failed, so the
// entry stays (the arbiter takes an acknowledgement it already has as a
// no-op). The redelivered cleanup drops nothing new, sends the same
// acknowledgement again and clears the entry in one write; nothing else
// changes.
func TestRedeliveredCleanupResendsAnAcceptedButUnclearedAck(t *testing.T) {
	role, fake, conn := newRecordingSNodeS(t)
	first, second := recordedPartS(t, role, "all_1_1_0"), recordedPartS(t, role, "all_2_2_0")
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{first, second}}
	want := cleanupAckS(cleanup)
	errClear := errors.New("injected: the write that clears the owed acknowledgement fails")
	setBeforeWriteS(role, func(next localState) error {
		if !slices.Contains(slices.Collect(maps.Values(next.OwedCleanupAcks)), want) {
			return errClear
		}
		return nil
	})
	if err := handleCleanupS(t, role, cleanup); !errors.Is(err, errClear) {
		t.Fatalf("first delivery: err = %v, want the failed clearing write", err)
	}
	if got := acceptedS(t, fake); !slices.Equal(got, []arbiter.CleanupAck{want}) {
		t.Fatalf("accepted acknowledgements = %+v, want [%+v]", got, want)
	}
	if got := role.state.OwedCleanupAcks(); !slices.Equal(got, []arbiter.CleanupAck{want}) {
		t.Fatalf("owed acknowledgements after the failed clearing write = %+v, want [%+v]", got, want)
	}
	if purgeGateOpenS(t, role) {
		t.Fatal("the purge gate is open although the owed acknowledgement was not cleared")
	}

	before := stateWithoutOwedS(role)
	writes := recordWritesS(t, role)
	conn.failWith(&clickhouseproto.Exception{Code: int32(chproto.ErrNoSuchDataPart), Message: "part is absent"})
	if err := handleCleanupS(t, role, cleanup); err != nil {
		t.Fatalf("redelivered cleanup: %v", err)
	}
	if got := acceptedS(t, fake); !slices.Equal(got, []arbiter.CleanupAck{want, want}) {
		t.Fatalf("accepted acknowledgements = %+v, want the same one twice", got)
	}
	if got := role.state.OwedCleanupAcks(); len(got) != 0 {
		t.Fatalf("owed acknowledgements after the redelivery = %+v, want none", got)
	}
	if got := writes(); len(got) != 1 || len(got[0].owed) != 0 {
		t.Fatalf("state writes of the redelivery = %+v, want one: the write that clears the owed acknowledgement", got)
	}
	if !reflect.DeepEqual(stateWithoutOwedS(role), before) {
		t.Fatal("the redelivered cleanup changed more than the owed acknowledgement")
	}
	if got := conn.executed(); len(got) != 4 {
		t.Fatalf("executed %q, want the two DROP PART statements twice", got)
	}
	if !purgeGateOpenS(t, role) {
		t.Fatal("the purge gate stays closed after the owed acknowledgement was cleared")
	}
}

// TestCleanupOfADroppedTableIsAcknowledged: once a table is gone so are its
// parts, and ClickHouse answers its DROP PART with UNKNOWN_TABLE (code 60). The
// cleanup is journaled and acknowledged as if the part were gone: an SNode
// that purged the table while an older release had not delivered the
// acknowledgement acknowledges the arbiter's redelivered cleanup and recovers.
func TestCleanupOfADroppedTableIsAcknowledged(t *testing.T) {
	part := func(name string) arbiter.PartRef {
		return arbiter.PartRef{TableID: "db.t", PartitionID: "all", PartRowLtHash: "0xaa", PartName: name}
	}
	for _, tc := range []struct {
		name    string
		cleanup arbiter.UnsafeCleanup
	}{
		{"promotion cleanup", arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 5, Parts: []arbiter.PartRef{part("all_9_9_0")}}},
		{"rejected-part cleanup", arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{part("all_1_1_0"), part("all_2_2_0")}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			role, fake, conn := newRecordingSNodeS(t)
			conn.failWith(&clickhouseproto.Exception{Code: int32(chproto.ErrUnknownTable), Message: "Table hg_unsafe.db__t does not exist"})
			if err := handleCleanupS(t, role, tc.cleanup); err != nil {
				t.Fatalf("cleanup of a dropped table: %v", err)
			}
			if got := conn.executed(); len(got) != len(tc.cleanup.Parts) {
				t.Fatalf("executed %q, want one DROP PART per part", got)
			}
			if got := acceptedS(t, fake); !slices.Equal(got, []arbiter.CleanupAck{cleanupAckS(tc.cleanup)}) {
				t.Fatalf("accepted acknowledgements = %+v, want [%+v]", got, cleanupAckS(tc.cleanup))
			}
			if got := role.state.OwedCleanupAcks(); len(got) != 0 {
				t.Fatalf("owed acknowledgements = %+v, want none", got)
			}
			if !purgeGateOpenS(t, role) {
				t.Fatal("the purge gate is closed after the acknowledged cleanup")
			}
		})
	}
}

// TestCleanupStillFailsOnOtherClickHouseErrors: only a missing part or a
// missing table counts as dropped. Any other ClickHouse error fails the
// cleanup before anything is journaled: no acknowledgement is sent or owed,
// and the promoted part keeps its exclusion and the purge gate closed.
func TestCleanupStillFailsOnOtherClickHouseErrors(t *testing.T) {
	for _, code := range []chproto.Error{chproto.ErrUnknownDatabase, chproto.ErrTimeoutExceeded, chproto.ErrTableIsReadOnly, chproto.ErrKeeperException} {
		t.Run(code.String(), func(t *testing.T) {
			role, fake, conn := newRecordingSNodeS(t)
			promoted := recordedPartS(t, role, "all_9_9_0")
			applyPromotionS(t, role, 5, promoted)
			conn.failWith(&clickhouseproto.Exception{Code: int32(code), Message: "injected"})
			err := handleCleanupS(t, role, arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 5, Parts: []arbiter.PartRef{promoted}})
			if err == nil {
				t.Fatalf("a DROP PART answered with %v did not fail the cleanup", code)
			}
			if got, refused := acceptedS(t, fake), fake.refused(); len(got) != 0 || len(refused) != 0 {
				t.Fatalf("acknowledgements sent: accepted %+v, refused %v; want none", got, refused)
			}
			if got := role.state.OwedCleanupAcks(); len(got) != 0 {
				t.Fatalf("owed acknowledgements = %+v, want none", got)
			}
			if got, err := role.PromotedUnsafeParts("db.t"); err != nil || !slices.Equal(got, []string{"all_9_9_0"}) {
				t.Fatalf("promoted unsafe parts = %v, %v: the part keeps its exclusion", got, err)
			}
			if purgeGateOpenS(t, role) {
				t.Fatal("the purge gate opened although the cleanup failed")
			}
		})
	}
}

// TestHandleCleanup_DroppedTableStillAcks (ClickHouse): the cleanup of a
// promoted part whose hg_unsafe table is gone, as after a purge, is journaled
// and acknowledged: ClickHouse answers its DROP PART with UNKNOWN_TABLE, which
// the cleanup path counts as dropped.
func TestHandleCleanup_DroppedTableStillAcks(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := promoteSeededPart(t, ctx, role, claims, signer, schema)
	cmd := cleanupCommand(schema.TableID, part)
	unsafe := role.cfg.UnsafeDatabase + "." + CHTableName(schema.TableID)
	mustExecIntake(t, role.d.Conn, "DROP TABLE "+unsafe+" SYNC")
	err := role.d.Conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PART '%s'", unsafe, escapeSQLString(part.PartName)))
	var exception *clickhouseproto.Exception
	if !errors.As(err, &exception) || exception.Code != int32(chproto.ErrUnknownTable) {
		t.Fatalf("DROP PART on the dropped table: err = %v, want UNKNOWN_TABLE", err)
	}
	if err := role.handleCleanup(ctx, wire.CleanupToPB(cmd), mustSignCleanup(t, signer, cmd)); err != nil {
		t.Fatalf("handleCleanup: %v", err)
	}
	want := arbiter.CleanupAck{NodeID: role.cfg.NodeID, PromotionSeq: 1, TableID: schema.TableID, PartitionID: part.PartitionID}
	if acks := claims.cleanupAcks(); len(acks) != 1 || acks[0] != want {
		t.Fatalf("cleanup acknowledgements = %+v, want [%+v]", acks, want)
	}
	if got, err := role.PromotedUnsafeParts(schema.TableID); err != nil || len(got) != 0 {
		t.Fatalf("promoted unsafe parts = %v, %v; want none", got, err)
	}
	if got := role.state.OwedCleanupAcks(); len(got) != 0 {
		t.Fatalf("owed acknowledgements = %+v, want none", got)
	}
}

// TestIsCleanupGoneErrAcceptsAMissingPartOrTable: the cleanup path treats
// NO_SUCH_DATA_PART and UNKNOWN_TABLE as dropped; the abort path's
// isMissingPartErr still accepts only NO_SUCH_DATA_PART.
func TestIsCleanupGoneErrAcceptsAMissingPartOrTable(t *testing.T) {
	missingTable := &clickhouseproto.Exception{Code: int32(chproto.ErrUnknownTable), Message: "table is absent"}
	missingPart := &clickhouseproto.Exception{Code: int32(chproto.ErrNoSuchDataPart), Message: "part is absent"}
	for _, err := range []error{missingTable, missingPart, fmt.Errorf("wrapped: %w", missingTable)} {
		if !isCleanupGoneErr(err) {
			t.Errorf("isCleanupGoneErr(%v) = false, want true", err)
		}
	}
	for _, err := range []error{
		errors.New("Table hg_unsafe.db__t does not exist"),
		&clickhouseproto.Exception{Code: int32(chproto.ErrUnknownDatabase), Message: "database is absent"},
		&clickhouseproto.Exception{Code: int32(chproto.ErrKeeperException), Message: "keeper"},
	} {
		if isCleanupGoneErr(err) {
			t.Errorf("isCleanupGoneErr(%v) = true, want false", err)
		}
	}
	if isMissingPartErr(missingTable) {
		t.Error("isMissingPartErr accepts UNKNOWN_TABLE; only the cleanup path may")
	}
}

// TestStateStore_OneOwedAckPerCleanup: one cleanup (table, partition,
// promotion_seq, rejected_block_seq) owes one acknowledgement: journaling it
// again writes nothing, another cleanup of the same partition owes its own,
// and clearing one clears exactly that one, once. An acknowledgement that
// is not the cleanup's is refused.
func TestStateStore_OneOwedAckPerCleanup(t *testing.T) {
	st, err := openStateStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	writes := 0
	st.beforeWrite = func(localState) error { writes++; return nil }
	ackOf := func(c arbiter.UnsafeCleanup) arbiter.CleanupAck {
		return arbiter.CleanupAck{NodeID: "s1", PromotionSeq: c.PromotionSeq, TableID: c.TableID, PartitionID: c.PartitionID, RejectedBlockSeq: c.RejectedBlockSeq}
	}
	promotion := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", PromotionSeq: 5}
	rejected := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12}
	other := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "p1", PromotionSeq: 5}
	for _, c := range []arbiter.UnsafeCleanup{promotion, rejected, other} {
		if err := st.RecordExecutedCleanup(c, ackOf(c)); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.RecordExecutedCleanup(promotion, ackOf(promotion)); err != nil || writes != 3 {
		t.Fatalf("journaling a cleanup again: %v, %d writes; want no error and no fourth write", err, writes)
	}
	owed := st.OwedCleanupAcks()
	if len(owed) != 3 || !slices.Contains(owed, ackOf(promotion)) || !slices.Contains(owed, ackOf(rejected)) || !slices.Contains(owed, ackOf(other)) {
		t.Fatalf("owed acknowledgements = %+v, want one per cleanup", owed)
	}
	if err := st.ClearOwedCleanupAck(ackOf(rejected)); err != nil {
		t.Fatal(err)
	}
	if err := st.ClearOwedCleanupAck(ackOf(rejected)); err != nil || writes != 4 {
		t.Fatalf("clearing twice: %v, %d writes; want no error and one write", err, writes)
	}
	if got, want := st.OwedCleanupAcks(), []arbiter.CleanupAck{ackOf(promotion), ackOf(other)}; !slices.Equal(got, want) {
		t.Fatalf("owed acknowledgements = %+v, want %+v", got, want)
	}
	mismatched := ackOf(promotion)
	mismatched.PromotionSeq = 6
	if err := st.RecordExecutedCleanup(promotion, mismatched); err == nil {
		t.Fatal("an acknowledgement of another cleanup was recorded as this cleanup's")
	}
}

// TestStateStore_OwedCleanupAcksIsAnOptionalJSONField: a state file written
// by v0.14.0 has no owed_cleanup_acks; it loads unchanged and owes nothing. A
// state written while nothing is owed keeps v0.14.0's shape, which v0.14.0
// reads; an owed acknowledgement is written under owed_cleanup_acks in
// CleanupAck's own JSON form and survives a reopen.
func TestStateStore_OwedCleanupAcksIsAnOptionalJSONField(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "state.json")
	v0140 := `{"watermarks":{"db.t\u0000all":5},"last_acks":{},"base_roots":{"db.t\u0000all":"0xpost"},` +
		`"base_snapshot_ids":{"db.t\u0000all":"snap"},"unpromoted_sums":{},` +
		`"promoted_unsafe_parts":{"db.t\u0000all":["all_9_9_0"]},"registration_seq":7}`
	if err := os.WriteFile(path, []byte(v0140), 0o600); err != nil {
		t.Fatal(err)
	}
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatalf("a v0.14.0 state file: %v", err)
	}
	k := partitionKey{Table: "db.t", Partition: "all"}
	root, snap := st.BaseRoot(k)
	if got, err := st.PromotedUnsafeParts("db.t"); err != nil || !slices.Equal(got, []string{"all_9_9_0"}) || st.Watermark(k) != 5 || root != "0xpost" || snap != "snap" {
		t.Fatalf("v0.14.0 state loaded as watermark %d, base %s/%s, promoted %v (%v)", st.Watermark(k), root, snap, got, err)
	}
	if got := st.OwedCleanupAcks(); len(got) != 0 {
		t.Fatalf("a v0.14.0 state owes %+v", got)
	}
	fields := func() map[string]json.RawMessage {
		t.Helper()
		b, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		var out map[string]json.RawMessage
		if err := json.Unmarshal(b, &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	if err := st.RecordCleanup(k, []string{"all_9_9_0"}); err != nil {
		t.Fatal(err)
	}
	if raw, ok := fields()["owed_cleanup_acks"]; ok {
		t.Fatalf("a state owing nothing was written with owed_cleanup_acks %s", raw)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12}
	ack := arbiter.CleanupAck{NodeID: "s1", TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12}
	if err := st.RecordExecutedCleanup(cleanup, ack); err != nil {
		t.Fatal(err)
	}
	var owed map[string]map[string]any
	if err := json.Unmarshal(fields()["owed_cleanup_acks"], &owed); err != nil {
		t.Fatalf("owed_cleanup_acks: %v", err)
	}
	wantJSON := map[string]any{"node_id": "s1", "promotion_seq": float64(0), "table_id": "db.t", "partition_id": "all", "rejected_block_seq": float64(12)}
	if len(owed) != 1 || !reflect.DeepEqual(slices.Collect(maps.Values(owed))[0], wantJSON) {
		t.Fatalf("owed_cleanup_acks = %v, want one entry %v", owed, wantJSON)
	}
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if got := reopened.OwedCleanupAcks(); !slices.Equal(got, []arbiter.CleanupAck{ack}) {
		t.Fatalf("owed acknowledgements after a reopen = %+v, want [%+v]", got, ack)
	}
}

// TestConfigCleanupAckResendInterval: zero selects the default and the
// minimum is accepted. A negative interval is refused, and so is one below the
// minimum: the interval also bounds each re-send pass, and a pass shorter
// than the arbiter's commit latency would time out every re-send, so an
// acknowledgement the arbiter accepted but whose entry was not cleared would
// stay owed for ever.
func TestConfigCleanupAckResendInterval(t *testing.T) {
	for _, tc := range []struct {
		name     string
		interval time.Duration
		want     time.Duration // after validation; 0 when it is refused
		refusal  []string      // what the refusal names
	}{
		{"zero selects the default", 0, DefaultCleanupAckResendInterval, nil},
		{"the minimum", MinCleanupAckResendInterval, MinCleanupAckResendInterval, nil},
		{"negative", -time.Second, 0, []string{"cleanup ack resend interval", "negative"}},
		{"below the minimum", MinCleanupAckResendInterval - time.Millisecond, 0, []string{"cleanup ack resend interval", "999ms", "1s"}},
		{"far below the minimum", 50 * time.Millisecond, 0, []string{"cleanup ack resend interval", "50ms", "1s"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfigS(t)
			cfg.CleanupAckResendInterval = tc.interval
			err := cfg.validate()
			if tc.refusal == nil {
				if err != nil || cfg.CleanupAckResendInterval != tc.want {
					t.Fatalf("validate: %v, interval %v; want no error and %v", err, cfg.CleanupAckResendInterval, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("an interval of %v was accepted", tc.interval)
			}
			for _, want := range tc.refusal {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v, want it to name %q", err, want)
				}
			}
		})
	}
}
