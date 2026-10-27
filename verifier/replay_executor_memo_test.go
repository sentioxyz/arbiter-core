package verifier

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	chproto "github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
	"github.com/sentioxyz/arbiter-core/wire"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// memoCH implements only the actual materializer's native-driver operations.
// Unexpected capabilities panic through the embedded nil interface.
type memoCH struct {
	clickhouse.Conn
	mu                             sync.Mutex
	creates, sends, queries, drops int
	fail                           bool
	cancel                         func()
	rows                           [][]any
}

func (c *memoCH) Exec(_ context.Context, q string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if strings.HasPrefix(q, "CREATE TABLE ") {
		c.creates++
		if c.fail {
			return errors.New("test CREATE failure")
		}
	}
	if strings.HasPrefix(q, "DROP TABLE IF EXISTS ") {
		c.drops++
	}
	return nil
}
func (c *memoCH) PrepareBatch(_ context.Context, _ string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return &memoBatch{c: c}, nil
}
func (c *memoCH) Query(_ context.Context, _ string, _ ...any) (driver.Rows, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries++
	return &memoRows{rows: append([][]any(nil), c.rows...)}, nil
}
func (c *memoCH) count() int { c.mu.Lock(); defer c.mu.Unlock(); return c.creates }

type memoBatch struct {
	driver.Batch
	c    *memoCH
	rows [][]any
}

func (b *memoBatch) Append(v ...any) error {
	b.rows = append(b.rows, append([]any(nil), v...))
	return nil
}
func (b *memoBatch) Send() error {
	b.c.mu.Lock()
	defer b.c.mu.Unlock()
	b.c.sends++
	b.c.rows = b.rows
	if b.c.cancel != nil {
		b.c.cancel()
	}
	return nil
}

type memoRows struct {
	driver.Rows
	rows [][]any
	n    int
}

func (r *memoRows) Next() bool { return r.n < len(r.rows) }
func (r *memoRows) Scan(dest ...any) error {
	for i, d := range dest {
		reflect.ValueOf(d).Elem().Set(reflect.ValueOf(r.rows[r.n][i]))
	}
	r.n++
	return nil
}
func (r *memoRows) Close() error { return nil }
func (r *memoRows) Err() error   { return nil }

type memoCountSigner struct {
	replay.Signer
	calls atomic.Int32
}

func (s *memoCountSigner) SignReplayReceipt(ctx context.Context, h string) (string, string, error) {
	s.calls.Add(1)
	return s.Signer.SignReplayReceipt(ctx, h)
}

type memoFixture struct {
	cfg       Config
	core      *replay.Verifier
	ch        *memoCH
	snapshots *payloadexec.MemSnapshotStore
	payloads  *payloadexec.MemPayloadStore
	job       replay.ReplayJob
	payload   []byte
	signer    *memoCountSigner
}

func newMemoFixture(t *testing.T) *memoFixture {
	t.Helper()
	cfg := testConfigV()
	raw, err := nativepayload.EncodeClientDataPacket(54460, []chproto.InputColumn{{Name: "v", Data: &chproto.ColUInt64{7}}})
	if err != nil {
		t.Fatal(err)
	}
	snap, err := payloadexec.New(cfg.NetworkID, cfg.Tables...).GenesisSnapshot(0, cfg.SchemaSnapshotID, cfg.ExecutorProfileID)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := payloadexec.NewMemSnapshotStore()
	snapshots.Put(snap)
	payloads := payloadexec.NewMemPayloadStore()
	payloads.Put("p", raw)
	ch := &memoCH{}
	core, err := NewReplayCore(cfg, ch, snapshots, payloads)
	if err != nil {
		t.Fatal(err)
	}
	signer := &memoCountSigner{Signer: core.Signer}
	core.Signer = signer
	sql := "INSERT INTO db.t VALUES (7)"
	job := replay.ReplayJob{BlockSeq: 1, PrevSafeSnapshotID: snap.SnapshotID, PrevStateRoot: snap.StateRoot, SchemaSnapshotID: cfg.SchemaSnapshotID, ExecutorProfileID: cfg.ExecutorProfileID, SourceClaimRoot: replay.DigestString("intentional-negative-claim"), Statements: []replay.Statement{{StatementID: "s1", StatementSeq: 1, SQL: sql, SQLHash: replay.DigestString(sql), SettingsHash: replay.DigestString("empty-settings"), PayloadRef: "p", PayloadHash: replay.DigestBytes(raw), PayloadLength: uint64(len(raw)), TargetTableID: "db.t", PayloadFormat: replay.PayloadFormatClickHouseNativeData, ClientRevision: 54460, SchemaHash: payloadexec.TableSchemaHash(cfg.NetworkID, cfg.Tables[0])}}}
	return &memoFixture{cfg, core, ch, snapshots, payloads, job, raw, signer}
}
func (f *memoFixture) scope(t *testing.T) (context.Context, *replayMemoRun) {
	t.Helper()
	ctx, run := newReplayMemoRun(context.Background(), f.cfg)
	t.Cleanup(run.close)
	return withReplayMemoJob(ctx, wire.ReplayJobToPB(f.job)), run
}
func TestReplayMemoActualVerifierMaterializesOnceAndSignsEveryTime(t *testing.T) {
	f := newMemoFixture(t)
	ctx, _ := f.scope(t)
	a, err := f.core.Verify(ctx, f.job)
	if err != nil {
		t.Fatal(err)
	}
	b, err := f.core.Verify(ctx, f.job)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) || a.MatchSourceRoot || a.Signature == "" {
		t.Fatal("valid signed negative attestation changed")
	}
	if f.ch.count() != 1 {
		t.Fatalf("materializer CREATE calls=%d, want1 completed execution", f.ch.count())
	}
	if f.signer.calls.Load() != 2 {
		t.Fatal("must sign each Verify, including hit")
	}
	if f.ch.sends != 1 || f.ch.queries != 1 || f.ch.drops != 1 {
		t.Fatal("actual materializer path incomplete")
	}
}

func TestReplayMemoRevalidatesMutableStoresBeforeHit(t *testing.T) {
	for _, mode := range []string{"payload-hash", "payload-length", "payload-missing", "snapshot", "schema"} {
		t.Run(mode, func(t *testing.T) {
			f := newMemoFixture(t)
			ctx, _ := f.scope(t)
			if _, err := f.core.Verify(ctx, f.job); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "payload-hash":
				raw := append([]byte(nil), f.payload...)
				raw[len(raw)-1] ^= 1
				f.payloads.Put("p", raw)
			case "payload-length":
				f.payloads.Put("p", nil)
			case "payload-missing":
				f.core.Payloads = payloadexec.NewMemPayloadStore()
			case "snapshot":
				f.core.Snapshots = payloadexec.NewMemSnapshotStore()
			case "schema":
				f.core.SchemaHashes = memoMissingSchema{}
			}
			if _, err := f.core.Verify(ctx, f.job); err == nil {
				t.Fatal("changed dependency validation was bypassed")
			}
			if f.ch.count() != 1 || f.signer.calls.Load() != 1 {
				t.Fatal("invalid input executed or signed")
			}
		})
	}
}

type memoMissingSchema struct{}

func (memoMissingSchema) TableSchemaHash(string) (string, bool) { return "", false }

func TestReplayMemoScopesUnknownFieldsAndLifecycle(t *testing.T) {
	f := newMemoFixture(t)
	ctx, run := f.scope(t)
	verify := func(ctx context.Context) {
		t.Helper()
		if _, err := f.core.Verify(ctx, f.job); err != nil {
			t.Fatal(err)
		}
	}
	verify(ctx)
	verify(ctx)
	pbjob := wire.ReplayJobToPB(f.job)
	pbjob.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	verify(withReplayMemoJob(ctx, pbjob))
	if f.ch.count() != 2 {
		t.Fatal("unknown wire field reused prior execution")
	}
	otherCfg := cloneReplayConfig(f.cfg)
	otherCfg.ExecutorProfileID = "different"
	other, otherRun := newReplayMemoRun(context.Background(), otherCfg)
	defer otherRun.close()
	verify(withReplayMemoJob(other, wire.ReplayJobToPB(f.job)))
	verify(withReplayMemoJob(other, wire.ReplayJobToPB(f.job)))
	if f.ch.count() != 4 {
		t.Fatal("changed context reused")
	}
	verify(context.Background())
	verify(context.Background())
	if f.ch.count() != 6 {
		t.Fatal("unscoped Verify must bypass")
	}
	m := f.core.Executor.(*replayExecutorMemo)
	run.close()
	m.mu.Lock()
	left := len(m.entries)
	m.mu.Unlock()
	if left != 0 {
		t.Fatal("run shutdown retained completed entries")
	}
	fresh, freshRun := newReplayMemoRun(context.Background(), f.cfg)
	defer freshRun.close()
	verify(withReplayMemoJob(fresh, wire.ReplayJobToPB(f.job)))
	if f.ch.count() != 7 {
		t.Fatal("new generation reused old result")
	}
}
func TestReplayMemoClonesCallerConfigAndResult(t *testing.T) {
	f := newMemoFixture(t)
	ctx, _ := f.scope(t)
	f.cfg.Tables[0].Columns[0].Name = "caller-mutated"
	if _, err := f.core.Verify(ctx, f.job); err != nil {
		t.Fatal("constructor retained cfg alias:", err)
	}
	a, err := f.core.Verify(ctx, f.job)
	if err != nil {
		t.Fatal(err)
	}
	if len(a.Receipt.PartitionCommitmentsAfter) == 0 || len(a.Receipt.AffectedParts) == 0 {
		t.Fatal("fixture lacks nested commitments")
	}
	a.Receipt.PartitionCommitmentsAfter[0].Root = "mutated"
	a.Receipt.AffectedParts[0].StorageRefs = []string{"mutated"}
	b, err := f.core.Verify(ctx, f.job)
	if err != nil {
		t.Fatal(err)
	}
	if b.Receipt.PartitionCommitmentsAfter[0].Root == "mutated" || len(b.Receipt.AffectedParts[0].StorageRefs) > 0 {
		t.Fatal("returned result aliases memo")
	}
	if f.ch.count() != 1 {
		t.Fatal("mutation caused an unexpected miss")
	}
}
func TestReplayMemoFailureAndCancellationNeverPublish(t *testing.T) {
	f := newMemoFixture(t)
	ctx, _ := f.scope(t)
	f.ch.fail = true
	if _, err := f.core.Verify(ctx, f.job); err == nil {
		t.Fatal("CREATE failure accepted")
	}
	f.ch.fail = false
	if _, err := f.core.Verify(ctx, f.job); err != nil {
		t.Fatal(err)
	}
	if f.ch.count() != 2 {
		t.Fatal("failed execution cached")
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := f.core.Verify(canceled, f.job); !errors.Is(err, context.Canceled) {
		t.Fatal("hit ignored cancellation", err)
	}
	g := newMemoFixture(t)
	ctx2, _ := g.scope(t)
	during, cancelDuring := context.WithCancel(ctx2)
	g.ch.cancel = cancelDuring
	if _, err := g.core.Verify(during, g.job); !errors.Is(err, context.Canceled) {
		t.Fatal("completion cancellation accepted", err)
	}
	g.ch.cancel = nil
	if _, err := g.core.Verify(ctx2, g.job); err != nil {
		t.Fatal(err)
	}
	if g.ch.count() != 2 {
		t.Fatal("canceled computation cached")
	}
}
func TestReplayMemoConcurrentMissesAndIndependentCopies(t *testing.T) {
	// Transition execution has no CH side effects, so concurrent misses exercise
	// the actual verifier/dispatcher deterministically without sharing test rows.
	f := newMemoFixture(t)
	f.job.Statements = nil
	f.job.SourceClaimRoot = ""
	f.job.TableSetTransition = &replay.ReplayTableSetTransition{NewSchemaRoot: f.cfg.SchemaRoot}
	// An empty transition is invalid; use a real retirement of the genesis table.
	f.job.TableSetTransition.Retires = []string{"db.t"}
	f.job.TableSetTransition.NewSchemaRoot = payloadexec.SchemaRootFromHashes(map[string]string{})
	ctx, _ := f.scope(t)
	var wg sync.WaitGroup
	errs := make(chan error, 24)
	for i := 0; i < 24; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := f.core.Verify(ctx, f.job); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if f.signer.calls.Load() != 24 {
		t.Fatal("concurrent calls skipped signing")
	}
	m := f.core.Executor.(*replayExecutorMemo)
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) != 1 {
		t.Fatal("concurrent publication created duplicate entries")
	}
}
func TestReplayMemoExplicitKeyIncludesEveryRequestField(t *testing.T) {
	f := newMemoFixture(t)
	snap, _ := f.snapshots.GetSafeSnapshot(context.Background(), f.job.PrevSafeSnapshotID)
	req := replay.ExecutionRequest{Job: f.job, Snapshot: snap, Statements: []replay.PreparedStatement{{Statement: f.job.Statements[0], Payload: f.payload}}}
	baseline, err := replayMemoRequestBytes(req, [32]byte{1})
	if err != nil {
		t.Fatal(err)
	}
	edits := []func(*replay.ExecutionRequest){
		func(r *replay.ExecutionRequest) { r.Statements[0].Payload[0] ^= 1 },
		func(r *replay.ExecutionRequest) { r.Statements[0].UserJWS = "changed-auth" },
		func(r *replay.ExecutionRequest) { r.Snapshot.TrackFrontiers = []replay.TrackFrontier{{IndexerID: 3}} },
		func(r *replay.ExecutionRequest) {
			r.Snapshot.Tables[0].ActiveParts = []replay.PartManifestEntry{{StorageRefs: []string{"changed"}}}
		},
		func(r *replay.ExecutionRequest) { r.Job.ExecutorProfileID = "changed-profile" },
		func(r *replay.ExecutionRequest) {
			r.Job.TableSchemas = []replay.ReplayTableSchema{{TableID: "db.t", SchemaJSON: "changed"}}
		},
	}
	for i, edit := range edits {
		var owned replayMemoRequest
		if err := json.Unmarshal(baseline, &owned); err != nil {
			t.Fatal(err)
		}
		q := replay.ExecutionRequest{Job: owned.Job, Snapshot: owned.Snapshot, Statements: []replay.PreparedStatement{{Statement: owned.Statements[0].Statement, Payload: owned.Statements[0].Payload}}}
		edit(&q)
		changed, err := replayMemoRequestBytes(q, [32]byte{1})
		if err != nil || string(changed) == string(baseline) {
			t.Fatalf("field mutation%d omitted", i)
		}
	}
	changed, _ := replayMemoRequestBytes(req, [32]byte{2})
	if string(changed) == string(baseline) {
		t.Fatal("full wire digest omitted")
	}
	// Frozen field-count guards make future json-omitted input/result additions
	// require an explicit eligibility/encoding review, rather than silent reuse.
	for _, tc := range []struct {
		v      any
		fields int
	}{{replay.ExecutionRequest{}, 5}, {replay.PreparedStatement{}, 2}, {replay.ExecutionResult{}, 10}} {
		if reflect.TypeOf(tc.v).NumField() != tc.fields {
			t.Fatalf("memo field audit required for %T", tc.v)
		}
	}
}
func TestReplayMemoLRUByteCapOversizeAndNestedStorageRefs(t *testing.T) {
	m := &replayExecutorMemo{entries: make(map[replayMemoKey]*list.Element)}
	run := &replayMemoRun{}
	key := func(i int) replayMemoKey {
		return replayMemoKey{run: run, digest: sha256.Sum256([]byte(strconv.Itoa(i)))}
	}
	for i := 0; i < replayMemoEntries; i++ {
		m.put(key(i), []byte(`{}`))
	}
	m.get(key(0))
	m.put(key(999), []byte(`{}`))
	if _, ok := m.get(key(1)); ok {
		t.Fatal("LRU victim retained")
	}
	if _, ok := m.get(key(0)); !ok {
		t.Fatal("recent entry evicted")
	}
	huge := make([]byte, replayMemoBytes)
	m.put(key(1000), huge)
	if _, ok := m.get(key(1000)); ok {
		t.Fatal("oversize inserted")
	}
	for i := 0; i < 4; i++ {
		m.put(key(2000+i), make([]byte, 3<<20))
	}
	if m.bytes > replayMemoBytes || len(m.entries) > replayMemoEntries {
		t.Fatal("memory bounds exceeded")
	}
	// The same result encoding used by the memo must detach nested StorageRefs.
	original := replay.ExecutionResult{AffectedParts: []replay.PartManifestEntry{{StorageRefs: []string{"original"}}}}
	raw, _ := json.Marshal(original)
	m.put(key(3000), raw)
	original.AffectedParts[0].StorageRefs[0] = "caller-mutated"
	cached, _ := m.get(key(3000))
	var first, second replay.ExecutionResult
	json.Unmarshal(cached, &first)
	first.AffectedParts[0].StorageRefs[0] = "returned-mutated"
	json.Unmarshal(cached, &second)
	if second.AffectedParts[0].StorageRefs[0] != "original" {
		t.Fatal("nested result alias")
	}
}

func TestReplayMemoActualRoleSubmitsEveryDispatchAndScansFresh(t *testing.T) {
	f := newMemoFixture(t)
	scanner := &fakeScanner{errs: []error{errors.New("missing physical part"), nil}}
	role, server := newRoleHarnessV(t, &fakeReplayCore{}, scanner)
	role.d.Replay = f.core
	server.mu.Lock()
	server.attestationErr = status.Error(codes.Internal, "test submit failure")
	server.mu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- role.Run(ctx) }()
	server.push(wire.ReplayJobDispatch(f.job))
	waitVerifier(t, "first actual submission", func() bool { _, _, a, _ := server.snapshot(); return len(a) == 1 })
	server.mu.Lock()
	server.attestationErr = nil
	server.mu.Unlock()
	server.push(wire.ReplayJobDispatch(f.job))
	waitVerifier(t, "second actual submission", func() bool { _, _, a, _ := server.snapshot(); return len(a) == 2 })
	if f.ch.count() != 1 || f.signer.calls.Load() != 2 {
		t.Fatal("failed submit retry skipped fresh signing or repeated materialization")
	}
	server.push(wire.ByteSideScanDispatch(1, nil))
	server.push(wire.ByteSideScanDispatch(1, nil))
	waitVerifier(t, "both fresh scans", func() bool { return scanner.callCount() == 2 })
	waitVerifier(t, "successful second scan", func() bool { _, _, _, s := server.snapshot(); return len(s) == 1 })
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	m := f.core.Executor.(*replayExecutorMemo)
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) != 0 {
		t.Fatal("Role.Run exit retained generation results")
	}
}

type memoExecutorFunc func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error)

func (f memoExecutorFunc) Replay(ctx context.Context, r replay.ExecutionRequest) (replay.ExecutionResult, error) {
	return f(ctx, r)
}
func TestReplayMemoQueryBypassAndContextValidation(t *testing.T) {
	f := newMemoFixture(t)
	ctx, _ := f.scope(t)
	if _, ok := f.core.Executor.(replay.GenesisSnapshotSource); ok {
		t.Fatal("memo added an optional executor capability")
	}
	if _, err := f.core.Executor.Replay(nil, replay.ExecutionRequest{}); err == nil || !strings.Contains(err.Error(), "context is required") {
		t.Fatal("nil context changed", err)
	}
	for _, req := range []replay.ExecutionRequest{{SnapshotQuery: &replay.SnapshotQueryJob{ExecutorProfileID: "x", QueryProfileID: "y"}, SnapshotQueryReferenceID: "r"}, {SnapshotQueryReferenceID: "r"}} {
		if _, err := f.core.Executor.Replay(ctx, req); err == nil {
			t.Fatal("query/reference path bypassed dispatcher rejection")
		}
	}
	// A future nonlegacy result is never cached, even on a legacy request.
	var calls atomic.Int32
	d, err := snapshotquery.NewCompositeExecutor(memoExecutorFunc(func(context.Context, replay.ExecutionRequest) (replay.ExecutionResult, error) {
		calls.Add(1)
		return replay.ExecutionResult{SnapshotQuery: &replay.SnapshotQueryEvidence{}}, nil
	}), nil)
	if err != nil {
		t.Fatal(err)
	}
	m := newReplayExecutorMemo(f.cfg, d)
	fresh, run := newReplayMemoRun(context.Background(), f.cfg)
	defer run.close()
	fresh = withReplayMemoJob(fresh, wire.ReplayJobToPB(f.job))
	for i := 0; i < 2; i++ {
		if _, err := m.Replay(fresh, replay.ExecutionRequest{Job: f.job}); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 || len(m.entries) != 0 {
		t.Fatal("query evidence cached")
	}
}

func TestReplayMemoEvidenceContextRejectionInvalidates(t *testing.T) {
	for _, code := range []codes.Code{codes.InvalidArgument, codes.Unauthenticated, codes.PermissionDenied} {
		t.Run(code.String(), func(t *testing.T) {
			f := newMemoFixture(t)
			role, server := newRoleHarnessV(t, &fakeReplayCore{}, &fakeScanner{})
			role.d.Replay = f.core
			scoped, run := newReplayMemoRun(context.Background(), role.cfg)
			defer run.close()
			if err := role.handleReplayJob(scoped, wire.ReplayJobToPB(f.job)); err != nil {
				t.Fatal(err)
			}
			server.mu.Lock()
			server.attestationErr = status.Error(code, "invalid evidence context")
			server.mu.Unlock()
			if err := role.handleReplayJob(scoped, wire.ReplayJobToPB(f.job)); status.Code(err) != code {
				t.Fatal("rejection changed", err)
			}
			if f.ch.count() != 1 {
				t.Fatal("rejection dispatch did not use its completed computation")
			}
			server.mu.Lock()
			server.attestationErr = nil
			server.mu.Unlock()
			if err := role.handleReplayJob(scoped, wire.ReplayJobToPB(f.job)); err != nil {
				t.Fatal(err)
			}
			if f.ch.count() != 2 || f.signer.calls.Load() != 3 {
				t.Fatal("explicit context rejection failed to invalidate")
			}
		})
	}
}
func TestReplayMemoChangedWireProjectionBypasses(t *testing.T) {
	f := newMemoFixture(t)
	ctx, _ := f.scope(t)
	changed := wire.ReplayJobToPB(f.job)
	changed.SourceClaimRoot = "different"
	for i := 0; i < 2; i++ {
		if _, err := f.core.Verify(withReplayMemoJob(ctx, changed), f.job); err != nil {
			t.Fatal(err)
		}
	}
	if f.ch.count() != 2 {
		t.Fatal("wire and validated job disagreement reused")
	}
}

func TestReplayMemoRetainsRealLeaderRetryOnEveryDispatch(t *testing.T) {
	f := newMemoFixture(t)
	role, server := newRoleHarnessV(t, &fakeReplayCore{}, &fakeScanner{})
	role.d.Replay = f.core
	server.mu.Lock()
	server.attestationErrors = []error{status.Error(codes.FailedPrecondition, "leader changed"), nil}
	server.mu.Unlock()
	ctx, run := newReplayMemoRun(context.Background(), role.cfg)
	defer run.close()
	if err := role.handleReplayJob(ctx, wire.ReplayJobToPB(f.job)); err != nil {
		t.Fatal(err)
	}
	if err := role.handleReplayJob(ctx, wire.ReplayJobToPB(f.job)); err != nil {
		t.Fatal(err)
	}
	_, _, atts, _ := server.snapshot()
	if len(atts) != 3 || f.ch.count() != 1 || f.signer.calls.Load() != 2 {
		t.Fatal("real leader retry/fresh second Submit/signing changed")
	}
}

func TestReplayMemoCannotRetainOldBindingOnInvalidWireInput(t *testing.T) {
	for _, mode := range []string{"nil", "invalid-utf8"} {
		t.Run(mode, func(t *testing.T) {
			f := newMemoFixture(t)
			ctx, _ := f.scope(t)
			if _, err := f.core.Verify(ctx, f.job); err != nil {
				t.Fatal(err)
			}
			input := wire.ReplayJobToPB(f.job)
			if mode == "nil" {
				input = nil
			} else {
				input.SourceClaimRoot = "\xff"
			}
			invalid := withReplayMemoJob(ctx, input)
			if _, err := f.core.Verify(invalid, f.job); err != nil {
				t.Fatal(err)
			}
			if f.ch.count() != 2 {
				t.Fatal("invalid wire input retained previous completed binding")
			}
		})
	}
}

func TestReplayMemoPrivateSchemaDoesNotEscapeViaSchemaHashes(t *testing.T) {
	f := newMemoFixture(t)
	ctx, _ := f.scope(t)
	if _, err := f.core.Verify(ctx, f.job); err != nil {
		t.Fatal(err)
	}
	// The replaceable schema-hash dependency exposes its own schema data. It
	// must not provide a mutable alias to the constructor-owned executor.
	exposed := f.core.SchemaHashes.(payloadexec.SchemaHashes)
	exposed.Tables[0].Columns[0].Type = "UInt32"
	f.core.SchemaHashes = payloadexec.SchemaHashes{NetworkID: f.cfg.NetworkID, Tables: cloneReplayConfig(f.cfg).Tables}
	if _, err := f.core.Verify(ctx, f.job); err != nil {
		t.Fatal("cached path failed", err)
	}
	// Change only the unknown protobuf field to force a conservative miss.
	job := wire.ReplayJobToPB(f.job)
	job.ProtoReflect().SetUnknown([]byte{0xa0, 0x06, 0x01})
	if _, err := f.core.Verify(withReplayMemoJob(ctx, job), f.job); err != nil {
		t.Fatalf("constructor-owned executor schema escaped through mutable SchemaHashes: cached result succeeded but fresh execution refused: %v", err)
	}
}

type memoEmptySigner struct{ calls int }

func (s *memoEmptySigner) SignReplayReceipt(context.Context, string) (string, string, error) {
	s.calls++
	return "", "", nil
}
func TestReplayMemoPublicSignerStillRunsOnEveryHit(t *testing.T) {
	f := newMemoFixture(t)
	ctx, _ := f.scope(t)
	if _, err := f.core.Verify(ctx, f.job); err != nil {
		t.Fatal(err)
	}
	original := f.core.Signer
	bad := &memoEmptySigner{}
	f.core.Signer = bad
	if _, err := f.core.Verify(ctx, f.job); err == nil {
		t.Fatal("cached execution bypassed current signer refusal")
	}
	if bad.calls != 1 || f.ch.count() != 1 {
		t.Fatal("signer was skipped or computation unexpectedly repeated")
	}
	f.core.Signer = nil
	if _, err := f.core.Verify(ctx, f.job); err == nil {
		t.Fatal("cached execution bypassed nil signer guard")
	}
	f.core.Signer = original
	if _, err := f.core.Verify(ctx, f.job); err != nil {
		t.Fatal(err)
	}
	if f.signer.calls.Load() != 2 || f.ch.count() != 1 {
		t.Fatal("restored signer was not invoked freshly")
	}
}
