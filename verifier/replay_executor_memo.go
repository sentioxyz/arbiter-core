package verifier

import (
	"container/list"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

const replayMemoEntries = 128
const replayMemoBytes = 8 << 20

// The fixed overhead conservatively accounts for the digest, map/list node and
// entry headers. Requests/payloads are hashed, never retained in the cache.
const replayMemoEntryOverhead = 256

type replayMemoRunKey struct{}
type replayMemoJobKey struct{}
type replayMemoRun struct {
	mu       sync.Mutex
	identity [32]byte
	owner    *replayExecutorMemo
	closed   bool
}
type replayMemoJob struct {
	wire [32]byte
	job  [32]byte
}

func cloneReplayConfig(cfg Config) Config {
	cfg.Ed25519Seed = append([]byte(nil), cfg.Ed25519Seed...)
	cfg.Tables = append([]payloadexec.TableSchema(nil), cfg.Tables...)
	for i := range cfg.Tables {
		cfg.Tables[i].Columns = append(cfg.Tables[i].Columns[:0:0], cfg.Tables[i].Columns...)
	}
	return cfg
}

func replayMemoIdentity(cfg Config) [32]byte {
	// Only public identity is retained; never encode the private signing seed.
	var public string
	if len(cfg.Ed25519Seed) == ed25519.SeedSize {
		public = hex.EncodeToString(ed25519.NewKeyFromSeed(cfg.Ed25519Seed).Public().(ed25519.PublicKey))
	}
	if cfg.UnsafeDatabase == "" {
		cfg.UnsafeDatabase = defaultUnsafeDatabase
	}
	if cfg.SafeDatabase == "" {
		cfg.SafeDatabase = defaultSafeDatabase
	}
	if cfg.PromoteDatabase == "" {
		cfg.PromoteDatabase = defaultPromoteDatabase
	}
	if cfg.GenesisSnapshotID == "" {
		cfg.GenesisSnapshotID, _ = dataplane.GenesisSnapshotID(cfg.NetworkID, cfg.SchemaSnapshotID, cfg.ExecutorProfileID, cfg.Tables)
	}
	data, _ := json.Marshal(struct {
		Domain, Replica, Public, Network, Genesis, Schema, Profile, Root, Unsafe, Safe, Promote string
		Source                                                                                  string
		Tables                                                                                  []payloadexec.TableSchema
	}{"verifier-replay-context-v1", cfg.ReplicaID, public, cfg.NetworkID, cfg.GenesisSnapshotID, cfg.SchemaSnapshotID, cfg.ExecutorProfileID, cfg.SchemaRoot, cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase, string(cfg.SchemaSource), cfg.Tables})
	return sha256.Sum256(data)
}

func newReplayMemoRun(ctx context.Context, cfg Config) (context.Context, *replayMemoRun) {
	run := &replayMemoRun{identity: replayMemoIdentity(cfg)}
	return context.WithValue(ctx, replayMemoRunKey{}, run), run
}
func (r *replayMemoRun) close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	if r.owner != nil {
		r.owner.removeRun(r)
	}
}
func withReplayMemoJob(ctx context.Context, m *pb.ReplayJob) context.Context {
	// Never retain a previous private binding when this input cannot bind.
	ctx = context.WithValue(ctx, replayMemoJobKey{}, struct{}{})
	if m == nil {
		return ctx
	}
	full, err := (proto.MarshalOptions{Deterministic: true}).Marshal(m)
	if err != nil {
		return ctx
	}
	projected, err := json.Marshal(wire.ReplayJobFromPB(m))
	if err != nil {
		return ctx
	}
	return context.WithValue(ctx, replayMemoJobKey{}, replayMemoJob{sha256.Sum256(full), sha256.Sum256(projected)})
}

type replayMemoKey struct {
	run    *replayMemoRun
	digest [32]byte
}
type replayMemoEntry struct {
	key   replayMemoKey
	value []byte
	cost  int
}
type replayExecutorMemo struct {
	// The only installed underlying executor is NewReplayCore's own dispatcher.
	// No optional capabilities are added to the baseline CompositeExecutor.
	executor *snapshotquery.CompositeExecutor
	identity [32]byte
	mu       sync.Mutex
	lru      list.List
	entries  map[replayMemoKey]*list.Element
	bytes    int
}

func newReplayExecutorMemo(cfg Config, e *snapshotquery.CompositeExecutor) *replayExecutorMemo {
	return &replayExecutorMemo{executor: e, identity: replayMemoIdentity(cfg), entries: make(map[replayMemoKey]*list.Element)}
}

// Explicit payload encoding is required: PreparedStatement.Payload is json:"-".
// Query requests/references are never eligible for this legacy-only memo.
type replayMemoPrepared struct {
	Statement replay.Statement
	Payload   []byte
}
type replayMemoRequest struct {
	Domain     string
	Wire       [32]byte
	Job        replay.ReplayJob
	Snapshot   replay.SafeSnapshotManifest
	Statements []replayMemoPrepared
}

func replayMemoRequestBytes(req replay.ExecutionRequest, w [32]byte) ([]byte, error) {
	key := replayMemoRequest{Domain: "verifier-replay-execution-v1", Wire: w, Job: req.Job, Snapshot: req.Snapshot}
	if req.Statements != nil {
		key.Statements = make([]replayMemoPrepared, len(req.Statements))
		for i, s := range req.Statements {
			key.Statements[i] = replayMemoPrepared{s.Statement, s.Payload}
		}
	}
	return json.Marshal(key)
}
func (m *replayExecutorMemo) Replay(ctx context.Context, req replay.ExecutionRequest) (replay.ExecutionResult, error) {
	// All mutable snapshot/payload/schema checks happen in real Verifier.Verify
	// before this seam. Every invocation still constructs/signs/submits evidence.
	if ctx == nil {
		return m.executor.Replay(ctx, req)
	}
	if err := ctx.Err(); err != nil {
		return replay.ExecutionResult{}, err
	}
	run, ok := ctx.Value(replayMemoRunKey{}).(*replayMemoRun)
	job, hasJob := ctx.Value(replayMemoJobKey{}).(replayMemoJob)
	if !ok || !hasJob || run.identity != m.identity || req.SnapshotQuery != nil || req.SnapshotQueryReferenceID != "" {
		return m.executor.Replay(ctx, req)
	}
	projected, err := json.Marshal(req.Job)
	if err != nil || sha256.Sum256(projected) != job.job {
		return m.executor.Replay(ctx, req)
	}
	data, err := replayMemoRequestBytes(req, job.wire)
	if err != nil || len(data) > replayMemoBytes {
		return m.executor.Replay(ctx, req)
	}
	key := replayMemoKey{run, sha256.Sum256(data)}
	run.mu.Lock()
	if run.closed || (run.owner != nil && run.owner != m) {
		run.mu.Unlock()
		return m.executor.Replay(ctx, req)
	}
	run.owner = m
	value, hit := m.get(key)
	run.mu.Unlock()
	if hit {
		var result replay.ExecutionResult
		if err := json.Unmarshal(value, &result); err == nil {
			if err := ctx.Err(); err != nil {
				return replay.ExecutionResult{}, err
			}
			return result, nil
		}
	}
	// Decode the explicit private key shape so executor input cannot alias a
	// caller's mutable snapshot/statement slices after key construction.
	var owned replayMemoRequest
	if err := json.Unmarshal(data, &owned); err != nil {
		return m.executor.Replay(ctx, req)
	}
	private := replay.ExecutionRequest{Job: owned.Job, Snapshot: owned.Snapshot}
	if owned.Statements != nil {
		private.Statements = make([]replay.PreparedStatement, len(owned.Statements))
		for i, s := range owned.Statements {
			private.Statements[i] = replay.PreparedStatement{Statement: s.Statement, Payload: s.Payload}
		}
	}
	result, err := m.executor.Replay(ctx, private)
	if err != nil {
		return result, err
	}
	if err := ctx.Err(); err != nil {
		return replay.ExecutionResult{}, err
	}
	if result.SnapshotQuery != nil {
		return result, nil
	}
	encoded, err := json.Marshal(result)
	if err != nil {
		return result, nil
	}
	// Encoding yields private immutable result bytes. Decode before returning so
	// the caller never aliases either the original result or a stored value.
	var copy replay.ExecutionResult
	if err := json.Unmarshal(encoded, &copy); err != nil {
		return result, nil
	}
	run.mu.Lock()
	if !run.closed && ctx.Err() == nil {
		m.put(key, encoded)
	}
	run.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return replay.ExecutionResult{}, err
	}
	return copy, nil
}
func (m *replayExecutorMemo) get(key replayMemoKey) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return nil, false
	}
	m.lru.MoveToFront(e)
	return e.Value.(replayMemoEntry).value, true // immutable; never exposed outside decoder
}
func (m *replayExecutorMemo) put(key replayMemoKey, value []byte) {
	if len(value) > replayMemoBytes-replayMemoEntryOverhead {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if e, ok := m.entries[key]; ok {
		m.lru.MoveToFront(e)
		return
	}
	cost := len(value) + replayMemoEntryOverhead
	for len(m.entries) >= replayMemoEntries || m.bytes > replayMemoBytes-cost {
		m.remove(m.lru.Back())
	}
	e := m.lru.PushFront(replayMemoEntry{key, append([]byte(nil), value...), cost})
	m.entries[key] = e
	m.bytes += cost
}
func (m *replayExecutorMemo) remove(e *list.Element) {
	if e == nil {
		return
	}
	entry := e.Value.(replayMemoEntry)
	delete(m.entries, entry.key)
	m.bytes -= entry.cost
	m.lru.Remove(e)
}
func (m *replayExecutorMemo) removeRun(run *replayMemoRun) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, e := range m.entries {
		if key.run == run {
			m.remove(e)
		}
	}
}

// An explicit evidence/context rejection must not preserve completed work for
// another dispatch. Delivery failures alone do not certify invalid computation.
func forgetReplayMemoRun(ctx context.Context) {
	run, ok := ctx.Value(replayMemoRunKey{}).(*replayMemoRun)
	if !ok {
		return
	}
	run.mu.Lock()
	defer run.mu.Unlock()
	if run.owner != nil {
		run.owner.removeRun(run)
	}
}
