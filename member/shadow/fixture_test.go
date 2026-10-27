package shadow

import (
	"context"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/sources"
	"github.com/sentioxyz/arbiter-core/wire"
)

const (
	testNetwork         = "testnet"
	testRevision        = 54460
	testSchemaSnapshot  = "schema-snapshot-1"
	testExecutorProfile = "executor-profile-1"
	testNodeID          = "member-test"
	testBootNonce       = "0a1b2c3d"

	ownerA        = uint64(0) // a configured peer owner
	hostIndexer   = uint64(1) // the member's host
	ownerB        = uint64(2) // a second configured peer owner
	unconfigured  = uint64(5) // enrolled, outside the group scope
	verifierCount = 3
)

func testSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
}

// fixture is a replayable chain: a genesis base and one single-statement
// block per call of block, each with the receipt every honest network
// verifier records and the candidate part an honest owner claims.
type fixture struct {
	t         *testing.T
	snapshots *payloadexec.MemSnapshotStore
	payloads  *payloadexec.MemPayloadStore
	genesis   replay.SafeSnapshotManifest
}

// testBlock is one sealed, dispatched block with honest evidence from three
// verifiers and a latched quorum.
type testBlock struct {
	BV      wire.BlockVerification
	Part    arbiter.CandidatePart // the honest candidate part
	Receipt string                // the honest receipt hash
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	executor := payloadexec.NewDynamic(testNetwork, nativepayload.Materializer{NetworkID: testNetwork, Revision: testRevision}, testSchema())
	genesis, err := executor.GenesisSnapshot(0, testSchemaSnapshot, testExecutorProfile)
	if err != nil {
		t.Fatalf("genesis snapshot: %v", err)
	}
	fx := &fixture{t: t, snapshots: payloadexec.NewMemSnapshotStore(), payloads: payloadexec.NewMemPayloadStore(), genesis: genesis}
	fx.snapshots.Put(genesis)
	return fx
}

// replayCore is an in-process replay core over the fixture's stores: the
// network verifiers' executor with the native in-process materializer in
// place of the ClickHouse one (receipts are replica-independent, K34).
func (fx *fixture) replayCore(replicaID string) *replay.Verifier {
	fx.t.Helper()
	signer, err := payloadexec.NewEd25519Signer(replicaID, make([]byte, 32))
	if err != nil {
		fx.t.Fatalf("signer: %v", err)
	}
	return &replay.Verifier{
		Snapshots:    fx.snapshots,
		Payloads:     fx.payloads,
		Executor:     payloadexec.NewDynamic(testNetwork, nativepayload.Materializer{NetworkID: testNetwork, Revision: testRevision}, testSchema()),
		Signer:       signer,
		SchemaHashes: payloadexec.SchemaHashes{NetworkID: testNetwork, Tables: []payloadexec.TableSchema{testSchema()}},
	}
}

func nativeRow(t *testing.T, p string, v uint64) []byte {
	t.Helper()
	pc, vc := proto.ColStr{}, proto.ColUInt64{}
	pc.Append(p)
	vc.Append(v)
	b, err := nativepayload.EncodeClientDataPacket(testRevision, []proto.InputColumn{{Name: "p", Data: &pc}, {Name: "v", Data: &vc}})
	if err != nil {
		t.Fatalf("encode native payload: %v", err)
	}
	return b
}

// statement is block seq's one statement inserting (p, v) into db.t.
func (fx *fixture) statement(seq uint64, p string, v uint64) replay.Statement {
	fx.t.Helper()
	payload := nativeRow(fx.t, p, v)
	ref := "payload-" + strconv.FormatUint(seq, 10) + ".native"
	fx.payloads.Put(ref, payload)
	sql := "INSERT INTO db.t FORMAT Native"
	return replay.Statement{
		StatementID:    arbiter.StatementIDString("0xacct", seq, "n"),
		StatementSeq:   seq,
		SQL:            sql,
		SQLHash:        replay.DigestString(sql),
		SettingsHash:   replay.DigestString("settings"),
		PayloadRef:     ref,
		PayloadHash:    replay.DigestBytes(payload),
		PayloadLength:  uint64(len(payload)),
		TargetTableID:  "db.t",
		UserJWS:        "x.y.z",
		PayloadFormat:  replay.PayloadFormatClickHouseNativeData,
		ClientRevision: testRevision,
		SchemaHash:     payloadexec.TableSchemaHash(testNetwork, testSchema()),
	}
}

// block builds block seq of owner on the genesis base: one statement
// inserting (p, v), its honest candidate part, three verifiers' honest
// attestations and scans, all evaluated, and a latched quorum.
func (fx *fixture) block(seq, owner uint64, p string, v uint64) testBlock {
	fx.t.Helper()
	st := fx.statement(seq, p, v)
	job := replay.ReplayJob{
		BlockSeq:           seq,
		PrevSafeSnapshotID: fx.genesis.SnapshotID,
		PrevStateRoot:      fx.genesis.StateRoot,
		SchemaSnapshotID:   testSchemaSnapshot,
		ExecutorProfileID:  testExecutorProfile,
		SourceClaimRoot:    replay.DigestString("source-claim-" + strconv.FormatUint(seq, 10)),
		Statements:         []replay.Statement{st},
	}
	ref, err := fx.replayCore("verifier-ref").Verify(context.Background(), job)
	if err != nil {
		fx.t.Fatalf("reference replay of block %d: %v", seq, err)
	}
	if len(ref.Receipt.AffectedParts) != 1 {
		fx.t.Fatalf("reference replay of block %d affected %d parts, want 1", seq, len(ref.Receipt.AffectedParts))
	}
	affected := ref.Receipt.AffectedParts[0]
	part := arbiter.CandidatePart{
		TableID:       "db.t",
		PartitionID:   affected.PartitionID,
		PartName:      fmt.Sprintf("%s_%d_%d_0", p, seq, seq),
		PartRowLtHash: affected.PartRowLtHash,
		PartPhysHash:  replay.DigestString("phys-" + strconv.FormatUint(seq, 10)),
		RowCount:      1,
	}
	track := owner
	bv := wire.BlockVerification{
		Header: wire.BlockHeaderView{
			L3BlockSeq:         seq,
			StatementSeqStart:  seq,
			StatementCount:     1,
			PrevSafeSnapshotID: job.PrevSafeSnapshotID,
			PrevStateRoot:      job.PrevStateRoot,
			TrackIndexerID:     &track,
		},
		ChainHash: replay.DigestString("chain-" + strconv.FormatUint(seq, 10)),
		ReplayJob: job,
		Statements: []wire.StatementCandidates{{
			StatementSeq:         seq,
			StatementID:          st.StatementID,
			Status:               "QuorumVerified",
			SourceNode:           "snode-" + strconv.FormatUint(owner+1, 10),
			RCBound:              true,
			SourceClaimRoot:      job.SourceClaimRoot,
			CandidateParts:       []arbiter.CandidatePart{part},
			PartitionNewPartSums: []arbiter.PartitionLtHashSum{{TableID: "db.t", PartitionID: part.PartitionID, NewPartsLtHashSum: part.PartRowLtHash}},
		}},
		ScanParts:        []arbiter.PartRef{{TableID: "db.t", PartitionID: part.PartitionID, PartRowLtHash: part.PartRowLtHash, PartName: part.PartName}},
		SourceNodes:      []string{"snode-" + strconv.FormatUint(owner+1, 10)},
		VerdictEvaluated: true,
		Quorum:           true,
	}
	for i := 1; i <= verifierCount; i++ {
		rid := "verifier-" + strconv.Itoa(i)
		att := ref
		att.ReplicaID = rid
		bv.VerifierSet = append(bv.VerifierSet, rid)
		bv.Evidence = append(bv.Evidence, wire.VerifierEvidence{
			ReplicaID:   rid,
			Attestation: &att,
			ByteSideScan: &arbiter.ByteSideScanMsg{ReplicaID: rid, BlockSeq: seq, Parts: []arbiter.PartScan{{
				TableID: "db.t", PartitionID: part.PartitionID, ClaimedPartRowLtHash: part.PartRowLtHash,
				ScannedPartRowLtHash: part.PartRowLtHash, LivePartName: part.PartName,
			}}},
			Evaluated: true, Check1RootMatch: true, Check2PartitionMatch: true, Check3ByteSideMatch: true, Pass: true,
		})
	}
	return testBlock{BV: bv, Part: part, Receipt: ref.ReceiptHash}
}

// evilHash is a well-formed row LtHash accumulator no honest part has.
func evilHash(label string) string {
	h := lthash.New()
	h.Add([]byte(label))
	return "0x" + hex.EncodeToString(h.Bytes())
}

// fakeArbiter is the only arbiter the shadow sees: a block reader and a
// watermark reader, nothing else. A block answers its queued reads in order
// and repeats the last one; a block without answers is not sealed.
type fakeArbiter struct {
	mu        sync.Mutex
	watermark uint64
	answers   map[uint64][]wire.BlockVerification
	errs      map[uint64]error
	reads     []uint64
	readAt    []time.Time
}

func newFakeArbiter(watermark uint64, blocks ...wire.BlockVerification) *fakeArbiter {
	a := &fakeArbiter{watermark: watermark, answers: map[uint64][]wire.BlockVerification{}, errs: map[uint64]error{}}
	for _, bv := range blocks {
		a.put(bv.Header.L3BlockSeq, bv)
	}
	return a
}

func (a *fakeArbiter) put(seq uint64, answers ...wire.BlockVerification) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.answers[seq] = append(a.answers[seq], answers...)
}

func (a *fakeArbiter) fail(seq uint64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.errs[seq] = err
}

func (a *fakeArbiter) GetBlockVerification(_ context.Context, seq uint64) (wire.BlockVerification, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reads = append(a.reads, seq)
	a.readAt = append(a.readAt, time.Now())
	if err := a.errs[seq]; err != nil {
		return wire.BlockVerification{}, err
	}
	queue := a.answers[seq]
	if len(queue) == 0 {
		return wire.BlockVerification{}, fmt.Errorf("%w: %d", dataplane.ErrBlockNotSealed, seq)
	}
	if len(queue) > 1 {
		a.answers[seq] = queue[1:]
	}
	return queue[0], nil
}

func (a *fakeArbiter) GetSafeWatermark(context.Context) (arbiter.SafeWatermark, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return arbiter.SafeWatermark{SnapshotID: "watermark", SafeBlockSeq: a.watermark, ManifestRoot: "watermark"}, nil
}

func (a *fakeArbiter) readLog() ([]uint64, []time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]uint64(nil), a.reads...), append([]time.Time(nil), a.readAt...)
}

// fakePull stands in for sources.NewOwnerUnsafePull: a part it knows is
// "pulled" into the scratch with the row LtHash it maps to; a part it does
// not know was cleaned from the owner's hg_unsafe (missed); an empty part
// arrives with no rows. scan reads back what Fetch wrote.
type fakePull struct {
	mu      sync.Mutex
	parts   map[string]string
	empty   map[string]bool
	scratch map[string]string
	needs   []sources.Need
}

func newFakePull(parts ...arbiter.CandidatePart) *fakePull {
	p := &fakePull{parts: map[string]string{}, empty: map[string]bool{}, scratch: map[string]string{}}
	for _, cp := range parts {
		p.parts[cp.PartName] = cp.PartRowLtHash
	}
	return p
}

func (p *fakePull) Name() string { return "owner_unsafe_pull" }

func (p *fakePull) Fetch(_ context.Context, need sources.Need, _ payloadexec.TableSchema, scratch sources.Scratch) sources.Result {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.needs = append(p.needs, need)
	if p.empty[need.PartName] {
		return sources.Result{Source: p.Name(), Outcome: sources.OutcomeFetched}
	}
	h, ok := p.parts[need.PartName]
	if !ok {
		return sources.Result{Source: p.Name(), Outcome: sources.OutcomeMissed}
	}
	p.scratch[scratch.Database+"."+scratch.Table] = h
	return sources.Result{Source: p.Name(), Outcome: sources.OutcomeFetched, Parts: []string{"all_1_1_0"}}
}

func (p *fakePull) scan(_ context.Context, _ clickhouse.Conn, qualified string, _ payloadexec.TableSchema) ([]chexec.PartScanResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	h, ok := p.scratch[qualified]
	if !ok {
		return []chexec.PartScanResult{}, nil
	}
	return []chexec.PartScanResult{{PartName: "all_1_1_0", RowLtHash: h, RowCount: 1}}, nil
}

func (p *fakePull) fetched() []sources.Need {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]sources.Need(nil), p.needs...)
}

// fakeConn records every statement; any method but Exec panics (the
// embedded nil Conn), so a unit test proves the shadow issues nothing else.
type fakeConn struct {
	clickhouse.Conn
	mu    sync.Mutex
	execs []string
}

func (c *fakeConn) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.execs = append(c.execs, query)
	return nil
}

func (c *fakeConn) statements() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.execs...)
}

func (c *fakeConn) count(prefix string) int {
	n := 0
	for _, s := range c.statements() {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

func testConfig(stateDir string) Config {
	return Config{
		NodeID:        testNodeID,
		BootNonce:     testBootNonce,
		NetworkID:     testNetwork,
		StateDir:      stateDir,
		Pinned:        ddl.DefaultMemberPinned(testNodeID, 0, "default"),
		Genesis:       []payloadexec.TableSchema{testSchema()},
		Owners:        []uint64{ownerA, ownerB},
		HostIndexerID: hostIndexer,
		VerdictWait:   time.Minute,
	}
}

// newTestShadow builds a shadow over the fakes with the fixture's
// in-process replay core and the fake pull's scan in place of the
// ClickHouse ones (both are package seams; New wires the real ones).
func newTestShadow(t *testing.T, fx *fixture, arb *fakeArbiter, pull *fakePull, cfg Config) (*Verifier, *fakeConn) {
	t.Helper()
	conn := &fakeConn{}
	v, err := New(cfg, Deps{
		Blocks:     arb,
		Watermark:  arb,
		Snapshots:  fx.snapshots,
		Payloads:   fx.payloads,
		Conn:       conn,
		UnsafePull: func(uint64) sources.Source { return pull },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	v.replay = fx.replayCore(testNodeID + "-shadow")
	v.scan = pull.scan
	return v, conn
}

// stepAll runs n Steps that must each advance the cursor.
func stepAll(t *testing.T, v *Verifier, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		advanced, err := v.Step(context.Background())
		if err != nil || !advanced {
			t.Fatalf("step %d: advanced=%v err=%v", i+1, advanced, err)
		}
	}
}

// wantCounts compares one owner's counters.
func wantCounts(t *testing.T, st Stats, owner, blocks, missed, unverdicted, receipt, scan, claim uint64) {
	t.Helper()
	got := [6]uint64{st.Blocks[owner], st.MissedScans[owner], st.Unverdicted[owner],
		st.Mismatches[owner][MismatchReceipt], st.Mismatches[owner][MismatchScan], st.Mismatches[owner][MismatchClaim]}
	want := [6]uint64{blocks, missed, unverdicted, receipt, scan, claim}
	if got != want {
		t.Fatalf("owner %d counters (blocks, missed, unverdicted, receipt, scan, claim) = %v, want %v", owner, got, want)
	}
}
