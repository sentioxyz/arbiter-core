package snode

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/fspayload"
	"github.com/sentioxyz/arbiter-core/wire"
)

// auditPartitionS is the logical partition of every fixture statement (p = 'p0').
const auditPartitionS = "p_p0"

// fsmPartitionsFakeS is the arbiter's SafeState as the owner audit reads it:
// GetPartitionStates answers the states set, filtered like the arbiter, or
// refuses every read with err when it is set.
type fsmPartitionsFakeS struct {
	pb.UnimplementedSafeStateServer

	mu     sync.Mutex
	states map[partitionKey]wire.PartitionState
	err    error
	reads  int
}

func (f *fsmPartitionsFakeS) GetPartitionStates(_ context.Context, req *pb.GetPartitionStatesRequest) (*pb.PartitionStates, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.reads++
	if f.err != nil {
		return nil, f.err
	}
	var out wire.PartitionStates
	keys := slices.SortedFunc(maps.Keys(f.states), func(a, b partitionKey) int { return strings.Compare(a.Partition, b.Partition) })
	for _, k := range keys {
		if k.Table == req.GetTableId() && (req.GetPartitionId() == "" || k.Partition == req.GetPartitionId()) {
			out.Partitions = append(out.Partitions, f.states[k])
		}
	}
	return wire.PartitionStatesToPB(out), nil
}

func (f *fsmPartitionsFakeS) set(s wire.PartitionState) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.states == nil {
		f.states = map[partitionKey]wire.PartitionState{}
	}
	f.states[partitionKey{Table: s.TableID, Partition: s.PartitionID}] = s
}

func (f *fsmPartitionsFakeS) readCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads
}

// fsmStateOfAckS is the partition state the arbiter keeps once it applied ack.
func fsmStateOfAckS(ack arbiter.PromotionAck) wire.PartitionState {
	s := wire.PartitionState{TableID: ack.TableID, PartitionID: ack.PartitionID, BasePartitionRoot: ack.PostPartitionCommitment,
		PublishSeq: ack.PromotionSeq, LastAckedPromotionSeq: ack.PromotionSeq}
	for _, m := range ack.SafePartitionParts {
		s.SafeParts = append(s.SafeParts, replay.PartManifestEntry{TableID: ack.TableID, PartitionID: ack.PartitionID,
			PartName: m.SafePartName, PartPhysHash: m.PartPhysHash, PartRowLtHash: m.PartRowLtHash})
	}
	return s
}

// newOwnerAuditHarnessS is newIntakeHarness whose arbiter also serves fsm's
// GetPartitionStates (none when fsm is nil: the read answers Unimplemented).
func newOwnerAuditHarnessS(t *testing.T, conn clickhouse.Conn, cfg Config, fsm *fsmPartitionsFakeS) (*Role, *sourceClaimsFake) {
	t.Helper()
	claims := &sourceClaimsFake{}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	pb.RegisterSourceClaimsServer(srv, claims)
	pb.RegisterPromotionGatewayServer(srv, claims)
	if fsm != nil {
		pb.RegisterSafeStateServer(srv, fsm)
	}
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
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	payloads, err := fspayload.New(t.TempDir())
	if err != nil {
		t.Fatalf("payload store: %v", err)
	}
	role, err := New(cfg, Deps{Client: client, Conn: conn, Payloads: payloads})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}
	return role, claims
}

// ownerAuditFixtureS is one SNode over promoteSchema's protocol tables; every
// statement lands in partition p_p0 and is promoted through handlePromote as
// the arbiter would issue it.
type ownerAuditFixtureS struct {
	t      *testing.T
	conn   clickhouse.Conn
	role   *Role
	claims *sourceClaimsFake
	signer commandSigner
	schema payloadexec.TableSchema
}

func newOwnerAuditFixtureS(t *testing.T, conn clickhouse.Conn, mode OwnerAuditMode, fsm *fsmPartitionsFakeS) *ownerAuditFixtureS {
	t.Helper()
	schema := promoteSchema()
	signer := mustPromoteSigner(t)
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.AuthorityAddresses = []string{signer.Address()}
	cfg.OwnerAudit = mode
	cfg.OwnerAuditProbeInterval = MinOwnerAuditProbeInterval
	setUniqueDatabases(t, &cfg)
	role, claims := newOwnerAuditHarnessS(t, conn, cfg, fsm)
	createSNodeTables(t, conn, role.cfg, schema)
	return &ownerAuditFixtureS{t: t, conn: conn, role: role, claims: claims, signer: signer, schema: schema}
}

func (f *ownerAuditFixtureS) table() string { return CHTableName(f.schema.TableID) }

func (f *ownerAuditFixtureS) safe() string { return f.role.cfg.SafeDatabase + "." + f.table() }

// submit commits statement seq (rows 2*seq-1 and 2*seq) and returns its
// candidate part.
func (f *ownerAuditFixtureS) submit(ctx context.Context, seq uint64) arbiter.CandidatePart {
	f.t.Helper()
	payload := nativePayload(f.t, pv{"p0", 2*seq - 1}, pv{"p0", 2 * seq})
	env := intakeEnvelope(payload)
	env.StatementID.ClientSeq = seq
	env.StatementID.ClientNonce = fmt.Sprintf("n%d", seq)
	env.PayloadRef = fmt.Sprintf("payload-%d.native", seq)
	if err := f.role.SubmitLocalStatement(ctx, env, payload); err != nil {
		f.t.Fatalf("SubmitLocalStatement %d: %v", seq, err)
	}
	rcs := f.claims.snapshot()
	if len(rcs) == 0 || len(rcs[len(rcs)-1].CandidateParts) != 1 || rcs[len(rcs)-1].CandidateParts[0].PartitionID != auditPartitionS {
		f.t.Fatalf("result claims after statement %d: %+v", seq, rcs)
	}
	return rcs[len(rcs)-1].CandidateParts[0]
}

// promote promotes part as promotion seq on the partition's current base.
func (f *ownerAuditFixtureS) promote(ctx context.Context, seq uint64, part arbiter.CandidatePart) error {
	f.t.Helper()
	base, _ := f.role.state.BaseRoot(partitionKey{Table: f.schema.TableID, Partition: part.PartitionID})
	snapshot := "genesis"
	if seq > 1 {
		snapshot = fmt.Sprintf("safe-%d", seq-1)
	}
	cmd := arbiter.PromoteSafePartition{TableID: f.schema.TableID, PartitionID: part.PartitionID, PromotionSeq: seq,
		BaseSafeSnapshotID: snapshot, BasePartitionRoot: base,
		CandidateParts: []arbiter.PartRef{{TableID: f.schema.TableID, PartitionID: part.PartitionID, PartRowLtHash: part.PartRowLtHash, PartName: part.PartName}}}
	return f.role.handlePromote(ctx, wire.PromoteToPB(cmd), mustSignPromotion(f.t, f.signer, cmd), false)
}

// cleanup drops part from hg_unsafe as promotion seq's cleanup.
func (f *ownerAuditFixtureS) cleanup(ctx context.Context, seq uint64, part arbiter.CandidatePart) {
	f.t.Helper()
	cleanup := arbiter.UnsafeCleanup{TableID: f.schema.TableID, PartitionID: part.PartitionID, PromotionSeq: seq,
		Parts: []arbiter.PartRef{{TableID: f.schema.TableID, PartitionID: part.PartitionID, PartRowLtHash: part.PartRowLtHash, PartName: part.PartName}}}
	if err := f.role.handleCleanup(ctx, wire.CleanupToPB(cleanup), mustSignCleanup(f.t, f.signer, cleanup)); err != nil {
		f.t.Fatalf("cleanup %d: %v", seq, err)
	}
}

// promoteAndClean submits statement seq, promotes it and cleans its unsafe part.
func (f *ownerAuditFixtureS) promoteAndClean(ctx context.Context, seq uint64) arbiter.CandidatePart {
	f.t.Helper()
	part := f.submit(ctx, seq)
	if err := f.promote(ctx, seq, part); err != nil {
		f.t.Fatalf("promotion %d: %v", seq, err)
	}
	f.cleanup(ctx, seq, part)
	return part
}

func (f *ownerAuditFixtureS) safeParts(ctx context.Context) []partInfo {
	f.t.Helper()
	return partsInLogicalPartition(activePartsMust(f.t, ctx, f.conn, f.role.cfg.SafeDatabase, f.table()), f.schema, auditPartitionS)
}

func (f *ownerAuditFixtureS) safeRows(ctx context.Context) uint64 {
	f.t.Helper()
	return rowCount(f.t, ctx, f.conn, f.safe())
}

func (f *ownerAuditFixtureS) lastAck() arbiter.PromotionAck {
	f.t.Helper()
	acks := f.claims.promotionAcks()
	if len(acks) == 0 {
		f.t.Fatal("no promotion acknowledgement yet")
	}
	return acks[len(acks)-1]
}

// injectSafeTwins copies the partition's hg_safe parts into hg_safe again
// through hg_promote. ATTACH PARTITION FROM keeps hash_of_all_files (spec
// 2026-10-14 §2, M2), so hg_safe then holds an equal-hash twin of every part
// under a name no acknowledgement carries: the state a restart leaves after it
// resurrected the parts a REPLACE PARTITION retired, without the restart. It
// returns the twins' names, sorted.
func (f *ownerAuditFixtureS) injectSafeTwins(ctx context.Context) []string {
	f.t.Helper()
	partition, err := quotePartition(f.schema, auditPartitionS)
	if err != nil {
		f.t.Fatal(err)
	}
	promote := f.role.cfg.PromoteDatabase + "." + f.table()
	before := f.safeParts(ctx)
	mustExecIntake(f.t, f.conn, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", promote, partition, f.safe()))
	mustExecIntake(f.t, f.conn, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", f.safe(), partition, promote))
	mustExecIntake(f.t, f.conn, fmt.Sprintf("ALTER TABLE %s DROP PARTITION %s", promote, partition))
	twins := diffParts(before, f.safeParts(ctx))
	if len(twins) != len(before) {
		f.t.Fatalf("twins %+v of %+v: want one per part", twins, before)
	}
	hashes := map[string]bool{}
	for _, p := range before {
		hashes[p.PhysHash] = true
	}
	names := make([]string, 0, len(twins))
	for _, p := range twins {
		if !hashes[p.PhysHash] {
			f.t.Fatalf("twin %s has hash_of_all_files %s that no original part has: ATTACH PARTITION FROM did not keep it (see M2)", p.Name, p.PhysHash)
		}
		names = append(names, p.Name)
	}
	slices.Sort(names)
	return names
}

// unacknowledgedPartsS returns, sorted, the active parts ack's inventory does
// not name.
func unacknowledgedPartsS(parts []partInfo, ack arbiter.PromotionAck) []string {
	var out []string
	for _, p := range parts {
		if !slices.ContainsFunc(ack.SafePartitionParts, func(m arbiter.SafePartMapping) bool { return m.SafePartName == p.Name }) {
			out = append(out, p.Name)
		}
	}
	slices.Sort(out)
	return out
}

// TestOwnerAuditReportModeNeverDropsOrHoldsBack: report mode (the default,
// clarification K1) finds the twins and reports them, but drops nothing, holds
// no table and lets the next promotion run into the shadow closure gate exactly
// as before the audit existed.
func TestOwnerAuditReportModeNeverDropsOrHoldsBack(t *testing.T) {
	ctx := context.Background()
	f := newOwnerAuditFixtureS(t, requireCH(t), OwnerAuditReport, nil)
	f.promoteAndClean(ctx, 1)
	f.promoteAndClean(ctx, 2)
	twins := f.injectSafeTwins(ctx)
	before := f.safeParts(ctx)

	f.role.runOwnerAuditPass(ctx)

	if got := f.safeParts(ctx); !reflect.DeepEqual(got, before) {
		t.Fatalf("report mode changed hg_safe:\n before %+v\n after  %+v", before, got)
	}
	if got := f.safeRows(ctx); got != 8 {
		t.Fatalf("safe rows = %d, want 8 (every row served twice)", got)
	}
	stats := f.role.OwnerAuditStats()
	if stats.Mode != OwnerAuditReport || stats.DroppedTwins != 0 || stats.Runs[OwnerAuditTriggerStartup] != 1 || len(stats.Unclean) != 1 {
		t.Fatalf("stats = %+v", stats)
	}
	if u := stats.Unclean[0]; u.TableID != f.schema.TableID || u.PartitionID != auditPartitionS || u.Reason != OwnerAuditUnacknowledgedPart ||
		!slices.Equal(u.Parts, twins) || u.Trigger != OwnerAuditTriggerStartup || u.At.IsZero() {
		t.Fatalf("finding = %+v, want unacknowledged_part naming %v", u, twins)
	}
	if !f.role.OwnerAuditClean(f.schema.TableID) {
		t.Fatal("report mode held the table back")
	}
	third := f.submit(ctx, 3)
	err := f.promote(ctx, 3, third)
	if err == nil || errors.Is(err, ErrSafePartitionUnclean) || !strings.Contains(err.Error(), "shadow closure mismatch") {
		t.Fatalf("third promotion = %v, want the shadow closure gate's refusal", err)
	}
	if got := f.safeParts(ctx); !reflect.DeepEqual(got, before) {
		t.Fatal("report mode dropped a part before the shadow")
	}
	if s := f.role.OwnerAuditStats(); s.Runs[OwnerAuditTriggerPromotion] != 3 || s.DroppedTwins != 0 || s.Unclean[0].Trigger != OwnerAuditTriggerPromotion {
		t.Fatalf("stats after the third promotion = %+v", s)
	}
}

// TestOwnerAuditRepairDropsResurrectedTwinsBeforeTheShadow: in repair mode the
// promotion's audit drops every twin of an acknowledged part with DROP PART
// (K2), and the promotion then publishes exactly base and candidate. The
// harness serves no GetPartitionStates, so every audit uses the local
// inventory and counts the failed read (K3).
func TestOwnerAuditRepairDropsResurrectedTwinsBeforeTheShadow(t *testing.T) {
	ctx := context.Background()
	f := newOwnerAuditFixtureS(t, requireCH(t), OwnerAuditRepair, nil)
	f.role.runOwnerAuditPass(ctx)
	if !f.role.OwnerAuditClean(f.schema.TableID) {
		t.Fatal("an empty hg_safe is unclean")
	}
	f.promoteAndClean(ctx, 1)
	f.promoteAndClean(ctx, 2)
	twins := f.injectSafeTwins(ctx)

	third := f.submit(ctx, 3)
	if err := f.promote(ctx, 3, third); err != nil {
		t.Fatalf("third promotion: %v", err)
	}

	if got := f.safeRows(ctx); got != 6 {
		t.Fatalf("safe rows = %d, want 6", got)
	}
	stats := f.role.OwnerAuditStats()
	if stats.DroppedTwins != uint64(len(twins)) || len(stats.Unclean) != 0 || stats.Runs[OwnerAuditTriggerPromotion] != 3 || stats.FSMUnavailable < 3 {
		t.Fatalf("stats = %+v, want %d twins dropped, nothing unclean, 3 promotion audits on the local inventory", stats, len(twins))
	}
	ack := f.lastAck()
	if !ack.Applied || ack.PromotionSeq != 3 || len(ack.SafePartitionParts) != 3 {
		t.Fatalf("third acknowledgement = %+v", ack)
	}
	assertCompleteSafeInventory(t, ctx, f.role, f.schema, auditPartitionS, ack.SafePartitionParts)
	for _, p := range f.safeParts(ctx) {
		if slices.Contains(twins, p.Name) {
			t.Fatalf("twin %s is still active", p.Name)
		}
	}
	if !f.role.OwnerAuditClean(f.schema.TableID) {
		t.Fatal("the repaired table is held")
	}
}

// TestOwnerAuditRepairHoldsAnUncleanPartitionAndBuildsNoShadow: a part without
// an acknowledged twin (a foreign insert) is never dropped; repair mode refuses
// the promotion with ErrSafePartitionUnclean before the shadow, holds the table,
// and the redelivered promotion goes through once an operator removed the part.
func TestOwnerAuditRepairHoldsAnUncleanPartitionAndBuildsNoShadow(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	f := newOwnerAuditFixtureS(t, conn, OwnerAuditRepair, nil)
	f.role.runOwnerAuditPass(ctx)
	f.promoteAndClean(ctx, 1)
	mustExecIntake(t, conn, fmt.Sprintf("INSERT INTO %s (_hg_row_id, p, v) VALUES (unhex('%064x'), 'p0', 99)", f.safe(), 99))
	foreign := unacknowledgedPartsS(f.safeParts(ctx), f.lastAck())
	if len(foreign) != 1 {
		t.Fatalf("foreign parts = %v, want one", foreign)
	}
	second := f.submit(ctx, 2)

	err := f.promote(ctx, 2, second)

	if !errors.Is(err, ErrSafePartitionUnclean) || !strings.Contains(err.Error(), string(OwnerAuditUnacknowledgedPart)) || !strings.Contains(err.Error(), foreign[0]) {
		t.Fatalf("promotion = %v, want ErrSafePartitionUnclean naming %s", err, foreign[0])
	}
	if got := len(f.claims.promotionAcks()); got != 1 {
		t.Fatalf("acknowledgements = %d, want only the first", got)
	}
	if got := f.safeRows(ctx); got != 3 {
		t.Fatalf("safe rows = %d, want 3 (unchanged)", got)
	}
	assertNoPromotePartition(t, ctx, f.role, f.schema, auditPartitionS)
	if f.role.OwnerAuditClean(f.schema.TableID) {
		t.Fatal("repair mode serves an unclean table")
	}
	if s := f.role.OwnerAuditStats(); len(s.Unclean) != 1 || s.Unclean[0].Reason != OwnerAuditUnacknowledgedPart || !slices.Equal(s.Unclean[0].Parts, foreign) ||
		s.Unclean[0].Trigger != OwnerAuditTriggerPromotion || s.DroppedTwins != 0 {
		t.Fatalf("stats = %+v", s)
	}
	mustExecIntake(t, conn, fmt.Sprintf("ALTER TABLE %s DROP PART '%s'", f.safe(), foreign[0]))
	if err := f.promote(ctx, 2, second); err != nil {
		t.Fatalf("redelivered promotion after the operator's repair: %v", err)
	}
	if got := f.safeRows(ctx); got != 4 || !f.role.OwnerAuditClean(f.schema.TableID) {
		t.Fatalf("after the repair: %d safe rows, clean %v; want 4 and clean", got, f.role.OwnerAuditClean(f.schema.TableID))
	}
}

// TestOwnerAuditTableReadyWaitsForTheAuditInRepairMode: TableReady folds the
// audit in (contract §3): in repair mode a ready table is not ready until its
// startup audit completed, and not while it is unclean, so the host answers it
// Pending; report mode leaves TableReady as it was. Without a local inventory
// and with the arbiter unreadable, a partition with parts has no reference
// (K3).
func TestOwnerAuditTableReadyWaitsForTheAuditInRepairMode(t *testing.T) {
	conn := requireCH(t)
	requireKeeperS(t, conn)
	for _, mode := range []OwnerAuditMode{OwnerAuditRepair, OwnerAuditReport} {
		t.Run(string(mode), func(t *testing.T) {
			ctx := context.Background()
			cfg := testConfigS(t)
			setUniqueDatabases(t, &cfg)
			cfg.SchemaSource = ddl.SchemaSourceNetworkState
			cfg.NodeID = "snode-" + strings.TrimPrefix(cfg.UnsafeDatabase, "hg_unsafe_")
			cfg.OwnerAudit = mode
			t.Cleanup(func() {
				for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
					_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
				}
			})
			role, _ := newIntakeHarness(t, conn, cfg)
			table := cfg.Tables[0].TableID
			if err := role.ensureProtocolTables(ctx); err != nil {
				t.Fatalf("ensure protocol tables: %v", err)
			}
			repair := mode == OwnerAuditRepair
			if got := role.TableReady(table); got == repair {
				t.Fatalf("TableReady before the startup audit = %v, want %v", got, !repair)
			}
			role.runOwnerAuditPass(ctx)
			if !role.TableReady(table) {
				t.Fatal("TableReady is false after a clean startup audit")
			}
			mustExecIntake(t, conn, fmt.Sprintf("INSERT INTO %s.%s (_hg_row_id, v) VALUES (unhex('%064x'), 7)", cfg.SafeDatabase, CHTableName(table), 7))
			role.runOwnerAuditPass(ctx)
			if got := role.TableReady(table); got == repair {
				t.Fatalf("TableReady with an unclean partition = %v, want %v", got, !repair)
			}
			s := role.OwnerAuditStats()
			if len(s.Unclean) != 1 || s.Unclean[0].Reason != OwnerAuditNoAcknowledgedInventory || s.Unclean[0].PartitionID != "all" || s.FSMUnavailable != 2 {
				t.Fatalf("stats = %+v, want one no_acknowledged_inventory finding of partition all and two failed arbiter reads", s)
			}
		})
	}
}

// TestOwnerAuditFSMAheadIsUncleanAndDropsNothing: an arbiter that recorded an
// applied promotion past this SNode's watermark means the local state is behind;
// the partition is unclean and its twins are not dropped (contract §3).
func TestOwnerAuditFSMAheadIsUncleanAndDropsNothing(t *testing.T) {
	ctx := context.Background()
	fsm := &fsmPartitionsFakeS{}
	f := newOwnerAuditFixtureS(t, requireCH(t), OwnerAuditRepair, fsm)
	f.promoteAndClean(ctx, 1)
	f.promoteAndClean(ctx, 2)
	ahead := fsmStateOfAckS(f.lastAck())
	ahead.PublishSeq = 9
	fsm.set(ahead)
	f.injectSafeTwins(ctx)
	before := f.safeParts(ctx)

	f.role.runOwnerAuditPass(ctx)

	if got := f.safeParts(ctx); !reflect.DeepEqual(got, before) {
		t.Fatal("twins were dropped although the arbiter is ahead")
	}
	s := f.role.OwnerAuditStats()
	if s.DroppedTwins != 0 || s.FSMUnavailable != 0 || len(s.Unclean) != 1 || s.Unclean[0].Reason != OwnerAuditFSMAhead {
		t.Fatalf("stats = %+v, want one fsm_ahead finding", s)
	}
	if f.role.OwnerAuditClean(f.schema.TableID) || fsm.readCount() < 3 {
		t.Fatalf("clean %v, arbiter reads %d; want held and one read per promotion and pass", f.role.OwnerAuditClean(f.schema.TableID), fsm.readCount())
	}
}

// TestOwnerAuditTakesTheArbitersInventoryWhenItIsNewer: a state written before
// acked_safe_inventories whose last acknowledgement was a base CAS refusal has
// no local inventory; the arbiter's SafeParts, at a publish seq the watermark
// covers, are the reference, and the twins are dropped against them.
func TestOwnerAuditTakesTheArbitersInventoryWhenItIsNewer(t *testing.T) {
	ctx := context.Background()
	fsm := &fsmPartitionsFakeS{}
	f := newOwnerAuditFixtureS(t, requireCH(t), OwnerAuditRepair, fsm)
	f.promoteAndClean(ctx, 1)
	f.promoteAndClean(ctx, 2)
	ack := f.lastAck()
	fsm.set(fsmStateOfAckS(ack))
	k := partitionKey{Table: f.schema.TableID, Partition: auditPartitionS}
	ks := key(k.Table, k.Partition)
	f.role.state.mu.Lock()
	delete(f.role.state.s.AckedSafeInventories, ks)
	f.role.state.s.LastAcks[ks] = arbiter.PromotionAck{NodeID: f.role.cfg.NodeID, PromotionSeq: 3, TableID: k.Table, PartitionID: k.Partition, Detail: "base CAS mismatch"}
	f.role.state.s.Watermarks[ks] = 3
	f.role.state.mu.Unlock()
	if _, ok := f.role.state.AckedSafeInventory(k); ok {
		t.Fatal("the fixture still has a local inventory")
	}
	twins := f.injectSafeTwins(ctx)

	f.role.runOwnerAuditPass(ctx)

	s := f.role.OwnerAuditStats()
	if s.DroppedTwins != uint64(len(twins)) || len(s.Unclean) != 0 || s.FSMUnavailable != 0 {
		t.Fatalf("stats = %+v, want the %d twins dropped against the arbiter's inventory", s, len(twins))
	}
	if got := f.safeRows(ctx); got != 4 {
		t.Fatalf("safe rows = %d, want 4", got)
	}
	assertCompleteSafeInventory(t, ctx, f.role, f.schema, auditPartitionS, ack.SafePartitionParts)
}

// TestOwnerAuditSkipsAPartitionWithAnUnresolvedIntent: a startup pass skips a
// partition whose promotion intent is journaled and not finalized and reports
// unresolved_intent (clarification K4); the redelivered promotion reconciles the
// published partition, which clears the finding.
func TestOwnerAuditSkipsAPartitionWithAnUnresolvedIntent(t *testing.T) {
	ctx := context.Background()
	f := newOwnerAuditFixtureS(t, requireCH(t), OwnerAuditRepair, nil)
	f.role.runOwnerAuditPass(ctx)
	f.promoteAndClean(ctx, 1)
	second := f.submit(ctx, 2)
	base := f.role.d.Conn
	failing := &failAfterReplaceConn{Conn: base}
	f.role.d.Conn = failing
	if err := f.promote(ctx, 2, second); err == nil || !strings.Contains(err.Error(), "injected post-REPLACE failure") {
		t.Fatalf("promotion = %v, want the injected post-REPLACE failure", err)
	}
	f.role.d.Conn = base
	if _, ok := f.role.state.PendingPromotion(partitionKey{Table: f.schema.TableID, Partition: auditPartitionS}); !ok {
		t.Fatal("the fixture left no unresolved intent")
	}
	before := f.safeParts(ctx)

	f.role.runOwnerAuditPass(ctx)

	s := f.role.OwnerAuditStats()
	if len(s.Unclean) != 1 || s.Unclean[0].Reason != OwnerAuditUnresolvedIntent || s.Unclean[0].Trigger != OwnerAuditTriggerStartup || s.DroppedTwins != 0 {
		t.Fatalf("stats = %+v, want one unresolved_intent finding", s)
	}
	if got := f.safeParts(ctx); !reflect.DeepEqual(got, before) || f.role.OwnerAuditClean(f.schema.TableID) {
		t.Fatal("the pass touched a partition with an unresolved intent, or does not hold it")
	}
	if err := f.promote(ctx, 2, second); err != nil {
		t.Fatalf("redelivered promotion: %v", err)
	}
	if s := f.role.OwnerAuditStats(); len(s.Unclean) != 0 || !f.role.OwnerAuditClean(f.schema.TableID) {
		t.Fatalf("after the finalized promotion: %+v", s)
	}
	if ack := f.lastAck(); !ack.Applied || ack.PromotionSeq != 2 {
		t.Fatalf("acknowledgement = %+v, want promotion 2 applied", ack)
	}
}
