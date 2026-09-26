package snode

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/fspayload"
	"github.com/sentioxyz/arbiter-core/wire"
)

// purgeReportingFakeS adds SubmitTablePurged to the source-claims fake.
type purgeReportingFakeS struct {
	*sourceClaimsFake
	mu     sync.Mutex
	purged []string
}

func (f *purgeReportingFakeS) SubmitTablePurged(_ context.Context, m *pb.RecordTablePurgedCmd) (*pb.Ack, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.purged = append(f.purged, fmt.Sprintf("%s/%d", m.GetNodeId(), m.GetIncarnationSeq()))
	return &pb.Ack{}, nil
}

func (f *purgeReportingFakeS) reported() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.purged...)
}

func startPurgeReportingFakeS(t *testing.T, fake *purgeReportingFakeS) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	pb.RegisterSourceClaimsServer(srv, fake)
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

// FR-C1: a key retired, purged and recreated under the same name (spec D9)
// promotes on the SNode from the empty base the arbiter's fresh ledger
// carries. The purge forgets the old incarnation's promotion ledger.
func TestRecreatedTable_PromotesFromAnEmptyBaseAfterThePurge(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	cfg := testConfigS(t)
	setUniqueDatabases(t, &cfg)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	suffix := strings.TrimPrefix(cfg.UnsafeDatabase, "hg_unsafe_")
	cfg.NodeID = "snode-" + suffix
	signer := mustPromoteSigner(t)
	cfg.AuthorityAddresses = []string{signer.Address()}
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	fake := &purgeReportingFakeS{sourceClaimsFake: &sourceClaimsFake{}}
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startPurgeReportingFakeS(t, fake)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	payloads, err := fspayload.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	// K is unpartitioned: every promotion targets partition "all".
	k := payloadexec.TableSchema{TableID: "db.k_" + suffix, Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "v", Type: "UInt64"}}}
	genesis := genesisIncarnationS(1, cfg.Tables[0])
	view := newFakeRegistryS()
	view.set(genesis, chainIncarnationS(t, 2, k, wire.TableStatusActive))
	role, err := New(cfg, Deps{Client: client, Conn: conn, Payloads: payloads, Registry: view})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.ensureProtocolTables(ctx); err != nil {
		t.Fatalf("reconcile incarnation 2: %v", err)
	}
	if !role.TableReady(k.TableID) {
		t.Fatal("incarnation 2 must be Ready")
	}

	intake := func(seq uint64, rows ...pv) arbiter.CandidatePart {
		t.Helper()
		mustExecIntake(t, conn, "SYSTEM STOP MERGES "+cfg.UnsafeDatabase+"."+CHTableName(k.TableID))
		payload := nativePayload(t, rows...)
		env := intakeEnvelope(payload)
		env.StatementID.ClientSeq = seq
		env.PayloadRef = fmt.Sprintf("payload-%d.native", seq)
		env.TargetTableID = k.TableID
		env.SQL = "INSERT INTO " + k.TableID + " FORMAT Native"
		env.SQLHash = replay.DigestString(env.SQL)
		env.SchemaHash = payloadexec.TableSchemaHash("testnet", k)
		before := len(fake.snapshot())
		if err := role.SubmitLocalStatement(ctx, env, payload); err != nil {
			t.Fatalf("intake %d: %v", seq, err)
		}
		rcs := fake.snapshot()
		if len(rcs) != before+1 || len(rcs[before].CandidateParts) != 1 {
			t.Fatalf("intake %d rc: %+v", seq, rcs)
		}
		t.Logf("intake %d: part %s", seq, rcs[before].CandidateParts[0].PartName)
		return rcs[before].CandidateParts[0]
	}
	promote := func(seq uint64, part arbiter.CandidatePart) arbiter.PromotionAck {
		t.Helper()
		cmd := arbiter.PromoteSafePartition{
			TableID: k.TableID, PartitionID: part.PartitionID, PromotionSeq: seq,
			BaseSafeSnapshotID: "safe-" + fmt.Sprint(seq-1),
			CandidateParts: []arbiter.PartRef{{
				TableID: k.TableID, PartitionID: part.PartitionID,
				PartRowLtHash: part.PartRowLtHash, PartName: part.PartName,
			}},
		}
		if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), mustSignPromotion(t, signer, cmd)); err != nil {
			t.Fatalf("promote %d: %v", seq, err)
		}
		acks := fake.promotionAcks()
		ack := acks[len(acks)-1]
		if ack.PromotionSeq != seq {
			t.Fatalf("promote %d: last ack %+v", seq, ack)
		}
		return ack
	}

	// Incarnation 2 promotes partition "all" from the empty base.
	part := intake(1, pv{"a", 1}, pv{"a", 2})
	if part.PartitionID != "all" {
		t.Fatalf("partition = %q, want all", part.PartitionID)
	}
	if ack := promote(1, part); !ack.Applied {
		t.Fatalf("first promotion: %+v", ack)
	}
	cleanup := arbiter.UnsafeCleanup{
		TableID: k.TableID, PartitionID: part.PartitionID, PromotionSeq: 1,
		Parts: []arbiter.PartRef{{TableID: k.TableID, PartitionID: part.PartitionID, PartRowLtHash: part.PartRowLtHash, PartName: part.PartName}},
	}
	if err := role.handleCleanup(ctx, wire.CleanupToPB(cleanup), mustSignCleanup(t, signer, cleanup)); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	if base, _ := role.state.BaseRoot(partitionKey{Table: k.TableID, Partition: "all"}); base == "" {
		t.Fatal("fixture: the applied promotion must record a base root")
	}

	// Retire and purge incarnation 2.
	view.set(genesis, chainIncarnationS(t, 2, k, wire.TableStatusPurging))
	if err := role.ensureProtocolTables(ctx); err != nil {
		t.Fatalf("purge pass: %v", err)
	}
	if got := fake.reported(); len(got) != 1 || got[0] != cfg.NodeID+"/2" {
		t.Fatalf("purge reports = %v", got)
	}
	if base, snap := role.state.BaseRoot(partitionKey{Table: k.TableID, Partition: "all"}); base != "" || snap != "" {
		t.Errorf("the purge must forget the old ledger: base %.18s... snapshot %q", base, snap)
	}

	// Recreate K as incarnation 3: Pending, then Active.
	purged := chainIncarnationS(t, 2, k, wire.TableStatusPurged)
	purged.PurgedBy = []string{cfg.NodeID}
	view.set(genesis, purged, chainIncarnationS(t, 3, k, wire.TableStatusPending))
	if err := role.ensureProtocolTables(ctx); err != nil {
		t.Fatalf("pending pass: %v", err)
	}
	view.set(genesis, purged, chainIncarnationS(t, 3, k, wire.TableStatusActive))
	if err := role.ensureProtocolTables(ctx); err != nil {
		t.Fatalf("active pass: %v", err)
	}
	if !role.TableReady(k.TableID) {
		t.Fatal("incarnation 3 must be Ready")
	}

	// The arbiter's fresh ledger promotes incarnation 3 from base "".
	next := intake(2, pv{"b", 3})
	if ack := promote(2, next); !ack.Applied {
		t.Fatalf("a recreated table's first promotion from the empty base must apply: applied=%v detail=%.60s", ack.Applied, ack.Detail)
	}
}

// FR-C1: ForgetTable removes one table's per-partition ledger in one durable
// write, keeps watermarks and other tables, is idempotent, and refuses while
// promotion work still references the table.
func TestStateStore_ForgetTable(t *testing.T) {
	dir := t.TempDir()
	st, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	k, other := partitionKey{Table: "db.k", Partition: "all"}, partitionKey{Table: "db.other", Partition: "all"}
	one := "0x" + strings.Repeat("01", len(lthash.New().Bytes()))
	for _, pk := range []partitionKey{k, other} {
		ack := arbiter.PromotionAck{PromotionSeq: 3, TableID: pk.Table, PartitionID: pk.Partition, Applied: true}
		if err := st.RecordAck(pk, 3, ack, "0xroot", "safe-3"); err != nil {
			t.Fatal(err)
		}
		if err := st.AddUnpromotedPart(pk, "all_0_0_0", one); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.ForgetTable(k.Table); err == nil || !strings.Contains(err.Error(), "unpromoted rows") {
		t.Fatalf("unpromoted rows: err = %v", err)
	}
	if err := st.DrainUnpromoted(k, []string{one}); err != nil {
		t.Fatal(err)
	}
	st.mu.Lock()
	st.s.PromotionIntents[key(k.Table, k.Partition)] = promotionIntent{PromotionSeq: 4}
	st.mu.Unlock()
	if err := st.ForgetTable(k.Table); err == nil || !strings.Contains(err.Error(), "promotion intent") {
		t.Fatalf("intent: err = %v", err)
	}
	st.mu.Lock()
	delete(st.s.PromotionIntents, key(k.Table, k.Partition))
	st.s.PromotedUnsafeParts[key(k.Table, k.Partition)] = []string{"all_0_0_0"}
	st.mu.Unlock()
	if err := st.ForgetTable(k.Table); err == nil || !strings.Contains(err.Error(), "await cleanup") {
		t.Fatalf("promoted parts: err = %v", err)
	}
	if err := st.RecordCleanup(k, []string{"all_0_0_0"}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := st.ForgetTable(k.Table); err != nil {
			t.Fatalf("forget: %v", err)
		}
	}
	reopened, err := openStateStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	if base, snap := reopened.BaseRoot(k); base != "" || snap != "" {
		t.Fatalf("forgotten base survives a reopen: %q %q", base, snap)
	}
	if _, ok := reopened.LastAck(k); ok {
		t.Fatal("forgotten last ack survives a reopen")
	}
	if reopened.Watermark(k) != 3 {
		t.Fatal("the watermark must stay: promotion seqs are global")
	}
	if err := reopened.AddUnpromotedPart(k, "all_0_0_0", "0x"+strings.Repeat("02", len(lthash.New().Bytes()))); err != nil {
		t.Fatalf("a reused part name after the forget: %v", err)
	}
	if base, _ := reopened.BaseRoot(other); base != "0xroot" || reopened.UnpromotedSum(other) == "" {
		t.Fatal("another table's ledger must be untouched")
	}
}
