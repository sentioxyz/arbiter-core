package snode

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/fspayload"
	"github.com/sentioxyz/arbiter-core/wire"
)

func intakeSchema() payloadexec.TableSchema {
	return payloadexec.TableSchema{
		TableID:     "db.t",
		PartitionBy: "p",
		Columns: []lthash.Column{
			{Name: "p", Type: "String"},
			{Name: "v", Type: "UInt64"},
		},
	}
}

func intakeEnvelope(payload []byte) arbiter.StatementEnvelope {
	sql := "INSERT INTO db.t FORMAT Native"
	schema := intakeSchema()
	return arbiter.StatementEnvelope{
		StatementID:     arbiter.StatementID{ClientAccount: "0xacct", ClientSeq: 1, ClientNonce: "n"},
		StatementKind:   arbiter.StatementKindInsert,
		SQL:             sql,
		SQLHash:         replay.DigestString(sql),
		SettingsHash:    "0x213f12b28bb47c05d226c4b86dad5b91e11d61040568a87dffe2ee87113ec006",
		PayloadRef:      "payload-1.native",
		PayloadHash:     replay.DigestBytes(payload),
		PayloadLength:   uint64(len(payload)),
		TargetTableID:   "db.t",
		UserJWS:         "x.y.z",
		EnvelopeVersion: 2,
		NetworkID:       "testnet",
		KeeperShardID:   0,
		PayloadFormat:   "clickhouse-native-data-v1",
		ClientRevision:  testRevision,
		SchemaHash:      payloadexec.TableSchemaHash("testnet", schema),
		RowIDProfileID:  payloadexec.RowIDProfileID,
	}
}

func newIntakeHarness(t *testing.T, conn clickhouse.Conn, cfg Config) (*Role, *sourceClaimsFake) {
	t.Helper()
	claims := &sourceClaimsFake{}
	addr := startSourceClaimsFake(t, claims)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
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

func createIntakeTable(t *testing.T, conn clickhouse.Conn, role *Role, schema payloadexec.TableSchema) {
	t.Helper()
	table := CHTableName(schema.TableID)
	db := role.cfg.UnsafeDatabase
	qualified := db + "." + table
	mustExecIntake(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
	mustExecIntake(t, conn, "DROP TABLE IF EXISTS "+qualified)
	mustExecIntake(t, conn, fmt.Sprintf(`
		CREATE TABLE %s (
			_hg_row_id FixedString(32),
			p String,
			v UInt64
		) ENGINE = MergeTree
		PARTITION BY p
		ORDER BY tuple()`, qualified))
	mustExecIntake(t, conn, "SYSTEM STOP MERGES "+qualified)
	t.Cleanup(func() { _ = conn.Exec(context.Background(), "DROP TABLE IF EXISTS "+qualified) })
}

func countActiveParts(t *testing.T, conn clickhouse.Conn, role *Role, schema payloadexec.TableSchema) int {
	t.Helper()
	var count uint64
	err := conn.QueryRow(context.Background(), `
		SELECT count()
		FROM system.parts
		WHERE active AND database = ? AND table = ?`,
		role.cfg.UnsafeDatabase, CHTableName(schema.TableID)).Scan(&count)
	if err != nil {
		t.Fatalf("count active parts: %v", err)
	}
	return int(count)
}

func assertSourceRows(t *testing.T, ctx context.Context, conn clickhouse.Conn, table, networkID, tableID, statementID string) {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT _hg_row_id FROM "+table+" ORDER BY v")
	if err != nil {
		t.Fatalf("query rows: %v", err)
	}
	defer rows.Close()
	var got [][]byte
	for rows.Next() {
		var rid []byte
		if err := rows.Scan(&rid); err != nil {
			t.Fatalf("scan row id: %v", err)
		}
		got = append(got, append([]byte(nil), rid...))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows err: %v", err)
	}
	want := [][]byte{
		payloadexec.RowID(networkID, tableID, statementID, 0),
		payloadexec.RowID(networkID, tableID, statementID, 1),
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("row ids: got %x want %x", got, want)
	}
}

func referenceSourceRoot(t *testing.T, ctx context.Context, cfg Config, schema payloadexec.TableSchema, env arbiter.StatementEnvelope, payload []byte) string {
	t.Helper()
	exec := payloadexec.NewWithMaterializer(cfg.NetworkID, nativepayload.Materializer{
		NetworkID: cfg.NetworkID,
		Revision:  testRevision,
	}, schema)
	genesis, err := exec.GenesisSnapshot(0, cfg.SchemaSnapshotID, cfg.ExecutorProfileID)
	if err != nil {
		t.Fatalf("genesis: %v", err)
	}
	st := replay.Statement{
		StatementID:    env.StatementID.Flat(),
		StatementSeq:   env.StatementID.ClientSeq,
		SQL:            env.SQL,
		SQLHash:        env.SQLHash,
		SettingsHash:   env.SettingsHash,
		PayloadRef:     env.PayloadRef,
		PayloadHash:    env.PayloadHash,
		PayloadLength:  env.PayloadLength,
		TargetTableID:  env.TargetTableID,
		UserJWS:        env.UserJWS,
		PayloadFormat:  env.PayloadFormat,
		ClientRevision: env.ClientRevision,
		SchemaHash:     env.SchemaHash,
	}
	job := replay.ReplayJob{
		BlockSeq:           1,
		PrevSafeSnapshotID: genesis.SnapshotID,
		PrevStateRoot:      genesis.StateRoot,
		SchemaSnapshotID:   cfg.SchemaSnapshotID,
		ExecutorProfileID:  cfg.ExecutorProfileID,
		Statements:         []replay.Statement{st},
	}
	_, res, err := exec.ApplyContext(ctx, genesis, job, []replay.PreparedStatement{{Statement: st, Payload: payload}})
	if err != nil {
		t.Fatalf("reference apply: %v", err)
	}
	return res.ComputedStateRoot
}

func activePartByName(t *testing.T, ctx context.Context, conn clickhouse.Conn, db, table, name string) partInfo {
	t.Helper()
	parts, err := activeParts(ctx, conn, db, table)
	if err != nil {
		t.Fatalf("active parts: %v", err)
	}
	for _, p := range parts {
		if p.Name == name {
			return p
		}
	}
	t.Fatalf("part %s not found in %+v", name, parts)
	return partInfo{}
}

func rowCount(t *testing.T, ctx context.Context, conn clickhouse.Conn, table string) uint64 {
	t.Helper()
	var count uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM "+table).Scan(&count); err != nil {
		t.Fatalf("row count: %v", err)
	}
	return count
}

func mustExecIntake(t *testing.T, conn clickhouse.Conn, query string) {
	t.Helper()
	if err := conn.Exec(context.Background(), query); err != nil {
		t.Fatalf("exec %q: %v", query, err)
	}
}

type sourceClaimsFake struct {
	pb.UnimplementedSourceClaimsServer
	pb.UnimplementedPromotionGatewayServer

	mu  sync.Mutex
	rcs []arbiter.RCRecord
	pas []arbiter.PromotionAck
	cas []arbiter.CleanupAck
	err error
}

func (s *sourceClaimsFake) RegisterResultClaim(_ context.Context, rc *pb.RCRecord) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rcs = append(s.rcs, wire.RCFromPB(rc))
	return &pb.Ack{}, s.err
}

func (s *sourceClaimsFake) snapshot() []arbiter.RCRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]arbiter.RCRecord(nil), s.rcs...)
}

func (s *sourceClaimsFake) count() int {
	return len(s.snapshot())
}

func (s *sourceClaimsFake) setErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.err = err
}

func (s *sourceClaimsFake) AckPromotion(_ context.Context, ack *pb.PromotionAck) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pas = append(s.pas, wire.PromotionAckFromPB(ack))
	return &pb.Ack{}, nil
}

func (s *sourceClaimsFake) promotionAcks() []arbiter.PromotionAck {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]arbiter.PromotionAck(nil), s.pas...)
}

func (s *sourceClaimsFake) AckCleanup(_ context.Context, ack *pb.CleanupAck) (*pb.Ack, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cas = append(s.cas, wire.CleanupAckFromPB(ack))
	return &pb.Ack{}, nil
}

func (s *sourceClaimsFake) cleanupAcks() []arbiter.CleanupAck {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]arbiter.CleanupAck(nil), s.cas...)
}

func startSourceClaimsFake(t *testing.T, fake *sourceClaimsFake) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
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
