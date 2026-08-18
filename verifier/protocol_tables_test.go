package verifier

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"os"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

func TestNew_ProtocolTablesModeRequiresConn(t *testing.T) {
	cfg := testConfigV()
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	defer client.Close()
	if _, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}}); err == nil {
		t.Fatal("ensure mode without a ClickHouse connection must fail at construction")
	}
}

func TestRegister_EnsuresProtocolTablesOnVerifier(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	var n uint64
	if err := conn.QueryRow(ctx, "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but no Keeper: %v", err)
		}
		t.Skipf("no Keeper configured: %v", err)
	}
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	server := newVerifierFakeServer()
	addr := startVerifierFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	cfg := testConfigV()
	cfg.ReplicaID = "verifier-" + suffix
	sch := scanTableSchema()
	sch.TableID = "db.t_" + suffix
	cfg.Tables = []payloadexec.TableSchema{sch}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.UnsafeDatabase = "hg_unsafe_" + suffix
	cfg.SafeDatabase = "hg_safe_" + suffix
	cfg.PromoteDatabase = "hg_promote_" + suffix
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	t.Cleanup(func() {
		for _, database := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" SYNC")
		}
	})
	role, err := New(cfg, Deps{Client: client, Replay: &fakeReplayCore{}, Scanner: &fakeScanner{}, Conn: conn})
	if err != nil {
		t.Fatalf("new role: %v", err)
	}
	if err := role.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	var replica string
	if err := conn.QueryRow(ctx, "SELECT replica_name FROM system.replicas WHERE database = ? AND table = ?", cfg.UnsafeDatabase, ddl.CHTableName(sch.TableID)).Scan(&replica); err != nil || replica != cfg.ReplicaID {
		t.Fatalf("replica_name = %q err=%v want %q", replica, err, cfg.ReplicaID)
	}
	if regs, _, _, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("registration after ensure: %+v", regs)
	}
}
