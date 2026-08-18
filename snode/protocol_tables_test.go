package snode

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

func requireKeeperS(t *testing.T, conn clickhouse.Conn) {
	t.Helper()
	var n uint64
	if err := conn.QueryRow(context.Background(), "SELECT count() FROM system.zookeeper WHERE path = '/'").Scan(&n); err != nil {
		if os.Getenv("ARBITER_CH_KEEPER") == "1" {
			t.Fatalf("ARBITER_CH_KEEPER=1 but no Keeper: %v", err)
		}
		t.Skipf("no Keeper configured: %v", err)
	}
}

func TestRegister_EnsuresProtocolTablesThenFailsClosedOnDrift(t *testing.T) {
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	server := &snodeFakeServer{}
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)

	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	schema := intakeSchema()
	schema.TableID = "db.t_" + suffix
	cfg := testConfigS(t)
	cfg.NodeID = "snode-" + suffix
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.ProtocolTables = ddl.ModeCreateAndVerify
	setUniqueDatabases(t, &cfg)
	t.Cleanup(func() {
		for _, database := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+database+" SYNC")
		}
	})
	role, err := New(cfg, Deps{Client: client, Conn: conn})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}

	if err := role.Register(ctx); err != nil {
		t.Fatalf("register: %v", err)
	}
	table := CHTableName(schema.TableID)
	var engine string
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", role.cfg.UnsafeDatabase, table).Scan(&engine); err != nil || engine != "ReplicatedMergeTree" {
		t.Fatalf("hg_unsafe engine = %q err=%v", engine, err)
	}
	if err := conn.QueryRow(ctx, "SELECT engine FROM system.tables WHERE database = ? AND name = ?", role.cfg.SafeDatabase, table).Scan(&engine); err != nil || engine != "MergeTree" {
		t.Fatalf("hg_safe engine = %q err=%v", engine, err)
	}
	if regs, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("registration must still happen after ensure: %+v", regs)
	}

	if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s.%s MODIFY SETTING max_bytes_to_merge_at_max_space_in_pool = 1", role.cfg.SafeDatabase, table)); err != nil {
		t.Fatalf("tamper: %v", err)
	}
	err = role.Register(ctx)
	if !errors.Is(err, ddl.ErrProtocolTableDrift) {
		t.Fatalf("drift must fail closed before re-registration, got %v", err)
	}
	if regs, _ := server.snapshot(); len(regs) != 1 {
		t.Fatalf("drifted role must not register again: %+v", regs)
	}
}

func TestRegister_ProtocolTablesModeRequiresConn(t *testing.T) {
	cfg := testConfigS(t)
	cfg.ProtocolTables = ddl.ModeVerifyOnly
	server := &snodeFakeServer{}
	addr := startSNodeFakeServer(t, server)
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: addr}}})
	if err != nil {
		t.Fatalf("new dataplane client: %v", err)
	}
	t.Cleanup(client.Close)
	role, err := New(cfg, Deps{Client: client})
	if err != nil {
		t.Fatalf("new snode: %v", err)
	}
	if err := role.Register(context.Background()); err == nil {
		t.Fatal("ensure mode without a ClickHouse connection must fail")
	}
}
