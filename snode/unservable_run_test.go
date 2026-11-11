package snode

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/fspayload"
	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// arbiter-core#76: an Active registry table this binary cannot build makes
// that one table unservable; Run keeps serving the others, at startup and
// when the table appears later.
func TestRun_SNodeKeepsServingWithAnUnservableRegistryTable(t *testing.T) {
	for _, phase := range []string{"at startup", "after startup"} {
		t.Run(phase, func(t *testing.T) {
			conn := requireCH(t)
			requireKeeperS(t, conn)
			server := &snodeFakeServer{}
			client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startSNodeFakeServer(t, server)}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			sum := sha1.Sum([]byte(t.Name()))
			suffix := hex.EncodeToString(sum[:])[:10]
			cfg := testConfigS(t)
			cfg.NodeID = "snode-u-" + suffix
			g := intakeSchema()
			g.TableID = "db.g_" + suffix
			cfg.Tables = []payloadexec.TableSchema{g}
			cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
			cfg.SchemaSource = ddl.SchemaSourceNetworkState
			cfg.ProtocolTablesReconcile = 20 * time.Millisecond
			setUniqueDatabases(t, &cfg)
			t.Cleanup(func() {
				for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
					_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
				}
			})
			payloads, err := fspayload.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			bad := chainSchemaS("db.u_" + suffix)
			bad.PartitionBy = "v" // UInt64: the P1c partition freeze refuses it
			view := newFakeRegistryS()
			install := func() {
				view.set(genesisIncarnationS(1, g), chainIncarnationS(t, 2, bad, wire.TableStatusActive))
			}
			if phase == "at startup" {
				install()
			} else {
				view.set(genesisIncarnationS(1, g))
			}
			role, err := New(cfg, Deps{Client: client, Conn: conn, Payloads: payloads, Registry: view})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- role.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
				}
			})
			waitSNodeSubscriptions(t, server, 1, 1)
			if phase == "after startup" {
				install()
			}
			// Many reconcile passes (20ms cadence, backoff capped by it).
			select {
			case err := <-done:
				t.Fatalf("Run returned %v; an unservable table must not end it", err)
			case <-time.After(time.Second):
			}
			if !role.TableReady(g.TableID) || role.TableReady(bad.TableID) {
				t.Fatalf("ready: genesis=%v unservable=%v, want true/false", role.TableReady(g.TableID), role.TableReady(bad.TableID))
			}
			st := role.TableSetStats()
			if len(st.Unservable) != 1 || st.Unservable[0].TableID != bad.TableID || st.Unservable[0].Reason != tableset.UnservablePartitionFreeze {
				t.Fatalf("Unservable = %+v", st.Unservable)
			}
			if _, active := server.subscriptionSnapshot(); active != 1 {
				t.Fatalf("active subscriptions = %d, want 1", active)
			}
		})
	}
}
