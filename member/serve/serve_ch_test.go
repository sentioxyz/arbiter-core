package serve

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"

	"github.com/sentioxyz/arbiter-core/dataplane/chtest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// requireCH opens CH_ADDR (default 127.0.0.1:9000) through chtest once
// ARBITER_CH_INTEGRATION=1 is set, as the other ClickHouse-gated packages do.
func requireCH(t *testing.T) clickhouse.Conn {
	t.Helper()
	if os.Getenv("ARBITER_CH_INTEGRATION") != "1" {
		t.Skip("set ARBITER_CH_INTEGRATION=1 (and run ClickHouse on CH_ADDR or 127.0.0.1:9000) to run")
	}
	addr := os.Getenv("CH_ADDR")
	if addr == "" {
		addr = "127.0.0.1:9000"
	}
	return chtest.Open(t, addr)
}

// waitFor polls cond every 20ms until it holds or 10s pass.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestServeHoldsInstallsOfAServedDatabase runs the lease protocol against a
// real ClickHouse and a real HTTP endpoint (clarification K44): while the host
// holds the lease of a served database, the install that the owner's next
// entry needs (the installer's ATTACH PARTITION ... FROM a scratch into the
// copy, both made by ddl's member intents) does not run and the host keeps
// reading the published rows; it runs once the host released the lease, and
// serving resumes at the new snapshot only after two polls.
func TestServeHoldsInstallsOfAServedDatabase(t *testing.T) {
	conn := requireCH(t)
	ctx := t.Context()
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(b[:])
	// Unique databases: CI runs every ClickHouse target against one server.
	pinned := ddl.MemberPinned{
		MemberUnsafeDB: "serve_munsafe_" + suffix, ReplicaDB: "serve_replica_" + suffix, StageDB: "serve_rstage_" + suffix, NodeID: "member-b",
	}
	schema := payloadexec.TableSchema{TableID: "dex.swaps", Columns: []lthash.Column{{Name: "day", Type: "String"}, {Name: "v", Type: "UInt64"}}, PartitionBy: "day"}
	exec := func(query string) {
		t.Helper()
		if err := conn.Exec(ctx, query); err != nil {
			t.Fatalf("%s: %v", query, err)
		}
	}
	if err := ddl.EnsureMemberDatabases(ctx, conn, pinned); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, db := range []string{pinned.ReplicaDB, pinned.StageDB} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS `"+db+"` SYNC")
		}
	})
	if err := ddl.EnsureCopy(ctx, conn, pinned, schema, 7); err != nil {
		t.Fatal(err)
	}
	copyTable := sitable.CopyTable("dex.swaps", 7)
	if got := ddl.CopyTableName("dex.swaps", 7); got != copyTable {
		t.Fatalf("ddl.CopyTableName = %q, sitable.CopyTable = %q", got, copyTable)
	}
	scratch, err := ddl.ScratchIntent(pinned, schema, "member-b_0123abcd_1")
	if err != nil {
		t.Fatal(err)
	}
	exec(scratch.SQL())
	insert := func(table string, vs ...uint64) {
		t.Helper()
		rows := make([]string, 0, len(vs))
		for _, v := range vs {
			rows = append(rows, fmt.Sprintf("(toFixedString('r%031d', 32), 'd1', %d)", v, v))
		}
		exec(fmt.Sprintf("INSERT INTO %s (`%s`, day, v) VALUES %s", table, ddl.RowIDColumn, strings.Join(rows, ", ")))
	}
	copyRef := "`" + pinned.ReplicaDB + "`.`" + copyTable + "`"
	insert(copyRef, 1, 2, 3)
	insert("`"+pinned.StageDB+"`.`member-b_0123abcd_1`", 4, 5)
	count := func() uint64 {
		t.Helper()
		var n uint64
		if err := conn.QueryRow(ctx, "SELECT count() FROM "+copyRef).Scan(&n); err != nil {
			t.Fatalf("count: %v", err)
		}
		return n
	}

	reg, pub := &fakeRegistry{}, &fakePublished{}
	copies := &fakeCopies{copies: map[OwnerDatabase]DatabaseCopy{}}
	snap := registrySnapshot(testOwner)
	snap.Incarnations = []wire.TableIncarnation{snap.Incarnations[0]} // dex.swaps, seq 7
	reg.set(snap)
	pub.set(manifestAt("0xs40", entry40), nil)
	oneTable := func(snapshotID string, e replay.TrackFrontier) DatabaseCopy {
		c := copyAt(snapshotID, e)
		c.Tables = c.Tables[1:] // dex.swaps
		return c
	}
	copies.set(testOwner, testDatabase, oneTable("0xs40", entry40))
	metrics := NewMetrics()
	cfg := testConfig()
	cfg.PollInterval, cfg.LeaseTTL = 100*time.Millisecond, MinLeaseTTL
	srv, err := New(cfg, Deps{Registry: reg, Published: pub, Copies: copies, Metrics: metrics, Logger: slog.New(slog.DiscardHandler), NodeID: "member-b", StateDir: t.TempDir()})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	runCtx, stop := context.WithCancel(ctx)
	ran := make(chan error, 1)
	go func() { ran <- srv.Run(runCtx) }()
	t.Cleanup(func() { stop(); <-ran })
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)
	host, err := NewClient(ts.URL, testToken, nil)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	served := func(snapshotID string) func() bool {
		return func() bool {
			view, err := host.Serving(ctx)
			return err == nil && len(view.Databases) == 1 && view.Databases[0].Serving && view.Databases[0].SnapshotID == snapshotID
		}
	}

	waitFor(t, "dex to serve at 0xs40", served("0xs40"))
	lease, err := host.Acquire(ctx, "dex", "0xs40")
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	if n := count(); n != 3 {
		t.Fatalf("the host reads %d rows, want the 3 of 0xs40", n)
	}

	// The owner's entry advances; the installer wants to attach two rows.
	pub.set(manifestAt("0xs41", entry41), nil)
	installed := make(chan error, 1)
	go func() {
		if err := srv.BeforeInstall(ctx, testOwner, testDatabase); err != nil {
			installed <- err
			return
		}
		err := conn.Exec(ctx, "ALTER TABLE "+copyRef+" ATTACH PARTITION 'd1' FROM `"+pinned.StageDB+"`.`member-b_0123abcd_1`")
		if err == nil {
			copies.set(testOwner, testDatabase, oneTable("0xs41", entry41))
		}
		srv.AfterInstall(testOwner, testDatabase)
		installed <- err
	}()
	for range 10 {
		time.Sleep(100 * time.Millisecond)
		select {
		case err := <-installed:
			t.Fatalf("the install ran while the host held its lease (err %v)", err)
		default:
		}
		if n := count(); n != 3 {
			t.Fatalf("the host reads %d rows under its lease, want 3", n)
		}
	}
	if _, err := host.Renew(ctx, lease.ID); !errors.Is(err, ErrLeaseRevoked) {
		t.Fatalf("Renew during the install: %v, want ErrLeaseRevoked", err)
	}
	view, err := host.Serving(ctx)
	if err != nil || view.Databases[0].Reason != ReasonInstalling || view.Databases[0].Lease == nil || view.Databases[0].Lease.State != "revoking" {
		t.Fatalf("view during the install: %+v %v", view, err)
	}

	// The host dropped dex from its snapshot and its reads drained.
	if err := host.Release(ctx, lease.ID); err != nil {
		t.Fatalf("Release: %v", err)
	}
	select {
	case err := <-installed:
		if err != nil {
			t.Fatalf("install: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the install did not run after the release")
	}
	if n := count(); n != 5 {
		t.Fatalf("after the install the copy holds %d rows, want 5", n)
	}
	waitFor(t, "dex to serve at 0xs41", served("0xs41"))
	if _, err := host.Acquire(ctx, "dex", "0xs40"); !errors.Is(err, ErrNotServed) {
		t.Fatalf("Acquire at the old snapshot: %v, want ErrNotServed", err)
	}
	if _, err := host.Acquire(ctx, "dex", "0xs41"); err != nil {
		t.Fatalf("Acquire at 0xs41: %v", err)
	}
	requireMetrics(t, metrics, map[string]float64{
		"sentio_node_si_member_serve_lease_waits_total{outcome=released}": 1,
		"sentio_node_si_member_serve_lease_waits_total{outcome=expired}":  0,
	})
}
