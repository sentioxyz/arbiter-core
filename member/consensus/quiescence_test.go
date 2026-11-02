package consensus

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

func readLedgerFile(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, QuiescenceFile))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestQuiescenceLedgerIsDurableBeforeTheJobRuns(t *testing.T) {
	cfg := testConfig(t)
	inc := chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true)
	intent, err := ddl.MemberUnsafeIntent(testMemberPinned(), swapsSchema, 3, "/sentio/0/unsafe_inc/dex__swaps/3")
	if err != nil {
		t.Fatal(err)
	}
	conn := newCHFake()
	conn.addIntent(intent)
	gw := newGatewayFake()
	core := &replayFake{att: replay.ReplayAttestation{ReplicaID: testNodeID, ReceiptHash: "0xr", Signature: "0xs", MatchSourceRoot: true}}
	core.before = func(job replay.ReplayJob) {
		// A second process reads the ledger while the job runs.
		l, err := ReadQuiescence(cfg.StateDir)
		if err != nil {
			t.Errorf("ReadQuiescence during block %d: %v", job.BlockSeq, err)
		}
		if l.Quiet("dex.swaps", 3) {
			t.Errorf("dex.swaps@3 is quiet while block %d replays", job.BlockSeq)
		}
	}
	v := newTestRole(t, cfg, VerifyDeps{Client: startGatewayFake(t, gw),
		Registry: newViewFake(groupSnapshot(true, []string{testNodeID}, nil, inc)), Conn: conn}, core)
	for range 2 { // the second delivery is the arbiter's redelivery of the same block
		if err := v.handler.HandleReplayJob(context.Background(), wire.ReplayJobToPB(swapsJob(5))); err != nil {
			t.Fatal(err)
		}
	}
	if core.count() != 2 {
		t.Fatalf("%d replays, want 2", core.count())
	}
	const want = `{"version":1,"blocks":[{"block_seq":5,"tables":[{"table_id":"dex.swaps","incarnation_seq":3}]}]}`
	if got := readLedgerFile(t, cfg.StateDir); got != want {
		t.Fatalf("quiescence.json = %s, want %s", got, want)
	}
	entries, err := os.ReadDir(cfg.StateDir)
	if err != nil || len(entries) != 1 || entries[0].Name() != QuiescenceFile {
		t.Fatalf("state dir holds %v (%v), want only %s", entries, err, QuiescenceFile)
	}

	// A ledger that cannot be written refuses the job before its replay.
	if err := os.RemoveAll(cfg.StateDir); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.StateDir, []byte("not a directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := v.handler.HandleReplayJob(context.Background(), wire.ReplayJobToPB(swapsJob(6))); err == nil {
		t.Fatal("a job whose ledger write failed was attested")
	}
	if core.count() != 2 {
		t.Fatalf("a job whose ledger write failed was replayed (%d replays)", core.count())
	}
}

func TestQuiescenceResolvesALatchedOrReselectedBlock(t *testing.T) {
	dir := t.TempDir()
	q, err := OpenQuiescence(dir)
	if err != nil {
		t.Fatal(err)
	}
	swaps, pools := QuiescentTable{TableID: "dex.swaps", IncarnationSeq: 3}, QuiescentTable{TableID: "dex.pools", IncarnationSeq: 4}
	for _, b := range []struct {
		seq    uint64
		tables []QuiescentTable
	}{{5, []QuiescentTable{swaps}}, {6, []QuiescentTable{pools}}, {7, []QuiescentTable{swaps}}, {8, []QuiescentTable{pools}}} {
		if err := q.Observe(b.seq, b.tables); err != nil {
			t.Fatal(err)
		}
	}
	gw := newGatewayFake()
	gw.blocks[5] = blockView(5, []string{testNodeID, "verifier-1", "verifier-2"}, true)  // latched
	gw.blocks[6] = blockView(6, []string{"member-c", "verifier-1", "verifier-2"}, false) // re-selected away
	gw.blocks[7] = blockView(7, []string{testNodeID, "verifier-1", "verifier-2"}, false) // still needs our evidence
	client := startGatewayFake(t, gw)
	err = q.Resolve(context.Background(), client, testNodeID)
	if !errors.Is(err, dataplane.ErrBlockNotSealed) || !strings.Contains(err.Error(), "member consensus: resolve block 8: ") {
		t.Fatalf("Resolve = %v, want block 8's read error", err)
	}
	l, err := ReadQuiescence(dir)
	if err != nil {
		t.Fatal(err)
	}
	if l.Quiet("dex.swaps", 3) || l.Quiet("dex.pools", 4) {
		t.Fatalf("after the first resolve: swaps@3 quiet %v (block 7 open), pools@4 quiet %v (block 8 unreadable)", l.Quiet("dex.swaps", 3), l.Quiet("dex.pools", 4))
	}
	const afterFirst = `{"version":1,"blocks":[{"block_seq":7,"tables":[{"table_id":"dex.swaps","incarnation_seq":3}]},{"block_seq":8,"tables":[{"table_id":"dex.pools","incarnation_seq":4}]}]}`
	if got := readLedgerFile(t, dir); got != afterFirst {
		t.Fatalf("quiescence.json = %s, want %s", got, afterFirst)
	}
	// The verdict of block 7 latches and block 8 becomes readable.
	gw.mu.Lock()
	gw.blocks[7] = blockView(7, []string{testNodeID, "verifier-1", "verifier-2"}, true)
	gw.blocks[8] = blockView(8, []string{"verifier-1", "verifier-2", "verifier-3"}, false)
	gw.mu.Unlock()
	if err := q.Resolve(context.Background(), client, testNodeID); err != nil {
		t.Fatal(err)
	}
	if l, err := ReadQuiescence(dir); err != nil || !l.Quiet("dex.swaps", 3) || !l.Quiet("dex.pools", 4) {
		t.Fatalf("after the second resolve the ledger still names a table (%v)", err)
	}
	if got := readLedgerFile(t, dir); got != `{"version":1,"blocks":[]}` {
		t.Fatalf("quiescence.json = %s", got)
	}
	// A reopened ledger starts from the file.
	if err := q.Observe(9, []QuiescentTable{pools}); err != nil {
		t.Fatal(err)
	}
	reopened, err := OpenQuiescence(dir)
	if err != nil {
		t.Fatal(err)
	}
	gw.mu.Lock()
	gw.reads = nil
	gw.blocks[9] = blockView(9, []string{testNodeID, "verifier-1", "verifier-2"}, false)
	gw.mu.Unlock()
	if err := reopened.Resolve(context.Background(), client, testNodeID); err != nil {
		t.Fatal(err)
	}
	gw.mu.Lock()
	reads := slices.Clone(gw.reads)
	gw.mu.Unlock()
	if !slices.Equal(reads, []uint64{9}) {
		t.Fatalf("a reopened ledger read blocks %v, want [9]", reads)
	}
}

func TestReadQuiescenceRefusesACorruptOrUnknownLedger(t *testing.T) {
	dir := t.TempDir()
	if l, err := ReadQuiescence(dir); err != nil || !l.Quiet("dex.swaps", 3) {
		t.Fatalf("a missing ledger = %v, %v; want an empty one", l, err)
	}
	for _, tc := range []struct{ body, want string }{
		{`{`, "member consensus: quiescence.json is corrupt: unexpected end of JSON input"},
		{`{"version":2,"blocks":[]}`, "member consensus: quiescence.json has version 2, want 1"},
	} {
		if err := os.WriteFile(filepath.Join(dir, QuiescenceFile), []byte(tc.body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := ReadQuiescence(dir); err == nil || err.Error() != tc.want {
			t.Fatalf("ReadQuiescence(%s) = %v, want %q", tc.body, err, tc.want)
		}
		if _, err := OpenQuiescence(dir); err == nil || err.Error() != tc.want {
			t.Fatalf("OpenQuiescence(%s) = %v, want %q", tc.body, err, tc.want)
		}
	}
	if _, err := OpenQuiescence(""); err == nil || err.Error() != "member consensus: the quiescence ledger needs a state dir" {
		t.Fatalf("OpenQuiescence(\"\") = %v", err)
	}
}
