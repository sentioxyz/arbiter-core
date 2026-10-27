package shadow

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

// The arbiter-core client is the production reader behind both interfaces
// (Task 34 passes it as Deps.Blocks and Deps.Watermark).
var (
	_ BlockReader     = (*dataplane.Client)(nil)
	_ WatermarkReader = (*dataplane.Client)(nil)
)

// TestShadowWaitsUntilTheBlockIsDispatched: a block read before every
// statement's result claim is bound and its verifiers are selected is
// re-read every PollMin; the candidates and the job of the dispatched read
// are the ones pulled and replayed.
func TestShadowWaitsUntilTheBlockIsDispatched(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := newFixture(t)
		b := fx.block(7, ownerA, "p0", 101)
		sealed := b.BV
		sealed.Statements = slices.Clone(b.BV.Statements)
		sealed.Statements[0].RCBound, sealed.Statements[0].CandidateParts = false, nil
		sealed.VerifierSet, sealed.Evidence, sealed.VerdictEvaluated, sealed.Quorum = nil, nil, false, false
		bound := sealed
		bound.Statements = slices.Clone(b.BV.Statements)
		arb := newFakeArbiter(6)
		arb.put(7, sealed, bound, b.BV)
		pull := newFakePull(b.Part)
		v, _ := newTestShadow(t, fx, arb, pull, testConfig(t.TempDir()))

		start := time.Now()
		stepAll(t, v, 1)

		if waited := time.Since(start); waited != 2*time.Second {
			t.Fatalf("waited %s, want 2s (dispatched at the third read)", waited)
		}
		if needs := pull.fetched(); len(needs) != 1 || needs[0].PartName != b.Part.PartName {
			t.Fatalf("pulled %+v, want the dispatched block's candidate", needs)
		}
		wantCounts(t, v.Stats(), ownerA, 1, 0, 0, 0, 0, 0)
	})
}

// TestShadowSkipsASnapshotQueryBlock: GetBlockVerification refuses a
// snapshot-query block (it has no INSERT verification); the shadow skips it
// instead of stalling on it.
func TestShadowSkipsASnapshotQueryBlock(t *testing.T) {
	fx := newFixture(t)
	b8 := fx.block(8, ownerA, "p0", 1)
	arb := newFakeArbiter(6, b8.BV)
	arb.fail(7, status.Error(codes.Internal, "l3 block 7: l3 block 7 is a snapshot-query block and has no INSERT verification"))
	v, _ := newTestShadow(t, fx, arb, newFakePull(b8.Part), testConfig(t.TempDir()))

	stepAll(t, v, 2)

	wantCounts(t, v.Stats(), ownerA, 1, 0, 0, 0, 0, 0)
	if got := v.Stats().NextBlockSeq; got != 9 {
		t.Fatalf("NextBlockSeq = %d, want 9", got)
	}

	other := newFakeArbiter(6)
	other.fail(7, status.Error(codes.Internal, "l3 block 7: something else"))
	w, _ := newTestShadow(t, fx, other, newFakePull(), testConfig(t.TempDir()))
	if advanced, err := w.Step(context.Background()); advanced || err == nil || !strings.Contains(err.Error(), "shadow: read block 7: ") {
		t.Fatalf("another Internal refusal = %v, %v; want a held cursor and the read error", advanced, err)
	}
}

// TestShadowHoldsTheCursorWhenTheReplayFails: a block the shadow cannot
// replay (here: its base manifest is unknown) is not counted, keeps the
// cursor for a retry, and its scratch is still dropped.
func TestShadowHoldsTheCursorWhenTheReplayFails(t *testing.T) {
	fx := newFixture(t)
	b := fx.block(7, ownerA, "p0", 1)
	b.BV.ReplayJob.PrevSafeSnapshotID = replay.DigestString("an unknown manifest")
	dir := t.TempDir()
	v, conn := newTestShadow(t, fx, newFakeArbiter(6, b.BV), newFakePull(b.Part), testConfig(dir))

	advanced, err := v.Step(context.Background())

	if advanced || err == nil || !strings.Contains(err.Error(), "shadow: replay block 7: ") {
		t.Fatalf("Step = %v, %v; want a held cursor and the replay error", advanced, err)
	}
	wantCounts(t, v.Stats(), ownerA, 0, 0, 0, 0, 0, 0)
	if got := v.Stats().NextBlockSeq; got != 7 {
		t.Fatalf("NextBlockSeq = %d, want 7", got)
	}
	if raw, err := os.ReadFile(filepath.Join(dir, CursorFile)); err != nil || string(raw) != `{"version":1,"next_block_seq":7}` {
		t.Fatalf("cursor file = %s (%v)", raw, err)
	}
	if got := conn.statements(); len(got) != 2 || !strings.HasPrefix(got[1], "DROP TABLE IF EXISTS `hg_rstage`.`member-test_0a1b2c3d_rscan_1` SYNC") {
		t.Fatalf("statements %q, want the scratch created and dropped", got)
	}
}

// TestShadowNeverCountsABlockTwiceWhenTheCursorCannotBePersisted: a block
// whose cursor write fails is counted once, the cursor moves past it in
// memory and Run goes on with the next block after the back-off; the file
// keeps the last cursor that was written, so only a restart repeats the
// blocks after it.
func TestShadowNeverCountsABlockTwiceWhenTheCursorCannotBePersisted(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := newFixture(t)
		b7, b8 := fx.block(7, ownerA, "p0", 1), fx.block(8, ownerA, "p0", 2)
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, CursorFile), []byte(`{"version":1,"next_block_seq":7}`), 0o600); err != nil {
			t.Fatal(err)
		}
		arb := newFakeArbiter(6, b7.BV, b8.BV)
		v, _ := newTestShadow(t, fx, arb, newFakePull(b7.Part, b8.Part), testConfig(dir))
		var logs bytes.Buffer
		v.d.Logger = slog.New(slog.NewTextHandler(&logs, nil))
		v.cfg.StateDir = filepath.Join(dir, "gone") // every cursor write from now on fails
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- v.Run(ctx) }()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		cancel()
		if err := <-done; err != context.Canceled {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}

		reads, _ := arb.readLog()
		if countOf(reads, 7) != 1 || countOf(reads, 8) != 1 || countOf(reads, 9) == 0 {
			t.Fatalf("reads %v, want 7 and 8 once each, then 9", reads)
		}
		wantCounts(t, v.Stats(), ownerA, 2, 0, 0, 0, 0, 0)
		if got := v.Stats().NextBlockSeq; got != 9 {
			t.Fatalf("NextBlockSeq = %d, want 9", got)
		}
		if raw, err := os.ReadFile(filepath.Join(dir, CursorFile)); err != nil || string(raw) != `{"version":1,"next_block_seq":7}` {
			t.Fatalf("cursor file = %s (%v), want the last cursor written", raw, err)
		}
		if n := strings.Count(logs.String(), `msg="shadow: the cursor could not be persisted; a restart repeats the block"`); n != 2 {
			t.Fatalf("%d persist failures logged, want 2:\n%s", n, logs.String())
		}
		if strings.Contains(logs.String(), "it is retried after the back-off") {
			t.Fatalf("a block that advanced was logged as retried:\n%s", logs.String())
		}
	})
}

// TestNewRefusesAnInvalidConfig: New's refusals.
func TestNewRefusesAnInvalidConfig(t *testing.T) {
	fx := newFixture(t)
	arb := newFakeArbiter(0)
	deps := Deps{Blocks: arb, Watermark: arb, Snapshots: fx.snapshots, Payloads: fx.payloads, Conn: &fakeConn{},
		UnsafePull: func(uint64) sources.Source { return newFakePull() }}
	badCursor := t.TempDir()
	if err := os.WriteFile(filepath.Join(badCursor, CursorFile), []byte(`{"version":2,"next_block_seq":5}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*Config, *Deps)
		want   string
	}{
		{"no node id", func(c *Config, _ *Deps) { c.NodeID = "" }, "shadow: NodeID, BootNonce, NetworkID and StateDir are required"},
		{"no stage database", func(c *Config, _ *Deps) { c.Pinned.StageDB = "" }, "shadow: Pinned.StageDB is required"},
		{"no unsafe pull", func(_ *Config, d *Deps) { d.UnsafePull = nil }, "shadow: Blocks, Watermark, Snapshots, Payloads, Conn and UnsafePull are required"},
		{"host as owner", func(c *Config, _ *Deps) { c.Owners = []uint64{ownerA, hostIndexer} }, "shadow: owner 1 is the host's own indexer"},
		{"unsorted owners", func(c *Config, _ *Deps) { c.Owners = []uint64{ownerB, ownerA} }, "shadow: owners [2 0] must be ascending and unique"},
		{"poll max below poll min", func(c *Config, _ *Deps) { c.PollMin, c.PollMax = time.Minute, time.Second }, "shadow: PollMax 1s is below PollMin 1m0s"},
		{"bad stage database", func(c *Config, _ *Deps) { c.Pinned.StageDB = "hg-rstage" }, `shadow: chexec: materializer database "hg-rstage" must match [a-z_][a-z0-9_]*`},
		{"no storage policy", func(c *Config, _ *Deps) { c.Pinned.StoragePolicy = "" }, `shadow: chexec: materializer storage policy "" must match`},
		{"invalid storage policy", func(c *Config, _ *Deps) { c.Pinned.StoragePolicy = "member's" }, `shadow: chexec: materializer storage policy`},
		{"bad cursor", func(c *Config, _ *Deps) { c.StateDir = badCursor }, "has version 2, want 1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, d := testConfig(t.TempDir()), deps
			tc.mutate(&cfg, &d)
			if _, err := New(cfg, d); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("New = %v, want %q", err, tc.want)
			}
		})
	}
}

// TestNewBuildsTheReplayCoreWithAThrowawaySigner: New wires the network
// verifier's replay core over the feed's store and the DA client, with a
// signer named <node id>-shadow whose key is drawn per boot.
func TestNewBuildsTheReplayCoreWithAThrowawaySigner(t *testing.T) {
	fx := newFixture(t)
	arb := newFakeArbiter(0)
	keys := make([]ed25519.PublicKey, 0, 2)
	for i := 0; i < 2; i++ {
		v, err := New(testConfig(t.TempDir()), Deps{Blocks: arb, Watermark: arb, Snapshots: fx.snapshots, Payloads: fx.payloads,
			Conn: &fakeConn{}, UnsafePull: func(uint64) sources.Source { return newFakePull() }})
		if err != nil {
			t.Fatalf("New: %v", err)
		}
		core, ok := v.replay.(*replay.Verifier)
		if !ok {
			t.Fatalf("replay core is %T, want *replay.Verifier", v.replay)
		}
		if core.Snapshots != replay.SnapshotStore(fx.snapshots) || core.Payloads != replay.PayloadStore(fx.payloads) {
			t.Fatal("the replay core does not read the given manifest store and payload store")
		}
		if _, ok := core.Executor.(*snapshotquery.CompositeExecutor); !ok {
			t.Fatalf("executor is %T, want the network verifier's *snapshotquery.CompositeExecutor", core.Executor)
		}
		if want := (payloadexec.SchemaHashes{NetworkID: testNetwork, Tables: []payloadexec.TableSchema{testSchema()}}); !reflect.DeepEqual(core.SchemaHashes, want) {
			t.Fatalf("schema hashes %+v, want %+v", core.SchemaHashes, want)
		}
		signer, ok := core.Signer.(*payloadexec.Ed25519Signer)
		if !ok {
			t.Fatalf("signer is %T", core.Signer)
		}
		if replica, _, err := signer.SignReplayReceipt(context.Background(), "0xabc"); err != nil || replica != "member-test-shadow" {
			t.Fatalf("signer replica %q (%v), want member-test-shadow", replica, err)
		}
		keys = append(keys, signer.PublicKey())
	}
	if keys[0].Equal(keys[1]) {
		t.Fatal("two boots drew the same signer key")
	}
}

// TestReplayPrefixNeverCollidesWithInstallScratch: the shadow's tables sit
// under the node prefix (so the startup drop removes them) and never under
// an install scratch name.
func TestReplayPrefixNeverCollidesWithInstallScratch(t *testing.T) {
	prefix := ReplayPrefix("member-a", "0a1b2c3d")
	if prefix != "member-a_0a1b2c3d_r" {
		t.Fatalf("ReplayPrefix = %q", prefix)
	}
	if got := scanScratchName("member-a", "0a1b2c3d", 3); got != "member-a_0a1b2c3d_rscan_3" {
		t.Fatalf("scanScratchName = %q", got)
	}
	if !strings.HasPrefix(prefix, install.NodePrefix("member-a")) || !strings.HasPrefix(prefix, install.ScratchPrefix("member-a", "0a1b2c3d")) {
		t.Fatalf("%q is not under the node's scratch prefix", prefix)
	}
	for n := uint64(0); n < 1000; n++ {
		if name := install.ScratchName("member-a", "0a1b2c3d", n); strings.HasPrefix(name, prefix) {
			t.Fatalf("install scratch %q starts with the replay prefix", name)
		}
	}
}

// TestStatsExportEveryOwnerAndKindWithZeros: before any block every
// configured owner has every counter and every mismatch kind at zero, and no
// other owner appears.
func TestStatsExportEveryOwnerAndKindWithZeros(t *testing.T) {
	fx := newFixture(t)
	v, _ := newTestShadow(t, fx, newFakeArbiter(0), newFakePull(), testConfig(t.TempDir()))
	st := v.Stats()
	for _, m := range []map[uint64]uint64{st.Blocks, st.MissedScans, st.Unverdicted} {
		if len(m) != 2 {
			t.Fatalf("owners %v, want exactly %d and %d", m, ownerA, ownerB)
		}
	}
	for _, owner := range []uint64{ownerA, ownerB} {
		wantCounts(t, st, owner, 0, 0, 0, 0, 0, 0)
		if len(st.Mismatches[owner]) != len(MismatchKinds()) {
			t.Fatalf("owner %d kinds %v, want %v", owner, st.Mismatches[owner], MismatchKinds())
		}
	}
	var names []string
	for _, k := range MismatchKinds() {
		names = append(names, string(k))
	}
	if got := strings.Join(names, ","); got != "receipt,scan,claim" {
		t.Fatalf("MismatchKinds = %s", got)
	}
}
