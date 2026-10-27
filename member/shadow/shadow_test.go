package shadow

import (
	"bytes"
	"context"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/wire"
)

// TestShadowComparesItsReceiptWithEveryRecordedAttestation: the shadow's own
// receipt hash is compared with every recorded attestation; one verifier
// that recorded another receipt is one receipt mismatch.
func TestShadowComparesItsReceiptWithEveryRecordedAttestation(t *testing.T) {
	fx := newFixture(t)
	honest := fx.block(7, ownerA, "p0", 101)
	lying := fx.block(8, ownerA, "p0", 102)
	lying.BV.Evidence[2].Attestation.ReceiptHash = replay.DigestString("another receipt")
	arb := newFakeArbiter(6, honest.BV, lying.BV)
	pull := newFakePull(honest.Part, lying.Part)
	v, _ := newTestShadow(t, fx, arb, pull, testConfig(t.TempDir()))

	stepAll(t, v, 2)

	st := v.Stats()
	wantCounts(t, st, ownerA, 2, 0, 0, 1, 0, 0)
	wantCounts(t, st, ownerB, 0, 0, 0, 0, 0, 0)
	if st.NextBlockSeq != 9 {
		t.Fatalf("NextBlockSeq = %d, want 9", st.NextBlockSeq)
	}
	needs := pull.fetched()
	if len(needs) != 2 || needs[0].PartName != honest.Part.PartName || needs[1].PartName != lying.Part.PartName {
		t.Fatalf("pulled %+v, want the two candidate parts in block order", needs)
	}
	for _, n := range needs {
		if n.TableKey != "db.t" || n.Table != "db__t" || n.RowCount != 1 || n.PartPhysHash == "" {
			t.Fatalf("need %+v does not carry the candidate part", n)
		}
	}
}

// TestShadowComparesItsScansWithEveryRecordedByteSideScan: every recorded
// byte-side scan is compared with the shadow's own scan of each candidate
// part, keyed by the claimed row LtHash; another scanned hash and a part the
// scan does not report are each one scan mismatch.
func TestShadowComparesItsScansWithEveryRecordedByteSideScan(t *testing.T) {
	fx := newFixture(t)
	b := fx.block(7, ownerA, "p0", 101)
	b.BV.Evidence[1].ByteSideScan.Parts[0].ScannedPartRowLtHash = evilHash("another scan")
	b.BV.Evidence[2].ByteSideScan.Parts = nil
	upper := fx.block(8, ownerA, "p0", 102)
	// Hex case is not a disagreement (check 3 compares with EqualFold).
	upper.BV.Evidence[0].ByteSideScan.Parts[0].ScannedPartRowLtHash = "0x" + strings.ToUpper(strings.TrimPrefix(upper.Part.PartRowLtHash, "0x"))
	arb := newFakeArbiter(6, b.BV, upper.BV)
	v, _ := newTestShadow(t, fx, arb, newFakePull(b.Part, upper.Part), testConfig(t.TempDir()))

	stepAll(t, v, 2)

	wantCounts(t, v.Stats(), ownerA, 2, 0, 0, 0, 2, 0)
}

// TestShadowComparesEvidenceRecordedAfterTheLatchAsRecorded: evidence the
// verdict did not evaluate (recorded after the quorum latched) is compared
// by its recorded body, exactly like evaluated evidence.
func TestShadowComparesEvidenceRecordedAfterTheLatchAsRecorded(t *testing.T) {
	fx := newFixture(t)
	late := fx.block(7, ownerA, "p0", 101)
	late.BV.Evidence[2].Evaluated, late.BV.Evidence[2].Pass = false, false
	late.BV.Evidence[2].Attestation.ReceiptHash = replay.DigestString("late receipt")
	late.BV.Evidence[2].ByteSideScan.Parts[0].ScannedPartRowLtHash = evilHash("late scan")
	honestLate := fx.block(8, ownerA, "p0", 102)
	honestLate.BV.Evidence[2].Evaluated, honestLate.BV.Evidence[2].Pass = false, false
	arb := newFakeArbiter(6, late.BV, honestLate.BV)
	v, _ := newTestShadow(t, fx, arb, newFakePull(late.Part, honestLate.Part), testConfig(t.TempDir()))

	stepAll(t, v, 2)

	wantCounts(t, v.Stats(), ownerA, 2, 0, 0, 1, 1, 0)
}

// TestShadowReproducesARejectedBlocksReceipts: the two fraud classes of
// arbiter chpipeline's fraud tests. A lying candidate part hash and a lying
// partition sum both end Rejected over complete evidence; the shadow
// reproduces every honest verifier's receipt and scan and counts no
// mismatch, because a claim is judged only after a passed quorum.
func TestShadowReproducesARejectedBlocksReceipts(t *testing.T) {
	fx := newFixture(t)
	lyingPart := fx.block(7, ownerA, "p0", 301)
	honestPart := lyingPart.Part
	evil := evilHash("evil-part")
	lyingPart.BV.Statements[0].CandidateParts[0].PartRowLtHash = evil
	lyingPart.BV.ScanParts[0].PartRowLtHash = evil
	lyingPart.BV.Statements[0].Status = "Rejected"
	lyingPart.BV.Quorum = false
	for i := range lyingPart.BV.Evidence {
		ev := &lyingPart.BV.Evidence[i]
		ev.ByteSideScan.Parts[0].ClaimedPartRowLtHash = evil // verifiers scan the true part by name
		ev.Check3ByteSideMatch, ev.Pass = false, false
	}

	lyingSum := fx.block(8, ownerA, "p0", 401)
	lyingSum.BV.Statements[0].PartitionNewPartSums[0].NewPartsLtHashSum = evilHash("evil-sum")
	lyingSum.BV.Statements[0].Status = "Rejected"
	lyingSum.BV.Quorum = false
	for i := range lyingSum.BV.Evidence {
		ev := &lyingSum.BV.Evidence[i]
		ev.Check1RootMatch, ev.Check2PartitionMatch, ev.Pass = false, false, false
	}

	arb := newFakeArbiter(6, lyingPart.BV, lyingSum.BV)
	pull := newFakePull(honestPart, lyingSum.Part) // the owner's hg_unsafe holds the true bytes
	v, _ := newTestShadow(t, fx, arb, pull, testConfig(t.TempDir()))

	stepAll(t, v, 2)

	wantCounts(t, v.Stats(), ownerA, 2, 0, 0, 0, 0, 0)
	if needs := pull.fetched(); len(needs) != 2 || needs[0].PartRowLtHash != evil {
		t.Fatalf("pulled %+v, want the lying candidate as claimed first", needs)
	}
}

// TestShadowClaimMismatchOnlyForAQuorumPassBlock: the shadow scans a
// candidate part to another row LtHash than claimed while every verifier
// reported the claim. That is a claim mismatch only when the quorum passed;
// the scan disagreement with each recorded scan counts either way.
func TestShadowClaimMismatchOnlyForAQuorumPassBlock(t *testing.T) {
	fx := newFixture(t)
	passed := fx.block(7, ownerA, "p0", 101)
	failed := fx.block(8, ownerA, "p0", 102)
	failed.BV.Quorum = false
	for i := range failed.BV.Evidence {
		failed.BV.Evidence[i].Pass = false
	}
	pull := newFakePull(passed.Part, failed.Part)
	pull.parts[passed.Part.PartName] = evilHash("other bytes 7")
	pull.parts[failed.Part.PartName] = evilHash("other bytes 8")
	arb := newFakeArbiter(6, passed.BV, failed.BV)
	v, _ := newTestShadow(t, fx, arb, pull, testConfig(t.TempDir()))

	stepAll(t, v, 1)
	wantCounts(t, v.Stats(), ownerA, 1, 0, 0, 0, 3, 1)
	stepAll(t, v, 1)
	wantCounts(t, v.Stats(), ownerA, 2, 0, 0, 0, 6, 1)
}

// TestShadowCountsACleanedCandidateAsAMissedScan: a candidate part the owner
// already cleaned from its hg_unsafe (the pull misses it), or one that
// arrives without rows, is a missed scan, never a mismatch; the receipt is
// still compared and every scratch is dropped.
func TestShadowCountsACleanedCandidateAsAMissedScan(t *testing.T) {
	fx := newFixture(t)
	cleaned := fx.block(7, ownerA, "p0", 101)
	empty := fx.block(8, ownerA, "p0", 102)
	pull := newFakePull(empty.Part) // block 7's part is gone
	pull.empty[empty.Part.PartName] = true
	arb := newFakeArbiter(6, cleaned.BV, empty.BV)
	v, conn := newTestShadow(t, fx, arb, pull, testConfig(t.TempDir()))

	stepAll(t, v, 2)

	wantCounts(t, v.Stats(), ownerA, 2, 2, 0, 0, 0, 0)
	if created, dropped := conn.count("CREATE TABLE IF NOT EXISTS `hg_rstage`.`member-test_0a1b2c3d_rscan_"), conn.count("DROP TABLE IF EXISTS `hg_rstage`.`member-test_0a1b2c3d_rscan_"); created != 2 || dropped != 2 {
		t.Fatalf("scratch created %d, dropped %d, want 2 and 2: %q", created, dropped, conn.statements())
	}
}

// TestShadowSkipsBlocksOfItsHostOtherOwnersAndTransitions: a block without a
// track, of the host's own track, of an owner outside the group scope, or a
// table-set transition is skipped and the cursor moves on.
func TestShadowSkipsBlocksOfItsHostOtherOwnersAndTransitions(t *testing.T) {
	fx := newFixture(t)
	preTracks := fx.block(7, ownerA, "p0", 1)
	preTracks.BV.Header.TrackIndexerID = nil
	host := fx.block(8, hostIndexer, "p0", 2)
	stranger := fx.block(9, unconfigured, "p0", 3)
	transition := fx.block(10, ownerA, "p0", 4)
	transition.BV.Header.IsTableSetTransition = true
	peer := fx.block(11, ownerB, "p0", 5)
	arb := newFakeArbiter(6, preTracks.BV, host.BV, stranger.BV, transition.BV, peer.BV)
	pull := newFakePull(preTracks.Part, host.Part, stranger.Part, transition.Part, peer.Part)
	v, conn := newTestShadow(t, fx, arb, pull, testConfig(t.TempDir()))
	var logs bytes.Buffer
	v.d.Logger = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))

	stepAll(t, v, 5)

	var reasons []string
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, `msg="shadow: skipping a block"`) {
			reasons = append(reasons, line[strings.Index(line, "reason=")+len("reason="):])
		}
	}
	if want := []string{"no_track", "host", "other_owner", "table_set_transition"}; !slices.Equal(reasons, want) {
		t.Fatalf("skip reasons %q, want %q", reasons, want)
	}

	st := v.Stats()
	wantCounts(t, st, ownerA, 0, 0, 0, 0, 0, 0)
	wantCounts(t, st, ownerB, 1, 0, 0, 0, 0, 0)
	if _, ok := st.Blocks[hostIndexer]; ok {
		t.Fatalf("the host has counters: %+v", st.Blocks)
	}
	if needs := pull.fetched(); len(needs) != 1 || needs[0].PartName != peer.Part.PartName {
		t.Fatalf("pulled %+v, want only block 11's part", needs)
	}
	if n := conn.count("CREATE TABLE"); n != 1 {
		t.Fatalf("created %d scratch tables, want 1", n)
	}
	if st.NextBlockSeq != 12 {
		t.Fatalf("NextBlockSeq = %d, want 12", st.NextBlockSeq)
	}
}

// TestShadowCountsAnUnverdictedBlockAfterVerdictWait: a block without a final
// verdict after VerdictWait (here: evaluated over one of three bundles, no
// quorum) is compared against the evidence recorded so far and counted
// unverdicted; a verdict that arrives in time is waited for.
func TestShadowCountsAnUnverdictedBlockAfterVerdictWait(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := newFixture(t)
		// Block 7: one verifier's bundle evaluated, no quorum yet, two
		// bundles missing: evaluated, but not final.
		pending := fx.block(7, ownerA, "p0", 101)
		pending.BV.Quorum = false
		for i := 1; i < verifierCount; i++ {
			pending.BV.Evidence[i] = wire.VerifierEvidence{ReplicaID: pending.BV.Evidence[i].ReplicaID}
		}
		later := fx.block(8, ownerA, "p0", 102)
		early := later.BV
		early.VerdictEvaluated, early.Quorum = false, false
		arb := newFakeArbiter(6, pending.BV)
		arb.put(8, early, early, later.BV)
		cfg := testConfig(t.TempDir())
		cfg.VerdictWait = time.Minute
		v, _ := newTestShadow(t, fx, arb, newFakePull(pending.Part, later.Part), cfg)

		start := time.Now()
		stepAll(t, v, 1)
		if waited := time.Since(start); waited != time.Minute {
			t.Fatalf("block 7 waited %s, want exactly the 1m verdict wait", waited)
		}
		wantCounts(t, v.Stats(), ownerA, 1, 0, 1, 0, 0, 0)

		start = time.Now()
		stepAll(t, v, 1)
		if waited := time.Since(start); waited != 2*time.Second {
			t.Fatalf("block 8 waited %s, want 2s (its verdict arrived at the third read)", waited)
		}
		wantCounts(t, v.Stats(), ownerA, 2, 0, 1, 0, 0, 0)
		reads, _ := arb.readLog()
		if n := countOf(reads, 7); n != 61 {
			t.Fatalf("block 7 was read %d times, want 61 (one read, then one per second for a minute)", n)
		}
	})
}

func countOf(seqs []uint64, seq uint64) int {
	n := 0
	for _, s := range seqs {
		if s == seq {
			n++
		}
	}
	return n
}

// TestShadowCursorSurvivesARestart: the cursor is persisted after every
// block, and a new shadow on the same state directory resumes from it, not
// from the watermark.
func TestShadowCursorSurvivesARestart(t *testing.T) {
	fx := newFixture(t)
	dir := t.TempDir()
	b7, b8, b9 := fx.block(7, ownerA, "p0", 1), fx.block(8, ownerA, "p0", 2), fx.block(9, ownerA, "p0", 3)
	first, _ := newTestShadow(t, fx, newFakeArbiter(6, b7.BV, b8.BV), newFakePull(b7.Part, b8.Part), testConfig(dir))
	stepAll(t, first, 2)
	raw, err := os.ReadFile(filepath.Join(dir, CursorFile))
	if err != nil {
		t.Fatalf("read cursor: %v", err)
	}
	if string(raw) != `{"version":1,"next_block_seq":9}` {
		t.Fatalf("cursor file = %s", raw)
	}

	arb := newFakeArbiter(100, b9.BV)
	second, _ := newTestShadow(t, fx, arb, newFakePull(b9.Part), testConfig(dir))
	if got := second.Stats().NextBlockSeq; got != 9 {
		t.Fatalf("restarted NextBlockSeq = %d, want 9", got)
	}
	stepAll(t, second, 1)
	if reads, _ := arb.readLog(); !slices.Equal(reads, []uint64{9}) {
		t.Fatalf("restarted shadow read %v, want [9]", reads)
	}
	wantCounts(t, second.Stats(), ownerA, 1, 0, 0, 0, 0, 0)
}

// TestShadowStartsAfterTheWatermark: without a cursor the first block is the
// safe watermark's plus one, and that cursor is persisted before any block is
// verified.
func TestShadowStartsAfterTheWatermark(t *testing.T) {
	fx := newFixture(t)
	dir := t.TempDir()
	arb := newFakeArbiter(41)
	v, _ := newTestShadow(t, fx, arb, newFakePull(), testConfig(dir))
	if got := v.Stats().NextBlockSeq; got != 0 {
		t.Fatalf("NextBlockSeq before the first step = %d, want 0", got)
	}

	advanced, err := v.Step(context.Background())
	if err != nil || advanced {
		t.Fatalf("Step on an unsealed block = %v, %v; want false, nil", advanced, err)
	}
	if reads, _ := arb.readLog(); !slices.Equal(reads, []uint64{42}) {
		t.Fatalf("read %v, want [42]", reads)
	}
	raw, err := os.ReadFile(filepath.Join(dir, CursorFile))
	if err != nil || string(raw) != `{"version":1,"next_block_seq":42}` {
		t.Fatalf("cursor file = %s (%v)", raw, err)
	}
	if got := v.Stats().NextBlockSeq; got != 42 {
		t.Fatalf("NextBlockSeq = %d, want 42", got)
	}
}

// TestShadowBacksOffOnAnUnsealedBlock: Run backs off from PollMin, doubling
// to PollMax, while the next block is not sealed, and resets the back-off
// once a block advanced.
func TestShadowBacksOffOnAnUnsealedBlock(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fx := newFixture(t)
		arb := newFakeArbiter(41)
		b42 := fx.block(42, ownerA, "p0", 1)
		v, _ := newTestShadow(t, fx, arb, newFakePull(b42.Part), testConfig(t.TempDir()))
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- v.Run(ctx) }()

		time.Sleep(2 * time.Minute)
		synctest.Wait()
		arb.put(42, b42.BV)
		time.Sleep(5 * time.Second)
		synctest.Wait()
		cancel()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}

		reads, at := arb.readLog()
		var got []string
		for i := range reads {
			got = append(got, strconv.FormatUint(reads[i], 10)+"@"+at[i].Sub(at[0]).String())
		}
		want := []string{"42@0s", "42@1s", "42@3s", "42@7s", "42@15s", "42@31s", "42@1m1s", "42@1m31s", "42@2m1s", "43@2m1s", "43@2m2s", "43@2m4s"}
		if !slices.Equal(got, want) {
			t.Fatalf("reads %v, want %v", got, want)
		}
		wantCounts(t, v.Stats(), ownerA, 1, 0, 0, 0, 0, 0)
	})
}

// TestShadowNeverSubmitsAnything: the package has no way to submit (it
// imports nothing outside its list, so no gRPC client, no arbiter-proto, no
// network verifier or SNode package and no net; of dataplane it uses only the
// not-sealed sentinel and of install only the scratch prefix), and a run over
// an honest, a fraud and an unverdicted block issues nothing but scratch DDL
// in the stage database.
func TestShadowNeverSubmitsAnything(t *testing.T) {
	t.Run("sources", func(t *testing.T) {
		allowed := map[string]bool{
			"context": true, "crypto/ed25519": true, "crypto/rand": true, "encoding/json": true, "errors": true,
			"fmt": true, "io/fs": true, "log/slog": true, "os": true, "path/filepath": true, "strconv": true,
			"strings": true, "sync": true, "time": true,
			"github.com/ClickHouse/clickhouse-go/v2":                  true,
			"google.golang.org/grpc/codes":                            true,
			"google.golang.org/grpc/status":                           true,
			"github.com/housegate/housegate/pkg/replay":               true,
			"github.com/housegate/housegate/pkg/replay/chexec":        true,
			"github.com/housegate/housegate/pkg/replay/payloadexec":   true,
			"github.com/housegate/housegate/pkg/replay/snapshotquery": true,
			"github.com/sentioxyz/arbiter-core":                       true,
			"github.com/sentioxyz/arbiter-core/dataplane":             true,
			"github.com/sentioxyz/arbiter-core/dataplane/ddl":         true,
			"github.com/sentioxyz/arbiter-core/member/install":        true,
			"github.com/sentioxyz/arbiter-core/member/sources":        true,
			"github.com/sentioxyz/arbiter-core/wire":                  true,
		}
		onlyUses := map[string]string{"dataplane": "ErrBlockNotSealed", "install": "ScratchPrefix"}
		files, err := filepath.Glob("*.go")
		if err != nil {
			t.Fatal(err)
		}
		checked := 0
		for _, name := range files {
			if strings.HasSuffix(name, "_test.go") {
				continue
			}
			f, err := parser.ParseFile(token.NewFileSet(), name, nil, 0)
			if err != nil {
				t.Fatalf("parse %s: %v", name, err)
			}
			checked++
			for _, imp := range f.Imports {
				path, _ := strconv.Unquote(imp.Path.Value)
				if !allowed[path] {
					t.Errorf("%s imports %s, which is not on the shadow's import list", name, path)
				}
			}
			ast.Inspect(f, func(n ast.Node) bool {
				sel, ok := n.(*ast.SelectorExpr)
				if !ok {
					return true
				}
				if x, ok := sel.X.(*ast.Ident); ok {
					if want, ok := onlyUses[x.Name]; ok && sel.Sel.Name != want {
						t.Errorf("%s uses %s.%s", name, x.Name, sel.Sel.Name)
					}
				}
				return true
			})
		}
		if checked != 3 {
			t.Fatalf("checked %d source files, want 3 (compare.go, cursor.go, shadow.go): run from the package directory", checked)
		}
	})
	t.Run("statements", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			fx := newFixture(t)
			honest := fx.block(7, ownerA, "p0", 1)
			fraud := fx.block(8, ownerA, "p0", 2)
			fraud.BV.Quorum = false
			fraud.BV.Statements[0].Status = "Rejected"
			pending := fx.block(9, ownerA, "p0", 3)
			pending.BV.VerdictEvaluated, pending.BV.Quorum = false, false
			arb := newFakeArbiter(6, honest.BV, fraud.BV, pending.BV)
			v, conn := newTestShadow(t, fx, arb, newFakePull(honest.Part, fraud.Part, pending.Part), testConfig(t.TempDir()))
			stepAll(t, v, 3)
			stmts := conn.statements()
			if len(stmts) != 6 {
				t.Fatalf("issued %d statements, want 3 scratch creates and 3 drops: %q", len(stmts), stmts)
			}
			for _, s := range stmts {
				if !strings.HasPrefix(s, "CREATE TABLE IF NOT EXISTS `hg_rstage`.`member-test_0a1b2c3d_r") &&
					!strings.HasPrefix(s, "DROP TABLE IF EXISTS `hg_rstage`.`member-test_0a1b2c3d_r") {
					t.Fatalf("statement outside the shadow's stage tables: %q", s)
				}
			}
		})
	})
}
