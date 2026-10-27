// Package shadow is the si-member's off-consensus shadow verifier (housegate
// spec 2026-10-14 §16 row P2; Plan S3-C2 clarification K34). For every sealed
// block of a peer owner it pulls each candidate part from the owner's
// hg_unsafe into its own scratch table of the member's stage database and
// scans it, replays the block's ReplayJob from DA in the stage database under
// ReplayPrefix with a throwaway signer, waits for the block's verdict, and
// compares its receipt and its scans with the evidence the arbiter recorded.
//
// It never re-derives a check flag or the verdict (they stay the FSM's): it
// compares bodies. Its only arbiter reads are GetBlockVerification and
// GetSafeWatermark, and it submits nothing anywhere.
package shadow

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/install"
	"github.com/sentioxyz/arbiter-core/member/sources"
	"github.com/sentioxyz/arbiter-core/wire"
)

// MismatchKind names what a shadow mismatch disagrees on (clarification K34).
// It is the metric label kind.
type MismatchKind string

const (
	// MismatchReceipt: a recorded attestation's receipt_hash differs from the
	// shadow's own receipt hash.
	MismatchReceipt MismatchKind = "receipt"
	// MismatchScan: a recorded byte-side scan reports another scanned row
	// LtHash for a candidate part than the shadow's own scan, or does not
	// report the part at all.
	MismatchScan MismatchKind = "scan"
	// MismatchClaim: the block's quorum passed while the shadow scanned one of
	// its candidate parts to a row LtHash other than the claimed one.
	MismatchClaim MismatchKind = "claim"
)

// MismatchKinds returns every kind, in export order, so a caller can export
// each one with zeros.
func MismatchKinds() []MismatchKind {
	return []MismatchKind{MismatchReceipt, MismatchScan, MismatchClaim}
}

// BlockReader reads one sealed block's verification; *dataplane.Client
// satisfies it.
type BlockReader interface {
	GetBlockVerification(ctx context.Context, blockSeq uint64) (wire.BlockVerification, error)
}

// WatermarkReader reads the arbiter's safe watermark; *dataplane.Client
// satisfies it.
type WatermarkReader interface {
	GetSafeWatermark(ctx context.Context) (arbiter.SafeWatermark, error)
}

const (
	// DefaultPollMin is the first back-off after an unsealed block or a failed
	// step, and the re-read interval while a block waits for its dispatch or
	// its verdict.
	DefaultPollMin = time.Second
	// DefaultPollMax caps the doubling back-off.
	DefaultPollMax = 30 * time.Second
	// DefaultVerdictWait bounds how long one block waits for its dispatch and
	// its final verdict before it is compared as it stands and counted
	// unverdicted.
	DefaultVerdictWait = 10 * time.Minute
)

// ReplayPrefix is the prefix of every table the shadow creates in the stage
// database: the replay materializer's `<prefix>replay_<n>` and the candidate
// scratch `<prefix>scan_<n>`. It ends in "r", never a digit, so it never
// collides with install.ScratchName, and it starts with
// install.NodePrefix(nodeID), so the startup drop of the node's stage tables
// removes whatever a crashed boot left.
func ReplayPrefix(nodeID, bootNonce string) string {
	return install.ScratchPrefix(nodeID, bootNonce) + "r"
}

// scanScratchName names the scratch of the n-th candidate part this boot pulls.
func scanScratchName(nodeID, bootNonce string, n uint64) string {
	return ReplayPrefix(nodeID, bootNonce) + "scan_" + strconv.FormatUint(n, 10)
}

// Config configures one shadow verifier.
type Config struct {
	NodeID, BootNonce, NetworkID, StateDir string
	// Pinned names the member databases; the shadow writes only StageDB.
	Pinned ddl.MemberPinned
	// Genesis is the configured genesis table set; chain-origin schemas come
	// from each job's TableSchemas, as for a network verifier.
	Genesis []payloadexec.TableSchema
	// Owners is the static group scope of P2 (ascending, unique, never the
	// host); only their blocks are shadow-verified.
	Owners        []uint64
	HostIndexerID uint64
	// PollMin, PollMax and VerdictWait: 0 selects the defaults.
	PollMin, PollMax, VerdictWait time.Duration
}

// Deps are the shadow's collaborators. Every arbiter-facing one is a read.
type Deps struct {
	Blocks    BlockReader
	Watermark WatermarkReader
	// Snapshots is the manifest feed's verifying store (*manifestfeed.Store).
	Snapshots replay.SnapshotStore
	// Payloads is the DA client the network verifiers replay from.
	Payloads replay.PayloadStore
	// Conn is the member's ClickHouse, logged in as si_member.
	Conn clickhouse.Conn
	// UnsafePull returns the pull from owner's hg_unsafe
	// (sources.NewOwnerUnsafePull on the owner's collection).
	UnsafePull func(owner uint64) sources.Source
	Logger     *slog.Logger
	Now        func() time.Time
}

// Stats are the shadow's counters, keyed by owner indexer id. Every
// configured owner and every mismatch kind is present, zeros included.
type Stats struct {
	// Blocks counts the blocks compared, unverdicted ones included.
	Blocks map[uint64]uint64
	// MissedScans counts candidate parts the shadow could not scan.
	MissedScans map[uint64]uint64
	// Unverdicted counts blocks compared without a final verdict.
	Unverdicted map[uint64]uint64
	Mismatches  map[uint64]map[MismatchKind]uint64
	// NextBlockSeq is the block the shadow reads next; 0 before its first
	// cursor is known.
	NextBlockSeq uint64
}

// replayer verifies one replay job; *replay.Verifier satisfies it.
type replayer interface {
	Verify(ctx context.Context, job replay.ReplayJob) (replay.ReplayAttestation, error)
}

// scanFunc scans every part of one table; chexec.ScanAllParts in production.
type scanFunc func(ctx context.Context, conn clickhouse.Conn, qualifiedTable string, schema payloadexec.TableSchema) ([]chexec.PartScanResult, error)

// Verifier is the shadow verifier. Run (or Step) must not be called from two
// goroutines at once; Stats may be called from any goroutine.
type Verifier struct {
	cfg    Config
	d      Deps
	owners map[uint64]bool
	replay replayer
	scan   scanFunc

	scratchSeq uint64 // only the stepping goroutine touches it

	mu    sync.Mutex
	next  uint64
	stats Stats
}

// New validates cfg and d, builds the replay core and loads the cursor.
func New(cfg Config, d Deps) (*Verifier, error) {
	if cfg.NodeID == "" || cfg.BootNonce == "" || cfg.NetworkID == "" || cfg.StateDir == "" {
		return nil, errors.New("shadow: NodeID, BootNonce, NetworkID and StateDir are required")
	}
	if cfg.Pinned.StageDB == "" {
		return nil, errors.New("shadow: Pinned.StageDB is required")
	}
	if d.Blocks == nil || d.Watermark == nil || d.Snapshots == nil || d.Payloads == nil || d.Conn == nil || d.UnsafePull == nil {
		return nil, errors.New("shadow: Blocks, Watermark, Snapshots, Payloads, Conn and UnsafePull are required")
	}
	owners := make(map[uint64]bool, len(cfg.Owners))
	for i, owner := range cfg.Owners {
		if owner == cfg.HostIndexerID {
			return nil, fmt.Errorf("shadow: owner %d is the host's own indexer", owner)
		}
		if i > 0 && owner <= cfg.Owners[i-1] {
			return nil, fmt.Errorf("shadow: owners %v must be ascending and unique", cfg.Owners)
		}
		owners[owner] = true
	}
	if cfg.PollMin <= 0 {
		cfg.PollMin = DefaultPollMin
	}
	if cfg.PollMax <= 0 {
		cfg.PollMax = DefaultPollMax
	}
	if cfg.VerdictWait <= 0 {
		cfg.VerdictWait = DefaultVerdictWait
	}
	if cfg.PollMax < cfg.PollMin {
		return nil, fmt.Errorf("shadow: PollMax %s is below PollMin %s", cfg.PollMax, cfg.PollMin)
	}
	if err := ddl.ValidatePhysicalTableNames(cfg.Genesis); err != nil {
		return nil, fmt.Errorf("shadow: %w", err)
	}
	cfg.Owners = append([]uint64(nil), cfg.Owners...)
	cfg.Genesis = append([]payloadexec.TableSchema(nil), cfg.Genesis...)
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	core, err := newReplayCore(cfg, d)
	if err != nil {
		return nil, err
	}
	next, err := loadCursor(cfg.StateDir)
	if err != nil {
		return nil, err
	}
	v := &Verifier{cfg: cfg, d: d, owners: owners, replay: core, scan: chexec.ScanAllParts, next: next}
	v.stats = Stats{
		Blocks:      make(map[uint64]uint64, len(cfg.Owners)),
		MissedScans: make(map[uint64]uint64, len(cfg.Owners)),
		Unverdicted: make(map[uint64]uint64, len(cfg.Owners)),
		Mismatches:  make(map[uint64]map[MismatchKind]uint64, len(cfg.Owners)),
	}
	for _, owner := range cfg.Owners {
		v.stats.Blocks[owner], v.stats.MissedScans[owner], v.stats.Unverdicted[owner] = 0, 0, 0
		v.stats.Mismatches[owner] = make(map[MismatchKind]uint64, 3)
		for _, kind := range MismatchKinds() {
			v.stats.Mismatches[owner][kind] = 0
		}
	}
	return v, nil
}

// newReplayCore is the network verifier's replay core (arbiter-core
// verifier.NewReplayCore) with two differences: the materializer keeps its
// scratch in the stage database under ReplayPrefix, and the signer is a
// throwaway key drawn for this boot, so a shadow receipt can never pass for a
// network verifier's.
func newReplayCore(cfg Config, d Deps) (*replay.Verifier, error) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return nil, fmt.Errorf("shadow: draw the signer seed: %w", err)
	}
	signer, err := payloadexec.NewEd25519Signer(cfg.NodeID+"-shadow", seed)
	if err != nil {
		return nil, fmt.Errorf("shadow: signer: %w", err)
	}
	materializer, err := chexec.NewMaterializerInWithStoragePolicy(cfg.NetworkID, d.Conn, cfg.Pinned.StageDB, ReplayPrefix(cfg.NodeID, cfg.BootNonce), cfg.Pinned.StoragePolicy)
	if err != nil {
		return nil, fmt.Errorf("shadow: %w", err)
	}
	executor, err := snapshotquery.NewCompositeExecutor(payloadexec.NewDynamic(cfg.NetworkID, materializer, cfg.Genesis...), nil)
	if err != nil {
		return nil, fmt.Errorf("shadow: executor: %w", err)
	}
	return &replay.Verifier{
		Snapshots:    d.Snapshots,
		Payloads:     d.Payloads,
		Executor:     executor,
		Signer:       signer,
		SchemaHashes: payloadexec.SchemaHashes{NetworkID: cfg.NetworkID, Tables: cfg.Genesis},
	}, nil
}

// Stats returns a copy of the counters.
func (v *Verifier) Stats() Stats {
	v.mu.Lock()
	defer v.mu.Unlock()
	out := Stats{
		Blocks:       make(map[uint64]uint64, len(v.stats.Blocks)),
		MissedScans:  make(map[uint64]uint64, len(v.stats.MissedScans)),
		Unverdicted:  make(map[uint64]uint64, len(v.stats.Unverdicted)),
		Mismatches:   make(map[uint64]map[MismatchKind]uint64, len(v.stats.Mismatches)),
		NextBlockSeq: v.next,
	}
	for owner, n := range v.stats.Blocks {
		out.Blocks[owner] = n
	}
	for owner, n := range v.stats.MissedScans {
		out.MissedScans[owner] = n
	}
	for owner, n := range v.stats.Unverdicted {
		out.Unverdicted[owner] = n
	}
	for owner, kinds := range v.stats.Mismatches {
		out.Mismatches[owner] = make(map[MismatchKind]uint64, len(kinds))
		for kind, n := range kinds {
			out.Mismatches[owner][kind] = n
		}
	}
	return out
}

// Run shadow-verifies block after block until ctx ends, and returns ctx's
// error. An unsealed next block and a failed step back off from PollMin,
// doubling to PollMax; a block that advanced resets the back-off. A failed
// step keeps the cursor, so the same block is tried again; a block whose
// cursor advanced in memory but could not be persisted is not repeated (a
// restart repeats it), and the next block follows after the back-off.
func (v *Verifier) Run(ctx context.Context) error {
	wait := v.cfg.PollMin
	for {
		advanced, err := v.Step(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err == nil && advanced {
			wait = v.cfg.PollMin
			continue
		}
		switch {
		case err != nil && advanced:
			v.d.Logger.Error("shadow: the cursor could not be persisted; a restart repeats the block",
				"next_block_seq", v.nextSeq(), "pause", wait, "err", err)
		case err != nil:
			v.d.Logger.Error("shadow verification of a block failed; it is retried after the back-off",
				"block", v.nextSeq(), "retry_in", wait, "err", err)
		}
		if err := sleep(ctx, wait); err != nil {
			return err
		}
		wait = min(2*wait, v.cfg.PollMax)
	}
}

// Step handles the next block. It returns false when that block is not sealed
// yet, true when the cursor advanced past it (shadow-verified or skipped), and
// false with an error, leaving the cursor where it was, when the block could
// not be read, pulled into a scratch, scanned or replayed. A cursor that
// advanced in memory but could not be persisted is true with that error: the
// block is not repeated in this process, and a restart repeats it.
func (v *Verifier) Step(ctx context.Context) (bool, error) {
	seq, err := v.cursor(ctx)
	if err != nil {
		return false, err
	}
	bv, err := v.d.Blocks.GetBlockVerification(ctx, seq)
	switch {
	case errors.Is(err, dataplane.ErrBlockNotSealed):
		return false, nil
	case isSnapshotQueryBlock(err):
		v.d.Logger.Info("shadow: skipping a snapshot-query block, which has no INSERT verification", "block", seq)
		return true, v.advance(seq)
	case err != nil:
		return false, fmt.Errorf("shadow: read block %d: %w", seq, err)
	}
	owner, reason := v.classify(bv)
	if reason != "" {
		v.d.Logger.Debug("shadow: skipping a block", "block", seq, "reason", reason)
		return true, v.advance(seq)
	}
	if err := v.verifyBlock(ctx, owner, seq, bv); err != nil {
		return false, err
	}
	return true, v.advance(seq)
}

// classify returns the block's owner, or why the shadow skips it: a
// table-set transition, a block without a track (before the tracks
// activation), the host's own track, or an owner outside the group scope.
func (v *Verifier) classify(bv wire.BlockVerification) (uint64, string) {
	switch track := bv.Header.TrackIndexerID; {
	case bv.Header.IsTableSetTransition:
		return 0, "table_set_transition"
	case track == nil:
		return 0, "no_track"
	case *track == v.cfg.HostIndexerID:
		return 0, "host"
	case !v.owners[*track]:
		return 0, "other_owner"
	default:
		return *track, ""
	}
}

// isSnapshotQueryBlock recognises GetBlockVerification's refusal of a
// snapshot-query block (arbiter: Internal "l3 block %d: l3 block %d is a
// snapshot-query block and has no INSERT verification").
func isSnapshotQueryBlock(err error) bool {
	if err == nil {
		return false
	}
	s, ok := status.FromError(err)
	return ok && s.Code() == codes.Internal && strings.Contains(s.Message(), "is a snapshot-query block")
}

// verifyBlock is the per-block order of contract §22.4: wait for the
// dispatch, pull and scan every candidate part, replay, wait for the final
// verdict, compare, count, drop the scratch.
func (v *Verifier) verifyBlock(ctx context.Context, owner, seq uint64, bv wire.BlockVerification) (err error) {
	deadline := v.d.Now().Add(v.cfg.VerdictWait)
	if bv, err = v.await(ctx, seq, bv, deadline, dispatched); err != nil {
		return err
	}
	var scratches []sources.Scratch
	defer func() { v.dropScratches(scratches) }()
	scans, missed, err := v.scanCandidates(ctx, owner, seq, bv, func(s sources.Scratch) { scratches = append(scratches, s) })
	if err != nil {
		return err
	}
	own, err := v.replay.Verify(ctx, bv.ReplayJob)
	if err != nil {
		return fmt.Errorf("shadow: replay block %d: %w", seq, err)
	}
	if bv, err = v.await(ctx, seq, bv, deadline, finalVerdict); err != nil {
		return err
	}
	final := finalVerdict(bv)
	found := compareBlock(own.ReceiptHash, scans, bv)
	v.count(owner, final, uint64(missed), found)
	for _, m := range found {
		v.d.Logger.Error("shadow verification mismatch", "block", seq, "owner", owner, "kind", string(m.Kind),
			"replica", m.ReplicaID, "table", m.TableID, "partition", m.PartitionID, "part", m.PartName,
			"own", logValue(m.Own), "recorded", logValue(m.Recorded))
	}
	if !final {
		v.d.Logger.Warn("shadow: block has no final verdict after the verdict wait; compared against the evidence recorded so far",
			"block", seq, "owner", owner, "verdict_wait", v.cfg.VerdictWait)
	}
	v.d.Logger.Info("shadow verified a block", "block", seq, "owner", owner, "quorum", bv.Quorum,
		"final", final, "mismatches", len(found), "missed_scans", missed)
	return nil
}

// dispatched reports whether the arbiter has bound every statement's result
// claim and selected the block's verifiers: from then on the replay job (its
// source_claim_root included) and the candidate parts are the ones the
// verifiers received.
func dispatched(bv wire.BlockVerification) bool {
	if len(bv.VerifierSet) == 0 {
		return false
	}
	for _, st := range bv.Statements {
		if !st.RCBound {
			return false
		}
	}
	return true
}

// finalVerdict reports whether the verdict can no longer change: the quorum
// latched, or the verdict was evaluated over every selected verifier's
// attestation and scan (a failed quorum over complete evidence).
func finalVerdict(bv wire.BlockVerification) bool {
	if !bv.VerdictEvaluated {
		return false
	}
	if bv.Quorum {
		return true
	}
	if len(bv.Evidence) == 0 {
		return false
	}
	for _, ev := range bv.Evidence {
		if ev.Attestation == nil || ev.ByteSideScan == nil {
			return false
		}
	}
	return true
}

// await re-reads block seq every PollMin until done holds or the deadline
// passes, and returns the last read. A failed re-read keeps the previous one.
func (v *Verifier) await(ctx context.Context, seq uint64, bv wire.BlockVerification, deadline time.Time, done func(wire.BlockVerification) bool) (wire.BlockVerification, error) {
	for !done(bv) && v.d.Now().Before(deadline) {
		if err := sleep(ctx, v.cfg.PollMin); err != nil {
			return bv, err
		}
		next, err := v.d.Blocks.GetBlockVerification(ctx, seq)
		if err != nil {
			if ctx.Err() != nil {
				return bv, ctx.Err()
			}
			v.d.Logger.Warn("shadow: re-reading a block failed; keeping the previous read", "block", seq, "err", err)
			continue
		}
		bv = next
	}
	return bv, nil
}

// scanCandidates pulls every candidate part of every statement into its own
// fresh scratch, at once and in statement order, and scans each one. A part
// the pull does not fetch (missed, refused, unreachable) or that arrives
// empty is a missed scan, never a mismatch. created is called for every
// scratch table created, before the pull, so the caller drops it.
func (v *Verifier) scanCandidates(ctx context.Context, owner, seq uint64, bv wire.BlockVerification, created func(sources.Scratch)) ([]ownScan, int, error) {
	schemas, err := payloadexec.ResolveJobSchemas(v.cfg.Genesis, bv.ReplayJob)
	if err != nil {
		return nil, 0, fmt.Errorf("shadow: block %d: %w", seq, err)
	}
	pull := v.d.UnsafePull(owner)
	if pull == nil {
		return nil, 0, fmt.Errorf("shadow: no unsafe pull for owner %d", owner)
	}
	var scans []ownScan
	missed := 0
	for _, st := range bv.Statements {
		for _, cp := range st.CandidateParts {
			schema, ok := schemas[cp.TableID]
			if !ok {
				return nil, 0, fmt.Errorf("shadow: block %d: no schema for table %s of candidate part %s", seq, cp.TableID, cp.PartName)
			}
			v.scratchSeq++
			scratch := sources.Scratch{Database: v.cfg.Pinned.StageDB, Table: scanScratchName(v.cfg.NodeID, v.cfg.BootNonce, v.scratchSeq)}
			intent, err := ddl.ScratchIntent(v.cfg.Pinned, schema, scratch.Table)
			if err != nil {
				return nil, 0, fmt.Errorf("shadow: block %d: scratch for %s: %w", seq, cp.TableID, err)
			}
			created(scratch)
			if err := v.d.Conn.Exec(ctx, intent.SQL()); err != nil {
				return nil, 0, fmt.Errorf("shadow: create scratch %s.%s: %w", scratch.Database, scratch.Table, err)
			}
			res := pull.Fetch(ctx, sources.Need{
				TableKey:      cp.TableID,
				Table:         ddl.CHTableName(cp.TableID),
				PartitionID:   cp.PartitionID,
				PartName:      cp.PartName,
				PartPhysHash:  cp.PartPhysHash,
				PartRowLtHash: cp.PartRowLtHash,
				RowCount:      cp.RowCount,
			}, schema, scratch)
			if res.Outcome != sources.OutcomeFetched {
				missed++
				v.d.Logger.Warn("shadow: candidate part not scanned (missed scan)", "block", seq, "owner", owner,
					"table", cp.TableID, "partition", cp.PartitionID, "part", cp.PartName,
					"source", pull.Name(), "outcome", string(res.Outcome), "err", res.Err)
				continue
			}
			results, err := v.scan(ctx, v.d.Conn, scratch.Database+"."+scratch.Table, schema)
			if err != nil {
				return nil, 0, fmt.Errorf("shadow: scan scratch %s.%s: %w", scratch.Database, scratch.Table, err)
			}
			if len(results) == 0 {
				missed++
				v.d.Logger.Warn("shadow: candidate part arrived empty (missed scan)", "block", seq, "owner", owner,
					"table", cp.TableID, "partition", cp.PartitionID, "part", cp.PartName, "source", pull.Name())
				continue
			}
			hashes := make([]string, 0, len(results))
			for _, r := range results {
				hashes = append(hashes, r.RowLtHash)
			}
			sum, err := payloadexec.SumPartRowLtHashes(hashes)
			if err != nil {
				return nil, 0, fmt.Errorf("shadow: scan scratch %s.%s: %w", scratch.Database, scratch.Table, err)
			}
			scans = append(scans, ownScan{Part: cp, Scanned: sum})
		}
	}
	return scans, missed, nil
}

// dropScratches drops the candidate scratch tables. A failed drop is logged
// and left to the startup drop of the node's stage tables.
func (v *Verifier) dropScratches(scratches []sources.Scratch) {
	for _, s := range scratches {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		err := v.d.Conn.Exec(ctx, "DROP TABLE IF EXISTS "+quoteIdent(s.Database)+"."+quoteIdent(s.Table)+" SYNC")
		cancel()
		if err != nil {
			v.d.Logger.Warn("shadow: dropping a scratch failed; the next startup drops it", "scratch", s.Database+"."+s.Table, "err", err)
		}
	}
}

// count adds one compared block to the owner's counters.
func (v *Verifier) count(owner uint64, final bool, missed uint64, found []mismatch) {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.stats.Blocks[owner]++
	v.stats.MissedScans[owner] += missed
	if !final {
		v.stats.Unverdicted[owner]++
	}
	for _, m := range found {
		v.stats.Mismatches[owner][m.Kind]++
	}
}

// nextSeq is the cursor as it stands (0 before the first one is known).
func (v *Verifier) nextSeq() uint64 {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.next
}

// logValue keeps a log line short: a receipt hash as it is, a 2048-byte row
// LtHash accumulator (4098 hex characters) as the digest of its hex form.
func logValue(h string) string {
	if len(h) <= 66 {
		return h
	}
	return "digest:" + replay.DigestString(h)
}

// cursor returns the next block, starting one after the safe watermark when
// there is no cursor yet; that first cursor is persisted at once, so a
// restart before the first block keeps it.
func (v *Verifier) cursor(ctx context.Context) (uint64, error) {
	next := v.nextSeq()
	if next != 0 {
		return next, nil
	}
	wm, err := v.d.Watermark.GetSafeWatermark(ctx)
	if err != nil {
		return 0, fmt.Errorf("shadow: read the safe watermark: %w", err)
	}
	next = wm.SafeBlockSeq + 1
	if err := storeCursor(v.cfg.StateDir, next); err != nil {
		return 0, err
	}
	v.mu.Lock()
	v.next = next
	v.mu.Unlock()
	v.d.Logger.Info("shadow: starting after the safe watermark", "safe_block_seq", wm.SafeBlockSeq, "next_block_seq", next)
	return next, nil
}

// advance moves the cursor past seq: in memory first, so a block is never
// counted twice in one process, then on disk.
func (v *Verifier) advance(seq uint64) error {
	v.mu.Lock()
	v.next = seq + 1
	v.mu.Unlock()
	return storeCursor(v.cfg.StateDir, seq+1)
}

// sleep waits d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// quoteIdent backquotes a ClickHouse identifier (dataplane/ddl's rule).
func quoteIdent(id string) string {
	id = strings.ReplaceAll(id, `\`, `\\`)
	id = strings.ReplaceAll(id, "`", "``")
	return "`" + id + "`"
}
