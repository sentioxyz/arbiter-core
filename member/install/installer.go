// Package install is the si-member's install gate executor (housegate spec
// 2026-10-14 §8.5-§8.7, Plan S3-C2 contract §22.2-§22.3): it brings every
// ready copy of every owner in the member's scope to a validated published
// manifest, only with parts that manifest lists, through Plan S3-C1's pure
// member/gate; it journals what each copy holds, audits the copies at
// startup, after every ClickHouse restart and on a slow self-audit cadence,
// and pauses installs above the member disk budget.
//
// A copy only grows by ATTACH PARTITION ... FROM a scratch table, and a
// Diverged partition is rebuilt with DROP PARTITION followed by ATTACH
// PARTITION ... FROM: the replacing form of the partition statement is never
// issued on a copy, because a ClickHouse reload inside the outdated-part
// window brings the replaced parts back (spec §2, measured).
//
// An owner's copies move together: one Apply verifies every ready copy of the
// owner against the target manifest before it changes any of them, and the
// journal records the new parts of every copy and the owner's new entry in
// one write. A copy therefore always holds exactly the parts of the owner's
// journaled manifest, or parts the journal does not list yet, which the next
// pass adopts or repairs (Plan S3-C2 K36).
package install

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/copyset"
	"github.com/sentioxyz/arbiter-core/member/gate"
	"github.com/sentioxyz/arbiter-core/member/manifestfeed"
	"github.com/sentioxyz/arbiter-core/member/sources"
)

const (
	// DefaultStopInstallsRatio is the share of DiskBudgetBytes at or above
	// which installs pause (Plan S3-C2 K35; devnet2 configures 0.5, D28).
	DefaultStopInstallsRatio = 0.8
	// DefaultSelfAuditInterval is the self-audit cadence the role ticks at.
	DefaultSelfAuditInterval = 6 * time.Hour
	// MinSelfAuditInterval is the shortest SelfAuditInterval accepted: a
	// self-audit scans every copy in full.
	MinSelfAuditInterval = 10 * time.Minute
	// DefaultDeferBackoffMin and DefaultDeferBackoffMax bound the back-off of
	// a part no source delivered (K35): 1m, doubling, at most 30m.
	DefaultDeferBackoffMin = time.Minute
	DefaultDeferBackoffMax = 30 * time.Minute
	// DefaultFetchTimeout bounds one source's Fetch of one part, the restore's
	// RESTORE and copy INSERT included. A fetch still running at the deadline
	// is refused and the next source is tried, so a hung source cannot hold
	// the install pass, and with it the role's audits, for longer.
	DefaultFetchTimeout = 20 * time.Minute
)

// Audit triggers, the metric label trigger.
const (
	TriggerStartup           = "startup"
	TriggerClickHouseRestart = "clickhouse_restart"
	TriggerSelfAudit         = "self_audit"
)

// restoreSource is the object-store restore's source name (contract §22.1,
// Task 31's sources.SourceRestore). Its scratch is classified in restore mode:
// the needed parts are kept, every other part is dropped, nothing is Foreign (a
// backup may be ahead of publication).
const restoreSource = sources.SourceRestore

// sourceLabels are the P2 sources, exported with zeros (contract §22.1).
var sourceLabels = []string{sources.SourceOwnerPull, sources.SourcePeerUnsafePull, restoreSource}

var triggers = []string{TriggerStartup, TriggerClickHouseRestart, TriggerSelfAudit}

var (
	nodeIDPattern    = regexp.MustCompile(`^[a-z][a-z0-9-]{0,30}[a-z0-9]$`)
	bootNoncePattern = regexp.MustCompile(`^[0-9a-f]{8}$`)
)

// Config is one si-member's installer.
type Config struct {
	Pinned            ddl.MemberPinned
	NetworkID         string
	StateDir          string
	BootNonce         string
	DiskBudgetBytes   uint64
	StopInstallsRatio float64
	SelfAuditInterval time.Duration
	// FetchTimeout is the deadline of one source's Fetch of one part; 0
	// selects DefaultFetchTimeout.
	FetchTimeout time.Duration
}

// CopySet is the copy reconciler's read side (*copyset.Reconciler).
type CopySet interface {
	Copies() []copyset.Copy
	Copy(tableKey string) (copyset.Copy, bool)
}

// Deps are the installer's collaborators.
type Deps struct {
	Conn   clickhouse.Conn
	Copies CopySet
	// Sources returns, in the order they are tried, the byte sources of one
	// owner: in P2 the owner pull, the peer unsafe pulls, the restore.
	Sources func(owner uint64) []sources.Source
	// BeforeInstall is called once per database and pass, before the first
	// statement that changes a copy of that database; an error stops the
	// owner's pass. Nil in P2 (Plan S3-C4's serving-lease seam).
	BeforeInstall func(ctx context.Context, owner uint64, database string) error
	// AfterAttach is called after every ATTACH PARTITION ... FROM, before the
	// journal records it; an error stops the pass. A fault-injection seam
	// for tests, nil in production.
	AfterAttach func(tableKey, partitionID string) error
	Logger      *slog.Logger
	Now         func() time.Time
}

// Outcome is the metric label outcome: the scratch classifications of
// member/gate and the fetch outcomes of member/sources.
type Outcome string

const (
	OutcomeKept        Outcome = "kept"
	OutcomeRenameRace  Outcome = "rename_race"
	OutcomeSplit       Outcome = "split"
	OutcomeDoubled     Outcome = "doubled"
	OutcomeForeign     Outcome = "foreign"
	OutcomeMissed      Outcome = "missed"
	OutcomeRefused     Outcome = "refused"
	OutcomeUnreachable Outcome = "unreachable"
)

var outcomes = []Outcome{OutcomeKept, OutcomeRenameRace, OutcomeSplit, OutcomeDoubled, OutcomeForeign, OutcomeMissed, OutcomeRefused, OutcomeUnreachable}

// TableState is one ready copy as the journal sees it. Entry and SnapshotID
// are the owner's journal entry once the copy holds exactly that manifest's
// parts, and empty before its first install. Diverged is true while a
// partition awaits a rebuild, the copy holds parts the journal does not list
// yet, or the copy was not audited since the start or the last ClickHouse
// restart; Foreign while the table is quarantined.
type TableState struct {
	Owner             uint64
	TableKey          string
	IncarnationSeq    uint64
	Copy, SchemaHash  string
	Entry             replay.TrackFrontier
	SnapshotID        string
	Diverged, Foreign bool
}

// Stats are the installer's counters and gauges. Every outcome, P2 source,
// owner with copies or a journal, and trigger is present, zeros included.
type Stats struct {
	Outcomes                       map[Outcome]uint64
	Installs                       map[string]uint64
	DivergedPartitions             map[uint64]uint64
	AuditRuns                      map[string]uint64
	AuditDroppedTwins              uint64
	AuditClean, InstallsPaused     bool
	DiskUsedBytes, DiskBudgetBytes uint64
}

// The refusals of contract §22.3. Each error the installer returns for one of
// them wraps it (errors.Is) and carries the contract's text.
var (
	ErrClosure           = errors.New("install: partition does not close")
	ErrPartitionSet      = errors.New("install: local partition set differs from the manifest's")
	ErrNoSourceSucceeded = errors.New("install: no source delivered the part")
	ErrForeign           = errors.New("install: foreign part; table quarantined locally")
	ErrInstallsPaused    = errors.New("install: installs paused")
)

type refusal struct {
	msg  string
	kind error
}

func (r *refusal) Error() string { return r.msg }
func (r *refusal) Unwrap() error { return r.kind }

func refuse(kind error, format string, args ...any) error {
	return &refusal{msg: fmt.Sprintf(format, args...), kind: kind}
}

type deferral struct {
	until   time.Time
	backoff time.Duration
	results []string
}

// Installer is the install gate executor of one si-member.
type Installer struct {
	cfg      Config
	d        Deps
	path     string
	zeroRoot string

	// work serializes Apply, Audit and DropStaleScratch: one statement
	// stream changes the copies at a time.
	work     sync.Mutex
	scratchN atomic.Uint64
	// beforeVerify runs between the fetches and the closure check of a pass;
	// tests use it to change a scratch behind the installer's back.
	beforeVerify func(ctx context.Context, scratches []sources.Scratch)

	// mu guards everything below; it is never held across a ClickHouse call.
	mu            sync.Mutex
	journal       *Journal
	pending       map[string]int // "<owner>/<table key>" -> unjournaled local parts awaiting adoption
	deferred      map[string]*deferral
	counts        map[Outcome]uint64
	installs      map[string]uint64
	diverged      map[uint64]uint64
	auditRuns     map[string]uint64
	droppedTwins  uint64
	auditDone     bool
	auditTrigger  string
	lastSelfAudit time.Time
	paused        bool
	diskUsed      uint64
	// audited holds the ready copies (auditKey) that the audit since the
	// start or the last ClickHouse restart covered; a ready copy outside it
	// is unaudited until Apply audits it (Plan S3-C2 review fix 245-1).
	audited map[string]bool
	// probed holds the copies (auditKey) whose bootstrap restore probe ran in
	// this process (Plan S3-C2 review fix 245-4).
	probed map[string]bool
}

// New validates cfg and reads the journal from cfg.StateDir, which must
// exist. It issues no ClickHouse statement.
func New(cfg Config, d Deps) (*Installer, error) {
	if !nodeIDPattern.MatchString(cfg.Pinned.NodeID) {
		return nil, fmt.Errorf("install: node id %q must match %s", cfg.Pinned.NodeID, nodeIDPattern)
	}
	if cfg.Pinned.ReplicaDB == "" || cfg.Pinned.StageDB == "" || cfg.Pinned.ReplicaDB == cfg.Pinned.StageDB {
		return nil, errors.New("install: Pinned needs distinct ReplicaDB and StageDB")
	}
	if !bootNoncePattern.MatchString(cfg.BootNonce) {
		return nil, fmt.Errorf("install: boot nonce %q must be 8 lowercase hex characters (NewBootNonce)", cfg.BootNonce)
	}
	if cfg.NetworkID == "" || cfg.StateDir == "" {
		return nil, errors.New("install: NetworkID and StateDir are required")
	}
	if cfg.DiskBudgetBytes == 0 {
		return nil, errors.New("install: disk budget is required")
	}
	if cfg.StopInstallsRatio == 0 {
		cfg.StopInstallsRatio = DefaultStopInstallsRatio
	}
	if !(cfg.StopInstallsRatio > 0 && cfg.StopInstallsRatio < 1) {
		return nil, fmt.Errorf("install: stop_installs_ratio must satisfy 0 < ratio < 1, got %v", cfg.StopInstallsRatio)
	}
	if cfg.SelfAuditInterval == 0 {
		cfg.SelfAuditInterval = DefaultSelfAuditInterval
	}
	if cfg.SelfAuditInterval < MinSelfAuditInterval {
		return nil, fmt.Errorf("install: self_audit_interval must be at least 10m, got %s", cfg.SelfAuditInterval)
	}
	if cfg.FetchTimeout == 0 {
		cfg.FetchTimeout = DefaultFetchTimeout
	}
	if cfg.FetchTimeout < 0 {
		return nil, fmt.Errorf("install: fetch_timeout must be positive, got %s", cfg.FetchTimeout)
	}
	if d.Conn == nil || d.Copies == nil || d.Sources == nil {
		return nil, errors.New("install: Deps needs Conn, Copies and Sources")
	}
	if d.Logger == nil {
		d.Logger = slog.Default()
	}
	if d.Now == nil {
		d.Now = time.Now
	}
	if st, err := os.Stat(cfg.StateDir); err != nil || !st.IsDir() {
		return nil, fmt.Errorf("install: state dir %s is not a directory (%v)", cfg.StateDir, err)
	}
	zeroRoot, err := payloadexec.SumPartRowLtHashes(nil)
	if err != nil {
		return nil, fmt.Errorf("install: the empty partition root: %w", err)
	}
	path := filepath.Join(cfg.StateDir, JournalFile)
	j, err := readJournal(path)
	if err != nil {
		return nil, err
	}
	if j == nil {
		j = newJournal(cfg.Pinned.NodeID)
	}
	if j.NodeID != cfg.Pinned.NodeID {
		return nil, fmt.Errorf("install: journal %s belongs to node %q, not %q", path, j.NodeID, cfg.Pinned.NodeID)
	}
	return &Installer{
		cfg: cfg, d: d, path: path, zeroRoot: zeroRoot, journal: j,
		pending: map[string]int{}, deferred: map[string]*deferral{},
		counts: map[Outcome]uint64{}, installs: map[string]uint64{}, diverged: map[uint64]uint64{}, auditRuns: map[string]uint64{},
		auditTrigger: TriggerStartup, audited: map[string]bool{}, probed: map[string]bool{},
	}, nil
}

// Apply brings every ready copy of every owner whose journal is not at
// u.SnapshotID to u.Manifest and then sets that owner's journal entry to
// (u.SnapshotID, u.Entries[owner]). The owners are those of the ready copies and
// those the journal already holds. Apply is idempotent: an owner whose journal
// is at u.SnapshotID and whose ready copies are all journaled, clean and
// quarantine-free is skipped, so the role may offer the same update again
// whenever the copy set changes or NeedsPass holds. Apply never takes an owner
// back: an owner whose journaled entry is ahead of its entry in u keeps its
// copies and its journal (ownerAhead, Plan S3-C2 review fix 245-5), because
// each owner's pass commits on its own and an earlier pass may have brought it
// past an update that is offered again. Before its first pass after
// the start or a ClickHouse restart it runs the audit the copies are waiting
// for, and before every pass it audits each ready copy that audit did not cover
// (a copy the copyset made ready afterwards, Plan S3-C2 review fix 245-1), so no
// copy is skipped as current without having been compared with its journal. The
// error joins every owner's failure; the caller offers the update again later.
func (i *Installer) Apply(ctx context.Context, u manifestfeed.Update) error {
	i.work.Lock()
	defer i.work.Unlock()
	if u.SnapshotID == "" {
		return errors.New("install: an update needs a snapshot id")
	}
	if done, trigger := i.auditState(); !done {
		if err := i.audit(ctx, trigger); err != nil {
			return fmt.Errorf("install: the %s audit must pass before installs: %w", trigger, err)
		}
	}
	ready := i.readyCopies()
	if err := i.auditUncovered(ctx, ready); err != nil {
		return fmt.Errorf("install: audit the copies the last audit did not cover: %w", err)
	}
	if err := i.checkDiskBudget(ctx); err != nil {
		return err
	}
	byOwner := map[uint64][]copyset.Copy{}
	for _, c := range ready {
		byOwner[c.Owner] = append(byOwner[c.Owner], c)
	}
	tables := make(map[string]*replay.TableManifest, len(u.Manifest.Tables))
	for k := range u.Manifest.Tables {
		tables[u.Manifest.Tables[k].TableID] = &u.Manifest.Tables[k]
	}
	var errs []error
	for _, owner := range i.owners(byOwner) {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := i.applyOwner(ctx, u, owner, byOwner[owner], tables); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// owners is every owner with a ready copy or a journal, ascending.
func (i *Installer) owners(byOwner map[uint64][]copyset.Copy) []uint64 {
	i.mu.Lock()
	defer i.mu.Unlock()
	set := map[uint64]bool{}
	for owner := range byOwner {
		set[owner] = true
	}
	for key := range i.journal.Owners {
		if owner, err := strconv.ParseUint(key, 10, 64); err == nil {
			set[owner] = true
		}
	}
	return slices.Sorted(maps.Keys(set))
}

// keptPart is one scratch part the gate kept.
type keptPart struct {
	scratch   sources.Scratch
	name      string // its name in the scratch
	partition string // its logical partition as ClickHouse placed it
	rowHash   string
	physHash  string
	rows      uint64
	source    string
}

// partitionJob is one partition of one copy that a pass changes.
type partitionJob struct {
	id        string
	root      string
	local     []string // L of spec §8.5; nil for a repair
	repair    bool
	needs     []replay.PartManifestEntry
	kept      []keptPart
	scratches []sources.Scratch
}

// tablePass is one copy's part of an owner's pass toward one manifest.
type tablePass struct {
	copy               copyset.Copy
	journal            *TableJournal  // the working journal: adoptions applied
	local              []chparts.Part // the copy's non-empty active parts before any change
	jobs               []*partitionJob
	manifestPartitions []string
}

func (p *tablePass) scratches() []sources.Scratch {
	var out []sources.Scratch
	for _, job := range p.jobs {
		out = append(out, job.scratches...)
	}
	return out
}

// applyOwner is one owner's pass: prepare every copy (reconcile its journal,
// plan, fetch and classify), verify them all, then change them all and write
// the journal once. Any failure before the first change leaves every copy of
// the owner as it was.
func (i *Installer) applyOwner(ctx context.Context, u manifestfeed.Update, owner uint64, copies []copyset.Copy, tables map[string]*replay.TableManifest) error {
	entry, ok := u.Entries[owner]
	if !ok {
		return fmt.Errorf("install: manifest %s has no track frontier for owner %d", u.SnapshotID, owner)
	}
	if i.ownerAhead(owner, u.SnapshotID, entry) {
		return nil
	}
	if i.ownerCurrent(owner, u.SnapshotID, copies) {
		return nil
	}
	for _, key := range i.foreignTables(owner) {
		if slices.ContainsFunc(copies, func(c copyset.Copy) bool { return c.TableKey == key }) {
			scratch := i.foreignScratchOf(owner, key)
			return refuse(ErrForeign, "install: %s of owner %d is quarantined locally (foreign scratch %s.%s); clear it offline with sentio-node si-member clear-foreign --table %s", key, owner, i.cfg.Pinned.StageDB, scratch, key)
		}
	}
	gates := &installGates{before: i.d.BeforeInstall, owner: owner, seen: map[string]bool{}}
	var passes []*tablePass
	keepForeign := sources.Scratch{}
	defer func() {
		for _, p := range passes {
			for _, s := range p.scratches() {
				if s != keepForeign {
					i.dropScratch(s)
				}
			}
		}
	}()
	for _, c := range copies {
		if err := ctx.Err(); err != nil {
			return err
		}
		pass, foreign, err := i.prepareTable(ctx, u.SnapshotID, gates, owner, c, tables[c.TableKey])
		if pass != nil {
			passes = append(passes, pass)
		}
		if foreign != (sources.Scratch{}) {
			keepForeign = foreign
		}
		if err != nil {
			return err
		}
	}
	if i.beforeVerify != nil {
		var all []sources.Scratch
		for _, p := range passes {
			all = append(all, p.scratches()...)
		}
		i.beforeVerify(ctx, all)
	}
	for _, p := range passes {
		if err := i.verify(ctx, u.SnapshotID, p); err != nil {
			return err
		}
	}
	for k, p := range passes {
		if err := i.commit(ctx, gates, p); err != nil {
			i.markUnjournaled(owner, passes[:k+1])
			return err
		}
	}
	return i.journalOwner(ctx, u, owner, entry, passes)
}

// ownerAhead reports whether owner's journaled entry is ahead of entry, the
// owner's entry in manifest snapshotID: a pass toward that manifest would take
// the owner's copies and journal back (Plan S3-C2 review fix 245-5). Each
// owner's pass commits and journals on its own, so when a pass fails for one
// owner the others may already be at a manifest newer than an update offered
// again; rebuilding their partitions to the older one would drop valid newer
// parts. Ahead is manifestfeed's rule for an entry that goes back: either
// component of the journaled entry is above the update's. Such an owner is
// skipped before anything is read, marked or dropped: no Diverged mark, no DROP
// PARTITION, no journal write. An equal entry is not ahead: the owner's tables
// change only with its entry, so a pass between two manifests of one entry
// changes no part.
func (i *Installer) ownerAhead(owner uint64, snapshotID string, entry replay.TrackFrontier) bool {
	i.mu.Lock()
	oj := i.journal.Owners[ownerKey(owner)]
	if oj == nil || oj.SnapshotID == "" || oj.SnapshotID == snapshotID ||
		(entry.SafeBlockSeq >= oj.Entry.SafeBlockSeq && entry.AppliedPromotionSeq >= oj.Entry.AppliedPromotionSeq) {
		i.mu.Unlock()
		return false
	}
	journaled, at := oj.Entry, oj.SnapshotID
	i.mu.Unlock()
	i.d.Logger.Warn("install: the update is behind the owner's journal; the owner keeps its copies",
		"owner", owner, "snapshot", snapshotID, "entry", fmt.Sprintf("(%d, %d)", entry.SafeBlockSeq, entry.AppliedPromotionSeq),
		"journal_snapshot", at, "journal_entry", fmt.Sprintf("(%d, %d)", journaled.SafeBlockSeq, journaled.AppliedPromotionSeq))
	return true
}

// ownerCurrent reports whether Apply has nothing to do for owner at
// snapshotID.
func (i *Installer) ownerCurrent(owner uint64, snapshotID string, copies []copyset.Copy) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	oj := i.journal.Owners[ownerKey(owner)]
	if oj == nil || oj.SnapshotID != snapshotID {
		return false
	}
	for _, c := range copies {
		tj := oj.Tables[c.TableKey]
		if tj == nil || tj.IncarnationSeq != c.IncarnationSeq || tj.Copy != c.Table || tj.Foreign || len(tj.Diverged) > 0 || i.pending[pendingKey(owner, c.TableKey)] > 0 {
			return false
		}
	}
	return true
}

// prepareTable reconciles copy c with its journal, adopts the unjournaled
// parts manifest table tm still needs (K36), plans every partition and
// fetches every needed part. tm nil means the manifest does not list the
// table, so the copy must be empty. It changes the copy only to drop
// equal-hash twins; it persists only new Diverged marks and a quarantine.
// A Foreign scratch is returned so the caller keeps it.
func (i *Installer) prepareTable(ctx context.Context, snapshotID string, gates *installGates, owner uint64, c copyset.Copy, tm *replay.TableManifest) (*tablePass, sources.Scratch, error) {
	key := c.TableKey
	if tm != nil {
		if want := payloadexec.TableSchemaHash(i.cfg.NetworkID, c.Schema); tm.SchemaHash != want {
			return nil, sources.Scratch{}, fmt.Errorf("install: manifest %s lists %s with schema hash %s, but copy %s was created from schema hash %s", snapshotID, key, tm.SchemaHash, c.Table, want)
		}
	}
	tj := i.tableJournal(owner, c)
	local, err := i.listCopy(ctx, c)
	if err != nil {
		return nil, sources.Scratch{}, err
	}
	rec, err := i.reconcileCopy(ctx, gates, owner, c, tj, local)
	if err != nil {
		return nil, sources.Scratch{}, err
	}
	manifestParts, roots := manifestPartitions(tm)
	marks := rec.diverged
	for _, cand := range rec.candidates {
		if tj.isDiverged(cand.PartitionID) {
			continue
		}
		need := multisetMinus(entryHashes(manifestParts[cand.PartitionID]), tj.partitionHashes(cand.PartitionID))
		if slices.Contains(need, cand.PartRowLtHash) {
			tj.addParts(cand)
			i.d.Logger.Info("install: adopted a part the journal did not list", "table", key, "copy", c.Table, "partition", cand.PartitionID, "part", cand.Name, "snapshot", snapshotID)
			continue
		}
		if tj.markDiverged(cand.PartitionID) {
			marks = append(marks, cand.PartitionID)
			i.d.Logger.Warn("install: the copy holds a part the manifest does not need; the partition is Diverged", "table", key, "copy", c.Table, "partition", cand.PartitionID, "part", cand.Name, "snapshot", snapshotID)
		}
	}
	pass := &tablePass{copy: c, journal: tj, local: rec.local}
	planMarks, err := i.plan(snapshotID, pass, manifestParts, roots)
	marks = append(marks, planMarks...)
	if perr := i.persistDiverged(owner, c, marks); perr != nil {
		return nil, sources.Scratch{}, errors.Join(err, perr)
	}
	if err != nil {
		return nil, sources.Scratch{}, err
	}
	for _, job := range pass.jobs {
		remaining := entryHashes(job.needs)
		for _, need := range job.needs {
			if !slices.Contains(remaining, need.PartRowLtHash) {
				continue // an earlier restore scratch kept it already
			}
			scratch, kept, foreign, err := i.fetchPart(ctx, owner, c, need, remaining)
			if foreign {
				job.scratches = append(job.scratches, scratch)
				return pass, scratch, i.quarantine(owner, c, snapshotID, scratch)
			}
			if err != nil {
				return pass, sources.Scratch{}, err
			}
			job.scratches = append(job.scratches, scratch)
			job.kept = append(job.kept, kept...)
			for _, k := range kept {
				remaining = removeOne(remaining, k.rowHash)
			}
		}
	}
	return pass, sources.Scratch{}, nil
}

// manifestPartitions indexes a manifest table: its parts and roots by
// partition id.
func manifestPartitions(tm *replay.TableManifest) (map[string][]replay.PartManifestEntry, map[string]string) {
	parts := map[string][]replay.PartManifestEntry{}
	roots := map[string]string{}
	if tm == nil {
		return parts, roots
	}
	for _, p := range tm.ActiveParts {
		parts[p.PartitionID] = append(parts[p.PartitionID], p)
	}
	for _, r := range tm.PartitionRoots {
		roots[r.PartitionID] = r.Root
	}
	return parts, roots
}

// plan runs gate.PlanPartition (spec §8.5 steps 1-3) over every partition the
// manifest, the journal or the copy knows and fills pass.jobs; a partition
// already Diverged, or Diverged now, is rebuilt from every part the manifest
// lists for it. It returns the partitions it marked Diverged.
func (i *Installer) plan(snapshotID string, pass *tablePass, manifestParts map[string][]replay.PartManifestEntry, roots map[string]string) ([]string, error) {
	c, tj := pass.copy, pass.journal
	ids := map[string]bool{}
	for id := range manifestParts {
		ids[id] = true
	}
	pass.manifestPartitions = slices.Sorted(maps.Keys(ids))
	for _, id := range tj.partitions() {
		ids[id] = true
	}
	for _, id := range tj.Diverged {
		ids[id] = true
	}
	for _, p := range pass.local {
		ids[chparts.LogicalPartitionID(c.Schema, p)] = true
	}
	var marks []string
	for _, id := range slices.Sorted(maps.Keys(ids)) {
		entries := manifestParts[id]
		root, ok := roots[id]
		if !ok {
			root = i.zeroRoot
		}
		local := tj.partitionHashes(id)
		plan, err := gate.PlanPartition(entryHashes(entries), root, local)
		if err != nil {
			return marks, fmt.Errorf("install: manifest %s, %s partition %s: %w", snapshotID, c.TableKey, id, err)
		}
		repair := tj.isDiverged(id)
		if plan.Diverged && !repair {
			repair = true
			tj.markDiverged(id)
			marks = append(marks, id)
			i.d.Logger.Warn("install: the journal lists parts the manifest does not; the partition is Diverged", "table", c.TableKey, "copy", c.Table, "partition", id, "snapshot", snapshotID)
		}
		switch {
		case repair:
			pass.jobs = append(pass.jobs, &partitionJob{id: id, root: root, repair: true, needs: slices.Clone(entries)})
		case len(plan.Need) > 0:
			pass.jobs = append(pass.jobs, &partitionJob{id: id, root: root, local: local, needs: pickEntries(entries, plan.Need)})
		}
	}
	return marks, nil
}

// fetchPart fetches one needed part into fresh scratches, trying the owner's
// sources in order (spec §8.5 steps 3-4, §8.6): a rename race or a doubled
// pull is retried once from a clean scratch; a split, or a missed, refused or
// unreachable fetch, moves to the next source; a part no source delivered is
// deferred with a back-off (K35). It returns the scratch of the kept parts,
// or, with foreign true, the scratch that held a foreign part.
func (i *Installer) fetchPart(ctx context.Context, owner uint64, c copyset.Copy, need replay.PartManifestEntry, remaining []string) (sources.Scratch, []keptPart, bool, error) {
	dkey := deferKey(owner, c.TableKey, need)
	if d := i.deferralOf(dkey); d != nil && i.d.Now().Before(d.until) {
		return sources.Scratch{}, nil, false, refuse(ErrNoSourceSucceeded, "install: no source delivered part %s of %s (%v); deferred until %s", need.PartName, c.TableKey, d.results, d.until.UTC().Format(time.RFC3339))
	}
	sn := sources.Need{
		TableKey: c.TableKey, Table: ddl.CHTableName(c.TableKey), PartitionID: need.PartitionID,
		PartName: need.PartName, PartPhysHash: need.PartPhysHash, PartRowLtHash: need.PartRowLtHash, RowCount: need.RowCount,
	}
	var results []string
	for _, src := range i.sourcesFor(owner, c) {
		for attempt := 0; attempt < 2; attempt++ {
			scratch, err := i.newScratch(ctx, c.Schema)
			if err != nil {
				return sources.Scratch{}, nil, false, err
			}
			res := i.fetchWithDeadline(ctx, src, sn, c.Schema, scratch)
			if err := ctx.Err(); err != nil {
				i.dropScratch(scratch)
				return sources.Scratch{}, nil, false, err
			}
			if res.Outcome != sources.OutcomeFetched || res.Err != nil {
				outcome := sourceOutcome(res)
				i.count(outcome)
				results = append(results, describe(src.Name(), string(outcome), res.Err))
				i.dropScratch(scratch)
				break
			}
			cls, kept, err := i.classify(ctx, src.Name(), c, scratch, sn, remaining)
			if err != nil {
				i.dropScratch(scratch)
				return sources.Scratch{}, nil, false, err
			}
			if cls == gate.OutcomeForeign {
				i.count(OutcomeForeign)
				return scratch, nil, true, nil
			}
			if cls == gate.OutcomeKept && len(kept) > 0 {
				i.count(OutcomeKept)
				i.clearDeferral(dkey)
				return scratch, kept, false, nil
			}
			i.dropScratch(scratch)
			if cls == gate.OutcomeKept {
				// A restore whose backup lists none of the needed parts.
				i.count(OutcomeMissed)
				results = append(results, describe(src.Name(), "missed (the scratch held no needed part)", nil))
				break
			}
			i.count(Outcome(cls))
			results = append(results, describe(src.Name(), string(cls), nil))
			if (cls == gate.OutcomeRenameRace || cls == gate.OutcomeDoubled) && attempt == 0 {
				i.d.Logger.Info("install: retrying a fetch once from a clean scratch", "table", c.TableKey, "part", need.PartName, "source", src.Name(), "outcome", cls)
				continue
			}
			break
		}
	}
	d := i.deferAgain(dkey, results)
	i.d.Logger.Warn("install: no source delivered a part; deferred", "table", c.TableKey, "partition", need.PartitionID, "part", need.PartName, "results", results, "until", d.until)
	return sources.Scratch{}, nil, false, refuse(ErrNoSourceSucceeded, "install: no source delivered part %s of %s (%v); deferred until %s", need.PartName, c.TableKey, results, d.until.UTC().Format(time.RFC3339))
}

// fetchWithDeadline runs one Fetch under FetchTimeout. A fetch the deadline
// ends is refused whatever the source reported, since its scratch may hold a
// partial write; clickhouse-go cancels the statement at the deadline and also
// sends it as max_execution_time, so the server stops too. The caller drops
// the scratch and tries the next source.
func (i *Installer) fetchWithDeadline(ctx context.Context, src sources.Source, need sources.Need, schema payloadexec.TableSchema, scratch sources.Scratch) sources.Result {
	fctx, cancel := context.WithTimeout(ctx, i.cfg.FetchTimeout)
	defer cancel()
	res := src.Fetch(fctx, need, schema, scratch)
	if ctx.Err() == nil && errors.Is(fctx.Err(), context.DeadlineExceeded) {
		cause := res.Err
		if cause == nil {
			cause = context.DeadlineExceeded
		}
		res.Outcome, res.Parts = sources.OutcomeRefused, nil
		res.Err = fmt.Errorf("fetch exceeded its %s deadline: %w", i.cfg.FetchTimeout, cause)
		i.d.Logger.Warn("install: a fetch exceeded its deadline; trying the next source", "table", need.TableKey, "part", need.PartName, "source", src.Name(), "fetch_timeout", i.cfg.FetchTimeout)
	}
	return res
}

// sourcesFor is the order fetchPart tries owner's sources in for copy c: the P2
// order of Deps.Sources, except for the first fetch of each copy while its owner
// bootstraps (the journal holds no entry for it yet), which tries the restore
// right before the owner pull (Plan S3-C2 review fix 245-4; housegate spec
// 2026-10-14 §8.6: the P1 backups are the bootstrap source). The probe runs once
// per copy and process, also when its pass fails afterwards: it installs that
// part from the newest backup that lists it and proves the member's restore path
// (its key, READ ON S3, the bucket) on every bootstrap, at the cost of one
// partition restore per copy; the restore copies one needed part per RESTORE, so
// restoring every part would restore each partition once per part. A probe that
// misses, is refused or unreachable costs that attempt, and the part comes from
// the next source.
func (i *Installer) sourcesFor(owner uint64, c copyset.Copy) []sources.Source {
	list := i.d.Sources(owner)
	key := auditKey(c)
	i.mu.Lock()
	oj := i.journal.Owners[ownerKey(owner)]
	probe := (oj == nil || oj.SnapshotID == "") && !i.probed[key]
	if probe {
		i.probed[key] = true
	}
	i.mu.Unlock()
	if !probe {
		return list
	}
	return restoreBeforeOwnerPull(list)
}

// restoreBeforeOwnerPull moves the source named restore right before the
// source named owner_pull and keeps every other source in its place, so a
// source in front of the owner pull (Plan S3-E1's hard-link source) stays in
// front. A list without either, or with the restore already in front of the
// owner pull, is returned as it is.
func restoreBeforeOwnerPull(list []sources.Source) []sources.Source {
	at := slices.IndexFunc(list, func(s sources.Source) bool { return s.Name() == sources.SourceOwnerPull })
	from := slices.IndexFunc(list, func(s sources.Source) bool { return s.Name() == restoreSource })
	if at < 0 || from < at {
		return list
	}
	out := make([]sources.Source, 0, len(list))
	out = append(out, list[:at]...)
	out = append(out, list[from])
	for k := at; k < len(list); k++ {
		if k != from {
			out = append(out, list[k])
		}
	}
	return out
}

// classify scans one fetched scratch and runs gate.Classify over it. For a
// kept classification it drops the unkept parts inside the scratch and checks
// that exactly the kept parts remain.
func (i *Installer) classify(ctx context.Context, source string, c copyset.Copy, scratch sources.Scratch, need sources.Need, remaining []string) (gate.Outcome, []keptPart, error) {
	scans, err := chexec.ScanAllParts(ctx, i.d.Conn, scratch.Database+"."+scratch.Table, c.Schema)
	if err != nil {
		return "", nil, fmt.Errorf("install: scan scratch %s.%s: %w", scratch.Database, scratch.Table, err)
	}
	parts := make([]gate.ScratchPart, 0, len(scans))
	for _, s := range scans {
		parts = append(parts, gate.ScratchPart{Name: s.PartName, RowLtHash: s.RowLtHash, Rows: s.RowCount})
	}
	fetch := gate.Fetch{Need: remaining}
	if source != restoreSource {
		fetch.Single = &gate.Expected{RowLtHash: need.PartRowLtHash, Rows: need.RowCount}
	}
	cls, err := gate.Classify(fetch, parts)
	if err != nil {
		return "", nil, fmt.Errorf("install: classify scratch %s.%s: %w", scratch.Database, scratch.Table, err)
	}
	if cls.Outcome != gate.OutcomeKept {
		return cls.Outcome, nil, nil
	}
	for _, name := range cls.Drop {
		stmt := "ALTER TABLE " + qualified(scratch.Database, scratch.Table) + " DROP PART " + quoteLiteral(name)
		if err := i.d.Conn.Exec(ctx, stmt); err != nil {
			return "", nil, fmt.Errorf("install: %s: %w", stmt, err)
		}
	}
	listed, err := nonEmptyParts(ctx, i.d.Conn, scratch.Database, scratch.Table)
	if err != nil {
		return "", nil, fmt.Errorf("install: list scratch %s.%s: %w", scratch.Database, scratch.Table, err)
	}
	if !slices.Equal(partNames(listed), slices.Sorted(slices.Values(cls.Keep))) {
		return "", nil, fmt.Errorf("install: scratch %s.%s holds %v after the drops, want the kept parts %v", scratch.Database, scratch.Table, partNames(listed), cls.Keep)
	}
	byName := make(map[string]chexec.PartScanResult, len(scans))
	for _, s := range scans {
		byName[s.PartName] = s
	}
	kept := make([]keptPart, 0, len(listed))
	for _, p := range listed {
		s := byName[p.Name]
		kept = append(kept, keptPart{
			scratch: scratch, name: p.Name, partition: chparts.LogicalPartitionID(c.Schema, p),
			rowHash: s.RowLtHash, physHash: p.PhysHash, rows: s.RowCount, source: source,
		})
	}
	return gate.OutcomeKept, kept, nil
}

// verify is spec §8.5 step 6 for one copy, before any statement changes it:
// per partition Σ(L) + Σ(kept) = R over what the scratches hold now, and the
// copy's partition set after the pass equal to the manifest's. A scratch that
// no longer lists exactly its kept parts is scanned again, so the closure is
// computed over its real content.
func (i *Installer) verify(ctx context.Context, snapshotID string, p *tablePass) error {
	c := p.copy
	repaired := map[string]bool{}
	for _, job := range p.jobs {
		if job.repair {
			repaired[job.id] = true
		}
	}
	after := map[string]bool{}
	for _, lp := range p.local {
		if id := chparts.LogicalPartitionID(c.Schema, lp); !repaired[id] {
			after[id] = true
		}
	}
	for _, job := range p.jobs {
		var held []string
		for _, s := range job.scratches {
			hashes, placed, err := i.scratchContent(ctx, c, s, job.kept)
			if err != nil {
				return err
			}
			held = append(held, hashes...)
			for _, id := range placed {
				after[id] = true
				if id != job.id {
					local := slices.Sorted(maps.Keys(after))
					return refuse(ErrPartitionSet, "install: %s has partitions %v, manifest %s lists %v", c.TableKey, local, snapshotID, p.manifestPartitions)
				}
			}
		}
		if err := gate.Closure(job.root, job.local, held); err != nil {
			localSum, _ := payloadexec.SumPartRowLtHashes(job.local)
			keptSum, _ := payloadexec.SumPartRowLtHashes(held)
			return refuse(ErrClosure, "install: partition %s of %s does not close: local %s + kept %s != root %s", job.id, c.TableKey, abbreviate(localSum), abbreviate(keptSum), abbreviate(job.root))
		}
	}
	local := slices.Sorted(maps.Keys(after))
	if err := gate.PartitionSet(p.manifestPartitions, local); err != nil {
		return refuse(ErrPartitionSet, "install: %s has partitions %v, manifest %s lists %v", c.TableKey, local, snapshotID, p.manifestPartitions)
	}
	return nil
}

// scratchContent is what scratch s holds now: the row hashes and logical
// partitions of its non-empty parts. When the listing is exactly the kept
// parts of s (name, hash_of_all_files, rows), their classified row hashes
// stand; otherwise s is scanned again.
func (i *Installer) scratchContent(ctx context.Context, c copyset.Copy, s sources.Scratch, kept []keptPart) ([]string, []string, error) {
	listed, err := nonEmptyParts(ctx, i.d.Conn, s.Database, s.Table)
	if err != nil {
		return nil, nil, fmt.Errorf("install: list scratch %s.%s: %w", s.Database, s.Table, err)
	}
	var mine []keptPart
	for _, k := range kept {
		if k.scratch == s {
			mine = append(mine, k)
		}
	}
	placed := make([]string, 0, len(listed))
	for _, p := range listed {
		placed = append(placed, chparts.LogicalPartitionID(c.Schema, p))
	}
	if sameParts(listed, mine) {
		hashes := make([]string, 0, len(mine))
		for _, k := range mine {
			hashes = append(hashes, k.rowHash)
		}
		return hashes, placed, nil
	}
	i.d.Logger.Error("install: a scratch changed after its classification", "scratch", s.Database+"."+s.Table, "parts", partNames(listed))
	scans, err := chexec.ScanAllParts(ctx, i.d.Conn, s.Database+"."+s.Table, c.Schema)
	if err != nil {
		return nil, nil, fmt.Errorf("install: scan scratch %s.%s: %w", s.Database, s.Table, err)
	}
	hashes := make([]string, 0, len(scans))
	for _, sc := range scans {
		hashes = append(hashes, sc.RowLtHash)
	}
	return hashes, placed, nil
}

func sameParts(listed []chparts.Part, kept []keptPart) bool {
	if len(listed) != len(kept) {
		return false
	}
	for _, p := range listed {
		if !slices.ContainsFunc(kept, func(k keptPart) bool { return k.name == p.Name && k.physHash == p.PhysHash && k.rows == p.Rows }) {
			return false
		}
	}
	return true
}

// commit changes one copy (spec §8.5 steps 2 and 5): per partition, a repair
// first drops the partition, then every scratch of the partition is attached
// with ATTACH PARTITION ... FROM, which appends and keeps hash_of_all_files.
func (i *Installer) commit(ctx context.Context, gates *installGates, p *tablePass) error {
	c := p.copy
	if len(p.jobs) == 0 {
		return nil
	}
	if err := gates.check(ctx, databaseOf(c.TableKey)); err != nil {
		return err
	}
	for _, job := range p.jobs {
		quoted, err := chparts.QuotePartition(c.Schema, job.id)
		if err != nil {
			return fmt.Errorf("install: %s partition %s: %w", c.TableKey, job.id, err)
		}
		if job.repair && len(chparts.PartsInLogicalPartition(p.local, c.Schema, job.id)) > 0 {
			stmt := "ALTER TABLE " + qualified(i.cfg.Pinned.ReplicaDB, c.Table) + " DROP PARTITION " + quoted
			if err := i.d.Conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("install: %s: %w", stmt, err)
			}
			i.d.Logger.Warn("install: dropped a Diverged partition to rebuild it", "table", c.TableKey, "copy", c.Table, "partition", job.id)
		}
		for _, s := range job.scratches {
			stmt := "ALTER TABLE " + qualified(i.cfg.Pinned.ReplicaDB, c.Table) + " ATTACH PARTITION " + quoted + " FROM " + qualified(s.Database, s.Table)
			if err := i.d.Conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("install: %s: %w", stmt, err)
			}
			if i.d.AfterAttach != nil {
				if err := i.d.AfterAttach(c.TableKey, job.id); err != nil {
					return fmt.Errorf("install: after attaching partition %s of %s: %w", job.id, c.TableKey, err)
				}
			}
		}
	}
	return nil
}

// journalOwner records what the pass attached, by the parts' names in the
// copies, matched to the kept scratch parts by hash_of_all_files and rows
// (ATTACH PARTITION ... FROM keeps the first, spec §2), and writes every
// copy's journal and the owner's new entry in one write. A partition whose
// new parts do not match is marked Diverged and the entry stays.
func (i *Installer) journalOwner(ctx context.Context, u manifestfeed.Update, owner uint64, entry replay.TrackFrontier, passes []*tablePass) error {
	var errs []error
	var marks []func(j *Journal)
	for _, p := range passes {
		c := p.copy
		if len(p.jobs) == 0 {
			continue
		}
		after, err := i.listCopy(ctx, c)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		before := map[string]bool{}
		for _, lp := range p.local {
			before[lp.Name] = true
		}
		for _, job := range p.jobs {
			var fresh []chparts.Part
			for _, lp := range chparts.PartsInLogicalPartition(after, c.Schema, job.id) {
				if job.repair || !before[lp.Name] {
					fresh = append(fresh, lp)
				}
			}
			parts, ok := matchAttached(job, fresh)
			if !ok {
				id := job.id
				marks = append(marks, func(j *Journal) {
					if tj := j.owner(owner).Tables[c.TableKey]; tj != nil && tj.IncarnationSeq == c.IncarnationSeq {
						tj.markDiverged(id)
					}
				})
				i.countDiverged(owner)
				i.d.Logger.Error("install: the attached parts do not match the kept scratch parts; the partition is Diverged", "table", c.TableKey, "copy", c.Table, "partition", job.id)
				errs = append(errs, fmt.Errorf("install: ATTACH into %s partition %s gave parts %v, want %d with the kept parts' hash_of_all_files", c.Table, job.id, partNames(fresh), len(job.kept)))
				continue
			}
			if job.repair {
				p.journal.setPartition(job.id, parts)
			} else {
				p.journal.addParts(parts...)
			}
		}
	}
	if len(errs) > 0 {
		i.markUnjournaled(owner, passes)
		return errors.Join(append(errs, i.update(func(j *Journal) bool {
			for _, m := range marks {
				m(j)
			}
			return len(marks) > 0
		}))...)
	}
	if err := i.update(func(j *Journal) bool {
		oj := j.owner(owner)
		for _, p := range passes {
			oj.Tables[p.copy.TableKey] = p.journal.clone()
		}
		oj.SnapshotID, oj.Entry = u.SnapshotID, entry
		return true
	}); err != nil {
		i.markUnjournaled(owner, passes)
		return err
	}
	i.mu.Lock()
	for _, p := range passes {
		delete(i.pending, pendingKey(owner, p.copy.TableKey))
		for _, job := range p.jobs {
			for _, k := range job.kept {
				i.installs[k.source]++
			}
		}
	}
	i.mu.Unlock()
	for _, p := range passes {
		if len(p.jobs) > 0 {
			i.d.Logger.Info("install: copy brought to the manifest", "owner", owner, "table", p.copy.TableKey, "copy", p.copy.Table, "snapshot", u.SnapshotID, "partitions", len(p.jobs))
		}
	}
	return nil
}

// matchAttached pairs the parts an attach added with the kept parts by
// hash_of_all_files and rows, as a multiset.
func matchAttached(job *partitionJob, fresh []chparts.Part) ([]JournalPart, bool) {
	if len(fresh) != len(job.kept) {
		return nil, false
	}
	unmatched := slices.Clone(job.kept)
	out := make([]JournalPart, 0, len(fresh))
	for _, p := range fresh {
		k := slices.IndexFunc(unmatched, func(k keptPart) bool { return k.physHash == p.PhysHash && k.rows == p.Rows })
		if k < 0 {
			return nil, false
		}
		out = append(out, JournalPart{Name: p.Name, PartitionID: job.id, PartRowLtHash: unmatched[k].rowHash, HashOfAllFiles: p.PhysHash, Rows: p.Rows})
		unmatched = slices.Delete(unmatched, k, k+1)
	}
	return out, true
}

// markUnjournaled records that the copies of passes may hold attached parts
// the journal does not list, so their TableState is Diverged and the owner is
// not current until a pass adopts or repairs them.
func (i *Installer) markUnjournaled(owner uint64, passes []*tablePass) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for _, p := range passes {
		n := 0
		for _, job := range p.jobs {
			n += len(job.kept)
		}
		if n > 0 || len(p.jobs) > 0 {
			i.pending[pendingKey(owner, p.copy.TableKey)] += max(n, 1)
		}
	}
}

// quarantine marks the table Foreign and keeps scratch for forensics (K36).
func (i *Installer) quarantine(owner uint64, c copyset.Copy, snapshotID string, scratch sources.Scratch) error {
	i.d.Logger.Error("install: a scratch holds a part the manifest does not list; the table is quarantined locally", "owner", owner, "table", c.TableKey, "copy", c.Table, "scratch", scratch.Database+"."+scratch.Table, "snapshot", snapshotID)
	err := refuse(ErrForeign, "install: scratch %s holds a part manifest %s does not list; %s is quarantined locally", scratch.Database+"."+scratch.Table, snapshotID, c.TableKey)
	if serr := i.update(func(j *Journal) bool {
		oj := j.owner(owner)
		tj := oj.Tables[c.TableKey]
		if tj == nil || tj.IncarnationSeq != c.IncarnationSeq || tj.Copy != c.Table {
			tj = &TableJournal{IncarnationSeq: c.IncarnationSeq, Copy: c.Table}
			oj.Tables[c.TableKey] = tj
		}
		tj.Foreign, tj.ForeignScratch = true, scratch.Table
		return true
	}); serr != nil {
		return errors.Join(err, serr)
	}
	return err
}

// persistDiverged writes Diverged marks for copy c into the stored journal.
func (i *Installer) persistDiverged(owner uint64, c copyset.Copy, partitions []string) error {
	if len(partitions) == 0 {
		return nil
	}
	added := 0
	err := i.update(func(j *Journal) bool {
		oj := j.owner(owner)
		tj := oj.Tables[c.TableKey]
		if tj == nil || tj.IncarnationSeq != c.IncarnationSeq || tj.Copy != c.Table {
			tj = &TableJournal{IncarnationSeq: c.IncarnationSeq, Copy: c.Table}
			oj.Tables[c.TableKey] = tj
		}
		for _, p := range partitions {
			if tj.markDiverged(p) {
				added++
			}
		}
		return added > 0
	})
	for range added {
		i.countDiverged(owner)
	}
	return err
}

// checkDiskBudget pauses installs at or above StopInstallsRatio of the member
// disk budget, over every part, active or not, of ReplicaDB and StageDB (K35).
func (i *Installer) checkDiskBudget(ctx context.Context) error {
	var used uint64
	query := "SELECT sum(bytes_on_disk) FROM system.parts WHERE database IN (" + quoteLiteral(i.cfg.Pinned.ReplicaDB) + ", " + quoteLiteral(i.cfg.Pinned.StageDB) + ")"
	if err := i.d.Conn.QueryRow(ctx, query).Scan(&used); err != nil {
		return fmt.Errorf("install: read the member disk use: %w", err)
	}
	paused := float64(used) >= i.cfg.StopInstallsRatio*float64(i.cfg.DiskBudgetBytes)
	i.mu.Lock()
	i.diskUsed, i.paused = used, paused
	i.mu.Unlock()
	if paused {
		i.d.Logger.Warn("install: member disk budget exceeded; installs paused", "used_bytes", used, "budget_bytes", i.cfg.DiskBudgetBytes, "stop_installs_ratio", i.cfg.StopInstallsRatio)
		return refuse(ErrInstallsPaused, "install: member disk budget exceeded (%d of %d bytes); installs paused", used, i.cfg.DiskBudgetBytes)
	}
	return nil
}

// Entry returns a copy of owner's journal.
func (i *Installer) Entry(owner uint64) (OwnerJournal, bool) {
	i.mu.Lock()
	defer i.mu.Unlock()
	oj := i.journal.Owners[ownerKey(owner)]
	if oj == nil {
		return OwnerJournal{}, false
	}
	return *oj.clone(), true
}

// Tables lists the ready copies of owner, sorted by table key, with what the
// journal records for each.
func (i *Installer) Tables(owner uint64) []TableState {
	var copies []copyset.Copy
	for _, c := range i.readyCopies() {
		if c.Owner == owner {
			copies = append(copies, c)
		}
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	oj := i.journal.Owners[ownerKey(owner)]
	out := make([]TableState, 0, len(copies))
	for _, c := range copies {
		st := TableState{Owner: owner, TableKey: c.TableKey, IncarnationSeq: c.IncarnationSeq, Copy: c.Table, SchemaHash: payloadexec.TableSchemaHash(i.cfg.NetworkID, c.Schema)}
		if oj != nil {
			if tj := oj.Tables[c.TableKey]; tj != nil {
				st.Foreign = tj.Foreign
				if tj.IncarnationSeq == c.IncarnationSeq && tj.Copy == c.Table {
					st.Entry, st.SnapshotID = oj.Entry, oj.SnapshotID
					st.Diverged = len(tj.Diverged) > 0
				}
			}
		}
		st.Diverged = st.Diverged || i.pending[pendingKey(owner, c.TableKey)] > 0 || !i.audited[auditKey(c)]
		out = append(out, st)
	}
	return out
}

// Stats returns the counters and gauges. AuditClean is true once an audit
// completed since the start or the last ClickHouse restart, covered every
// ready copy (a copy made ready later counts once Apply audited it), and no
// ready copy has a partition awaiting a rebuild or a part awaiting adoption.
func (i *Installer) Stats() Stats {
	copies := i.readyCopies()
	i.mu.Lock()
	defer i.mu.Unlock()
	s := Stats{
		Outcomes: map[Outcome]uint64{}, Installs: map[string]uint64{}, DivergedPartitions: map[uint64]uint64{}, AuditRuns: map[string]uint64{},
		AuditDroppedTwins: i.droppedTwins, InstallsPaused: i.paused, DiskUsedBytes: i.diskUsed, DiskBudgetBytes: i.cfg.DiskBudgetBytes,
	}
	for _, o := range outcomes {
		s.Outcomes[o] = i.counts[o]
	}
	for _, name := range sourceLabels {
		s.Installs[name] = 0
	}
	for name, n := range i.installs {
		s.Installs[name] = n
	}
	for _, t := range triggers {
		s.AuditRuns[t] = i.auditRuns[t]
	}
	for key := range i.journal.Owners {
		if owner, err := strconv.ParseUint(key, 10, 64); err == nil {
			s.DivergedPartitions[owner] = 0
		}
	}
	clean := i.auditDone
	for _, c := range copies {
		s.DivergedPartitions[c.Owner] = 0
		if i.pending[pendingKey(c.Owner, c.TableKey)] > 0 || !i.audited[auditKey(c)] {
			clean = false
		}
		if oj := i.journal.Owners[ownerKey(c.Owner)]; oj != nil {
			if tj := oj.Tables[c.TableKey]; tj != nil && tj.IncarnationSeq == c.IncarnationSeq && tj.Copy == c.Table && !tj.Foreign && len(tj.Diverged) > 0 {
				clean = false
			}
		}
	}
	for owner, n := range i.diverged {
		s.DivergedPartitions[owner] = n
	}
	s.AuditClean = clean
	return s
}

// NeedsPass reports whether Apply of the newest update has work that waits for
// no new manifest (Plan S3-C2 review fix 245-2): the audit the copies wait for has
// not completed, a ready copy was not audited since the start or the last
// ClickHouse restart, or a ready copy has a partition awaiting a rebuild or a
// part the journal does not list. An owner with a quarantined table is left out:
// every pass of it is refused until an operator clears the quarantine offline.
// The role re-applies the newest update the feed offered while NeedsPass holds
// (Plan S3-C2 review fix 245-5), so the damage a startup, restart or self-audit
// records is repaired while the watermark stands still.
func (i *Installer) NeedsPass() bool {
	copies := i.readyCopies()
	i.mu.Lock()
	defer i.mu.Unlock()
	if !i.auditDone {
		return true
	}
	quarantined := map[uint64]bool{}
	for _, c := range copies {
		if oj := i.journal.Owners[ownerKey(c.Owner)]; oj != nil {
			if tj := oj.Tables[c.TableKey]; tj != nil && tj.Foreign {
				quarantined[c.Owner] = true
			}
		}
	}
	for _, c := range copies {
		if quarantined[c.Owner] {
			continue
		}
		if !i.audited[auditKey(c)] || i.pending[pendingKey(c.Owner, c.TableKey)] > 0 {
			return true
		}
		if oj := i.journal.Owners[ownerKey(c.Owner)]; oj != nil {
			if tj := oj.Tables[c.TableKey]; tj != nil && tj.IncarnationSeq == c.IncarnationSeq && tj.Copy == c.Table && len(tj.Diverged) > 0 {
				return true
			}
		}
	}
	return false
}

// update applies fn to a copy of the journal and, when fn reports a change,
// writes it durably before it becomes the installer's journal.
func (i *Installer) update(fn func(j *Journal) bool) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	next := i.journal.clone()
	if !fn(next) {
		return nil
	}
	if err := writeJournal(i.path, next); err != nil {
		return err
	}
	i.journal = next
	return nil
}

// tableJournal returns a working copy of c's journal, or a fresh one when the
// journal has none for c's incarnation.
func (i *Installer) tableJournal(owner uint64, c copyset.Copy) *TableJournal {
	i.mu.Lock()
	defer i.mu.Unlock()
	if oj := i.journal.Owners[ownerKey(owner)]; oj != nil {
		if tj := oj.Tables[c.TableKey]; tj != nil && tj.IncarnationSeq == c.IncarnationSeq && tj.Copy == c.Table {
			return tj.clone()
		}
	}
	return &TableJournal{IncarnationSeq: c.IncarnationSeq, Copy: c.Table}
}

func (i *Installer) foreignScratchOf(owner uint64, key string) string {
	i.mu.Lock()
	defer i.mu.Unlock()
	if oj := i.journal.Owners[ownerKey(owner)]; oj != nil && oj.Tables[key] != nil {
		return oj.Tables[key].ForeignScratch
	}
	return ""
}

func (i *Installer) readyCopies() []copyset.Copy {
	var out []copyset.Copy
	for _, c := range i.d.Copies.Copies() {
		if c.Ready {
			out = append(out, c)
		}
	}
	slices.SortFunc(out, func(a, b copyset.Copy) int {
		if a.Owner != b.Owner {
			if a.Owner < b.Owner {
				return -1
			}
			return 1
		}
		return strings.Compare(a.TableKey, b.TableKey)
	})
	return out
}

func (i *Installer) auditState() (bool, string) {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.auditDone, i.auditTrigger
}

func (i *Installer) count(o Outcome) {
	i.mu.Lock()
	i.counts[o]++
	i.mu.Unlock()
}

func (i *Installer) countDiverged(owner uint64) {
	i.mu.Lock()
	i.diverged[owner]++
	i.mu.Unlock()
}

func (i *Installer) deferralOf(key string) *deferral {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.deferred[key]
}

func (i *Installer) deferAgain(key string, results []string) *deferral {
	i.mu.Lock()
	defer i.mu.Unlock()
	d := i.deferred[key]
	if d == nil {
		d = &deferral{backoff: DefaultDeferBackoffMin}
		i.deferred[key] = d
	} else {
		d.backoff = min(2*d.backoff, DefaultDeferBackoffMax)
	}
	d.until = i.d.Now().Add(d.backoff)
	d.results = results
	return &deferral{until: d.until, backoff: d.backoff, results: slices.Clone(results)}
}

func (i *Installer) clearDeferral(key string) {
	i.mu.Lock()
	delete(i.deferred, key)
	i.mu.Unlock()
}

func deferKey(owner uint64, tableKey string, need replay.PartManifestEntry) string {
	return fmt.Sprintf("%d/%s/%s/%s", owner, tableKey, need.PartitionID, need.PartRowLtHash)
}

func pendingKey(owner uint64, tableKey string) string { return fmt.Sprintf("%d/%s", owner, tableKey) }

func sourceOutcome(res sources.Result) Outcome {
	switch res.Outcome {
	case sources.OutcomeMissed:
		return OutcomeMissed
	case sources.OutcomeUnreachable:
		return OutcomeUnreachable
	}
	return OutcomeRefused
}

func describe(source, outcome string, err error) string {
	if err != nil {
		return fmt.Sprintf("%s: %s (%v)", source, outcome, err)
	}
	return source + ": " + outcome
}

// installGates calls Deps.BeforeInstall at most once per database in one
// owner's pass, before the first statement that changes a copy of it.
type installGates struct {
	before func(ctx context.Context, owner uint64, database string) error
	owner  uint64
	seen   map[string]bool
}

func (g *installGates) check(ctx context.Context, database string) error {
	if g.seen[database] {
		return nil
	}
	if g.before != nil {
		if err := g.before(ctx, g.owner, database); err != nil {
			return fmt.Errorf("install: before changing the copies of %s of owner %d: %w", database, g.owner, err)
		}
	}
	g.seen[database] = true
	return nil
}

// databaseOf is the database of a logical table key <database>.<table>.
func databaseOf(tableKey string) string {
	database, _, _ := strings.Cut(tableKey, ".")
	return database
}

// listCopy lists the copy's active parts that hold rows: a zero-row active
// part (the cover a DROP PART leaves, Plan S3-A M11) holds nothing to install
// or audit.
func (i *Installer) listCopy(ctx context.Context, c copyset.Copy) ([]chparts.Part, error) {
	parts, err := nonEmptyParts(ctx, i.d.Conn, i.cfg.Pinned.ReplicaDB, c.Table)
	if err != nil {
		return nil, fmt.Errorf("install: list copy %s.%s: %w", i.cfg.Pinned.ReplicaDB, c.Table, err)
	}
	return parts, nil
}

func nonEmptyParts(ctx context.Context, conn clickhouse.Conn, database, table string) ([]chparts.Part, error) {
	parts, err := chparts.ActiveParts(ctx, conn, database, table)
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(parts, func(p chparts.Part) bool { return p.Rows == 0 }), nil
}

func partNames(parts []chparts.Part) []string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, p.Name)
	}
	sort.Strings(out)
	return out
}

func entryHashes(entries []replay.PartManifestEntry) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, e.PartRowLtHash)
	}
	return out
}

// pickEntries returns, for the multiset need, one manifest entry per
// occurrence, in manifest order.
func pickEntries(entries []replay.PartManifestEntry, need []string) []replay.PartManifestEntry {
	left := slices.Clone(need)
	var out []replay.PartManifestEntry
	for _, e := range entries {
		if k := slices.Index(left, e.PartRowLtHash); k >= 0 {
			out = append(out, e)
			left = slices.Delete(left, k, k+1)
		}
	}
	return out
}

func multisetMinus(a, b []string) []string {
	out := slices.Clone(a)
	for _, h := range b {
		out = removeOne(out, h)
	}
	return out
}

func removeOne(s []string, h string) []string {
	if k := slices.Index(s, h); k >= 0 {
		return slices.Delete(slices.Clone(s), k, k+1)
	}
	return s
}

// abbreviate shortens a 4098-character LtHash accumulator for a refusal.
func abbreviate(h string) string {
	if len(h) <= 26 {
		return h
	}
	return h[:18] + "…" + h[len(h)-8:]
}
