// Package rebuild is the same-identity owner rebuild of a lost source
// ClickHouse (housegate spec 2026-10-14 §16 P4; plan S3-D contract §34,
// clarifications K52 and K54). The host keeps its identity, its signer key
// and its volume, so the SNode journal survives; only the ClickHouse (pod,
// PVC, PV) is new. Rebuild fences the lost server's Keeper replicas, creates
// the owned tables (the new hg_unsafe replica clones from the pool
// verifiers), restores every hg_safe partition to the arbiter's SafeParts
// through the restore gate (member/gate) from the own hg_unsafe replica and
// then the object store, and records the restored inventories in the journal
// (snode.RecordRebuiltSafeInventories). It refuses a live track and a track
// with issued, unacknowledged promotions before it touches anything.
package rebuild

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/chparts"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/member/gate"
	"github.com/sentioxyz/arbiter-core/snode"
	"github.com/sentioxyz/arbiter-core/wire"
)

// DefaultUnsafeSyncTimeout is Config.UnsafeSyncTimeout's default.
const DefaultUnsafeSyncTimeout = 30 * time.Minute

// Config is one rebuild: the network, the indexer whose source ClickHouse is
// rebuilt, its protocol DDL inputs (Pinned.NodeID is its SNode's node id, the
// replica name the fence drops and the new tables take), its SNode state
// directory, its genesis table schemas, how long the new hg_unsafe replicas
// may take to catch up with the pool (0: DefaultUnsafeSyncTimeout), and
// whether only to plan.
type Config struct {
	NetworkID         string
	IndexerID         uint64
	Pinned            ddl.Pinned
	StateDir          string
	GenesisTables     []payloadexec.TableSchema
	UnsafeSyncTimeout time.Duration
	DryRun            bool
}

// Inputs are the arbiter's answers the rebuild works from: the table
// registry, one PartitionStates per owned table (Client.GetPartitionStates)
// and the outcomes with Acked false of every owned table
// (Client.GetPromotionOutcomes, paged by NextAfterPromotionSeq).
type Inputs struct {
	Registry wire.TableRegistrySnapshot
	States   []wire.PartitionStates
	Unacked  []wire.PromotionOutcome
}

// Phase names a stage of the rebuild in its report and its errors.
type Phase string

const (
	PhaseFence   Phase = "fence"
	PhaseTables  Phase = "tables"
	PhaseRestore Phase = "restore"
	PhaseJournal Phase = "journal"
	PhasePlan    Phase = "plan"
	PhaseDone    Phase = "done"
)

// Source is where a restored part came from.
type Source string

const (
	// SourceOwnUnsafe: a promoted part whose cleanup has not run, still in
	// the re-cloned hg_unsafe replica, found by hash_of_all_files.
	SourceOwnUnsafe Source = "own_unsafe"
	// SourceObjectStore: the backup the rebuild names.
	SourceObjectStore Source = "object_store"
)

// The rebuild's refusals. Each sentinel's text is the format of the message
// of the refusal it matches (errors.Is), so the verbatim message lives once.
var (
	ErrRebuildNotQuarantined   = errors.New("rebuild: indexer %d is not quarantined; a live track's hg_safe is never rebuilt")
	ErrRebuildUnacked          = errors.New("rebuild: indexer %d has issued, unacknowledged promotions %v; settle them first (arbiter-admin indexer settle)")
	ErrRebuildForeignSafeParts = errors.New("rebuild: hg_safe.%s partition %s holds parts outside the arbiter's SafeParts; only an empty or already rebuilt table is rebuilt")
	ErrRebuildSourceMissing    = errors.New("rebuild: no source holds part %s (row hash %s, hash_of_all_files %s) of %s partition %s")
	ErrRebuildPhysHashMismatch = errors.New("rebuild: part %s of %s partition %s came back with hash_of_all_files %s; the arbiter records %s")
	ErrRebuildClosure          = errors.New("rebuild: %s partition %s does not close: local %s plus kept %s is not the base root %s")
	ErrFenceReplicaActive      = errors.New("fence: replica %s of %s is active at %s; the old ClickHouse is not fenced")
	ErrFenceScopedReplica      = errors.New("fence: %s has children %v; the scoped root must be empty before the purge switch")
)

// refusal is one of the refusals above: its message is the sentinel's text
// formatted with the refusal's values, and it unwraps to the sentinel.
type refusal struct {
	kind error
	msg  string
}

func (e *refusal) Error() string { return e.msg }
func (e *refusal) Unwrap() error { return e.kind }

func refuse(kind error, args ...any) error {
	return &refusal{kind: kind, msg: fmt.Sprintf(kind.Error(), args...)}
}

// ownedTable is one table the indexer owns: its registry key, its schema, the
// incarnation marker ddl.EnsureTable takes (0 for a genesis table) and its
// physical name.
type ownedTable struct {
	TableID  string
	Schema   payloadexec.TableSchema
	Marker   uint64
	Physical string
}

// ownedPartition is one partition the arbiter acknowledged safe parts of.
type ownedPartition struct {
	table ownedTable
	state wire.PartitionState
}

// partContent is a part's identity across names: its row LtHash and its
// hash_of_all_files.
type partContent struct{ Row, Phys string }

// planned is one partition's restore plan: the safe parts it still needs,
// each with the source that holds it.
type planned struct {
	part    ownedPartition
	local   []string // row LtHashes of the parts hg_safe holds already
	need    []arbiter.SafePartMapping
	sources []Source
	report  PartitionReport
}

// Rebuild rebuilds cfg.IndexerID's source ClickHouse on conn (see the package
// comment). A dry run plans every owned partition and writes nothing. A real
// run returns AlreadyRebuilt without writing when hg_safe and the journal
// already equal the arbiter's state, so a second run changes nothing.
func Rebuild(ctx context.Context, cfg Config, conn clickhouse.Conn, in Inputs, store ObjectStore) (Report, error) {
	if err := cfg.check(); err != nil {
		return Report{}, err
	}
	if !slices.Contains(in.Registry.QuarantinedIndexers, cfg.IndexerID) {
		return Report{}, refuse(ErrRebuildNotQuarantined, cfg.IndexerID)
	}
	var unacked []uint64
	for _, o := range in.Unacked {
		if !o.Acked {
			unacked = append(unacked, o.PromotionSeq)
		}
	}
	if len(unacked) > 0 {
		slices.Sort(unacked)
		return Report{}, refuse(ErrRebuildUnacked, cfg.IndexerID, unacked)
	}
	tables, err := ownedTables(cfg, in.Registry)
	if err != nil {
		return Report{}, err
	}
	partitions := ownedPartitions(tables, in.States)
	inventories := make([]snode.RebuiltSafeInventory, 0, len(partitions))
	for _, p := range partitions {
		inventories = append(inventories, snode.RebuiltSafeInventory{TableID: p.table.TableID, PartitionID: p.state.PartitionID, PublishSeq: p.state.PublishSeq})
	}
	journaled, err := snode.CheckRebuildJournal(cfg.StateDir, inventories)
	if err != nil {
		return Report{}, err
	}
	if cfg.DryRun {
		return dryRun(ctx, cfg, conn, partitions, store)
	}
	if done, report, err := alreadyRebuilt(ctx, cfg, conn, tables, partitions, journaled); err != nil || done {
		return report, err
	}
	report := Report{}
	local, err := locallyHeld(ctx, conn, cfg.Pinned, tables)
	if err != nil {
		return report, err
	}
	var toFence []string
	for _, t := range tables {
		// A table this server already holds was fenced by an earlier,
		// interrupted run (its replica is created only after the fence),
		// and its replica is this server's own.
		if !local[t.TableID] {
			toFence = append(toFence, t.TableID)
		}
	}
	if report.Fence, err = Fence(ctx, conn, cfg.Pinned, toFence); err != nil {
		return report, err
	}
	for _, t := range tables {
		if err := ddl.EnsureTable(ctx, conn, cfg.Pinned, t.Schema, t.Marker, ddl.ModeCreateAndVerify); err != nil {
			return report, fmt.Errorf("rebuild %s: %w", PhaseTables, err)
		}
	}
	if err := waitUnsafeSynced(ctx, conn, cfg.Pinned.UnsafeDB, tables, cfg.unsafeSyncTimeout()); err != nil {
		return report, err
	}
	if err := dropScratches(ctx, conn, cfg.Pinned.PromoteDB); err != nil {
		return report, err
	}
	plans := make([]planned, 0, len(partitions))
	for _, p := range partitions {
		pl, err := planPartition(ctx, cfg, conn, p, store, false)
		if err != nil {
			return report, err
		}
		plans = append(plans, pl)
	}
	for _, pl := range plans {
		if len(pl.report.Missing) > 0 {
			return report, missingRefusal(pl)
		}
	}
	scratch := 0
	for _, pl := range plans {
		if err := restorePartition(ctx, cfg, conn, pl, store, &scratch); err != nil {
			return report, err
		}
		report.Partitions = append(report.Partitions, pl.report)
	}
	for _, t := range tables {
		if err := checkPartitionSet(ctx, cfg, conn, t, partitions); err != nil {
			return report, err
		}
	}
	if err := dropScratches(ctx, conn, cfg.Pinned.PromoteDB); err != nil {
		return report, err
	}
	for i, p := range partitions {
		parts, err := readBack(ctx, cfg, conn, p)
		if err != nil {
			return report, err
		}
		inventories[i].Parts = parts
	}
	if err := snode.RecordRebuiltSafeInventories(cfg.StateDir, inventories); err != nil {
		return report, fmt.Errorf("rebuild %s: %w", PhaseJournal, err)
	}
	return report, nil
}

func (cfg Config) check() error {
	p := cfg.Pinned
	if cfg.NetworkID == "" || cfg.StateDir == "" || p.UnsafeDB == "" || p.SafeDB == "" || p.PromoteDB == "" || p.NodeID == "" {
		return errors.New("rebuild: Config needs NetworkID, StateDir and Pinned's three databases and node id")
	}
	return nil
}

func (cfg Config) unsafeSyncTimeout() time.Duration {
	if cfg.UnsafeSyncTimeout > 0 {
		return cfg.UnsafeSyncTimeout
	}
	return DefaultUnsafeSyncTimeout
}

// ownedTables are the registry keys whose live incarnation the indexer owns
// and that hold tables (Pending, Active, Retiring), sorted by key: a genesis
// incarnation takes its schema from cfg.GenesisTables and marker 0, a chain
// incarnation its registry schema and its seq. A schema whose hash is not the
// registry's is refused: tables built from it would not take the backup's
// parts.
func ownedTables(cfg Config, reg wire.TableRegistrySnapshot) ([]ownedTable, error) {
	genesis := map[string]payloadexec.TableSchema{}
	for _, s := range cfg.GenesisTables {
		genesis[s.TableID] = s
	}
	seen := map[string]bool{}
	var out []ownedTable
	for _, inc := range reg.Incarnations {
		key := inc.Key()
		if seen[key] {
			continue
		}
		seen[key] = true
		live := reg.Live(key)
		if live == nil || reg.Owner(*live) != cfg.IndexerID {
			continue
		}
		switch live.Status {
		case wire.TableStatusPending, wire.TableStatusActive, wire.TableStatusRetiring:
		default:
			continue
		}
		t := ownedTable{TableID: key, Physical: ddl.CHTableName(key)}
		if live.Origin == wire.TableOriginGenesis {
			s, ok := genesis[key]
			if !ok {
				return nil, fmt.Errorf("rebuild: genesis table %s is not in the configured genesis tables", key)
			}
			t.Schema = s
		} else {
			s, err := live.Schema()
			if err != nil {
				return nil, fmt.Errorf("rebuild: %w", err)
			}
			t.Schema, t.Marker = s, live.Seq
		}
		if h := payloadexec.TableSchemaHash(cfg.NetworkID, t.Schema); live.SchemaHash != "" && live.SchemaHash != h {
			return nil, fmt.Errorf("rebuild: table %s has schema hash %s here, the registry has %s", key, h, live.SchemaHash)
		}
		out = append(out, t)
	}
	slices.SortFunc(out, func(a, b ownedTable) int { return strings.Compare(a.TableID, b.TableID) })
	return out, nil
}

// ownedPartitions are the partitions with safe parts the arbiter answered for
// the owned tables, by table then partition.
func ownedPartitions(tables []ownedTable, states []wire.PartitionStates) []ownedPartition {
	byTable := map[string]ownedTable{}
	for _, t := range tables {
		byTable[t.TableID] = t
	}
	var out []ownedPartition
	for _, s := range states {
		for _, p := range s.Partitions {
			t, ok := byTable[p.TableID]
			if !ok || len(p.SafeParts) == 0 {
				continue
			}
			out = append(out, ownedPartition{table: t, state: p})
		}
	}
	slices.SortFunc(out, func(a, b ownedPartition) int {
		if c := strings.Compare(a.table.TableID, b.table.TableID); c != 0 {
			return c
		}
		return strings.Compare(a.state.PartitionID, b.state.PartitionID)
	})
	return slices.CompactFunc(out, func(a, b ownedPartition) bool {
		return a.table.TableID == b.table.TableID && a.state.PartitionID == b.state.PartitionID
	})
}

// manifest is a partition's SafeParts as mappings: what the restore must
// produce, by content.
func manifest(p ownedPartition) []arbiter.SafePartMapping {
	out := make([]arbiter.SafePartMapping, 0, len(p.state.SafeParts))
	for _, e := range p.state.SafeParts {
		out = append(out, arbiter.SafePartMapping{PartRowLtHash: e.PartRowLtHash, SafePartName: e.PartName, PartPhysHash: e.PartPhysHash})
	}
	return out
}

func rowHashes(parts []arbiter.SafePartMapping) []string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, p.PartRowLtHash)
	}
	return out
}

// localContent scans db.table's partition: each active part with its row
// LtHash and hash_of_all_files. A table that does not exist holds nothing.
func localContent(ctx context.Context, conn clickhouse.Conn, db, table string, sch payloadexec.TableSchema, partitionID string) ([]chparts.Part, []partContent, error) {
	parts, err := chparts.ActiveParts(ctx, conn, db, table)
	if err != nil {
		return nil, nil, err
	}
	parts = chparts.PartsInLogicalPartition(parts, sch, partitionID)
	if len(parts) == 0 {
		return nil, nil, nil
	}
	_, scanned, err := chparts.PartitionContentScan(ctx, conn, db, table, sch, partitionID)
	if err != nil {
		return nil, nil, err
	}
	rows := map[string]string{}
	for _, s := range scanned {
		rows[s.Name] = s.RowLtHash
	}
	out := make([]partContent, 0, len(parts))
	for _, p := range parts {
		out = append(out, partContent{Row: rows[p.Name], Phys: p.PhysHash})
	}
	return parts, out, nil
}

// subtract removes have from want by content and reports whether every
// element of have was in want: what is left is still needed.
func subtract(want []arbiter.SafePartMapping, have []partContent) ([]arbiter.SafePartMapping, bool) {
	left := slices.Clone(want)
	for _, h := range have {
		i := slices.IndexFunc(left, func(m arbiter.SafePartMapping) bool { return m.PartRowLtHash == h.Row && m.PartPhysHash == h.Phys })
		if i < 0 {
			return nil, false
		}
		left = slices.Delete(left, i, i+1)
	}
	return left, true
}

// planPartition finds a source for each safe part the partition needs, by
// hash_of_all_files: the own hg_unsafe replica first, then the object store.
// A dry run plans every part of SafeParts (the local hg_safe is the one the
// rebuild will lose); a real run plans what hg_safe does not hold yet and
// refuses a partition that holds anything else.
func planPartition(ctx context.Context, cfg Config, conn clickhouse.Conn, p ownedPartition, store ObjectStore, dry bool) (planned, error) {
	t := p.table
	m := manifest(p)
	pl := planned{part: p, report: PartitionReport{TableID: t.TableID, PartitionID: p.state.PartitionID,
		PublishSeq: p.state.PublishSeq, Parts: len(m), Root: p.state.BasePartitionRoot}}
	need := m
	if !dry {
		_, have, err := localContent(ctx, conn, cfg.Pinned.SafeDB, t.Physical, t.Schema, p.state.PartitionID)
		if err != nil {
			return pl, fmt.Errorf("rebuild %s: hg_safe.%s partition %s: %w", PhaseRestore, t.Physical, p.state.PartitionID, err)
		}
		var ok bool
		if need, ok = subtract(m, have); !ok {
			return pl, refuse(ErrRebuildForeignSafeParts, t.Physical, p.state.PartitionID)
		}
		for _, h := range have {
			pl.local = append(pl.local, h.Row)
		}
		plan, err := gate.PlanPartition(rowHashes(m), p.state.BasePartitionRoot, pl.local)
		if err != nil {
			return pl, fmt.Errorf("rebuild %s: %s partition %s: %w", PhaseRestore, t.TableID, p.state.PartitionID, err)
		}
		if plan.Diverged {
			return pl, refuse(ErrRebuildForeignSafeParts, t.Physical, p.state.PartitionID)
		}
	}
	unsafe, err := chparts.ActiveParts(ctx, conn, cfg.Pinned.UnsafeDB, t.Physical)
	if err != nil {
		return pl, fmt.Errorf("rebuild %s: hg_unsafe.%s: %w", PhaseRestore, t.Physical, err)
	}
	unsafePhys := map[string]int{}
	for _, u := range chparts.PartsInLogicalPartition(unsafe, t.Schema, p.state.PartitionID) {
		unsafePhys[u.PhysHash]++
	}
	storePhys := map[string]int{}
	if store != nil {
		for _, b := range store.Parts(t.TableID, p.state.PartitionID) {
			storePhys[b.HashOfAllFiles]++
		}
	}
	for _, part := range need {
		switch {
		case unsafePhys[part.PartPhysHash] > 0:
			unsafePhys[part.PartPhysHash]--
			pl.need, pl.sources = append(pl.need, part), append(pl.sources, SourceOwnUnsafe)
			pl.report.FromOwnUnsafe++
		case storePhys[part.PartPhysHash] > 0:
			storePhys[part.PartPhysHash]--
			pl.need, pl.sources = append(pl.need, part), append(pl.sources, SourceObjectStore)
			pl.report.FromObjectStore++
		default:
			pl.report.Missing = append(pl.report.Missing, part.SafePartName)
		}
	}
	return pl, nil
}

func missingRefusal(pl planned) error {
	for _, part := range manifest(pl.part) {
		if slices.Contains(pl.report.Missing, part.SafePartName) {
			return refuse(ErrRebuildSourceMissing, part.SafePartName, abbreviate(part.PartRowLtHash), part.PartPhysHash, pl.part.table.TableID, pl.part.state.PartitionID)
		}
	}
	return refuse(ErrRebuildSourceMissing, pl.report.Missing[0], "?", "?", pl.part.table.TableID, pl.part.state.PartitionID)
}

func dryRun(ctx context.Context, cfg Config, conn clickhouse.Conn, partitions []ownedPartition, store ObjectStore) (Report, error) {
	report := Report{DryRun: true}
	var first *planned
	for _, p := range partitions {
		pl, err := planPartition(ctx, cfg, conn, p, store, true)
		if err != nil {
			return report, err
		}
		report.Partitions = append(report.Partitions, pl.report)
		if len(pl.report.Missing) > 0 && first == nil {
			first = &pl
		}
	}
	if first != nil {
		return report, missingRefusal(*first)
	}
	return report, nil
}

// alreadyRebuilt reports whether a run would change nothing: every owned
// table's hg_safe exists and equals the arbiter's SafeParts by content in
// every partition, holds no other partition, and the journal records every
// partition rebuilt at its PublishSeq under the part names hg_safe holds now
// (a rebuild that restored again and stopped before its journal write leaves
// other names journaled, and is not done). Then the report lists the partitions
// with nothing fetched, and nothing is written, the fence included (the
// server holds the replicas itself).
func alreadyRebuilt(ctx context.Context, cfg Config, conn clickhouse.Conn, tables []ownedTable, partitions []ownedPartition, journaled []bool) (bool, Report, error) {
	if slices.Contains(journaled, false) {
		return false, Report{}, nil
	}
	for _, t := range tables {
		exists, err := tableExists(ctx, conn, cfg.Pinned.SafeDB, t.Physical)
		if err != nil || !exists {
			return false, Report{}, err
		}
		if err := checkPartitionSet(ctx, cfg, conn, t, partitions); err != nil {
			return false, Report{}, nil
		}
	}
	report := Report{AlreadyRebuilt: true}
	current := make([]snode.RebuiltSafeInventory, 0, len(partitions))
	for _, p := range partitions {
		parts, have, err := localContent(ctx, conn, cfg.Pinned.SafeDB, p.table.Physical, p.table.Schema, p.state.PartitionID)
		if err != nil {
			return false, Report{}, err
		}
		if left, ok := subtract(manifest(p), have); !ok || len(left) > 0 {
			return false, Report{}, nil
		}
		names := make([]arbiter.SafePartMapping, 0, len(parts))
		for i, part := range parts {
			names = append(names, arbiter.SafePartMapping{PartRowLtHash: have[i].Row, SafePartName: part.Name, PartPhysHash: have[i].Phys})
		}
		current = append(current, snode.RebuiltSafeInventory{TableID: p.table.TableID, PartitionID: p.state.PartitionID, PublishSeq: p.state.PublishSeq, Parts: names})
		report.Partitions = append(report.Partitions, PartitionReport{TableID: p.table.TableID, PartitionID: p.state.PartitionID,
			PublishSeq: p.state.PublishSeq, Parts: len(p.state.SafeParts), Root: p.state.BasePartitionRoot})
	}
	journaledNames, err := snode.CheckRebuildJournal(cfg.StateDir, current)
	if err != nil {
		return false, Report{}, err
	}
	if slices.Contains(journaledNames, false) {
		return false, Report{}, nil
	}
	return true, report, nil
}

func tableExists(ctx context.Context, conn clickhouse.Conn, db, table string) (bool, error) {
	var n uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.tables WHERE database = ? AND name = ?`, db, table).Scan(&n); err != nil {
		return false, fmt.Errorf("rebuild: look up %s.%s: %w", db, table, err)
	}
	return n > 0, nil
}

// locallyHeld reports, per owned table, whether this server already holds
// its hg_unsafe replica (system.replicas at its Keeper path with p.NodeID).
func locallyHeld(ctx context.Context, conn clickhouse.Conn, p ddl.Pinned, tables []ownedTable) (map[string]bool, error) {
	out := map[string]bool{}
	for _, t := range tables {
		var n uint64
		if err := conn.QueryRow(ctx, `SELECT count() FROM system.replicas WHERE database = ? AND table = ? AND zookeeper_path = ? AND replica_name = ?`,
			p.UnsafeDB, t.Physical, ddl.ZooKeeperPath(p, t.TableID), p.NodeID).Scan(&n); err != nil {
			return nil, fmt.Errorf("rebuild %s: look up the local replica of %s: %w", PhaseFence, t.TableID, err)
		}
		out[t.TableID] = n > 0
	}
	return out, nil
}

// waitUnsafeSynced waits until every new hg_unsafe replica has worked off its
// replication queue (system.replicas queue_size 0), or until timeout passes;
// then the plan takes what the replicas hold, and the object store covers the
// rest. A new replica's CREATE copies the source replica's queue before it
// returns, so a queue of 0 is never read before the clone started.
func waitUnsafeSynced(ctx context.Context, conn clickhouse.Conn, db string, tables []ownedTable, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, t := range tables {
		for {
			var queue uint32
			err := conn.QueryRow(ctx, `SELECT queue_size FROM system.replicas WHERE database = ? AND table = ?`, db, t.Physical).Scan(&queue)
			if err != nil {
				return fmt.Errorf("rebuild %s: replication queue of %s.%s: %w", PhaseTables, db, t.Physical, err)
			}
			if queue == 0 || !time.Now().Before(deadline) {
				break
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}
	}
	return nil
}

const scratchMarker = "__rebuild_"

// dropScratches drops every rebuild scratch table in db.
func dropScratches(ctx context.Context, conn clickhouse.Conn, db string) error {
	rows, err := conn.Query(ctx, `SELECT name FROM system.tables WHERE database = ? AND position(name, ?) > 0`, db, scratchMarker)
	if err != nil {
		return fmt.Errorf("rebuild %s: list scratch tables: %w", PhaseRestore, err)
	}
	var names []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			rows.Close()
			return err
		}
		names = append(names, n)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("rebuild %s: list scratch tables: %w", PhaseRestore, err)
	}
	rows.Close()
	for _, n := range names {
		if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s` SYNC", db, n)); err != nil {
			return fmt.Errorf("rebuild %s: drop scratch %s.%s: %w", PhaseRestore, db, n, err)
		}
	}
	return nil
}

// restorePartition fetches the plan's parts, own hg_unsafe first and then the
// object store, each source into a fresh scratch hg_promote.<t>__rebuild_<n>
// whose parts the gate classifies; it keeps exactly the needed parts, checks
// each kept part's hash_of_all_files against SafeParts, attaches them to
// hg_safe with ATTACH PARTITION FROM and checks the closure.
func restorePartition(ctx context.Context, cfg Config, conn clickhouse.Conn, pl planned, store ObjectStore, scratch *int) error {
	if len(pl.need) == 0 {
		return nil
	}
	t, partitionID := pl.part.table, pl.part.state.PartitionID
	quoted, err := chparts.QuotePartition(t.Schema, partitionID)
	if err != nil {
		return err
	}
	safe := fmt.Sprintf("`%s`.`%s`", cfg.Pinned.SafeDB, t.Physical)
	var kept []string
	for _, source := range []Source{SourceOwnUnsafe, SourceObjectStore} {
		var group []arbiter.SafePartMapping
		for i, part := range pl.need {
			if pl.sources[i] == source {
				group = append(group, part)
			}
		}
		if len(group) == 0 {
			continue
		}
		*scratch++
		name := fmt.Sprintf("%s%s%d", t.Physical, scratchMarker, *scratch)
		qualified := fmt.Sprintf("`%s`.`%s`", cfg.Pinned.PromoteDB, name)
		switch source {
		case SourceOwnUnsafe:
			if err := conn.Exec(ctx, fmt.Sprintf("CREATE TABLE %s AS %s", qualified, safe)); err != nil {
				return fmt.Errorf("rebuild %s: create scratch %s: %w", PhaseRestore, qualified, err)
			}
			if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM `%s`.`%s`", qualified, quoted, cfg.Pinned.UnsafeDB, t.Physical)); err != nil {
				return fmt.Errorf("rebuild %s: attach %s partition %s from hg_unsafe: %w", PhaseRestore, t.TableID, partitionID, err)
			}
		case SourceObjectStore:
			if err := store.RestorePartitions(ctx, conn, t.TableID, []string{partitionID}, name); err != nil {
				return fmt.Errorf("rebuild %s: %w", PhaseRestore, err)
			}
		}
		groupKept, err := gateScratch(ctx, conn, cfg.Pinned.PromoteDB, name, qualified, t, partitionID, group)
		if err != nil {
			return err
		}
		if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s ATTACH PARTITION %s FROM %s", safe, quoted, qualified)); err != nil {
			return fmt.Errorf("rebuild %s: attach %s partition %s to hg_safe: %w", PhaseRestore, t.TableID, partitionID, err)
		}
		if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s SYNC", qualified)); err != nil {
			return fmt.Errorf("rebuild %s: drop scratch %s: %w", PhaseRestore, qualified, err)
		}
		kept = append(kept, groupKept...)
	}
	if err := gate.Closure(pl.part.state.BasePartitionRoot, pl.local, kept); err != nil {
		localSum, _ := payloadexec.SumPartRowLtHashes(pl.local)
		keptSum, _ := payloadexec.SumPartRowLtHashes(kept)
		return refuse(ErrRebuildClosure, t.TableID, partitionID, abbreviate(localSum), abbreviate(keptSum), abbreviate(pl.part.state.BasePartitionRoot))
	}
	_, have, err := localContent(ctx, conn, cfg.Pinned.SafeDB, t.Physical, t.Schema, partitionID)
	if err != nil {
		return fmt.Errorf("rebuild %s: hg_safe.%s partition %s: %w", PhaseRestore, t.Physical, partitionID, err)
	}
	if left, ok := subtract(manifest(pl.part), have); !ok || len(left) > 0 {
		return refuse(ErrRebuildForeignSafeParts, t.Physical, partitionID)
	}
	return nil
}

// gateScratch classifies the scratch's parts with the restore gate in restore
// mode (keep each needed row LtHash once, drop the rest), drops the others,
// and requires each kept part's hash_of_all_files to be the one SafeParts
// records for its row LtHash. It returns the kept row LtHashes.
func gateScratch(ctx context.Context, conn clickhouse.Conn, db, name, qualified string, t ownedTable, partitionID string, group []arbiter.SafePartMapping) ([]string, error) {
	scans, err := chexec.ScanAllParts(ctx, conn, db+"."+name, t.Schema)
	if err != nil {
		return nil, fmt.Errorf("rebuild %s: scan %s: %w", PhaseRestore, qualified, err)
	}
	parts, err := chparts.ActiveParts(ctx, conn, db, name)
	if err != nil {
		return nil, fmt.Errorf("rebuild %s: parts of %s: %w", PhaseRestore, qualified, err)
	}
	phys := map[string]string{}
	for _, p := range parts {
		phys[p.Name] = p.PhysHash
	}
	want := map[string]string{} // row LtHash -> hash_of_all_files SafeParts records
	for _, g := range group {
		want[g.PartRowLtHash] = g.PartPhysHash
	}
	scratchParts := make([]gate.ScratchPart, 0, len(scans))
	for _, s := range scans {
		scratchParts = append(scratchParts, gate.ScratchPart{Name: s.PartName, RowLtHash: s.RowLtHash, Rows: s.RowCount})
	}
	// Offer the gate the parts whose files match SafeParts first, so of two
	// parts with one row LtHash it keeps the one the arbiter recorded.
	slices.SortStableFunc(scratchParts, func(a, b gate.ScratchPart) int {
		am, bm := phys[a.Name] == want[a.RowLtHash], phys[b.Name] == want[b.RowLtHash]
		switch {
		case am && !bm:
			return -1
		case bm && !am:
			return 1
		}
		return 0
	})
	c, err := gate.Classify(gate.Fetch{Need: rowHashes(group)}, scratchParts)
	if err != nil {
		return nil, fmt.Errorf("rebuild %s: classify %s: %w", PhaseRestore, qualified, err)
	}
	for _, drop := range c.Drop {
		if err := conn.Exec(ctx, fmt.Sprintf("ALTER TABLE %s DROP PART '%s'", qualified, chparts.EscapeSQLString(drop))); err != nil {
			return nil, fmt.Errorf("rebuild %s: drop part %s of %s: %w", PhaseRestore, drop, qualified, err)
		}
	}
	byName := map[string]string{}
	for _, s := range scans {
		byName[s.PartName] = s.RowLtHash
	}
	for _, keep := range c.Keep {
		if got, recorded := phys[keep], want[byName[keep]]; got != recorded {
			return nil, refuse(ErrRebuildPhysHashMismatch, keep, t.TableID, partitionID, got, recorded)
		}
	}
	return c.Kept, nil
}

// checkPartitionSet requires hg_safe's partitions of t to be exactly the
// arbiter's partitions with safe parts.
func checkPartitionSet(ctx context.Context, cfg Config, conn clickhouse.Conn, t ownedTable, partitions []ownedPartition) error {
	var manifestIDs []string
	for _, p := range partitions {
		if p.table.TableID == t.TableID {
			manifestIDs = append(manifestIDs, p.state.PartitionID)
		}
	}
	parts, err := chparts.ActiveParts(ctx, conn, cfg.Pinned.SafeDB, t.Physical)
	if err != nil {
		return fmt.Errorf("rebuild %s: hg_safe.%s: %w", PhaseRestore, t.Physical, err)
	}
	var local []string
	for _, p := range parts {
		local = append(local, chparts.LogicalPartitionID(t.Schema, p))
	}
	if err := gate.PartitionSet(manifestIDs, local); err != nil {
		return fmt.Errorf("rebuild %s: hg_safe.%s: %w", PhaseRestore, t.Physical, err)
	}
	return nil
}

// readBack is the partition's rebuilt inventory: each SafeParts entry with
// the name its content has in hg_safe now.
func readBack(ctx context.Context, cfg Config, conn clickhouse.Conn, p ownedPartition) ([]arbiter.SafePartMapping, error) {
	parts, have, err := localContent(ctx, conn, cfg.Pinned.SafeDB, p.table.Physical, p.table.Schema, p.state.PartitionID)
	if err != nil {
		return nil, fmt.Errorf("rebuild %s: hg_safe.%s partition %s: %w", PhaseJournal, p.table.Physical, p.state.PartitionID, err)
	}
	out := make([]arbiter.SafePartMapping, 0, len(parts))
	for i, part := range parts {
		out = append(out, arbiter.SafePartMapping{PartRowLtHash: have[i].Row, SafePartName: part.Name, PartPhysHash: have[i].Phys})
	}
	if left, ok := subtract(manifest(p), have); !ok || len(left) > 0 {
		return nil, refuse(ErrRebuildForeignSafeParts, p.table.Physical, p.state.PartitionID)
	}
	return out, nil
}

// abbreviate shortens a 4098-character accumulator for a message.
func abbreviate(h string) string {
	if len(h) <= 26 {
		return h
	}
	return h[:18] + "…" + h[len(h)-8:]
}
