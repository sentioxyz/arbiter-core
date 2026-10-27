package ddl

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/sitable"
)

// The stage-3 member databases (housegate spec 2026-10-14 §8.1). They equal
// housegate's sitable.MemberUnsafeDatabase, ReplicaDatabase and
// ReplicaStageDatabase, which HouseGate reserves; a test pins the equality.
const (
	// MemberUnsafeDatabase holds a member's ReplicatedMergeTree replicas of
	// its owners' unsafe tables (created by si_member_ddl from Plan S3-E1).
	MemberUnsafeDatabase = "hg_member_unsafe"
	// ReplicaDatabase holds a member's verified copies, one MergeTree table
	// per registry incarnation (CopyTableName).
	ReplicaDatabase = "hg_replica"
	// StageDatabase holds a member's install, restore and replay scratch
	// tables, each named under the member's node-id prefix.
	StageDatabase = "hg_rstage"
)

// MemberPinned carries the deployment-specific inputs of the member DDL: the
// three member databases, the member's node id (its replica name in
// hg_member_unsafe and the prefix of its scratch tables), the Keeper shard and
// the storage policy of every member table. StoragePolicy "" keeps the
// server's default policy; devnet2 uses "member" (spec §8.1).
type MemberPinned struct {
	MemberUnsafeDB, ReplicaDB, StageDB, NodeID, StoragePolicy string
	KeeperShardID                                             uint32
}

// DefaultMemberPinned is MemberPinned with the three reserved member
// databases.
func DefaultMemberPinned(nodeID string, keeperShardID uint32, storagePolicy string) MemberPinned {
	return MemberPinned{
		MemberUnsafeDB: MemberUnsafeDatabase, ReplicaDB: ReplicaDatabase, StageDB: StageDatabase,
		NodeID: nodeID, StoragePolicy: storagePolicy, KeeperShardID: keeperShardID,
	}
}

// ErrMemberDatabasesIntersect reports a member configuration whose databases
// include one of the host SNode's protocol databases (spec §8.3: the
// si-member never creates or opens hg_safe, hg_unsafe or hg_promote).
var ErrMemberDatabasesIntersect = errors.New("ddl: member databases intersect the protocol databases")

// storagePolicyPattern is ClickHouse's rule for storage policy names
// (alphanumeric and '_'); a policy is rendered as a quoted SETTINGS literal.
var storagePolicyPattern = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// sentinelError is an error with its own text that still matches a sentinel
// through errors.Is.
type sentinelError struct {
	msg      string
	sentinel error
}

func (e sentinelError) Error() string { return e.msg }
func (e sentinelError) Unwrap() error { return e.sentinel }

// ValidateMemberPinned is the member's startup refusal (spec §8.3): every
// member database and the node id must be set, the three databases distinct,
// the storage policy a ClickHouse policy name, and no member database one of
// the protocol databases of the host SNode.
func ValidateMemberPinned(m MemberPinned, protocol Pinned) error {
	if err := checkMemberPinned(m); err != nil {
		return err
	}
	if protocol.UnsafeDB == "" || protocol.SafeDB == "" || protocol.PromoteDB == "" {
		return errors.New("ddl: the protocol Pinned needs UnsafeDB, SafeDB and PromoteDB to check the member databases against")
	}
	protocolDBs := []string{protocol.UnsafeDB, protocol.SafeDB, protocol.PromoteDB}
	var intersect []string
	for _, database := range []string{m.MemberUnsafeDB, m.ReplicaDB, m.StageDB} {
		for _, p := range protocolDBs {
			if database == p {
				intersect = append(intersect, database)
				break
			}
		}
	}
	if len(intersect) > 0 {
		return sentinelError{
			msg:      fmt.Sprintf("ddl: member databases %v intersect the protocol databases %v", intersect, protocolDBs),
			sentinel: ErrMemberDatabasesIntersect,
		}
	}
	return nil
}

func checkMemberPinned(m MemberPinned) error {
	if m.MemberUnsafeDB == "" || m.ReplicaDB == "" || m.StageDB == "" || m.NodeID == "" ||
		m.MemberUnsafeDB == m.ReplicaDB || m.MemberUnsafeDB == m.StageDB || m.ReplicaDB == m.StageDB {
		return errors.New("ddl: MemberPinned needs MemberUnsafeDB, ReplicaDB, StageDB and NodeID, all distinct")
	}
	if m.StoragePolicy != "" && !storagePolicyPattern.MatchString(m.StoragePolicy) {
		return fmt.Errorf("ddl: MemberPinned.StoragePolicy %q must be a ClickHouse storage policy name ([A-Za-z0-9_]+)", m.StoragePolicy)
	}
	return nil
}

func validateMember(conn clickhouse.Conn, m MemberPinned) error {
	if conn == nil {
		return errors.New("ddl: clickhouse connection is required")
	}
	return checkMemberPinned(m)
}

// CopyTableName is the physical name of the copy of incarnation
// incarnationSeq of tableID in ReplicaDB: CHTableName(tableID) + "__i" + seq,
// housegate's sitable.CopyTable. Copies are incarnation-scoped, so a dropped
// and re-created name never adopts an old copy (spec §8.3).
func CopyTableName(tableID string, incarnationSeq uint64) string {
	return sitable.CopyTable(tableID, incarnationSeq)
}

// memberSettings appends the member storage policy to the given pins.
func memberSettings(base []PinnedSetting, storagePolicy string) []PinnedSetting {
	if storagePolicy == "" {
		return base
	}
	return append(base, PinnedSetting{Name: "storage_policy", Value: quoteLiteral(storagePolicy)})
}

// memberMergeTree is t's hg_safe intent (Intents: the reserved row id, the
// partition freeze and the column whitelist) with the member's settings:
// SafeSettings plus the storage policy.
func memberMergeTree(m MemberPinned, t payloadexec.TableSchema) (TableIntent, error) {
	if err := checkMemberPinned(m); err != nil {
		return TableIntent{}, err
	}
	_, safe, _, err := Intents(Pinned{UnsafeDB: m.MemberUnsafeDB, SafeDB: m.ReplicaDB, PromoteDB: m.StageDB, NodeID: m.NodeID, KeeperShardID: m.KeeperShardID}, t)
	if err != nil {
		return TableIntent{}, err
	}
	safe.Settings = memberSettings(SafeSettings(), m.StoragePolicy)
	return safe, nil
}

// CopyIntent is the copy of incarnation incarnationSeq of t: a MergeTree
// table in ReplicaDB named CopyTableName, with an explicit ENGINE = MergeTree
// (never AS another table, so it can never inherit a replicated engine), the
// columns, partition key and sorting key of t's hg_safe table, SafeSettings
// plus the storage policy, and IncarnationComment(incarnationSeq).
func CopyIntent(m MemberPinned, t payloadexec.TableSchema, incarnationSeq uint64) (TableIntent, error) {
	if incarnationSeq == 0 {
		return TableIntent{}, errors.New("ddl: a copy needs a registry incarnation seq")
	}
	intent, err := memberMergeTree(m, t)
	if err != nil {
		return TableIntent{}, err
	}
	intent.Database = m.ReplicaDB
	intent.Table = CopyTableName(t.TableID, incarnationSeq)
	intent.Comment = IncarnationComment(incarnationSeq)
	return intent, nil
}

// ScratchIntent is a scratch table of t named name in StageDB: the structure,
// settings and storage policy of CopyIntent and no comment, so that ATTACH
// PARTITION ... FROM it into a copy is accepted (same structure, partition
// key and storage policy).
func ScratchIntent(m MemberPinned, t payloadexec.TableSchema, name string) (TableIntent, error) {
	if name == "" {
		return TableIntent{}, errors.New("ddl: a scratch table needs a name")
	}
	intent, err := memberMergeTree(m, t)
	if err != nil {
		return TableIntent{}, err
	}
	intent.Database = m.StageDB
	intent.Table = name
	return intent, nil
}

// MemberUnsafeIntent is the member's hg_member_unsafe replica of incarnation
// incarnationSeq of t (spec §8.3): ReplicatedMergeTree at zooKeeperPath with
// the member's node id as replica name, UnsafeSettings verbatim plus the
// storage policy, and IncarnationComment(incarnationSeq). The caller passes
// the incarnation's Keeper path. Plan S3-E1 is its first caller; in P2 nothing
// creates a replicated table (TestMemberUnsafeIntentHasNoCallerInP2).
func MemberUnsafeIntent(m MemberPinned, t payloadexec.TableSchema, incarnationSeq uint64, zooKeeperPath string) (TableIntent, error) {
	if incarnationSeq == 0 {
		return TableIntent{}, errors.New("ddl: a member replica needs a registry incarnation seq")
	}
	if zooKeeperPath == "" {
		return TableIntent{}, errors.New("ddl: a member replica needs a Keeper path")
	}
	if err := checkMemberPinned(m); err != nil {
		return TableIntent{}, err
	}
	unsafe, _, _, err := Intents(Pinned{UnsafeDB: m.MemberUnsafeDB, SafeDB: m.ReplicaDB, PromoteDB: m.StageDB, NodeID: m.NodeID, KeeperShardID: m.KeeperShardID}, t)
	if err != nil {
		return TableIntent{}, err
	}
	unsafe.Database = m.MemberUnsafeDB
	unsafe.ZooKeeperPath = zooKeeperPath
	unsafe.ReplicaName = m.NodeID
	unsafe.Settings = memberSettings(UnsafeSettings(), m.StoragePolicy)
	unsafe.Comment = IncarnationComment(incarnationSeq)
	return unsafe, nil
}

// EnsureMemberDatabases creates ReplicaDB and StageDB if they do not exist,
// with CREATE DATABASE IF NOT EXISTS and no ENGINE clause: ClickHouse 26.3
// checks a database ENGINE as TABLE ENGINE ON <engine>, which GRANT cannot
// name (spec §8.2). MemberUnsafeDB is never created here; si_member_ddl's
// reconciler creates it (Plan S3-E1).
func EnsureMemberDatabases(ctx context.Context, conn clickhouse.Conn, m MemberPinned) error {
	if err := validateMember(conn, m); err != nil {
		return err
	}
	for _, database := range []string{m.ReplicaDB, m.StageDB} {
		if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(database)); err != nil {
			return fmt.Errorf("ddl: create database %s: %w", database, err)
		}
	}
	return nil
}

// EnsureCopy creates the copy of incarnation incarnationSeq of t if it does
// not exist (CREATE TABLE IF NOT EXISTS of CopyIntent) and verifies its live
// shape against the intent; ReplicaDB must exist (EnsureMemberDatabases).
func EnsureCopy(ctx context.Context, conn clickhouse.Conn, m MemberPinned, t payloadexec.TableSchema, incarnationSeq uint64) error {
	if err := validateMember(conn, m); err != nil {
		return err
	}
	intent, err := CopyIntent(m, t, incarnationSeq)
	if err != nil {
		return err
	}
	if err := conn.Exec(ctx, intent.SQL()); err != nil {
		return fmt.Errorf("ddl: create %s.%s: %w", intent.Database, intent.Table, err)
	}
	return VerifyProtocolTable(ctx, conn, intent)
}

// DropCopy drops the copy of incarnation incarnationSeq of tableID with DROP
// TABLE IF EXISTS ... SYNC; dropping an absent copy succeeds.
func DropCopy(ctx context.Context, conn clickhouse.Conn, m MemberPinned, tableID string, incarnationSeq uint64) error {
	return DropCopyTable(ctx, conn, m, CopyTableName(tableID, incarnationSeq))
}

// DropCopyTable drops one table of ReplicaDB by name with DROP TABLE IF
// EXISTS ... SYNC: DropCopy's form for a table the registry cannot attribute
// to an incarnation. It never names another database.
func DropCopyTable(ctx context.Context, conn clickhouse.Conn, m MemberPinned, table string) error {
	if err := validateMember(conn, m); err != nil {
		return err
	}
	if table == "" {
		return errors.New("ddl: DropCopyTable needs a table name")
	}
	if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC", quoteIdent(m.ReplicaDB), quoteIdent(table))); err != nil {
		return fmt.Errorf("ddl: drop %s.%s: %w", m.ReplicaDB, table, err)
	}
	return nil
}

// ListMemberTables returns every table of MemberUnsafeDB and ReplicaDB,
// sorted by table then database. StageDB is never listed: its tables are
// scratch, owned by the installer, the restore source and the replay.
func ListMemberTables(ctx context.Context, conn clickhouse.Conn, m MemberPinned) ([]LocalTable, error) {
	if err := validateMember(conn, m); err != nil {
		return nil, err
	}
	rows, err := conn.Query(ctx, `SELECT database, name, comment FROM system.tables WHERE database IN (?, ?)`, m.MemberUnsafeDB, m.ReplicaDB)
	if err != nil {
		return nil, fmt.Errorf("ddl: list member tables: %w", err)
	}
	defer rows.Close()
	var out []LocalTable
	for rows.Next() {
		var t LocalTable
		if err := rows.Scan(&t.Database, &t.Table, &t.Comment); err != nil {
			return nil, fmt.Errorf("ddl: scan member tables: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ddl: list member tables: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Database < out[j].Database
	})
	return out, nil
}

// ListStageTables returns, sorted, the StageDB table names that start with
// prefix (a node's scratch prefix, spec §8.5). An empty prefix is refused: no
// caller may list, and so drop, another node's scratch.
func ListStageTables(ctx context.Context, conn clickhouse.Conn, m MemberPinned, prefix string) ([]string, error) {
	if err := validateMember(conn, m); err != nil {
		return nil, err
	}
	if prefix == "" {
		return nil, errors.New("ddl: ListStageTables needs a prefix")
	}
	rows, err := conn.Query(ctx, `SELECT name FROM system.tables WHERE database = ? AND startsWith(name, ?) ORDER BY name`, m.StageDB, prefix)
	if err != nil {
		return nil, fmt.Errorf("ddl: list stage tables: %w", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("ddl: scan stage tables: %w", err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ddl: list stage tables: %w", err)
	}
	return out, nil
}
