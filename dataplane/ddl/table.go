package ddl

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ClickHouse error codes the per-table lifecycle distinguishes.
const (
	codeReplicaAlreadyExists = 253 // REPLICA_ALREADY_EXISTS
	codeTableWasNotDropped   = 305 // TABLE_WAS_NOT_DROPPED ("because it's active")
)

// incarnationCommentPrefix starts the COMMENT of every table created for a
// chain-origin registry incarnation.
const incarnationCommentPrefix = "hg_incarnation="

// IncarnationComment is the COMMENT that marks the protocol tables of
// registry incarnation seq. It lets a node tell its tables for the current
// incarnation apart from leftovers of an earlier incarnation of the same key.
func IncarnationComment(seq uint64) string {
	return incarnationCommentPrefix + strconv.FormatUint(seq, 10)
}

// ParseIncarnationComment returns the incarnation a table COMMENT names; ok is
// false for an empty or foreign comment (a genesis table has none).
func ParseIncarnationComment(comment string) (uint64, bool) {
	rest, found := strings.CutPrefix(comment, incarnationCommentPrefix)
	if !found {
		return 0, false
	}
	seq, err := strconv.ParseUint(rest, 10, 64)
	if err != nil || seq == 0 || strconv.FormatUint(seq, 10) != rest {
		return 0, false
	}
	return seq, true
}

// IncarnationIntents is Intents for one registry incarnation: incarnationSeq
// 0 renders the unmarked genesis tables, any other value marks all three with
// IncarnationComment(incarnationSeq).
func IncarnationIntents(p Pinned, t payloadexec.TableSchema, incarnationSeq uint64) (TableIntent, TableIntent, TableIntent, error) {
	unsafe, safe, promote, err := Intents(p, t)
	if err != nil {
		return TableIntent{}, TableIntent{}, TableIntent{}, err
	}
	if incarnationSeq != 0 {
		comment := IncarnationComment(incarnationSeq)
		unsafe.Comment, safe.Comment, promote.Comment = comment, comment, comment
	}
	return unsafe, safe, promote, nil
}

// EnsureTable creates (ModeCreateAndVerify) and verifies the hg_unsafe,
// hg_safe and hg_promote tables of one table; ModeVerifyOnly only verifies and
// ModeOff does nothing. incarnationSeq is 0 for a genesis table and the
// registry incarnation otherwise (IncarnationIntents). A crash between
// ClickHouse registering this node's hg_unsafe replica in Keeper and writing
// the local table leaves the replica behind, and the next CREATE fails with
// REPLICA_ALREADY_EXISTS; EnsureTable then removes the stale replica (refused
// by ClickHouse while any server holds it active) and creates once more. It
// does so only when the table is absent locally, attached or detached: a
// detached table's replica is its own Keeper metadata, never stale.
func EnsureTable(ctx context.Context, conn clickhouse.Conn, p Pinned, t payloadexec.TableSchema, incarnationSeq uint64, mode Mode) error {
	if mode == ModeOff {
		return nil
	}
	if err := validatePinned(conn, p); err != nil {
		return err
	}
	unsafe, safe, promote, err := IncarnationIntents(p, t, incarnationSeq)
	if err != nil {
		return err
	}
	intents := []TableIntent{unsafe, safe, promote}
	if mode == ModeCreateAndVerify {
		if err := ensureDatabases(ctx, conn, p); err != nil {
			return err
		}
		for _, intent := range intents {
			err := conn.Exec(ctx, intent.SQL())
			if err != nil && intent.Engine == EngineReplicatedMergeTree && clickHouseCode(err) == codeReplicaAlreadyExists {
				if dropErr := dropStaleReplica(ctx, conn, p, t.TableID, intent); dropErr != nil {
					return fmt.Errorf("ddl: create %s.%s: stale replica %s: %w", intent.Database, intent.Table, p.NodeID, dropErr)
				}
				err = conn.Exec(ctx, intent.SQL())
			}
			if err != nil {
				return fmt.Errorf("ddl: create %s.%s: %w", intent.Database, intent.Table, err)
			}
		}
	}
	var errs []error
	for _, intent := range intents {
		if err := VerifyProtocolTable(ctx, conn, intent); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// dropStaleReplica removes this node's replica of tableID's hg_unsafe table
// after its CREATE failed with REPLICA_ALREADY_EXISTS. The replica is stale
// only if no local table of that name exists: a detached one (DETACH, DETACH
// PERMANENTLY, or a table that failed to attach) still owns its Keeper
// metadata, and SYSTEM DROP REPLICA would destroy it. Such a table fails the
// create instead, for an operator to attach or drop.
func dropStaleReplica(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID string, intent TableIntent) error {
	var attached, detached uint64
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.tables WHERE database = ? AND name = ?`, intent.Database, intent.Table).Scan(&attached); err != nil {
		return fmt.Errorf("ddl: look up local table %s.%s: %w", intent.Database, intent.Table, err)
	}
	if err := conn.QueryRow(ctx, `SELECT count() FROM system.detached_tables WHERE database = ? AND table = ?`, intent.Database, intent.Table).Scan(&detached); err != nil {
		return fmt.Errorf("ddl: look up detached table %s.%s: %w", intent.Database, intent.Table, err)
	}
	if attached != 0 || detached != 0 {
		return fmt.Errorf("ddl: %s.%s exists locally (attached %d, detached %d); not removing its keeper replica %s", intent.Database, intent.Table, attached, detached, p.NodeID)
	}
	return DropReplica(ctx, conn, p, tableID, p.NodeID)
}

// DropTable drops tableID's hg_promote, hg_safe and hg_unsafe tables, in that
// order, each with DROP TABLE IF EXISTS ... SYNC. Dropping hg_unsafe removes
// this node's replica from Keeper, and the last replica removes the table's
// Keeper path. If a crash separated the local drop from the Keeper removal,
// this node's replica is still listed with no local table; DropTable removes
// it too, so a repeated call converges.
func DropTable(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID string) error {
	if err := validatePinned(conn, p); err != nil {
		return err
	}
	table := CHTableName(tableID)
	for _, database := range []string{p.PromoteDB, p.SafeDB, p.UnsafeDB} {
		if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s.%s SYNC", quoteIdent(database), quoteIdent(table))); err != nil {
			return fmt.Errorf("ddl: drop %s.%s: %w", database, table, err)
		}
	}
	replicas, err := KeeperReplicas(ctx, conn, p, tableID)
	if err != nil {
		return err
	}
	for _, replica := range replicas {
		if replica == p.NodeID {
			return DropReplica(ctx, conn, p, tableID, replica)
		}
	}
	return nil
}

// KeeperReplicas returns the sorted replica names registered under tableID's
// Keeper path; none when the path does not exist.
func KeeperReplicas(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID string) ([]string, error) {
	return keeperChildren(ctx, conn, ZooKeeperPath(p, tableID)+"/replicas")
}

// KeeperUnsafeTables returns the physical table names that have a Keeper path
// under /sentio/<keeper_shard_id>/unsafe.
func KeeperUnsafeTables(ctx context.Context, conn clickhouse.Conn, p Pinned) ([]string, error) {
	return keeperChildren(ctx, conn, fmt.Sprintf("/sentio/%d/unsafe", p.KeeperShardID))
}

// DropReplica removes one replica of tableID's hg_unsafe table from Keeper
// (SYSTEM DROP REPLICA ... FROM ZKPATH). ClickHouse refuses an active replica;
// removing the last replica also removes the table's Keeper path.
func DropReplica(ctx context.Context, conn clickhouse.Conn, p Pinned, tableID, replica string) error {
	path := ZooKeeperPath(p, tableID)
	if err := conn.Exec(ctx, fmt.Sprintf("SYSTEM DROP REPLICA %s FROM ZKPATH %s", quoteLiteral(replica), quoteLiteral(path))); err != nil {
		return fmt.Errorf("ddl: drop replica %s from %s: %w", replica, path, err)
	}
	return nil
}

// ReplicaActive reports whether err is ClickHouse refusing to drop an active
// replica.
func ReplicaActive(err error) bool { return clickHouseCode(err) == codeTableWasNotDropped }

// LocalTable is one table in a protocol database.
type LocalTable struct {
	Database string
	Table    string
	Comment  string
}

// ListProtocolTables returns every table of the three protocol databases,
// sorted by table then database.
func ListProtocolTables(ctx context.Context, conn clickhouse.Conn, p Pinned) ([]LocalTable, error) {
	rows, err := conn.Query(ctx, `SELECT database, name, comment FROM system.tables WHERE database IN (?, ?, ?)`, p.UnsafeDB, p.SafeDB, p.PromoteDB)
	if err != nil {
		return nil, fmt.Errorf("ddl: list protocol tables: %w", err)
	}
	defer rows.Close()
	var out []LocalTable
	for rows.Next() {
		var t LocalTable
		if err := rows.Scan(&t.Database, &t.Table, &t.Comment); err != nil {
			return nil, fmt.Errorf("ddl: scan protocol tables: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ddl: list protocol tables: %w", err)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Table != out[j].Table {
			return out[i].Table < out[j].Table
		}
		return out[i].Database < out[j].Database
	})
	return out, nil
}

func keeperChildren(ctx context.Context, conn clickhouse.Conn, path string) ([]string, error) {
	rows, err := conn.Query(ctx, `SELECT name FROM system.zookeeper WHERE path = ?`, path)
	if err != nil {
		return nil, fmt.Errorf("ddl: list keeper path %s: %w", path, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("ddl: scan keeper path %s: %w", path, err)
		}
		out = append(out, name)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("ddl: list keeper path %s: %w", path, err)
	}
	sort.Strings(out)
	return out, nil
}

// ensureDatabases creates p's three protocol databases if they don't already
// exist. Shared by EnsureTable and EnsureProtocolTables so the two entry
// points issue identical CREATE DATABASE DDL.
func ensureDatabases(ctx context.Context, conn clickhouse.Conn, p Pinned) error {
	for _, database := range []string{p.UnsafeDB, p.SafeDB, p.PromoteDB} {
		if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(database)); err != nil {
			return fmt.Errorf("ddl: create database %s: %w", database, err)
		}
	}
	return nil
}

func validatePinned(conn clickhouse.Conn, p Pinned) error {
	if conn == nil {
		return errors.New("ddl: clickhouse connection is required")
	}
	if p.UnsafeDB == "" || p.SafeDB == "" || p.PromoteDB == "" || p.NodeID == "" {
		return errors.New("ddl: Pinned needs UnsafeDB, SafeDB, PromoteDB and NodeID")
	}
	return nil
}

func clickHouseCode(err error) int32 {
	var exception *clickhouse.Exception
	if errors.As(err, &exception) {
		return exception.Code
	}
	return 0
}
