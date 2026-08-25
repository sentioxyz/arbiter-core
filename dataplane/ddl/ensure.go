package ddl

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// Mode selects what EnsureProtocolTables may do.
type Mode int

const (
	// ModeOff leaves DDL ownership to the host. Role production wiring cannot
	// reach it; SchemaSourceUnmanaged preserves explicit test harnesses.
	ModeOff Mode = iota
	// ModeVerifyOnly verifies existing tables without creating anything.
	ModeVerifyOnly
	// ModeCreateAndVerify creates missing tables and verifies their live shape.
	ModeCreateAndVerify
)

// SchemaSource names where a role's authoritative table schemas come from.
// Spec L D2: the protocol-table mode is DERIVED from it, so a deployment can
// never silently end up with the lifecycle disabled by omitting a field.
type SchemaSource string

const (
	// SchemaSourceNetworkState resolves schemas from the network-state
	// registry; the role may create protocol tables.
	SchemaSourceNetworkState SchemaSource = "network_state"
	// SchemaSourceChain resolves schemas from the on-chain declaration; the
	// role may create protocol tables.
	SchemaSourceChain SchemaSource = "chain"
	// SchemaSourceClickHouse derives schemas from the local ClickHouse, so the
	// role can only verify: creating from what it reads would be circular.
	SchemaSourceClickHouse SchemaSource = "clickhouse"
	// SchemaSourceUnmanaged is TEST/HARNESS ONLY: the host owns protocol DDL.
	// Production config loaders must reject it; it exists so in-package tests
	// that create their own tables keep a way to express that intent
	// explicitly instead of relying on a fail-open zero value.
	SchemaSourceUnmanaged SchemaSource = "unmanaged"
)

// ModeFromSchemaSource is the only supported way to obtain a Mode for a role.
func ModeFromSchemaSource(source SchemaSource) (Mode, error) {
	switch source {
	case SchemaSourceNetworkState, SchemaSourceChain:
		return ModeCreateAndVerify, nil
	case SchemaSourceClickHouse:
		return ModeVerifyOnly, nil
	case SchemaSourceUnmanaged:
		return ModeOff, nil
	default:
		return ModeOff, fmt.Errorf("ddl: unknown schema source %q (want network_state|chain|clickhouse, or unmanaged in tests)", source)
	}
}

// DefaultReconcileInterval is the periodic role reconciliation cadence.
const DefaultReconcileInterval = 60 * time.Second

func (m Mode) String() string {
	switch m {
	case ModeOff:
		return "off"
	case ModeVerifyOnly:
		return "verify"
	case ModeCreateAndVerify:
		return "create"
	default:
		return fmt.Sprintf("Mode(%d)", int(m))
	}
}

// ParseMode parses off, verify, or create.
func ParseMode(value string) (Mode, error) {
	switch value {
	case "off":
		return ModeOff, nil
	case "verify":
		return ModeVerifyOnly, nil
	case "create":
		return ModeCreateAndVerify, nil
	default:
		return ModeOff, fmt.Errorf("ddl: unknown ensure-tables mode %q (want off|verify|create)", value)
	}
}

// EnsureProtocolTables creates (when permitted) and verifies hg_unsafe,
// hg_safe and hg_promote for every startup schema. Partition-freeze violations
// are skipped with a warning; missing or drifted protocol tables fail closed.
func EnsureProtocolTables(ctx context.Context, conn clickhouse.Conn, pinned Pinned, tables []payloadexec.TableSchema, mode Mode, logger *slog.Logger) error {
	if err := ValidatePhysicalTableNames(tables); err != nil {
		return err
	}
	if mode == ModeOff {
		return nil
	}
	if logger == nil {
		logger = slog.Default()
	}
	if conn == nil {
		return errors.New("ddl: clickhouse connection is required")
	}
	if pinned.UnsafeDB == "" || pinned.SafeDB == "" || pinned.PromoteDB == "" || pinned.NodeID == "" {
		return errors.New("ddl: Pinned needs UnsafeDB, SafeDB, PromoteDB and NodeID")
	}
	// Compile and validate the complete batch before issuing any DDL. In
	// particular, a fatal declaration after a valid one must not leave a
	// partially-created protocol-table set that then fails closed on restart.
	plan := make([]TableIntent, 0, len(tables)*3)
	for _, table := range tables {
		unsafe, safe, promote, err := Intents(pinned, table)
		if err != nil {
			if errors.Is(err, ErrPartitionFreeze) {
				logger.Warn("skipping protocol tables for declaration outside the partition freeze", "table_id", table.TableID, "err", err)
				continue
			}
			return err
		}
		plan = append(plan, unsafe, safe, promote)
	}
	if mode == ModeCreateAndVerify {
		for _, database := range []string{pinned.UnsafeDB, pinned.SafeDB, pinned.PromoteDB} {
			if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(database)); err != nil {
				return fmt.Errorf("ddl: create database %s: %w", database, err)
			}
		}
	}
	var errs []error
	for _, intent := range plan {
		if mode == ModeCreateAndVerify {
			if err := conn.Exec(ctx, intent.SQL()); err != nil {
				errs = append(errs, fmt.Errorf("ddl: create %s.%s: %w", intent.Database, intent.Table, err))
				continue
			}
		}
		if err := VerifyProtocolTable(ctx, conn, intent); err != nil {
			errs = append(errs, err)
		}
	}
	if err := errors.Join(errs...); err != nil {
		return err
	}
	logger.Info("protocol tables ensured", "mode", mode.String(), "tables", len(tables), "unsafe_db", pinned.UnsafeDB, "safe_db", pinned.SafeDB, "node_id", pinned.NodeID)
	return nil
}
