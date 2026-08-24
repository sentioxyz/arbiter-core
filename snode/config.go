package snode

import (
	"errors"
	"fmt"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

const (
	defaultUnsafeDatabase  = "hg_unsafe"
	defaultSafeDatabase    = "hg_safe"
	defaultPromoteDatabase = "hg_promote"
	// DefaultHardPartsPerPartition stays below the pinned ClickHouse
	// parts_to_throw_insert setting and mirrors ingress back-pressure.
	DefaultHardPartsPerPartition = 2950
)

type Config struct {
	NodeID             string
	NetworkID          string
	SchemaSnapshotID   string
	ExecutorProfileID  string
	SchemaRoot         string
	Tables             []payloadexec.TableSchema
	StateDir           string
	UnsafeDatabase     string
	SafeDatabase       string
	PromoteDatabase    string
	AuthorityAddresses []string
	// SchemaSource names where Tables came from. It derives the protocol-table
	// mode (Spec L D2); there is no configurable mode and no fail-open zero.
	SchemaSource ddl.SchemaSource
	// protocolTables is the derived mode; validate() sets it.
	protocolTables ddl.Mode
	// ProtocolTablesReconcile is the periodic re-run cadence (0 = 60s).
	ProtocolTablesReconcile time.Duration
	// ProtocolTablesMaxFailures bounds consecutive transient reconcile failures
	// before the role exits (0 = ddl.DefaultReconcileMaxFailures).
	ProtocolTablesMaxFailures int
	// KeeperShardID feeds /sentio/<shard>/unsafe/<table>; v1 uses zero.
	KeeperShardID uint32
	// HardPartsPerPartition refuses a prepare before journal or ClickHouse
	// writes when any touched unsafe partition is already at this limit.
	HardPartsPerPartition int
}

func (c *Config) validate() error {
	var errs []error
	if c.NodeID == "" {
		errs = append(errs, errors.New("node id is required"))
	}
	if c.NetworkID == "" {
		errs = append(errs, errors.New("network id is required"))
	}
	if c.SchemaSnapshotID == "" {
		errs = append(errs, errors.New("schema snapshot id is required"))
	}
	if c.ExecutorProfileID == "" {
		errs = append(errs, errors.New("executor profile id is required"))
	}
	if len(c.Tables) == 0 {
		errs = append(errs, errors.New("at least one table schema is required"))
	}
	if err := ddl.ValidatePhysicalTableNames(c.Tables); err != nil {
		errs = append(errs, err)
	}
	for i, tbl := range c.Tables {
		if err := ddl.ValidatePartitionFreeze(tbl); err != nil {
			errs = append(errs, fmt.Errorf("tables[%d] (%s): %w", i, tbl.TableID, err))
		}
		if err := payloadexec.ValidateTableSchemaColumns(tbl); err != nil {
			errs = append(errs, fmt.Errorf("tables[%d]: %w", i, err))
		}
	}
	if c.StateDir == "" {
		errs = append(errs, errors.New("state dir is required"))
	}
	if len(c.AuthorityAddresses) == 0 {
		errs = append(errs, errors.New("at least one authority address is required"))
	}
	if c.UnsafeDatabase == "" {
		c.UnsafeDatabase = defaultUnsafeDatabase
	}
	if c.SafeDatabase == "" {
		c.SafeDatabase = defaultSafeDatabase
	}
	if c.PromoteDatabase == "" {
		c.PromoteDatabase = defaultPromoteDatabase
	}
	if c.ProtocolTablesReconcile < 0 {
		errs = append(errs, errors.New("protocol tables reconcile interval must not be negative"))
	} else if c.ProtocolTablesReconcile == 0 {
		c.ProtocolTablesReconcile = ddl.DefaultReconcileInterval
	}
	if c.ProtocolTablesMaxFailures < 0 {
		errs = append(errs, errors.New("protocol tables reconcile max failures must not be negative"))
	}
	mode, modeErr := ddl.ModeFromSchemaSource(c.SchemaSource)
	if modeErr != nil {
		errs = append(errs, modeErr)
	} else {
		c.protocolTables = mode
	}
	if c.HardPartsPerPartition < 0 {
		errs = append(errs, errors.New("hard parts per partition must not be negative"))
	} else if c.HardPartsPerPartition == 0 {
		c.HardPartsPerPartition = DefaultHardPartsPerPartition
	}
	if len(errs) == 0 {
		if got := payloadexec.SchemaRoot(c.NetworkID, c.Tables); got != c.SchemaRoot {
			errs = append(errs, fmt.Errorf("schema_root mismatch: configured %s, computed %s", c.SchemaRoot, got))
		}
	}
	return errors.Join(errs...)
}

// ProtocolTablesMode derives the mode from SchemaSource without relying on
// validate mutating this Config value. Invalid or unset sources return an
// error instead of silently exposing the ModeOff zero value.
func (c Config) ProtocolTablesMode() (ddl.Mode, error) {
	return ddl.ModeFromSchemaSource(c.SchemaSource)
}
