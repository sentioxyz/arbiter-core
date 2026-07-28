package snode

import (
	"errors"
	"fmt"
	"strings"

	"housegate/housegate/pkg/replay/payloadexec"
)

const (
	defaultUnsafeDatabase  = "hg_unsafe"
	defaultSafeDatabase    = "hg_safe"
	defaultPromoteDatabase = "hg_promote"
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
	for i, tbl := range c.Tables {
		if err := validatePartitionBy(tbl); err != nil {
			errs = append(errs, fmt.Errorf("tables[%d] (%s): %w", i, tbl.TableID, err))
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
	if len(errs) == 0 {
		if got := payloadexec.SchemaRoot(c.NetworkID, c.Tables); got != c.SchemaRoot {
			errs = append(errs, fmt.Errorf("schema_root mismatch: configured %s, computed %s", c.SchemaRoot, got))
		}
	}
	return errors.Join(errs...)
}

// validatePartitionBy enforces the P1c MVP freeze: PARTITION BY a bare String
// column. The source reconstructs logical partition ids from
// system.parts.partition, which matches payloadexec's "p_"+<raw CSV token>
// derivation only for a String column named directly — not an expression, and
// not a type whose ClickHouse partition rendering differs from the raw token.
// Outside the freeze the divergence is a silent check-1 failure (the honest
// source root won't match the verifier's replay); catch it at startup instead.
func validatePartitionBy(t payloadexec.TableSchema) error {
	if t.PartitionBy == "" {
		return nil
	}
	if strings.ContainsAny(t.PartitionBy, "()") {
		return errors.New("partition_by must be a bare column name; expressions are outside the MVP freeze")
	}
	for _, col := range t.Columns {
		if col.Name == t.PartitionBy {
			if col.Type != "String" {
				return fmt.Errorf("partition_by column %q must be String in the MVP freeze, got %s", t.PartitionBy, col.Type)
			}
			return nil
		}
	}
	return fmt.Errorf("partition_by %q names no declared column", t.PartitionBy)
}
