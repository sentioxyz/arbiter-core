package verifier

import (
	"crypto/ed25519"
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
)

type Config struct {
	ReplicaID               string
	Ed25519Seed             []byte
	NetworkID               string
	SchemaSnapshotID        string
	ExecutorProfileID       string
	SchemaRoot              string
	Tables                  []payloadexec.TableSchema
	UnsafeDatabase          string
	SafeDatabase            string
	PromoteDatabase         string
	ProtocolTables          ddl.Mode
	ProtocolTablesReconcile time.Duration
	KeeperShardID           uint32
}

func (c *Config) validate() error {
	var errs []error
	if c.ReplicaID == "" {
		errs = append(errs, errors.New("replica id is required"))
	}
	if len(c.Ed25519Seed) != ed25519.SeedSize {
		errs = append(errs, fmt.Errorf("ed25519 seed must be %d bytes", ed25519.SeedSize))
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
	if c.UnsafeDatabase == "" {
		c.UnsafeDatabase = defaultUnsafeDatabase
	}
	if c.SafeDatabase == "" {
		c.SafeDatabase = defaultSafeDatabase
	}
	if c.PromoteDatabase == "" {
		c.PromoteDatabase = defaultPromoteDatabase
	}
	if c.ProtocolTablesReconcile == 0 {
		c.ProtocolTablesReconcile = ddl.DefaultReconcileInterval
	}
	if len(errs) == 0 {
		if got := payloadexec.SchemaRoot(c.NetworkID, c.Tables); got != c.SchemaRoot {
			errs = append(errs, fmt.Errorf("schema_root mismatch: configured %s, computed %s", c.SchemaRoot, got))
		}
	}
	return errors.Join(errs...)
}
