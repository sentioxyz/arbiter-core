package verifier

import (
	"context"
	"fmt"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/chexec"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/evidence"
)

// NewReplayCore assembles the real replay verifier for this verifier role.
func NewReplayCore(cfg Config, conn clickhouse.Conn, manifests replay.SnapshotStore, payloads replay.PayloadStore) (*replay.Verifier, error) {
	cfg = cloneReplayConfig(cfg)
	core, err := evidence.NewReplayCore(evidence.ReplayCoreConfig{NetworkID: cfg.NetworkID, ReplicaID: cfg.ReplicaID, Ed25519Seed: cfg.Ed25519Seed, Tables: cfg.Tables}, chexec.NewMaterializer(cfg.NetworkID, conn), manifests, payloads)
	if err != nil {
		return nil, err
	}
	dispatcher, ok := core.Executor.(*snapshotquery.CompositeExecutor)
	if !ok {
		return nil, fmt.Errorf("verifier: unexpected replay dispatcher %T", core.Executor)
	}
	core.Executor = newReplayExecutorMemo(cfg, dispatcher)
	core.SchemaHashes = payloadexec.SchemaHashes{NetworkID: cfg.NetworkID, Tables: cloneReplayConfig(cfg).Tables}
	return core, nil
}

// SnapshotQueryReplayCore adapts the query-only verifier to the role's
// dispatch port. Construction is explicit: callers that do not install a
// verified query route and historical policy leave the role default-off.
type SnapshotQueryReplayCore struct {
	verifier *snapshotquery.Verifier
}

// NewSnapshotQueryReplayCore installs no routes or authority itself. The
// caller must construct snapshotquery.Verifier with its immutable dispatcher,
// signer, and authenticated historical policy before injecting this core.
func NewSnapshotQueryReplayCore(v *snapshotquery.Verifier) (*SnapshotQueryReplayCore, error) {
	if v == nil {
		return nil, fmt.Errorf("snapshot query verifier is required")
	}
	return &SnapshotQueryReplayCore{verifier: v}, nil
}

// VerifySnapshotQuery forwards the exact trusted reference without deriving
// or normalising it. snapshotquery.Verifier owns the query-specific validation
// and receipt signing order.
func (c *SnapshotQueryReplayCore) VerifySnapshotQuery(ctx context.Context, job replay.SnapshotQueryJob, referenceID string) (replay.SnapshotQueryAttestation, error) {
	if c == nil || c.verifier == nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("snapshot query verifier is not configured")
	}
	return c.verifier.Verify(ctx, snapshotquery.VerifyRequest{Job: job, ReferenceID: referenceID})
}

// CHScanner recomputes byte-side part commitments from this verifier's
// ClickHouse: the evidence package's scanner over the verifier's hg_unsafe.
type CHScanner = evidence.CHScanner

// NewScanner builds a ClickHouse-backed byte-side scanner over the
// configured tables only.
func NewScanner(cfg Config, conn clickhouse.Conn) *CHScanner {
	return NewRegistryScanner(cfg, conn, nil)
}

// NewRegistryScanner builds a scanner that resolves tables through the table
// registry while it is enabled (nil registry: configured tables only).
func NewRegistryScanner(cfg Config, conn clickhouse.Conn, registry dataplane.RegistryView) *CHScanner {
	if cfg.UnsafeDatabase == "" {
		cfg.UnsafeDatabase = defaultUnsafeDatabase
	}
	return evidence.NewCHScanner(cfg.NetworkID, cfg.Tables, cfg.UnsafeDatabase, conn, registry)
}
