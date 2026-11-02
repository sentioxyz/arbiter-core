package evidence

import (
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
)

// ReplayCoreConfig names the replica whose replay core NewReplayCore builds:
// its network, the id and ed25519 seed that sign its receipts, and the
// genesis table set.
type ReplayCoreConfig struct {
	NetworkID, ReplicaID string
	Ed25519Seed          []byte
	Tables               []payloadexec.TableSchema
}

// NewReplayCore assembles the real replay verifier over the materializer m:
// the verifier passes chexec.NewMaterializer (scratch in its connection's
// default database), a member chexec.NewMaterializerInWithStoragePolicy (scratch in hg_rstage
// under its prefix).
func NewReplayCore(cfg ReplayCoreConfig, m payloadexec.Materializer, manifests replay.SnapshotStore, payloads replay.PayloadStore) (*replay.Verifier, error) {
	signer, err := payloadexec.NewEd25519Signer(cfg.ReplicaID, cfg.Ed25519Seed)
	if err != nil {
		return nil, fmt.Errorf("verifier signer: %w", err)
	}
	// NewDynamic: the configured tables are the genesis set; a table-set
	// transition job and the schemas a job carries extend it per job, so the
	// replay needs no registry access (dynamic SI table set, sub-project 2b).
	payload := payloadexec.NewDynamic(cfg.NetworkID, m, cfg.Tables...)
	// Query routes stay empty so the snapshot-query lane remains default-off.
	dispatcher, err := snapshotquery.NewCompositeExecutor(payload, nil)
	if err != nil {
		return nil, fmt.Errorf("verifier dispatcher: %w", err)
	}
	return &replay.Verifier{
		Snapshots:    manifests,
		Payloads:     payloads,
		Executor:     dispatcher,
		Signer:       signer,
		SchemaHashes: payloadexec.SchemaHashes{NetworkID: cfg.NetworkID, Tables: cfg.Tables},
	}, nil
}
