package verifier

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// The replay core verifies a table-set transition block from the job alone:
// the added schema rides in the job and no ClickHouse connection is touched.
func TestNewReplayCore_VerifiesTableSetTransition(t *testing.T) {
	cfg := testConfigV()
	snapshots := payloadexec.NewMemSnapshotStore()
	core, err := NewReplayCore(cfg, nil, snapshots, payloadexec.NewMemPayloadStore())
	if err != nil {
		t.Fatalf("NewReplayCore: %v", err)
	}
	genesis, err := payloadexec.New(cfg.NetworkID, cfg.Tables...).GenesisSnapshot(0, cfg.SchemaSnapshotID, cfg.ExecutorProfileID)
	if err != nil {
		t.Fatal(err)
	}
	snapshots.Put(genesis)
	added := payloadexec.TableSchema{TableID: "db.new", Columns: []lthash.Column{{Name: "v", Type: "UInt64"}}}
	addedJSON, err := json.Marshal(added)
	if err != nil {
		t.Fatal(err)
	}
	hashes := map[string]string{"db.t": genesis.Tables[0].SchemaHash, "db.new": payloadexec.TableSchemaHash(cfg.NetworkID, added)}
	newRoot := payloadexec.SchemaRootFromHashes(hashes)
	job := replay.ReplayJob{
		BlockSeq: 1, PrevSafeSnapshotID: genesis.SnapshotID, PrevStateRoot: genesis.StateRoot,
		SchemaSnapshotID: cfg.SchemaSnapshotID, ExecutorProfileID: cfg.ExecutorProfileID,
		TableSetTransition: &replay.ReplayTableSetTransition{
			Adds:          []replay.ReplayTableSchema{{TableID: "db.new", SchemaJSON: string(addedJSON)}},
			NewSchemaRoot: newRoot,
		},
	}
	att, err := core.Verify(context.Background(), job)
	if err != nil {
		t.Fatalf("Verify(transition): %v", err)
	}
	_, want, err := replay.AssembleStateRoot(cfg.SchemaSnapshotID, newRoot, cfg.ExecutorProfileID, []replay.TableManifest{
		{TableID: "db.new", SchemaHash: hashes["db.new"]}, {TableID: "db.t", SchemaHash: hashes["db.t"]},
	})
	if err != nil {
		t.Fatal(err)
	}
	if att.Receipt.ComputedStateRoot != want || att.Signature == "" {
		t.Fatalf("transition receipt = %+v, want computed root %s", att.Receipt, want)
	}
	if _, ok := core.SchemaHashes.(replay.JobSchemaHashSource); !ok {
		t.Fatalf("schema hash source %T must resolve job-carried schemas", core.SchemaHashes)
	}
}

// A transition block claims no parts, so the arbiter dispatches an empty
// byte-side scan; answering it must not touch ClickHouse.
func TestScanner_EmptyRequestTouchesNoClickHouse(t *testing.T) {
	scans, err := NewScanner(testConfigV(), nil).Scan(context.Background(), nil)
	if err != nil || len(scans) != 0 {
		t.Fatalf("empty scan = %+v, %v", scans, err)
	}
}
