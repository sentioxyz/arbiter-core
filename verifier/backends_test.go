package verifier

import (
	"context"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

func TestNewReplayCore_AssemblesVerifier(t *testing.T) {
	cfg := testConfigV()
	snapshots := payloadexec.NewMemSnapshotStore()
	payloads := payloadexec.NewMemPayloadStore()

	core, err := NewReplayCore(cfg, nil, snapshots, payloads)
	if err != nil {
		t.Fatalf("NewReplayCore: %v", err)
	}
	if core.Snapshots != snapshots || core.Payloads != payloads || core.Executor == nil || core.Signer == nil {
		t.Fatalf("incomplete replay core: %+v", core)
	}
	if _, ok := core.Executor.(*snapshotquery.CompositeExecutor); !ok {
		t.Fatalf("replay core executor %T is not the query dispatcher", core.Executor)
	}
}

func TestNewReplayCore_QueryDispatchStaysDefaultOff(t *testing.T) {
	cfg := testConfigV()
	core, err := NewReplayCore(cfg, nil, payloadexec.NewMemSnapshotStore(), payloadexec.NewMemPayloadStore())
	if err != nil {
		t.Fatalf("NewReplayCore: %v", err)
	}
	_, err = core.Executor.Replay(context.Background(), replay.ExecutionRequest{
		SnapshotQuery: &replay.SnapshotQueryJob{
			ExecutorProfileID: "executor-b5",
			QueryProfileID:    "0x" + strings.Repeat("ab", 32),
		},
		SnapshotQueryReferenceID: "replay:ns:1",
	})
	if err == nil || !strings.Contains(err.Error(), "snapshot query profile route unavailable") {
		t.Fatalf("default-off query dispatch: %v", err)
	}
}

func TestNewReplayCore_RejectsBadSignerSeed(t *testing.T) {
	cfg := testConfigV()
	cfg.Ed25519Seed = []byte("bad")

	_, err := NewReplayCore(cfg, nil, payloadexec.NewMemSnapshotStore(), payloadexec.NewMemPayloadStore())
	if err == nil || !strings.Contains(err.Error(), "signer") {
		t.Fatalf("bad signer seed must fail, got %v", err)
	}
}

func TestNewReplayCore_WiresSchemaHashSourceFromTables(t *testing.T) {
	cfg := testConfigV()
	core, err := NewReplayCore(cfg, nil, payloadexec.NewMemSnapshotStore(), payloadexec.NewMemPayloadStore())
	if err != nil {
		t.Fatalf("NewReplayCore: %v", err)
	}
	if core.SchemaHashes == nil {
		t.Fatal("verifier must verify signed schema_hash against its own tables")
	}
	got, ok := core.SchemaHashes.TableSchemaHash("db.t")
	if !ok || got != payloadexec.TableSchemaHash(cfg.NetworkID, cfg.Tables[0]) {
		t.Fatalf("schema hash for db.t = %q ok=%v", got, ok)
	}
	if _, ok := core.SchemaHashes.TableSchemaHash("db.unknown"); ok {
		t.Fatal("unknown table must not resolve")
	}
}

func TestNewScanner_DefaultsUnsafeDatabaseAndMapsTableName(t *testing.T) {
	cfg := testConfigV()

	scanner := NewScanner(cfg, nil)

	if scanner.cfg.UnsafeDatabase != "hg_unsafe" {
		t.Fatalf("unsafe db default: %q", scanner.cfg.UnsafeDatabase)
	}
	if got := ddl.CHTableName("db.table.with.dots"); got != "db__table__with__dots" {
		t.Fatalf("physical table name: %q", got)
	}
}
