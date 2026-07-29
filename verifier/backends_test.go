package verifier

import (
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
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
}

func TestNewReplayCore_RejectsBadSignerSeed(t *testing.T) {
	cfg := testConfigV()
	cfg.Ed25519Seed = []byte("bad")

	_, err := NewReplayCore(cfg, nil, payloadexec.NewMemSnapshotStore(), payloadexec.NewMemPayloadStore())
	if err == nil || !strings.Contains(err.Error(), "signer") {
		t.Fatalf("bad signer seed must fail, got %v", err)
	}
}

func TestNewScanner_DefaultsUnsafeDatabaseAndMapsTableName(t *testing.T) {
	cfg := testConfigV()

	scanner := NewScanner(cfg, nil)

	if scanner.cfg.UnsafeDatabase != "hg_unsafe" {
		t.Fatalf("unsafe db default: %q", scanner.cfg.UnsafeDatabase)
	}
	if got := chTableName("db.table.with.dots"); got != "db__table__with__dots" {
		t.Fatalf("physical table name: %q", got)
	}
}
