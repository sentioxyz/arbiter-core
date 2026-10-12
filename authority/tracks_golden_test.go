package authority

import (
	"testing"

	"github.com/sentioxyz/arbiter-core"
)

// goldenCleanup is a one-part promotion cleanup.
func goldenCleanup() arbiter.UnsafeCleanup {
	return arbiter.UnsafeCleanup{TableID: "db1.t", PartitionID: "all", PromotionSeq: 7,
		Parts: []arbiter.PartRef{{TableID: "db1.t", PartitionID: "all", PartRowLtHash: "0xaa", PartName: "all_1_1_0"}}}
}

// TestCleanupCommandHashWithoutRejectedBlockIsFrozen pins the cmd_hash every
// release before the tracks stage computes for a promotion cleanup: the leader
// signs it and every SNode authorizes it, so rejected_block_seq must stay out
// of it while zero (housegate spec 2026-10-10 §7.5; plan S2-A clarification
// C4). Computed outside Go as sha256 over
// "housegate-replay-mvp-v0:arbiter-cleanup-command-v1" + "\x00" +
// {"table_id":"db1.t","partition_id":"all","promotion_seq":7,"parts":[{"table_id":"db1.t","partition_id":"all","part_row_lthash":"0xaa","part_name":"all_1_1_0"}]}.
// Never regenerate this value.
func TestCleanupCommandHashWithoutRejectedBlockIsFrozen(t *testing.T) {
	got, err := CleanupCommandHash(goldenCleanup())
	if err != nil {
		t.Fatal(err)
	}
	const want = "0xa59e973a18b018c944f92c2a012f03264bc5cfee319a3ff29bf3835e37acb0df"
	if got != want {
		t.Fatalf("CleanupCommandHash = %s, want %s", got, want)
	}
}
