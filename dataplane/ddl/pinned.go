package ddl

const (
	RowIDColumn = "_hg_row_id"
	RowIDType   = "FixedString(32)"

	EngineReplicatedMergeTree = "ReplicatedMergeTree"
	EngineMergeTree           = "MergeTree"
)

// Pinned carries the deployment-specific inputs of the protocol DDL: the three
// database names, the replica name (the node id the role registers with the
// Arbiter) and the keeper shard (0 in v1).
type Pinned struct {
	UnsafeDB, SafeDB, PromoteDB string
	NodeID                      string
	KeeperShardID               uint32
}

// PinnedSetting is one frozen MergeTree setting (spec D3).
type PinnedSetting struct {
	Name  string
	Value string
}

// UnsafeSettings are the hg_unsafe pins, in DDL order.
func UnsafeSettings() []PinnedSetting {
	return []PinnedSetting{
		{Name: "max_bytes_to_merge_at_max_space_in_pool", Value: "0"},
		{Name: "parts_to_delay_insert", Value: "1000"},
		{Name: "parts_to_throw_insert", Value: "3000"},
		{Name: "max_parts_in_total", Value: "100000"},
		{Name: "replicated_deduplication_window", Value: "0"},
	}
}

// SafeSettings are the hg_safe pins (interim: merges stay stopped, spec §6).
func SafeSettings() []PinnedSetting {
	return []PinnedSetting{{Name: "max_bytes_to_merge_at_max_space_in_pool", Value: "0"}}
}
