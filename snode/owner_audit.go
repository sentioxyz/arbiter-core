package snode

import (
	"errors"
	"time"
)

// The owner hg_safe audit (housegate spec 2026-10-14 §14; plan S3-A contract
// §3, clarifications K1-K4). A ClickHouse restart within old_parts_lifetime
// reactivates the parts a REPLACE PARTITION retired, under their old names and
// with the hash_of_all_files of their replacements (spec §2), so hg_safe
// serves rows twice and the next promotion of the partition fails its shadow
// closure gate. Before building each promotion's shadow, at startup and after
// every ClickHouse restart, the owner compares each partition's active parts
// with the newer of its own last acknowledged inventory and the arbiter's
// SafeParts.

// OwnerAuditMode selects what the audit does with an unclean partition.
type OwnerAuditMode string

const (
	// OwnerAuditReport, also the meaning of "", audits, logs at error level
	// and counts, but never drops a part and never holds a table back.
	OwnerAuditReport OwnerAuditMode = "report"
	// OwnerAuditRepair drops each resurrected equal-hash twin that carries no
	// acknowledged name (clarification K2) and holds an unclean table
	// not-ready: TableReady is false, so the host answers it Pending, and the
	// SNode builds no shadow for an unclean partition.
	OwnerAuditRepair OwnerAuditMode = "repair"
)

// OwnerAuditReason says why a partition is unclean.
type OwnerAuditReason string

const (
	// OwnerAuditUnacknowledgedPart: an active part whose name and
	// hash_of_all_files the reference does not carry; in repair mode it is
	// dropped only when it is the twin of an acknowledged part.
	OwnerAuditUnacknowledgedPart OwnerAuditReason = "unacknowledged_part"
	// OwnerAuditAcknowledgedPartMissing: a part of the reference is not active.
	OwnerAuditAcknowledgedPartMissing OwnerAuditReason = "acknowledged_part_missing"
	// OwnerAuditFSMAhead: the arbiter recorded an applied promotion of the
	// partition past this SNode's own promotion watermark.
	OwnerAuditFSMAhead OwnerAuditReason = "fsm_ahead"
	// OwnerAuditFSMDisagrees: this SNode and the arbiter acknowledged
	// different inventories for the same promotion.
	OwnerAuditFSMDisagrees OwnerAuditReason = "fsm_disagrees"
	// OwnerAuditNoAcknowledgedInventory: the partition has parts and there is
	// no reference at all (no local inventory, arbiter unreadable).
	OwnerAuditNoAcknowledgedInventory OwnerAuditReason = "no_acknowledged_inventory"
	// OwnerAuditUnresolvedIntent: a promotion intent of the partition is
	// journaled and not finalized; the audit skips it (clarification K4).
	OwnerAuditUnresolvedIntent OwnerAuditReason = "unresolved_intent"
)

// The audit's triggers, the keys of OwnerAuditStats.Runs.
const (
	OwnerAuditTriggerStartup           = "startup"
	OwnerAuditTriggerPromotion         = "promotion"
	OwnerAuditTriggerClickHouseRestart = "clickhouse_restart"
)

// ErrSafePartitionUnclean refuses, in repair mode, a promotion of a partition
// the audit found unclean, before its shadow is built; the subscription
// redelivers the command.
var ErrSafePartitionUnclean = errors.New("snode: hg_safe partition failed the owner audit")

// OwnerAuditFinding is the current unclean finding of one partition: why, the
// parts it names, the trigger of the audit that found it and when.
type OwnerAuditFinding struct {
	TableID     string
	PartitionID string
	Reason      OwnerAuditReason
	Parts       []string
	Trigger     string
	At          time.Time
}

// OwnerAuditStats is a point-in-time copy of the audit's counters for the host
// to export: the mode, the runs per trigger (every trigger present), the
// current findings sorted by table and partition, the twins dropped, the
// ClickHouse restarts detected, the arbiter reads that failed, and the end of
// the last run.
type OwnerAuditStats struct {
	Mode               OwnerAuditMode
	Runs               map[string]uint64
	Unclean            []OwnerAuditFinding
	DroppedTwins       uint64
	ClickHouseRestarts uint64
	FSMUnavailable     uint64
	LastRun            time.Time
}
