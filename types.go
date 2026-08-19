// Package arbiter defines the frozen P0 interface seams and canonical
// command/coordinate types of the Sentio Arbiter (design §3.4). It stays
// dependency-light so every subpackage (fsm, accumulator, orchestrator,
// raftnode, server, authority) can import it without cycles.
package arbiter

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
)

// StatementCoord is the statement_id uniqueness coordinate: one statement
// per (account, client_seq); client_nonce is NOT part of the key (§6.1).
type StatementCoord struct {
	Account   string `json:"account"`
	ClientSeq uint64 `json:"client_seq"`
}

// TablePartition addresses one partition of one logical table.
type TablePartition struct {
	TableID     string `json:"table_id"`
	PartitionID string `json:"partition_id"`
}

// StatementIDString renders the canonical flat statement_id used by the
// replay projection (pkg/replay.Statement.StatementID), RCRecord linkage,
// and _hg_row_id derivation: "<lowercase account>:<decimal seq>:<nonce>".
// The accumulator's binary leaf encoding is frozen separately (P0b plan);
// this string form is the cross-component linking id.
func StatementIDString(clientAccount string, clientSeq uint64, clientNonce string) string {
	return strings.ToLower(clientAccount) + ":" + strconv.FormatUint(clientSeq, 10) + ":" + clientNonce
}

// PartRef identifies a verified part by content commitment (design §8.1).
// JSON tags mirror the arbiter-proto PartRef field names: these structs are
// the CANONICAL SIGNING FORM — authority JWS payloads hash them through
// replay.CanonicalDigest, never re-encoded proto bytes (§4.3).
type PartRef struct {
	TableID       string `json:"table_id"`
	PartitionID   string `json:"partition_id"`
	PartRowLtHash string `json:"part_row_lthash"`
	PartName      string `json:"part_name,omitempty"`
}

// PromoteSafePartition is the canonical signing form of the promotion
// command (design §8.1); wire form is pb.PromoteSafePartition.
type PromoteSafePartition struct {
	TableID            string    `json:"table_id"`
	PartitionID        string    `json:"partition_id"`
	PromotionSeq       uint64    `json:"promotion_seq"`
	BaseSafeSnapshotID string    `json:"base_safe_snapshot_id"`
	BasePartitionRoot  string    `json:"base_partition_root"`
	CandidateParts     []PartRef `json:"candidate_parts"`
}

// UnsafeCleanup is the canonical signing form of the promoted-unsafe-part
// cleanup command (design §8.5); wire form is pb.UnsafeCleanup.
type UnsafeCleanup struct {
	TableID      string    `json:"table_id"`
	PartitionID  string    `json:"partition_id"`
	PromotionSeq uint64    `json:"promotion_seq"`
	Parts        []PartRef `json:"parts"`
}

// ---- Canonical Go mirror types (P1a, design §3) ----
//
// JSON tags are byte-equal to the arbiter-proto field names and are pinned
// by conformance/arbiter_wire_test.go. These structs are the canonical
// forms: FSM state, canonical hashing (via replay.CanonicalDigest), and
// authority signing all use them; proto is transport only (§4.3).

// StatementKind mirrors pb.StatementKind. v1 admits INSERT only.
type StatementKind int32

const (
	StatementKindUnspecified StatementKind = 0
	StatementKindInsert      StatementKind = 1
)

// AdmissionCode mirrors pb.AdmissionCode (numbers pinned by conformance).
type AdmissionCode int32

const (
	AdmissionCodeUnspecified        AdmissionCode = 0
	AdmissionCodeAccepted           AdmissionCode = 1
	AdmissionCodeDuplicateClientSeq AdmissionCode = 2
	AdmissionCodeSchemaNotAllowed   AdmissionCode = 3
	AdmissionCodeKindNotAdmitted    AdmissionCode = 4
	AdmissionCodeInvalidSignature   AdmissionCode = 5
	AdmissionCodeInvalidProof       AdmissionCode = 6
	AdmissionCodeMalformed          AdmissionCode = 7
	// AdmissionCodeGapBudgetExceeded: the P0b K=64 open-range budget
	// (arbiter-proto v0.2.0 append).
	AdmissionCodeGapBudgetExceeded AdmissionCode = 8
)

// NodeRole mirrors pb.NodeRole.
type NodeRole int32

const (
	NodeRoleUnspecified NodeRole = 0
	NodeRoleVerifier    NodeRole = 1
	NodeRoleSNode       NodeRole = 2
)

// StatementID is the structured client-assigned statement identity
// (uniqueness key = (client_account, client_seq); nonce is entropy, §6.1).
type StatementID struct {
	ClientAccount string `json:"client_account"`
	ClientSeq     uint64 `json:"client_seq"`
	ClientNonce   string `json:"client_nonce"`
}

// Flat renders the canonical flat statement_id string form.
func (id StatementID) Flat() string {
	return StatementIDString(id.ClientAccount, id.ClientSeq, id.ClientNonce)
}

// Coord is the accumulator uniqueness coordinate (account normalized).
func (id StatementID) Coord() StatementCoord {
	return StatementCoord{Account: strings.ToLower(id.ClientAccount), ClientSeq: id.ClientSeq}
}

// StatementEnvelope is the canonical Go form of pb.StatementEnvelopeV2 (the
// V2 suffix is frozen on the wire only; the Go world drops it).
type StatementEnvelope struct {
	StatementID     StatementID   `json:"statement_id"`
	StatementKind   StatementKind `json:"statement_kind"`
	SQL             string        `json:"sql"`
	SQLHash         string        `json:"sql_hash"`
	SettingsHash    string        `json:"settings_hash,omitempty"`
	PayloadRef      string        `json:"payload_ref,omitempty"`
	PayloadHash     string        `json:"payload_hash,omitempty"`
	PayloadLength   uint64        `json:"payload_length,omitempty"`
	TargetTableID   string        `json:"target_table_id,omitempty"`
	UserJWS         string        `json:"user_jws"`
	EnvelopeVersion uint32        `json:"envelope_version"`
	NetworkID       string        `json:"network_id"`
	KeeperShardID   uint32        `json:"keeper_shard_id"`
	PayloadFormat   string        `json:"payload_format"`
	ClientRevision  uint32        `json:"client_revision"`
	SchemaHash      string        `json:"schema_hash"`
	RowIDProfileID  string        `json:"row_id_profile_id"`
}

// CandidatePart is one hg_unsafe part the source claims a statement
// produced; part_row_lthash is its identity everywhere downstream.
type CandidatePart struct {
	TableID       string `json:"table_id"`
	PartitionID   string `json:"partition_id"`
	PartName      string `json:"part_name,omitempty"`
	PartRowLtHash string `json:"part_row_lthash"`
	PartPhysHash  string `json:"part_phys_hash,omitempty"`
	RowCount      uint64 `json:"row_count,omitempty"`
	Bytes         uint64 `json:"bytes,omitempty"`
}

// PartitionLtHashSum is the source's claimed per-partition new-part LtHash
// sum — check 2's "claimed" side (§7.3).
type PartitionLtHashSum struct {
	TableID           string `json:"table_id"`
	PartitionID       string `json:"partition_id"`
	NewPartsLtHashSum string `json:"new_parts_lthash_sum"`
}

// RCRecord is the source's result claim (late-bindable by statement_id).
type RCRecord struct {
	StatementID          StatementID          `json:"statement_id"`
	SourceNode           string               `json:"source_node"`
	CandidateParts       []CandidatePart      `json:"candidate_parts"`
	SourceClaimRoot      string               `json:"source_claim_root"`
	PartitionNewPartSums []PartitionLtHashSum `json:"partition_new_part_sums"`
}

// PartScan is one scanned part's byte-side result (check 3).
type PartScan struct {
	TableID              string `json:"table_id"`
	PartitionID          string `json:"partition_id"`
	ClaimedPartRowLtHash string `json:"claimed_part_row_lthash"`
	ScannedPartRowLtHash string `json:"scanned_part_row_lthash"`
	LivePartName         string `json:"live_part_name,omitempty"`
}

// ByteSideScanBody is the canonical hash/sign form of a scan: the message
// minus its own hash and signature. scan_hash = CanonicalDigest(
// DomainByteSideScan, msg.Body()); signature = ed25519 over the scan_hash
// string bytes, hex — the ReplayAttestation convention.
type ByteSideScanBody struct {
	ReplicaID string     `json:"replica_id"`
	BlockSeq  uint64     `json:"block_seq"`
	Parts     []PartScan `json:"parts"`
}

// ByteSideScanMsg mirrors pb.ByteSideScanMsg.
type ByteSideScanMsg struct {
	ReplicaID string     `json:"replica_id"`
	BlockSeq  uint64     `json:"block_seq"`
	Parts     []PartScan `json:"parts"`
	ScanHash  string     `json:"scan_hash"`
	Signature string     `json:"signature"`
}

// Body returns the canonical hash/sign form.
func (m ByteSideScanMsg) Body() ByteSideScanBody {
	return ByteSideScanBody{ReplicaID: m.ReplicaID, BlockSeq: m.BlockSeq, Parts: m.Parts}
}

// AnchorRef references the L2 anchor of one L3 block (§5.2).
type AnchorRef struct {
	L3BlockHash   string `json:"l3_block_hash"`
	StateRoot     string `json:"state_root"`
	L2TxRef       string `json:"l2_tx_ref,omitempty"`
	L2BlockNumber uint64 `json:"l2_block_number,omitempty"`
	DARef         string `json:"da_ref,omitempty"`
}

// NodeRegistration enters a data-plane node into FSM membership.
type NodeRegistration struct {
	NodeID        string     `json:"node_id"`
	Roles         []NodeRole `json:"roles"`
	Ed25519Pubkey []byte     `json:"ed25519_pubkey"`
	DialAddr      string     `json:"dial_addr,omitempty"`
}

// SafePartMapping records where a promoted part landed in hg_safe.
type SafePartMapping struct {
	PartRowLtHash string `json:"part_row_lthash"`
	SafePartName  string `json:"safe_part_name"`
	PartPhysHash  string `json:"part_phys_hash,omitempty"`
}

// PromotionAck reports REPLACE PARTITION completion (§8.3).
type PromotionAck struct {
	NodeID                  string            `json:"node_id"`
	PromotionSeq            uint64            `json:"promotion_seq"`
	TableID                 string            `json:"table_id"`
	PartitionID             string            `json:"partition_id"`
	PostPartitionCommitment string            `json:"post_partition_commitment"`
	Parts                   []SafePartMapping `json:"parts"`
	Applied                 bool              `json:"applied"`
	Detail                  string            `json:"detail,omitempty"`
}

// CleanupAck acknowledges a scheduled unsafe cleanup.
type CleanupAck struct {
	NodeID       string `json:"node_id"`
	PromotionSeq uint64 `json:"promotion_seq"`
	TableID      string `json:"table_id"`
	PartitionID  string `json:"partition_id"`
}

// MarshalText/UnmarshalText let TablePartition key JSON maps in FSM
// snapshots. NUL is the delimiter — table/partition ids never contain it.
func (p TablePartition) MarshalText() ([]byte, error) {
	return []byte(p.TableID + "\x00" + p.PartitionID), nil
}

func (p *TablePartition) UnmarshalText(b []byte) error {
	i := bytes.IndexByte(b, 0)
	if i < 0 {
		return fmt.Errorf("table partition key: missing NUL delimiter")
	}
	p.TableID, p.PartitionID = string(b[:i]), string(b[i+1:])
	return nil
}
