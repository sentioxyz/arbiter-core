package authority

import (
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

// SnapshotQueryAbortPurpose is the token family for C3's authorized terminal
// abort of a sequenced snapshot query. A promotion, cleanup, consensus-update,
// disposition or control token cannot authorize an abort, and an abort token
// authorizes nothing else. It is never inferred from a client cancellation, a
// source's local timeout or an Arbiter leader timer.
const SnapshotQueryAbortPurpose = "housegate-snapshot-query-abort-v1"

// snapshotQueryAbortCommandDomain keeps the signed command digest apart from
// the record's own snapshot-query-abort-v1 digest, which is the receipt's
// abort_record_root: one domain per command kind, as payload.go explains.
const snapshotQueryAbortCommandDomain = "arbiter-snapshot-query-abort-command-v1"

// ValidateSnapshotQueryAbortRecord enforces the constraints every abort record
// must satisfy regardless of committed state. State-dependent validation
// (block, reservation, generation, predecessor and child snapshot ids) belongs
// to the Arbiter FSM. cleanup_authorization_root may be empty here; its
// contract is the FSM's.
func ValidateSnapshotQueryAbortRecord(r replay.SnapshotQueryAbortRecord) error {
	if r.BlockSeq == 0 || r.FencingGeneration == 0 {
		return fmt.Errorf("snapshot query abort: block_seq and fencing_generation must be positive")
	}
	fields := []struct{ name, value string }{
		{"statement_id", r.StatementID}, {"input_root", r.InputRoot}, {"reservation_id", r.ReservationID},
		{"reason_code", r.ReasonCode}, {"prev_snapshot_id", r.PrevSnapshotID}, {"next_snapshot_id", r.NextSnapshotID},
	}
	for _, f := range fields {
		if strings.TrimSpace(f.value) == "" || strings.ContainsRune(f.value, 0) {
			return fmt.Errorf("snapshot query abort: %s must be non-empty and NUL-free", f.name)
		}
	}
	if strings.ContainsRune(r.CleanupAuthorizationRoot, 0) {
		return fmt.Errorf("snapshot query abort: cleanup_authorization_root must be NUL-free")
	}
	if r.PrevSnapshotID == r.NextSnapshotID {
		return fmt.Errorf("snapshot query abort: next_snapshot_id must differ from prev_snapshot_id")
	}
	return nil
}

// SnapshotQueryAbortHash binds every record field under the command domain.
// Both live authorization and deterministic replay use it.
func SnapshotQueryAbortHash(r replay.SnapshotQueryAbortRecord) (string, error) {
	if err := ValidateSnapshotQueryAbortRecord(r); err != nil {
		return "", err
	}
	h, err := replay.CanonicalDigest(snapshotQueryAbortCommandDomain, r)
	if err != nil {
		return "", fmt.Errorf("hash snapshot query abort: %w", err)
	}
	return h, nil
}

// SignSnapshotQueryAbort signs one complete abort record at the current time.
func (s *Signer) SignSnapshotQueryAbort(r replay.SnapshotQueryAbortRecord) (string, error) {
	return s.SignSnapshotQueryAbortAt(r, time.Now().Unix())
}

// SignSnapshotQueryAbortAt signs with an explicit Unix issue time; issue time
// is an API-boundary freshness guard, never a replay guard.
func (s *Signer) SignSnapshotQueryAbortAt(r replay.SnapshotQueryAbortRecord, iat int64) (string, error) {
	h, err := SnapshotQueryAbortHash(r)
	if err != nil {
		return "", err
	}
	return s.signPayload(JWSCommandPayload{Iat: iat, Purpose: SnapshotQueryAbortPurpose, CmdHash: h})
}

// VerifySnapshotQueryAbort checks signature, purpose, command hash and the
// authority allowlist without reading the clock. Raft Apply and snapshot
// restore must use this path; an empty allowlist still fails closed.
func (v *Validator) VerifySnapshotQueryAbort(r replay.SnapshotQueryAbortRecord, token string) (string, error) {
	h, err := SnapshotQueryAbortHash(r)
	if err != nil {
		return "", err
	}
	return v.verify(h, SnapshotQueryAbortPurpose, token, false)
}

// AuthorizeSnapshotQueryAbort additionally enforces token age for an API
// boundary. It must not be used inside replicated Apply or snapshot replay.
func (v *Validator) AuthorizeSnapshotQueryAbort(r replay.SnapshotQueryAbortRecord, token string) (string, error) {
	h, err := SnapshotQueryAbortHash(r)
	if err != nil {
		return "", err
	}
	return v.verify(h, SnapshotQueryAbortPurpose, token, true)
}
