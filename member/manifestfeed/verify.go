// Package manifestfeed follows the arbiter's published manifests for a
// stage-3 si-member (housegate spec 2026-10-14 §8.4; plan S3-C2 contract
// §21.4, clarification K37): one verifying follower per process walks the
// ParentSnapshotID chain from the safe watermark back to the last accepted
// manifest, never asks for a manifest by block, demultiplexes every accepted
// manifest into its owners' track entries, and keeps verified manifests in a
// bounded LRU store that also serves the shadow verifier's replays.
package manifestfeed

import (
	"errors"
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// ErrManifestRefused is wrapped by every refusal of this package: a manifest
// that fails Verify, or a watermark the follower does not accept.
var ErrManifestRefused = errors.New("manifestfeed: manifest refused")

// refusal keeps a refusal's text verbatim while errors.Is reports
// ErrManifestRefused.
type refusal struct{ msg string }

func (r *refusal) Error() string { return r.msg }

func (r *refusal) Unwrap() error { return ErrManifestRefused }

func refuse(format string, args ...any) error {
	return &refusal{msg: fmt.Sprintf(format, args...)}
}

// Verify is the acceptance rule of spec §8.4 for one manifest fetched by id,
// in this order: it is the manifest that was asked for, it validates (Validate
// recomputes the data, state and manifest roots and checks the track-frontier
// order), its snapshot id is its own manifest root (so the id commits to the
// content), and every partition root equals the sum of the row hashes of the
// parts it lists. It does not require track_frontiers: a block's
// PrevSafeSnapshotID and the genesis manifest may predate the tracks
// activation.
func Verify(requested string, m replay.SafeSnapshotManifest) error {
	if m.SnapshotID != requested {
		return refuse("manifestfeed: GetManifest(%s) answered manifest %s", requested, m.SnapshotID)
	}
	if err := m.Validate(); err != nil {
		return refuse("manifestfeed: manifest %s: %v", m.SnapshotID, err)
	}
	if m.SnapshotID != m.ManifestRoot {
		return refuse("manifestfeed: manifest %s has snapshot_id %s but manifest_root %s", m.SnapshotID, m.SnapshotID, m.ManifestRoot)
	}
	if err := payloadexec.VerifyLedger(m); err != nil {
		return refuse("manifestfeed: manifest %s fails the partition ledger: %v", m.SnapshotID, err)
	}
	return nil
}
