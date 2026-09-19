package verifier

import (
	"context"
	"fmt"
	"strings"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/snapshotquery"
)

// historicalSnapshotQueryProjection is the narrow, detached view of an
// already authenticated private reservation record. It deliberately has no
// Arbiter FSM dependency and is not a wire or runtime configuration type.
//
// RequestID is retained as an historical identity: a source must not return a
// projection without the request which originally created the reservation.
type historicalSnapshotQueryProjection struct {
	Found             bool
	Terminal          bool
	NetworkID         string
	RequestID         string
	ClientAccount     string
	StatementID       string
	ReservationID     string
	FencingGeneration uint64
}

// historicalSnapshotQueryProjectionSource is a private C5 injection point.
// Implementations authenticate their records before returning them; this
// adapter only checks that the detached result still binds exactly to the job.
type historicalSnapshotQueryProjectionSource interface {
	SnapshotQueryHistoricalReservation(context.Context, replay.SnapshotQueryJob) (historicalSnapshotQueryProjection, error)
}

// historicalSnapshotQueryTrustedReference supplies the registered external
// reference after the historical projection has passed every local gate. Its
// output is passed through verbatim and is never derived from a job or a
// reservation field.
type historicalSnapshotQueryTrustedReference interface {
	SnapshotQueryHistoricalReference(context.Context, historicalSnapshotQueryProjection) (string, error)
}

// historicalSnapshotQueryAdapter is intentionally injection-only. A caller
// must explicitly construct it and inject it into both SnapshotQuery and
// SnapshotQueryReference dependencies; New never installs it by default.
type historicalSnapshotQueryAdapter struct {
	core       SnapshotQueryCore
	source     historicalSnapshotQueryProjectionSource
	references historicalSnapshotQueryTrustedReference
}

func newHistoricalSnapshotQueryAdapter(
	v *snapshotquery.Verifier,
	source historicalSnapshotQueryProjectionSource,
	references historicalSnapshotQueryTrustedReference,
) (*historicalSnapshotQueryAdapter, error) {
	core, err := NewSnapshotQueryReplayCore(v)
	if err != nil {
		return nil, err
	}
	if source == nil || references == nil {
		return nil, fmt.Errorf("snapshot query historical projection source and trusted reference are required")
	}
	return &historicalSnapshotQueryAdapter{core: core, source: source, references: references}, nil
}

func (a *historicalSnapshotQueryAdapter) VerifySnapshotQuery(ctx context.Context, job replay.SnapshotQueryJob, referenceID string) (replay.SnapshotQueryAttestation, error) {
	if a == nil || a.core == nil {
		return replay.SnapshotQueryAttestation{}, fmt.Errorf("snapshot query verifier is not configured")
	}
	return a.core.VerifySnapshotQuery(ctx, cloneSnapshotQueryJob(job), referenceID)
}

func (a *historicalSnapshotQueryAdapter) SnapshotQueryReference(ctx context.Context, job replay.SnapshotQueryJob) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if a == nil || a.source == nil || a.references == nil {
		return "", fmt.Errorf("snapshot query historical adapter is not configured")
	}
	job = cloneSnapshotQueryJob(job)
	projection, err := a.source.SnapshotQueryHistoricalReservation(ctx, job)
	if err != nil {
		return "", fmt.Errorf("snapshot query historical reservation: %w", err)
	}
	if err := projection.checkJob(job); err != nil {
		return "", err
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	referenceID, err := a.references.SnapshotQueryHistoricalReference(ctx, projection)
	if err != nil {
		return "", fmt.Errorf("snapshot query trusted reference: %w", err)
	}
	if strings.TrimSpace(referenceID) == "" {
		return "", fmt.Errorf("snapshot query trusted reference is required")
	}
	return referenceID, nil
}

func (p historicalSnapshotQueryProjection) checkJob(job replay.SnapshotQueryJob) error {
	if !p.Found {
		return fmt.Errorf("snapshot query historical reservation was not found")
	}
	if p.Terminal {
		return fmt.Errorf("snapshot query historical reservation is terminal")
	}
	if strings.TrimSpace(p.NetworkID) == "" || strings.TrimSpace(p.RequestID) == "" ||
		strings.TrimSpace(p.ClientAccount) == "" || strings.TrimSpace(p.StatementID) == "" ||
		strings.TrimSpace(p.ReservationID) == "" {
		return fmt.Errorf("snapshot query historical reservation projection is incomplete")
	}
	r := job.Reservation
	if p.NetworkID != r.ReadSnapshot.NetworkID {
		return fmt.Errorf("snapshot query historical reservation network mismatch")
	}
	if p.ClientAccount != r.ClientAccount {
		return fmt.Errorf("snapshot query historical reservation account mismatch")
	}
	if p.StatementID != r.StatementID {
		return fmt.Errorf("snapshot query historical reservation statement mismatch")
	}
	if p.ReservationID != r.ReservationID {
		return fmt.Errorf("snapshot query historical reservation id mismatch")
	}
	if p.FencingGeneration != r.FencingGeneration {
		return fmt.Errorf("snapshot query historical reservation fence mismatch")
	}
	return nil
}

// cloneSnapshotQueryJob stops injected historical sources from retaining or
// mutating nested caller-owned job slices. Keep the copy structural rather
// than serializing through wire: an injection point receives exact job values,
// including nil-versus-empty collection states.
func cloneSnapshotQueryJob(job replay.SnapshotQueryJob) replay.SnapshotQueryJob {
	out := job
	input := &out.Statement.Envelope.Input
	input.ReadSet.Tables = append([]replay.SnapshotReadTable(nil), input.ReadSet.Tables...)
	for i := range input.ReadSet.Tables {
		table := &input.ReadSet.Tables[i]
		table.PartitionRoots = append([]replay.PartitionCommitment(nil), table.PartitionRoots...)
		table.ActiveParts = append([]replay.SnapshotReadPart(nil), table.ActiveParts...)
	}
	if job.SourceClaim != nil {
		claim := *job.SourceClaim
		claim.PartitionDeltas = append([]replay.PartitionCommitment(nil), claim.PartitionDeltas...)
		claim.PartitionCommitmentsAfter = append([]replay.PartitionCommitment(nil), claim.PartitionCommitmentsAfter...)
		claim.CandidateParts = append([]replay.SnapshotReadPart(nil), claim.CandidateParts...)
		out.SourceClaim = &claim
	}
	return out
}
