package verifier

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/wire"
)

type historicalProjectionSourceFake struct {
	mu         sync.Mutex
	projection historicalSnapshotQueryAuthenticatedRecord
	err        error
	jobs       []replay.SnapshotQueryJob
	mutate     func(*replay.SnapshotQueryJob)
}

func (f *historicalProjectionSourceFake) SnapshotQueryHistoricalReservation(_ context.Context, job replay.SnapshotQueryJob) (historicalSnapshotQueryAuthenticatedRecord, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.jobs = append(f.jobs, cloneSnapshotQueryJob(job))
	if f.mutate != nil {
		f.mutate(&job)
	}
	return f.projection, f.err
}

func (f *historicalProjectionSourceFake) snapshot() []replay.SnapshotQueryJob {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]replay.SnapshotQueryJob(nil), f.jobs...)
}

type historicalReferenceFake struct {
	mu          sync.Mutex
	referenceID string
	err         error
	projections []historicalSnapshotQueryAuthenticatedRecord
}

func (f *historicalReferenceFake) SnapshotQueryHistoricalReference(_ context.Context, projection historicalSnapshotQueryAuthenticatedRecord) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.projections = append(f.projections, projection)
	return f.referenceID, f.err
}

func (f *historicalReferenceFake) snapshot() []historicalSnapshotQueryAuthenticatedRecord {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]historicalSnapshotQueryAuthenticatedRecord(nil), f.projections...)
}

func historicalJob() replay.SnapshotQueryJob {
	return replay.SnapshotQueryJob{
		BlockSeq: 9,
		Reservation: replay.SnapshotQueryReservation{
			ReservationID: "reservation-1", FencingGeneration: 7, ClientAccount: "account-1", StatementID: "statement-1",
			ReadSnapshot: replay.SnapshotPin{NetworkID: "network-1"},
		},
		Statement:   replay.SnapshotQueryStatement{Envelope: replay.SnapshotQueryEnvelope{Input: replay.SnapshotQueryInput{ReadSet: replay.SnapshotReadSet{Tables: []replay.SnapshotReadTable{{ActiveParts: []replay.SnapshotReadPart{{PartName: "part-1"}}}}}}}},
		SourceClaim: &replay.SnapshotQueryClaim{CandidateParts: []replay.SnapshotReadPart{{PartName: "claim-part"}}},
	}
}

// signedHistoricalJob keeps historicalJob's reservation identity but carries a
// genuine v3 envelope. A test that drives the role must clear its signature
// gate to reach the historical boundary at all; historicalJob itself stays
// unsigned so the adapter-level detachment test keeps its nested read-set
// slices, which the immutable identity fixture does not have.
func signedHistoricalJob(t *testing.T) replay.SnapshotQueryJob {
	t.Helper()
	job := historicalJob()
	job.Statement.Envelope = signedSnapshotQueryJob(t).Statement.Envelope
	return job
}

func historicalProjectionFor(job replay.SnapshotQueryJob) historicalSnapshotQueryAuthenticatedRecord {
	return historicalSnapshotQueryAuthenticatedRecord{
		Found: true, NetworkID: job.Reservation.ReadSnapshot.NetworkID, RequestID: "request-1", ClientAccount: job.Reservation.ClientAccount,
		StatementID: job.Reservation.StatementID, ReservationID: job.Reservation.ReservationID, FencingGeneration: job.Reservation.FencingGeneration,
	}
}

func newHistoricalAdapterForTest(core SnapshotQueryCore, source *historicalProjectionSourceFake, references *historicalReferenceFake) *historicalSnapshotQueryAdapter {
	return &historicalSnapshotQueryAdapter{core: core, source: source, references: references}
}

func TestHistoricalSnapshotQueryAdapter_GatesProjectionBeforeReferenceOrSubmission(t *testing.T) {
	job := signedHistoricalJob(t)
	for name, mutate := range map[string]func(*historicalSnapshotQueryAuthenticatedRecord){
		"not found":   func(p *historicalSnapshotQueryAuthenticatedRecord) { p.Found = false },
		"terminal":    func(p *historicalSnapshotQueryAuthenticatedRecord) { p.Terminal = true },
		"network":     func(p *historicalSnapshotQueryAuthenticatedRecord) { p.NetworkID = "wrong" },
		"request":     func(p *historicalSnapshotQueryAuthenticatedRecord) { p.RequestID = "" },
		"account":     func(p *historicalSnapshotQueryAuthenticatedRecord) { p.ClientAccount = "wrong" },
		"statement":   func(p *historicalSnapshotQueryAuthenticatedRecord) { p.StatementID = "wrong" },
		"reservation": func(p *historicalSnapshotQueryAuthenticatedRecord) { p.ReservationID = "wrong" },
		"fence":       func(p *historicalSnapshotQueryAuthenticatedRecord) { p.FencingGeneration++ },
	} {
		t.Run(name, func(t *testing.T) {
			projection := historicalProjectionFor(job)
			mutate(&projection)
			source := &historicalProjectionSourceFake{projection: projection}
			references := &historicalReferenceFake{referenceID: "external-reference"}
			core := &fakeSnapshotQueryCore{}
			adapter := newHistoricalAdapterForTest(core, source, references)
			role, server := newRoleHarnessVWithSnapshotQuery(t, adapter, adapter)

			err := role.handleSnapshotQueryJob(context.Background(), wire.SnapshotQueryJobToPB(job))
			if err == nil || !strings.Contains(err.Error(), "historical reservation") {
				t.Fatalf("error=%v", err)
			}
			if got := references.snapshot(); len(got) != 0 {
				t.Fatalf("reference called after rejected projection: %+v", got)
			}
			if jobs, refs := core.snapshot(); len(jobs) != 0 || len(refs) != 0 {
				t.Fatalf("core called after rejected projection: jobs=%+v refs=%q", jobs, refs)
			}
			if got := server.queryAttestationsSnapshot(); len(got) != 0 {
				t.Fatalf("unexpected submission: %+v", got)
			}
		})
	}
}

func TestHistoricalSnapshotQueryAuthenticatedRecord_RequestIDIsSourceBoundNotJobComparable(t *testing.T) {
	job := historicalJob()
	// SnapshotQueryJob v2 deliberately has no RequestID. These two complete
	// records differ only in request identity, so this adapter boundary cannot
	// choose between them. A real source must authenticate and resolve that
	// association before it injects either record.
	for _, requestID := range []string{"request-a", "unrelated-request-b"} {
		record := historicalProjectionFor(job)
		record.RequestID = requestID
		if err := record.checkJob(job); err != nil {
			t.Fatalf("request %q rejected despite no job-addressable comparison: %v", requestID, err)
		}
	}
}

func TestHistoricalSnapshotQueryAdapter_SourceAndCoreGetDetachedJobsAndExactReference(t *testing.T) {
	job := historicalJob()
	source := &historicalProjectionSourceFake{projection: historicalProjectionFor(job), mutate: func(got *replay.SnapshotQueryJob) {
		got.Statement.Envelope.Input.ReadSet.Tables[0].ActiveParts[0].PartName = "mutated"
		got.SourceClaim.CandidateParts[0].PartName = "mutated-claim"
	}}
	references := &historicalReferenceFake{referenceID: " external/reference "}
	core := &fakeSnapshotQueryCore{}
	adapter := newHistoricalAdapterForTest(core, source, references)

	reference, err := adapter.SnapshotQueryReference(context.Background(), job)
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	if reference != " external/reference " {
		t.Fatalf("reference=%q", reference)
	}
	wantJob := job
	if got := source.snapshot(); len(got) != 1 || !reflect.DeepEqual(got[0], wantJob) {
		t.Fatalf("source jobs=%+v want=%+v", got, wantJob)
	}
	if job.Statement.Envelope.Input.ReadSet.Tables[0].ActiveParts[0].PartName != "part-1" || job.SourceClaim.CandidateParts[0].PartName != "claim-part" {
		t.Fatalf("source mutated caller job: %+v", job)
	}
	if _, err := adapter.VerifySnapshotQuery(context.Background(), job, reference); err != nil {
		t.Fatalf("verify: %v", err)
	}
	jobs, refs := core.snapshot()
	if len(jobs) != 1 || !reflect.DeepEqual(jobs[0], wantJob) || !reflect.DeepEqual(refs, []string{" external/reference "}) {
		t.Fatalf("core calls jobs=%+v refs=%q", jobs, refs)
	}
}

func TestHistoricalSnapshotQueryAdapter_ReferenceErrorOrCancelCannotReachCore(t *testing.T) {
	job := signedHistoricalJob(t)
	canceledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	for name, tc := range map[string]struct {
		ctx          context.Context
		sourceErr    error
		referenceErr error
	}{
		"reference error": {ctx: context.Background(), referenceErr: errors.New("reference failed")},
		"source error":    {ctx: context.Background(), sourceErr: errors.New("source failed")},
		"canceled":        {ctx: canceledCtx},
	} {
		t.Run(name, func(t *testing.T) {
			source := &historicalProjectionSourceFake{projection: historicalProjectionFor(job), err: tc.sourceErr}
			references := &historicalReferenceFake{referenceID: "ref", err: tc.referenceErr}
			core := &fakeSnapshotQueryCore{}
			adapter := newHistoricalAdapterForTest(core, source, references)
			role, server := newRoleHarnessVWithSnapshotQuery(t, adapter, adapter)
			if err := role.handleSnapshotQueryJob(tc.ctx, wire.SnapshotQueryJobToPB(job)); err == nil {
				t.Fatal("expected failure")
			}
			if jobs, refs := core.snapshot(); len(jobs) != 0 || len(refs) != 0 {
				t.Fatalf("core called: jobs=%+v refs=%q", jobs, refs)
			}
			if got := server.queryAttestationsSnapshot(); len(got) != 0 {
				t.Fatalf("unexpected submission: %+v", got)
			}
		})
	}
}

func TestNewHistoricalSnapshotQueryAdapter_DefaultDisabledAndRequiresDependencies(t *testing.T) {
	if _, err := newHistoricalSnapshotQueryAdapter(nil, nil, nil); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("nil verifier error=%v", err)
	}
	// Role construction does not install this adapter: the existing nil deps are
	// still the default-disabled path and refuse before any submission.
	role, server := newRoleHarnessV(t, &fakeReplayCore{}, &fakeScanner{})
	if err := role.handleSnapshotQueryJob(context.Background(), wire.SnapshotQueryJobToPB(historicalJob())); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Fatalf("default-disabled error=%v", err)
	}
	if got := server.queryAttestationsSnapshot(); len(got) != 0 {
		t.Fatalf("default-disabled submitted: %+v", got)
	}
}
