package snode

import (
	"errors"
	"testing"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

type recordedBindingMutation struct {
	name   string
	want   error
	mutate func(*intakeRecord)
}

func recordedBindingMutations() []recordedBindingMutation {
	return []recordedBindingMutation{
		{
			name: "unsupported payload format",
			want: ErrEncodingNotSupported,
			mutate: func(rec *intakeRecord) {
				rec.Envelope.PayloadFormat = "csv-with-names-v1"
				rec.PayloadEncoding = rec.Envelope.PayloadFormat
			},
		},
		{
			name: "payload encoding disagrees with envelope",
			want: ErrPayloadMismatch,
			mutate: func(rec *intakeRecord) {
				rec.PayloadEncoding = "csv-with-names-v1"
			},
		},
		{
			name: "signed and recorded revision are zero",
			want: ErrPayloadMismatch,
			mutate: func(rec *intakeRecord) {
				rec.Envelope.ClientRevision = 0
				rec.Revision = 0
			},
		},
		{
			name: "recorded revision is zero",
			want: ErrPayloadMismatch,
			mutate: func(rec *intakeRecord) {
				rec.Revision = 0
			},
		},
		{
			name: "recorded revision disagrees with envelope",
			want: ErrPayloadMismatch,
			mutate: func(rec *intakeRecord) {
				rec.Revision++
			},
		},
	}
}

func newRecordedBindingTestRole(t *testing.T) (*Role, *sourceClaimsFake) {
	t.Helper()
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	return newIntakeHarness(t, unexpectedQueryConn{}, cfg)
}

func TestLookupPreparedStatement_RejectsInvalidRecordedBindings(t *testing.T) {
	for _, lifecycle := range []IntakeLifecycle{LifecycleUnsafeWritten, LifecyclePreparing, LifecycleAbortPending} {
		for _, mutation := range recordedBindingMutations() {
			t.Run(string(lifecycle)+"/"+mutation.name, func(t *testing.T) {
				role, _ := newRecordedBindingTestRole(t)
				rec := seedBoundIntakeRecord(t, role, lifecycle)
				mutation.mutate(&rec)
				overwriteIntakeRecordForTest(t, role.journal, rec)

				got, found, err := role.LookupPreparedStatement(t.Context(), rec.StatementID)
				if !errors.Is(err, mutation.want) {
					t.Fatalf("invalid recorded binding must reject with %v: result=%+v found=%v err=%v", mutation.want, got, found, err)
				}
				if found {
					t.Fatal("invalid recorded binding must not return a cached prepare")
				}
				stored, ok, loadErr := role.journal.load(rec.StatementID)
				if loadErr != nil || !ok || stored.Lifecycle != lifecycle {
					t.Fatalf("lookup rejection must leave %s durable: ok=%v err=%v record=%+v", lifecycle, ok, loadErr, stored)
				}
			})
		}
	}
}

func TestRegisterPreparedClaim_RejectsInvalidRecordedBindings(t *testing.T) {
	for _, lifecycle := range []IntakeLifecycle{LifecycleUnsafeWritten, LifecyclePreparing, LifecycleAbortPending} {
		for _, mutation := range recordedBindingMutations() {
			t.Run(string(lifecycle)+"/"+mutation.name, func(t *testing.T) {
				role, claims := newRecordedBindingTestRole(t)
				rec := seedBoundIntakeRecord(t, role, lifecycle)
				mutation.mutate(&rec)
				overwriteIntakeRecordForTest(t, role.journal, rec)

				if _, err := role.RegisterPreparedClaim(t.Context(), rec.StatementID); !errors.Is(err, mutation.want) {
					t.Fatalf("invalid recorded binding must reject with %v, got %v", mutation.want, err)
				}
				if got := claims.count(); got != 0 {
					t.Fatalf("invalid recorded binding must not reach Arbiter, got %d claim calls", got)
				}
				stored, ok, loadErr := role.journal.load(rec.StatementID)
				if loadErr != nil || !ok || stored.Lifecycle != lifecycle {
					t.Fatalf("claim rejection must leave %s durable: ok=%v err=%v record=%+v", lifecycle, ok, loadErr, stored)
				}
			})
		}
	}
}

func TestAbortPreparedStatement_RejectsInvalidRecordedBindings(t *testing.T) {
	for _, lifecycle := range []IntakeLifecycle{LifecycleUnsafeWritten, LifecyclePreparing, LifecycleAbortPending} {
		for _, mutation := range recordedBindingMutations() {
			t.Run(string(lifecycle)+"/"+mutation.name, func(t *testing.T) {
				role, _ := newRecordedBindingTestRole(t)
				rec := seedBoundIntakeRecord(t, role, lifecycle)
				mutation.mutate(&rec)
				overwriteIntakeRecordForTest(t, role.journal, rec)

				if err := role.AbortPreparedStatement(t.Context(), rec.StatementID, nil, "recovery"); !errors.Is(err, mutation.want) {
					t.Fatalf("invalid recorded binding must reject with %v, got %v", mutation.want, err)
				}
				stored, ok, loadErr := role.journal.load(rec.StatementID)
				if loadErr != nil || !ok || stored.Lifecycle != lifecycle {
					t.Fatalf("abort rejection must leave %s durable: ok=%v err=%v record=%+v", lifecycle, ok, loadErr, stored)
				}
			})
		}
	}
}

func TestConvergeStartup_RejectsInvalidRecordedBindings(t *testing.T) {
	for _, lifecycle := range []IntakeLifecycle{LifecyclePreparing, LifecycleAbortPending} {
		for _, mutation := range recordedBindingMutations() {
			t.Run(string(lifecycle)+"/"+mutation.name, func(t *testing.T) {
				role, _ := newRecordedBindingTestRole(t)
				rec := seedBoundIntakeRecord(t, role, lifecycle)
				mutation.mutate(&rec)
				overwriteIntakeRecordForTest(t, role.journal, rec)

				if err := role.convergeStartup(t.Context()); !errors.Is(err, mutation.want) {
					t.Fatalf("invalid recorded binding must reject with %v, got %v", mutation.want, err)
				}
				stored, ok, loadErr := role.journal.load(rec.StatementID)
				if loadErr != nil || !ok || stored.Lifecycle != lifecycle {
					t.Fatalf("startup rejection must leave %s durable: ok=%v err=%v record=%+v", lifecycle, ok, loadErr, stored)
				}
			})
		}
	}
}
