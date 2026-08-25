package ddl

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

func TestFatalReconcileError(t *testing.T) {
	fatal := []error{
		fmt.Errorf("wrapped: %w", ErrProtocolTableDrift),
		fmt.Errorf("wrapped: %w", ErrProtocolTableMissing),
		fmt.Errorf("wrapped: %w", ErrPhysicalTableNameCollision),
		fmt.Errorf("wrapped: %w", ErrPartitionFreeze),
		fmt.Errorf("wrapped: %w", payloadexec.ErrUnsupportedColumnType),
	}
	for _, err := range fatal {
		if !FatalReconcileError(err) {
			t.Errorf("FatalReconcileError(%v) = false, want true", err)
		}
	}

	transient := []error{
		errors.New("read tcp 127.0.0.1:9000: connection reset by peer"),
		fmt.Errorf("ddl: read system.tables for hg_unsafe.db__t: %w", errors.New("EOF")),
		errors.New("code: 210, message: Connection refused"),
	}
	for _, err := range transient {
		if FatalReconcileError(err) {
			t.Errorf("FatalReconcileError(%v) = true, want false", err)
		}
	}
}

func TestReconcileBackoff_IsBoundedByTheInterval(t *testing.T) {
	interval := 60 * time.Second
	for _, tc := range []struct {
		failures int
		want     time.Duration
	}{
		{failures: 0, want: time.Second},
		{failures: 1, want: time.Second},
		{failures: 2, want: 2 * time.Second},
		{failures: 3, want: 4 * time.Second},
		{failures: 20, want: interval},
	} {
		if got := ReconcileBackoff(tc.failures, interval); got != tc.want {
			t.Errorf("ReconcileBackoff(%d, %s) = %s, want %s", tc.failures, interval, got, tc.want)
		}
	}
	if got := ReconcileBackoff(20, 500*time.Millisecond); got != 500*time.Millisecond {
		t.Fatalf("backoff must never exceed the interval, got %s", got)
	}
}

func TestReconcileBackoff_DoesNotOverflowBeforeTheIntervalCap(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	if got := ReconcileBackoff(1000, maxDuration); got != maxDuration {
		t.Fatalf("ReconcileBackoff overflowed before its cap: got %s, want %s", got, maxDuration)
	}
}
