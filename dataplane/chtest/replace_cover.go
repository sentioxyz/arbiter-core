package chtest

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
)

// ReplaceLeavesACoveringPart reports, for the server conn reaches, whether
// REPLACE PARTITION leaves an empty part covering the replaced block range
// (arbiter-core#88). Measured on upstream images:
//
//   - 25.8 and 26.3: no covering part. The parts a REPLACE retired stay
//     outdated, uncovered, for old_parts_lifetime, and a restart inside that
//     window makes them active again beside their replacements (M3).
//   - 26.8: a covering part, outdated from the start. A restart keeps the
//     retired parts outdated; the covering part itself is active, with no
//     rows, until remove_empty_parts retires it about a minute later.
//
// A version in neither group fails the test: measure it and add it here.
func ReplaceLeavesACoveringPart(t *testing.T, conn clickhouse.Conn) bool {
	t.Helper()
	var version string
	if err := conn.QueryRow(context.Background(), "SELECT version()").Scan(&version); err != nil {
		t.Fatalf("SELECT version(): %v", err)
	}
	covers, err := replaceLeavesACoveringPart(version)
	if err != nil {
		t.Fatal(err)
	}
	return covers
}

func replaceLeavesACoveringPart(version string) (bool, error) {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return false, fmt.Errorf("cannot read ClickHouse version %q", version)
	}
	major, err1 := strconv.Atoi(parts[0])
	minor, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil {
		return false, fmt.Errorf("cannot read ClickHouse version %q", version)
	}
	switch {
	case major < 26 || major == 26 && minor <= 3:
		return false, nil
	case major == 26 && minor >= 8:
		return true, nil
	case major > 26:
		return true, nil
	}
	return false, fmt.Errorf("ClickHouse %s: whether REPLACE PARTITION leaves a covering part is not measured for 26.4-26.7", version)
}
