package sources

import (
	"errors"
	"fmt"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
)

// TestNeedsOfManifestAndCandidateParts: a need carries the part's logical
// table id and its owner-side physical name (ddl.CHTableName), and every
// field the pulls and the restore read.
func TestNeedsOfManifestAndCandidateParts(t *testing.T) {
	want := Need{
		TableKey: "dex.swaps", Table: "dex__swaps", PartitionID: "p_2026", PartName: "p_1_1_0",
		PartPhysHash: "4adff322bb8fd41ea3d90f10b04424fb", PartRowLtHash: "0x01", RowCount: 3,
	}
	if got := NeedOfManifestPart(replay.PartManifestEntry{
		TableID: "dex.swaps", PartitionID: "p_2026", PartName: "p_1_1_0", PartPhysHash: "4adff322bb8fd41ea3d90f10b04424fb",
		PartRowLtHash: "0x01", RowCount: 3, Bytes: 400,
	}); got != want {
		t.Fatalf("NeedOfManifestPart = %+v, want %+v", got, want)
	}
	if got := NeedOfCandidatePart(arbiter.CandidatePart{
		TableID: "dex.swaps", PartitionID: "p_2026", PartName: "p_1_1_0", PartPhysHash: "4adff322bb8fd41ea3d90f10b04424fb",
		PartRowLtHash: "0x01", RowCount: 3, Bytes: 400,
	}); got != want {
		t.Fatalf("NeedOfCandidatePart = %+v, want %+v", got, want)
	}
	if got := SourceNames(); fmt.Sprint(got) != "[owner_pull peer_unsafe_pull restore owner_unsafe_pull]" {
		t.Fatalf("SourceNames = %v", got)
	}
	if got := Outcomes(); fmt.Sprint(got) != "[fetched missed refused unreachable]" {
		t.Fatalf("Outcomes = %v", got)
	}
}

// TestOutcomeOfClickHouseErrors pins plan S3-C2 K35's mapping: a memory
// limit, too many simultaneous queries or a timeout is refused; a network
// error, a socket timeout, failed connection tries or a remote table whose
// structure cannot be fetched is unreachable; anything else is refused.
func TestOutcomeOfClickHouseErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want Outcome
	}{
		{&clickhouse.Exception{Code: 241, Message: "Query memory limit exceeded"}, OutcomeRefused},
		{&clickhouse.Exception{Code: 202, Message: "Too many simultaneous queries"}, OutcomeRefused},
		{&clickhouse.Exception{Code: 159, Message: "Timeout exceeded"}, OutcomeRefused},
		{&clickhouse.Exception{Code: 210, Message: "Connection refused"}, OutcomeUnreachable},
		{&clickhouse.Exception{Code: 209, Message: "Timeout exceeded while reading from socket"}, OutcomeUnreachable},
		{&clickhouse.Exception{Code: 279, Message: "All connection tries failed"}, OutcomeUnreachable},
		{&clickhouse.Exception{Code: 519, Message: "All attempts to get table structure failed"}, OutcomeUnreachable},
		{fmt.Errorf("pull: %w", &clickhouse.Exception{Code: 279}), OutcomeUnreachable},
		{&clickhouse.Exception{Code: 497, Message: "Not enough privileges"}, OutcomeRefused},
		{&clickhouse.Exception{Code: 60, Message: "Unknown table"}, OutcomeRefused},
		{errors.New("read tcp 127.0.0.1:9000: i/o timeout"), OutcomeRefused},
	} {
		if got := outcomeOf(tc.err); got != tc.want {
			t.Errorf("outcomeOf(%v) = %s, want %s", tc.err, got, tc.want)
		}
	}
}
