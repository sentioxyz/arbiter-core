package chparts

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
)

// rulesV3PartitionCase is one partition key of table admission rules v3 with
// literal values of its source column c, chosen to land in several
// partitions.
type rulesV3PartitionCase struct {
	key, columnType string
	values          []string
}

func rulesV3PartitionCases() []rulesV3PartitionCase {
	return []rulesV3PartitionCase{
		// No empty String: QuotePartition refuses p_ (pre-existing, not v3).
		{"c", "String", []string{"'a'", "'it\\'s'", "'with space'", "'tuple()'"}},
		{"c", "UInt8", []string{"0", "255"}},
		{"c", "Int64", []string{"-9223372036854775808", "-1", "9223372036854775807"}},
		{"c", "UInt64", []string{"0", "18446744073709551615"}},
		{"c", "Date", []string{"'1970-01-01'", "'2149-06-06'"}},
		{"c", "Date32", []string{"'1900-01-01'", "'2299-12-31'"}},
		{"c", "DateTime('UTC')", []string{"'1970-01-01 00:00:00'", "'2106-02-07 06:28:15'"}},
		{"c", "DateTime64(3, 'UTC')", []string{"'1969-12-31 23:59:59.999'", "'2024-01-15 23:59:59.999'"}},
		{"c", "DateTime64(9, 'UTC')", []string{"'2024-01-15 23:59:59.123456789'", "'2024-01-15 23:59:59.123456788'"}},
		{"toYYYYMM(c)", "DateTime('UTC')", []string{"'2024-01-31 23:59:59'", "'2024-02-01 00:00:00'"}},
		{"toYYYYMMDD(c)", "Date32", []string{"'1900-01-01'", "'2299-12-31'"}},
		{"toDate(c)", "DateTime('UTC')", []string{"'2024-01-14 23:59:59'", "'2024-01-15 00:00:00'"}},
		{"toStartOfMonth(c)", "Date", []string{"'2024-01-31'", "'2024-02-29'"}},
		{"toMonday(c)", "Date", []string{"'1970-01-03'", "'2024-02-29'"}},
		{"toStartOfDay(c)", "DateTime('UTC')", []string{"'2024-01-15 10:00:00'", "'2024-01-16 00:00:00'"}},
		{"intDiv(c, 1000)", "Int64", []string{"-1500", "-999", "1000"}},
		{"intDiv(c, 4294967295)", "UInt32", []string{"0", "4294967295"}},
	}
}

// TestQuotePartitionSelectsRulesV3Partitions: for every rules-v3 partition key
// shape, ddl.Intents creates hg_safe as ClickHouse prints it back
// (VerifyProtocolTable finds no drift), each part's logical partition id is
// one the key can name (ValidatePartitionID), and QuotePartition's literal
// '<text>' selects exactly that partition in ATTACH PARTITION ... FROM,
// REPLACE PARTITION ... FROM and DROP PARTITION (DropPartitionIfPresent).
func TestQuotePartitionSelectsRulesV3Partitions(t *testing.T) {
	conn := requireCH(t)
	ctx := context.Background()
	for i, tc := range rulesV3PartitionCases() {
		t.Run(tc.key+"/"+tc.columnType, func(t *testing.T) {
			sch := payloadexec.TableSchema{TableID: fmt.Sprintf("chparts.v3_%d", i), PartitionBy: tc.key,
				Columns: []lthash.Column{{Name: "c", Type: tc.columnType}, {Name: "v", Type: "UInt64"}}}
			key, err := payloadexec.ParsePartitionKey(sch)
			if err != nil {
				t.Fatal(err)
			}
			db, table := safeTable(t, conn, sch)
			_, safe, _, err := ddl.Intents(ddl.Pinned{UnsafeDB: db + "_unsafe", SafeDB: db, PromoteDB: db + "_promote", NodeID: "chparts"}, sch)
			if err != nil {
				t.Fatal(err)
			}
			if err := ddl.VerifyProtocolTable(ctx, conn, safe); err != nil {
				t.Fatalf("VerifyProtocolTable: %v", err)
			}
			tuples := make([]string, len(tc.values))
			for j, v := range tc.values {
				tuples[j] = fmt.Sprintf("('%s', %s, %d)", rowID(uint64(j)), v, j)
			}
			mustExec(t, conn, fmt.Sprintf("INSERT INTO %s.%s (_hg_row_id, c, v) VALUES %s", db, table, strings.Join(tuples, ", ")))
			parts, err := ActiveParts(ctx, conn, db, table)
			if err != nil || len(parts) < 2 {
				t.Fatalf("active parts %+v, %v: want at least two partitions", parts, err)
			}
			rows := map[string]uint64{}
			for _, p := range parts {
				id := LogicalPartitionID(sch, p)
				if err := key.ValidatePartitionID(id); err != nil {
					t.Fatalf("part %s: %v", p.Name, err)
				}
				rows[id] += p.Rows
			}
			mustExec(t, conn, fmt.Sprintf("CREATE TABLE %s.x AS %s.%s", db, db, table))
			mustExec(t, conn, fmt.Sprintf("SYSTEM STOP MERGES %s.x", db))
			count := func() uint64 {
				var n uint64
				if err := conn.QueryRow(ctx, fmt.Sprintf("SELECT count() FROM %s.x", db)).Scan(&n); err != nil {
					t.Fatal(err)
				}
				return n
			}
			for id, want := range rows {
				literal, err := QuotePartition(sch, id)
				if err != nil {
					t.Fatal(err)
				}
				mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s.x ATTACH PARTITION %s FROM %s.%s", db, literal, db, table))
				if got := count(); got != want {
					t.Fatalf("ATTACH PARTITION %s FROM: %d rows, want %d", literal, got, want)
				}
				dropped, err := DropPartitionIfPresent(ctx, conn, db, "x", sch, id)
				if err != nil || !dropped || count() != 0 {
					t.Fatalf("DROP PARTITION %s: dropped=%v err=%v, %d rows left", literal, dropped, err, count())
				}
				mustExec(t, conn, fmt.Sprintf("ALTER TABLE %s.x REPLACE PARTITION %s FROM %s.%s", db, literal, db, table))
				if got := count(); got != want {
					t.Fatalf("REPLACE PARTITION %s FROM: %d rows, want %d", literal, got, want)
				}
				if _, err := DropPartitionIfPresent(ctx, conn, db, "x", sch, id); err != nil {
					t.Fatal(err)
				}
			}
		})
	}
}
