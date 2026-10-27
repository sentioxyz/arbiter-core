package snode

import (
	"context"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
)

// zeroPartRows supplies deterministic metadata and real row-hash input. It
// embeds only the unused driver surface, so any unexpected operation fails.
type zeroPartRows struct {
	driver.Rows
	values [][]any
	index  int
}

func (r *zeroPartRows) Next() bool   { r.index++; return r.index <= len(r.values) }
func (r *zeroPartRows) Err() error   { return nil }
func (r *zeroPartRows) Close() error { return nil }
func (r *zeroPartRows) Scan(dest ...any) error {
	row := r.values[r.index-1]
	if len(dest) != len(row) {
		return fmt.Errorf("scan destinations %d, values %d", len(dest), len(row))
	}
	for n, d := range dest {
		p := reflect.ValueOf(d).Elem()
		v := reflect.ValueOf(row[n])
		if !v.Type().AssignableTo(p.Type()) {
			return fmt.Errorf("column %d: got %T, want %s", n, row[n], p.Type())
		}
		p.Set(v)
	}
	return nil
}

type zeroPartConn struct {
	clickhouse.Conn
	parts       []partInfo
	rowID       []byte
	missingData bool
	scanned     []string
	attributed  []string
	dropped     []string
}

func (c *zeroPartConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	rows := &zeroPartRows{}
	switch {
	case strings.Contains(query, "FROM system.parts"):
		// The shared metadata query is a pure extraction and remains unfiltered.
		if strings.Contains(query, "rows >") || strings.Contains(query, "rows>") {
			return nil, fmt.Errorf("active metadata must retain its original SQL")
		}
		for _, p := range c.parts {
			rows.values = append(rows.values, []any{p.Name, p.PartitionID, p.PartitionValue, p.PhysHash, p.Path, p.Rows, p.Bytes})
		}
	case strings.HasPrefix(query, "SELECT _hg_row_id FROM "):
		name := args[0].(driver.NamedValue).Value.(string)
		c.attributed = append(c.attributed, name)
		for _, p := range c.parts {
			if p.Name == name && p.Rows > 0 && !c.missingData {
				rows.values = append(rows.values, []any{string(c.rowID)})
			}
		}
	case strings.HasPrefix(query, "SELECT _part, "):
		for _, arg := range args {
			name := arg.(string)
			c.scanned = append(c.scanned, name)
			for _, p := range c.parts {
				if p.Name == name && p.Rows > 0 && !c.missingData {
					rows.values = append(rows.values, []any{name, c.rowID, "p0", uint64(7)})
				}
			}
		}
	default:
		return nil, fmt.Errorf("unexpected query %s", query)
	}
	return rows, nil
}

func (c *zeroPartConn) Exec(_ context.Context, query string, _ ...any) error {
	c.dropped = append(c.dropped, query)
	return nil
}

func zeroPartsFixture(t *testing.T) (*Role, *zeroPartConn, intakeRecord, payloadexec.TableSchema) {
	t.Helper()
	schema := intakeSchema()
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	conn := &zeroPartConn{}
	r, _ := newIntakeHarness(t, conn, cfg)
	env := intakeEnvelope([]byte("test payload"))
	rec := intakeRecord{
		StatementID: env.StatementID.Flat(), Envelope: env,
		Lifecycle: LifecyclePreparing, ExpectedRowCount: 1,
		PayloadEncoding: env.PayloadFormat, Revision: int(env.ClientRevision),
		TouchedPartitions: []string{"p_p0"}, PreWriteInventory: map[string][]string{"p_p0": {"p0_73_73_0"}},
	}
	if err := r.journal.save(rec); err != nil {
		t.Fatal(err)
	}
	conn.rowID = payloadexec.RowID(cfg.NetworkID, schema.TableID, rec.StatementID, 0)
	conn.parts = []partInfo{
		{Name: "p0_73_73_1", PartitionValue: "p0", Rows: 0}, // DROP PART's covering tombstone.
		{Name: "p0_77_77_0", PartitionValue: "p0", Rows: 1, PhysHash: strings.Repeat("a", 32), Bytes: 123},
	}
	return r, conn, rec, schema
}

func TestSNodeDiffPartsExcludesZeroRowTombstones(t *testing.T) {
	before := []partInfo{{Name: "old", Rows: 1}}
	after := []partInfo{{Name: "old", Rows: 1}, {Name: "drop_cover", Rows: 0}, {Name: "new", Rows: 1}}
	got := diffParts(before, after)
	if len(got) != 1 || got[0].Name != "new" {
		t.Fatalf("candidates = %+v, want only new with positive rows", got)
	}
}

func TestSNodeFreshIntakeIgnoresZeroRowCleanupPart(t *testing.T) {
	r, c, rec, schema := zeroPartsFixture(t)
	before := []partInfo{{Name: "p0_73_73_0", PartitionValue: "p0", Rows: 1}}
	result, rc, err := r.assembleAfterWrite(t.Context(), rec.Envelope, schema, CHTableName(schema.TableID), before, rec)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.scanned, []string{"p0_77_77_0"}) || len(result.CandidateParts) != 1 || len(rc.CandidateParts) != 1 || rc.CandidateParts[0].PartName != "p0_77_77_0" || rc.CandidateParts[0].RowCount != 1 {
		t.Fatalf("scanned %v, result %+v, RC %+v; want only the actual nonempty insert", c.scanned, result, rc)
	}
}

func TestSNodeConvergenceIgnoresZeroRowCleanupPart(t *testing.T) {
	r, c, rec, schema := zeroPartsFixture(t)
	got, err := r.convergeIntake(t.Context(), rec, schema)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(c.attributed, []string{"p0_77_77_0"}) || !slices.Equal(c.scanned, []string{"p0_77_77_0"}) || got.Lifecycle != LifecycleUnsafeWritten || got.RC == nil || len(got.RC.CandidateParts) != 1 || got.RC.CandidateParts[0].PartName != "p0_77_77_0" {
		t.Fatalf("attributed %v, scanned %v, record %+v; want only the actual nonempty insert", c.attributed, c.scanned, got)
	}
}

func TestSNodeZeroRowOnlyIsNotANewPart(t *testing.T) {
	t.Run("fresh refuses no-new-parts", func(t *testing.T) {
		r, c, rec, schema := zeroPartsFixture(t)
		c.parts = c.parts[:1]
		_, _, err := r.assembleAfterWrite(t.Context(), rec.Envelope, schema, CHTableName(schema.TableID), nil, rec)
		if err == nil || err.Error() != "source write produced no new parts" || len(c.scanned) != 0 {
			t.Fatalf("err %v, scanned %v; want no-new-parts before a scan", err, c.scanned)
		}
	})
	t.Run("convergence cleans with no attributed part", func(t *testing.T) {
		r, c, rec, schema := zeroPartsFixture(t)
		c.parts = c.parts[:1]
		got, err := r.convergeIntake(t.Context(), rec, schema)
		if err != nil || got.Lifecycle != LifecycleCleaned || got.RC != nil || got.Result != nil || len(c.attributed) != 0 || len(c.scanned) != 0 || len(c.dropped) != 0 {
			t.Fatalf("record %+v, err %v, attributed %v, scanned %v, dropped %v; want empty cleanup without an RC", got, err, c.attributed, c.scanned, c.dropped)
		}
	})
}

func TestSNodeMissingNonemptyPartStillFailsClosed(t *testing.T) {
	r, c, rec, schema := zeroPartsFixture(t)
	c.parts = c.parts[1:]
	c.missingData = true
	result, rc, err := r.assembleAfterWrite(t.Context(), rec.Envelope, schema, CHTableName(schema.TableID), nil, rec)
	if err == nil || !strings.Contains(err.Error(), `part "p0_77_77_0" not found (or empty)`) || len(result.CandidateParts) != 0 || len(rc.CandidateParts) != 0 {
		t.Fatalf("result %+v, RC %+v, err %v; missing nonempty data must not produce a claim", result, rc, err)
	}
}
