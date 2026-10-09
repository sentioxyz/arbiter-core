package snode

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sort"
	"strings"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// These tests pin subset promotion: the arbiter promotes a strict subset of the
// active parts in one hg_unsafe partition. It is the devnet2 promotion-16
// incident, where a later statement wrote to the same partition between its
// predecessor's block seal and that block's promotion. The SNode reaches
// ClickHouse over TCP only (it runs in another pod on another node), and so do
// these tests: the ClickHouse data directory is inside a container the test
// process cannot see. Any filesystem access to a ClickHouse part path fails
// here exactly as it failed on devnet2.

// subsetFixture is a role with two committed statements in one partition of
// hg_unsafe, neither promoted yet.
type subsetFixture struct {
	role   *Role
	claims *sourceClaimsFake
	signer commandSigner
	schema payloadexec.TableSchema
	envs   []arbiter.StatementEnvelope
	parts  []arbiter.CandidatePart
}

func unpartitionedPromoteSchema() payloadexec.TableSchema {
	s := promoteSchema()
	s.PartitionBy = ""
	return s
}

func newSubsetFixture(t *testing.T, ctx context.Context, schema payloadexec.TableSchema, statements int) *subsetFixture {
	t.Helper()
	conn := requireCH(t)
	signer := mustPromoteSigner(t)
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.AuthorityAddresses = []string{signer.Address()}
	setUniqueDatabases(t, &cfg)
	role, claims := newIntakeHarness(t, conn, cfg)
	createSNodeTables(t, conn, role.cfg, schema)
	f := &subsetFixture{role: role, claims: claims, signer: signer, schema: schema}
	for i := 0; i < statements; i++ {
		f.submit(t, ctx)
	}
	return f
}

// submit commits one more two-row statement into partition p0 (or "all").
func (f *subsetFixture) submit(t *testing.T, ctx context.Context) arbiter.CandidatePart {
	t.Helper()
	n := uint64(len(f.envs) + 1)
	payload := nativePayload(t, pv{"p0", 2*n - 1}, pv{"p0", 2 * n})
	env := intakeEnvelope(payload)
	env.StatementID.ClientSeq = n
	env.StatementID.ClientNonce = fmt.Sprintf("n%d", n)
	env.PayloadRef = fmt.Sprintf("payload-%d.native", n)
	env.SchemaHash = payloadexec.TableSchemaHash(f.role.cfg.NetworkID, f.schema)
	if err := f.role.SubmitLocalStatement(ctx, env, payload); err != nil {
		t.Fatalf("SubmitLocalStatement %d: %v", n, err)
	}
	rcs := f.claims.snapshot()
	if len(rcs) != int(n) || len(rcs[n-1].CandidateParts) != 1 {
		t.Fatalf("rc for statement %d: %+v", n, rcs)
	}
	part := rcs[n-1].CandidateParts[0]
	f.envs = append(f.envs, env)
	f.parts = append(f.parts, part)
	return part
}

func (f *subsetFixture) table() string { return CHTableName(f.schema.TableID) }

func (f *subsetFixture) partitionKey() partitionKey {
	return partitionKey{Table: f.schema.TableID, Partition: f.parts[0].PartitionID}
}

// command builds the next promotion on top of the role's current safe base.
func (f *subsetFixture) command(seq uint64, candidates ...arbiter.CandidatePart) arbiter.PromoteSafePartition {
	baseRoot, baseSnap := f.role.state.BaseRoot(f.partitionKey())
	if baseSnap == "" {
		baseSnap = "genesis"
	}
	cmd := arbiter.PromoteSafePartition{
		TableID: f.schema.TableID, PartitionID: f.parts[0].PartitionID, PromotionSeq: seq,
		BaseSafeSnapshotID: baseSnap, BasePartitionRoot: baseRoot,
	}
	for _, c := range candidates {
		cmd.CandidateParts = append(cmd.CandidateParts, arbiter.PartRef{
			TableID: c.TableID, PartitionID: c.PartitionID,
			PartRowLtHash: c.PartRowLtHash, PartName: c.PartName,
		})
	}
	return cmd
}

func (f *subsetFixture) promote(ctx context.Context, role *Role, cmd arbiter.PromoteSafePartition) error {
	jws, err := f.signer.SignPromotion(cmd)
	if err != nil {
		return err
	}
	return role.handlePromote(ctx, wire.PromoteToPB(cmd), jws)
}

// assertSafeHoldsExactly asserts that hg_safe holds exactly the rows of the
// given statements, each exactly once, and nothing else.
func (f *subsetFixture) assertSafeHoldsExactly(t *testing.T, ctx context.Context, statements ...int) {
	t.Helper()
	var want []string
	for _, i := range statements {
		id := f.envs[i].StatementID.Flat()
		for ordinal := uint64(0); ordinal < 2; ordinal++ {
			want = append(want, fmt.Sprintf("%x", payloadexec.RowID(f.role.cfg.NetworkID, f.schema.TableID, id, ordinal)))
		}
	}
	sort.Strings(want)
	got := rowIDs(t, ctx, f.role.d.Conn, f.role.cfg.SafeDatabase+"."+f.table())
	if !slices.Equal(got, want) {
		t.Fatalf("hg_safe row ids = %v, want exactly %v", got, want)
	}
}

func (f *subsetFixture) unsafePartNames(t *testing.T, ctx context.Context) []string {
	t.Helper()
	var names []string
	for _, p := range activePartsMust(t, ctx, f.role.d.Conn, f.role.cfg.UnsafeDatabase, f.table()) {
		names = append(names, p.Name)
	}
	return names
}

// assertRejectedUntouched asserts the fail-closed outcome of a rejected
// promotion: no ACK, no journaled intent, hg_safe still at its base, the shadow
// partition gone, and hg_unsafe untouched.
func (f *subsetFixture) assertRejectedUntouched(t *testing.T, ctx context.Context, acksBefore int, unsafeBefore []string, safeStatements ...int) {
	t.Helper()
	if got := len(f.claims.promotionAcks()); got != acksBefore {
		t.Fatalf("rejected promotion sent %d acks, want %d", got, acksBefore)
	}
	if _, pending := f.role.state.PendingPromotion(f.partitionKey()); pending {
		t.Fatal("rejected promotion journaled an intent")
	}
	if len(safeStatements) == 0 {
		if got := rowCount(t, ctx, f.role.d.Conn, f.role.cfg.SafeDatabase+"."+f.table()); got != 0 {
			t.Fatalf("rejected promotion published %d safe rows", got)
		}
	} else {
		f.assertSafeHoldsExactly(t, ctx, safeStatements...)
	}
	assertNoPromotePartition(t, ctx, f.role, f.schema, f.parts[0].PartitionID)
	if got := f.unsafePartNames(t, ctx); !slices.Equal(got, unsafeBefore) {
		t.Fatalf("rejected promotion changed hg_unsafe: %v -> %v", unsafeBefore, got)
	}
}

func rowIDs(t *testing.T, ctx context.Context, conn clickhouse.Conn, table string) []string {
	t.Helper()
	rows, err := conn.Query(ctx, "SELECT hex(_hg_row_id) FROM "+table)
	if err != nil {
		t.Fatalf("query row ids: %v", err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan row id: %v", err)
		}
		out = append(out, strings.ToLower(id))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("row ids: %v", err)
	}
	sort.Strings(out)
	return out
}

// insertStrayUnsafeRow writes one row that no statement or candidate accounts
// for into the hg_unsafe partition, as a concurrent source write would.
func insertStrayUnsafeRow(ctx context.Context, conn clickhouse.Conn, role *Role, table string, v uint64) error {
	return conn.Exec(ctx, fmt.Sprintf("INSERT INTO %s.%s (_hg_row_id, p, v) VALUES (unhex('%064x'), 'p0', %d)",
		role.cfg.UnsafeDatabase, table, v, v))
}

// hookConn records every statement and lets a test inject a failure or a
// concurrent write around chosen statements. Hooks see the statement text and
// may use base to reach ClickHouse without re-entering the hooks.
type hookConn struct {
	clickhouse.Conn
	mu          sync.Mutex
	execs       []string
	beforeExec  func(query string) error
	afterExec   func(query string) error
	beforeQuery func(query string) error
}

func (c *hookConn) Exec(ctx context.Context, query string, args ...any) error {
	c.mu.Lock()
	c.execs = append(c.execs, query)
	c.mu.Unlock()
	if c.beforeExec != nil {
		if err := c.beforeExec(query); err != nil {
			return err
		}
	}
	if err := c.Conn.Exec(ctx, query, args...); err != nil {
		return err
	}
	if c.afterExec != nil {
		return c.afterExec(query)
	}
	return nil
}

func (c *hookConn) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if c.beforeQuery != nil {
		if err := c.beforeQuery(query); err != nil {
			return nil, err
		}
	}
	return c.Conn.Query(ctx, query, args...)
}

func (c *hookConn) alters() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for _, q := range c.execs {
		if strings.HasPrefix(q, "ALTER TABLE ") {
			out = append(out, q)
		}
	}
	return out
}

func isAttachFromUnsafe(role *Role, query string) bool {
	return strings.Contains(query, " ATTACH PARTITION ") && strings.HasSuffix(query, " FROM "+role.cfg.UnsafeDatabase+"."+CHTableName(promoteSchema().TableID))
}

func isPromoteDropPart(role *Role, query string) bool {
	return strings.HasPrefix(query, "ALTER TABLE "+role.cfg.PromoteDatabase+".") && strings.Contains(query, " DROP PART '")
}

func isPromoteScan(role *Role, query string) bool {
	return strings.Contains(query, "_part IN (") && strings.Contains(query, "`"+role.cfg.PromoteDatabase+"`")
}

func subsetSchemas() []struct {
	name   string
	schema payloadexec.TableSchema
} {
	return []struct {
		name   string
		schema payloadexec.TableSchema
	}{
		{"partitioned", promoteSchema()},
		{"unpartitioned", unpartitionedPromoteSchema()},
	}
}

// TestHandlePromote_SubsetCandidatesPromoteOnlyCandidateRows reproduces devnet2
// promotion 16: two active parts in one hg_unsafe partition and a candidate set
// of one. The old per-part path hardlinked system.parts.path from the SNode's
// own filesystem and failed with "hardlink part ...: lstat ...: no such file or
// directory". Promotion must go through ClickHouse only and publish exactly the
// candidate's rows. The second promotion is devnet2's block 19: the first
// candidate is published but not yet cleaned up, so hg_unsafe again holds a
// strict superset of the candidates.
func TestHandlePromote_SubsetCandidatesPromoteOnlyCandidateRows(t *testing.T) {
	for _, tc := range subsetSchemas() {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newSubsetFixture(t, ctx, tc.schema, 2)
			hooks := &hookConn{Conn: f.role.d.Conn}
			f.role.d.Conn = hooks
			unsafeBefore := f.unsafePartNames(t, ctx)
			if len(unsafeBefore) != 2 {
				t.Fatalf("fixture wants two active unsafe parts, got %v", unsafeBefore)
			}

			if err := f.promote(ctx, f.role, f.command(1, f.parts[0])); err != nil {
				t.Fatalf("subset promotion: %v", err)
			}
			f.assertSafeHoldsExactly(t, ctx, 0)
			acks := f.claims.promotionAcks()
			if len(acks) != 1 || !acks[0].Applied {
				t.Fatalf("acks = %+v", acks)
			}
			wantPost, err := lthashCombineHexAll("", []string{f.parts[0].PartRowLtHash})
			if err != nil {
				t.Fatal(err)
			}
			if acks[0].PostPartitionCommitment != wantPost {
				t.Fatal("ack post root is not base ⊕ candidate")
			}
			assertExactSafePartMappings(t, ctx, f.role, f.schema, f.parts[0], acks[0].Parts)
			assertCompleteSafeInventory(t, ctx, f.role, f.schema, f.parts[0].PartitionID, acks[0].SafePartitionParts)
			assertNoPromotePartition(t, ctx, f.role, f.schema, f.parts[0].PartitionID)
			if got := f.unsafePartNames(t, ctx); !slices.Equal(got, unsafeBefore) {
				t.Fatalf("promotion changed hg_unsafe: %v -> %v", unsafeBefore, got)
			}
			if got, err := f.role.PromotedUnsafeParts(f.schema.TableID); err != nil || !reflect.DeepEqual(got, []string{f.parts[0].PartName}) {
				t.Fatalf("PromotedUnsafeParts = %v %v", got, err)
			}

			// Block 19: candidate 2 while the published candidate 1 is still
			// in hg_unsafe awaiting cleanup.
			if err := f.promote(ctx, f.role, f.command(2, f.parts[1])); err != nil {
				t.Fatalf("second subset promotion: %v", err)
			}
			f.assertSafeHoldsExactly(t, ctx, 0, 1)
			acks = f.claims.promotionAcks()
			if len(acks) != 2 || !acks[1].Applied {
				t.Fatalf("acks = %+v", acks)
			}
			assertExactSafePartMappings(t, ctx, f.role, f.schema, f.parts[1], acks[1].Parts)
			assertCompleteSafeInventory(t, ctx, f.role, f.schema, f.parts[0].PartitionID, acks[1].SafePartitionParts)
			assertNoPromotePartition(t, ctx, f.role, f.schema, f.parts[0].PartitionID)

			var drops int
			for _, q := range hooks.alters() {
				if strings.Contains(q, " ATTACH PART '") {
					t.Fatalf("subset promotion attached a part by name: %s", q)
				}
				if isPromoteDropPart(f.role, q) {
					drops++
				}
			}
			if drops != 2 {
				t.Fatalf("want one shadow DROP PART per promotion, got %d in %v", drops, hooks.alters())
			}
		})
	}
}

// TestHandlePromote_SubsetMatchesIdenticalPartsAsMultiset gives hg_unsafe two
// parts with identical content (a duplicated statement part). One candidate
// must consume exactly one of them, so its rows reach hg_safe once.
func TestHandlePromote_SubsetMatchesIdenticalPartsAsMultiset(t *testing.T) {
	ctx := context.Background()
	f := newSubsetFixture(t, ctx, promoteSchema(), 2)
	mustExecIntake(t, f.role.d.Conn, fmt.Sprintf("INSERT INTO %[1]s.%[2]s SELECT * FROM %[1]s.%[2]s WHERE _part = '%[3]s'",
		f.role.cfg.UnsafeDatabase, f.table(), f.parts[0].PartName))
	if got := f.unsafePartNames(t, ctx); len(got) != 3 {
		t.Fatalf("want three active unsafe parts (one duplicated), got %v", got)
	}
	if err := f.promote(ctx, f.role, f.command(1, f.parts[0])); err != nil {
		t.Fatalf("promotion with a duplicated candidate part: %v", err)
	}
	f.assertSafeHoldsExactly(t, ctx, 0)
	assertNoPromotePartition(t, ctx, f.role, f.schema, f.parts[0].PartitionID)
}

// TestHandlePromote_SubsetFailsClosed pins the fail-closed outcomes: a
// candidate whose content is not in the shadow, or a scan error, aborts the
// promotion before REPLACE with hg_safe untouched and no ACK.
func TestHandlePromote_SubsetFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(t *testing.T, ctx context.Context, f *subsetFixture) arbiter.PromoteSafePartition
		hook    func(f *subsetFixture, c *hookConn)
		wantErr string
	}{
		{
			name: "candidate hash matches no unsafe part",
			prepare: func(t *testing.T, ctx context.Context, f *subsetFixture) arbiter.PromoteSafePartition {
				lie := f.parts[0]
				lie.PartRowLtHash = lthashLie(lie.PartRowLtHash)
				return f.command(1, lie)
			},
			wantErr: "not present in hg_unsafe",
		},
		{
			name: "candidate part no longer in hg_unsafe",
			prepare: func(t *testing.T, ctx context.Context, f *subsetFixture) arbiter.PromoteSafePartition {
				mustExecIntake(t, f.role.d.Conn, fmt.Sprintf("ALTER TABLE %s.%s DROP PART '%s'",
					f.role.cfg.UnsafeDatabase, f.table(), f.parts[1].PartName))
				return f.command(1, f.parts[1])
			},
			wantErr: "not present in hg_unsafe",
		},
		{
			name: "one of two candidates unmatched",
			prepare: func(t *testing.T, ctx context.Context, f *subsetFixture) arbiter.PromoteSafePartition {
				f.submit(t, ctx)
				lie := f.parts[1]
				lie.PartRowLtHash = lthashLie(lie.PartRowLtHash)
				return f.command(1, f.parts[0], lie)
			},
			wantErr: "not present in hg_unsafe",
		},
		{
			name: "candidate hash malformed",
			prepare: func(t *testing.T, ctx context.Context, f *subsetFixture) arbiter.PromoteSafePartition {
				bad := f.parts[0]
				bad.PartRowLtHash = "0xnot-hex"
				return f.command(1, bad)
			},
			wantErr: "invalid part_row_lthash",
		},
		{
			name: "shadow scan error",
			prepare: func(t *testing.T, ctx context.Context, f *subsetFixture) arbiter.PromoteSafePartition {
				return f.command(1, f.parts[0])
			},
			hook: func(f *subsetFixture, c *hookConn) {
				c.beforeQuery = func(q string) error {
					if isPromoteScan(f.role, q) {
						return errors.New("injected shadow scan failure")
					}
					return nil
				}
			},
			wantErr: "injected shadow scan failure",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newSubsetFixture(t, ctx, promoteSchema(), 2)
			cmd := tc.prepare(t, ctx, f)
			unsafeBefore := f.unsafePartNames(t, ctx)
			hooks := &hookConn{Conn: f.role.d.Conn}
			if tc.hook != nil {
				tc.hook(f, hooks)
			}
			f.role.d.Conn = hooks
			err := f.promote(ctx, f.role, cmd)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("promotion = %v, want error containing %q", err, tc.wantErr)
			}
			for _, q := range hooks.alters() {
				if strings.Contains(q, " REPLACE PARTITION ") {
					t.Fatalf("rejected promotion reached REPLACE: %s", q)
				}
			}
			f.role.d.Conn = hooks.Conn
			f.assertRejectedUntouched(t, ctx, 0, unsafeBefore)
		})
	}
}

// TestHandlePromote_SubsetRedeliveryAfterMidStepFailureConverges crashes the
// subset promotion at each step between the shadow attach and the closure gate,
// restarts the role, and redelivers the same command. Every failure must leave
// hg_safe untouched with no intent, and the redelivery must converge on exactly
// the candidate rows.
func TestHandlePromote_SubsetRedeliveryAfterMidStepFailureConverges(t *testing.T) {
	injected := errors.New("injected crash")
	for _, tc := range []struct {
		name string
		hook func(f *subsetFixture, c *hookConn)
	}{
		{"before attach from unsafe", func(f *subsetFixture, c *hookConn) {
			c.beforeExec = func(q string) error {
				if isAttachFromUnsafe(f.role, q) {
					return injected
				}
				return nil
			}
		}},
		{"after attach from unsafe", func(f *subsetFixture, c *hookConn) {
			c.afterExec = func(q string) error {
				if isAttachFromUnsafe(f.role, q) {
					return injected
				}
				return nil
			}
		}},
		{"before first shadow drop part", func(f *subsetFixture, c *hookConn) {
			c.beforeExec = func(q string) error {
				if isPromoteDropPart(f.role, q) {
					return injected
				}
				return nil
			}
		}},
		{"after first shadow drop part", func(f *subsetFixture, c *hookConn) {
			c.afterExec = func(q string) error {
				if isPromoteDropPart(f.role, q) {
					return injected
				}
				return nil
			}
		}},
		{"shadow scan", func(f *subsetFixture, c *hookConn) {
			c.beforeQuery = func(q string) error {
				if isPromoteScan(f.role, q) {
					return injected
				}
				return nil
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			// Three statements so the shadow has two non-candidate parts to
			// drop and the "after first drop" crash leaves one behind.
			f := newSubsetFixture(t, ctx, promoteSchema(), 3)
			cmd := f.command(1, f.parts[1])
			unsafeBefore := f.unsafePartNames(t, ctx)
			base := f.role.d.Conn
			hooks := &hookConn{Conn: base}
			tc.hook(f, hooks)
			f.role.d.Conn = hooks
			if err := f.promote(ctx, f.role, cmd); err == nil || !errors.Is(err, injected) {
				t.Fatalf("faulted promotion = %v, want the injected crash", err)
			}
			if got := rowCount(t, ctx, base, f.role.cfg.SafeDatabase+"."+f.table()); got != 0 {
				t.Fatalf("faulted promotion published %d safe rows", got)
			}
			if _, pending := f.role.state.PendingPromotion(f.partitionKey()); pending {
				t.Fatal("faulted promotion journaled an intent")
			}
			if got := len(f.claims.promotionAcks()); got != 0 {
				t.Fatalf("faulted promotion sent %d acks", got)
			}

			deps := f.role.d
			deps.Conn = base
			restarted, err := New(f.role.cfg, deps)
			if err != nil {
				t.Fatalf("restart role: %v", err)
			}
			if err := f.promote(ctx, restarted, cmd); err != nil {
				t.Fatalf("redelivery: %v", err)
			}
			f.role = restarted
			f.assertSafeHoldsExactly(t, ctx, 1)
			acks := f.claims.promotionAcks()
			if len(acks) != 1 || !acks[0].Applied || acks[0].PromotionSeq != 1 {
				t.Fatalf("acks after redelivery = %+v", acks)
			}
			assertExactSafePartMappings(t, ctx, restarted, f.schema, f.parts[1], acks[0].Parts)
			assertNoPromotePartition(t, ctx, restarted, f.schema, f.parts[0].PartitionID)
			if got := f.unsafePartNames(t, ctx); !slices.Equal(got, unsafeBefore) {
				t.Fatalf("promotion changed hg_unsafe: %v -> %v", unsafeBefore, got)
			}
			// A second redelivery after the ACK is a stale duplicate.
			if err := f.promote(ctx, restarted, cmd); err != nil {
				t.Fatalf("duplicate redelivery: %v", err)
			}
			f.assertSafeHoldsExactly(t, ctx, 1)
		})
	}
}

// TestHandlePromote_SubsetIgnoresConcurrentUnsafeWrites lands an unrelated
// source write in the hg_unsafe partition at each point around the shadow
// attach. A write before the attach is copied into the shadow and must be
// dropped as a non-candidate; a write after it is not in the shadow at all.
// Either way only the candidate's rows reach hg_safe, and the raced part stays
// in hg_unsafe for its own promotion.
func TestHandlePromote_SubsetIgnoresConcurrentUnsafeWrites(t *testing.T) {
	for _, tc := range []struct {
		name   string
		before bool
	}{
		{"write lands just before the attach", true},
		{"write lands just after the attach", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newSubsetFixture(t, ctx, promoteSchema(), 2)
			base := f.role.d.Conn
			hooks := &hookConn{Conn: base}
			var raced bool
			race := func(q string) error {
				if raced || !isAttachFromUnsafe(f.role, q) {
					return nil
				}
				raced = true
				return insertStrayUnsafeRow(ctx, base, f.role, f.table(), 99)
			}
			if tc.before {
				hooks.beforeExec = race
			} else {
				hooks.afterExec = race
			}
			f.role.d.Conn = hooks
			if err := f.promote(ctx, f.role, f.command(1, f.parts[0])); err != nil {
				t.Fatalf("promotion with a concurrent write: %v", err)
			}
			if !raced {
				t.Fatal("the concurrent write never fired")
			}
			f.role.d.Conn = base
			f.assertSafeHoldsExactly(t, ctx, 0)
			if got := len(f.unsafePartNames(t, ctx)); got != 3 {
				t.Fatalf("raced part must stay in hg_unsafe: %d active parts", got)
			}
			assertNoPromotePartition(t, ctx, f.role, f.schema, f.parts[0].PartitionID)
		})
	}
}

// TestHandlePromote_WholePartitionRaceSelfHealsThroughSubset races a write into
// the partition between the whole-partition cover check and its ATTACH. The
// closure gate must reject that attempt before REPLACE; the redelivery then
// sees a strict superset and promotes through the subset path.
func TestHandlePromote_WholePartitionRaceSelfHealsThroughSubset(t *testing.T) {
	ctx := context.Background()
	f := newSubsetFixture(t, ctx, promoteSchema(), 1)
	base := f.role.d.Conn
	hooks := &hookConn{Conn: base}
	var raced bool
	hooks.beforeExec = func(q string) error {
		if raced || !isAttachFromUnsafe(f.role, q) {
			return nil
		}
		raced = true
		return insertStrayUnsafeRow(ctx, base, f.role, f.table(), 99)
	}
	f.role.d.Conn = hooks
	cmd := f.command(1, f.parts[0])
	err := f.promote(ctx, f.role, cmd)
	if err == nil || !strings.Contains(err.Error(), "shadow closure mismatch") {
		t.Fatalf("raced whole-partition attach = %v, want the closure gate", err)
	}
	f.role.d.Conn = base
	if got := rowCount(t, ctx, base, f.role.cfg.SafeDatabase+"."+f.table()); got != 0 {
		t.Fatalf("raced attach published %d safe rows", got)
	}
	if err := f.promote(ctx, f.role, cmd); err != nil {
		t.Fatalf("redelivery: %v", err)
	}
	f.assertSafeHoldsExactly(t, ctx, 0)
	if got := len(f.unsafePartNames(t, ctx)); got != 2 {
		t.Fatalf("raced part must stay in hg_unsafe: %d active parts", got)
	}
}

// TestHandlePromote_WholePartitionStatementsArePinned pins the exact ALTER
// sequence of a promotion whose candidates cover the unsafe partition, first
// onto an empty base and then onto a non-empty one. Subset support must not
// change it.
func TestHandlePromote_WholePartitionStatementsArePinned(t *testing.T) {
	ctx := context.Background()
	f := newSubsetFixture(t, ctx, promoteSchema(), 1)
	hooks := &hookConn{Conn: f.role.d.Conn}
	f.role.d.Conn = hooks
	if err := f.promote(ctx, f.role, f.command(1, f.parts[0])); err != nil {
		t.Fatalf("first promotion: %v", err)
	}
	cleanup := cleanupCommand(f.schema.TableID, f.parts[0])
	if err := f.role.handleCleanup(ctx, wire.CleanupToPB(cleanup), mustSignCleanup(t, f.signer, cleanup)); err != nil {
		t.Fatalf("cleanup: %v", err)
	}
	f.submit(t, ctx)
	if err := f.promote(ctx, f.role, f.command(2, f.parts[1])); err != nil {
		t.Fatalf("second promotion: %v", err)
	}
	f.assertSafeHoldsExactly(t, ctx, 0, 1)
	promote := f.role.cfg.PromoteDatabase + "." + f.table()
	safe := f.role.cfg.SafeDatabase + "." + f.table()
	unsafe := f.role.cfg.UnsafeDatabase + "." + f.table()
	want := []string{
		"ALTER TABLE " + promote + " ATTACH PARTITION 'p0' FROM " + unsafe,
		"ALTER TABLE " + safe + " REPLACE PARTITION 'p0' FROM " + promote,
		"ALTER TABLE " + promote + " DROP PARTITION 'p0'",
		"ALTER TABLE " + unsafe + " DROP PART '" + f.parts[0].PartName + "'",
		"ALTER TABLE " + promote + " ATTACH PARTITION 'p0' FROM " + safe,
		"ALTER TABLE " + promote + " ATTACH PARTITION 'p0' FROM " + unsafe,
		"ALTER TABLE " + safe + " REPLACE PARTITION 'p0' FROM " + promote,
		"ALTER TABLE " + promote + " DROP PARTITION 'p0'",
	}
	if got := hooks.alters(); !slices.Equal(got, want) {
		t.Fatalf("whole-partition ALTER sequence changed:\n got %q\nwant %q", got, want)
	}
}
