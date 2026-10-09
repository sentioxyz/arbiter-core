package snode

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// promoteAndClean publishes one candidate and cleans its unsafe part, leaving
// a non-empty safe base for the next promotion.
func (f *subsetFixture) promoteAndClean(t *testing.T, ctx context.Context, seq uint64, part arbiter.CandidatePart) {
	t.Helper()
	if err := f.promote(ctx, f.role, f.command(seq, part)); err != nil {
		t.Fatalf("promote %d: %v", seq, err)
	}
	cleanup := cleanupCommand(f.schema.TableID, part)
	cleanup.PromotionSeq = seq
	if err := f.role.handleCleanup(ctx, wire.CleanupToPB(cleanup), mustSignCleanup(t, f.signer, cleanup)); err != nil {
		t.Fatalf("cleanup %d: %v", seq, err)
	}
}

// duplicateUnsafePart copies one unsafe part's rows into a new unsafe part
// with identical content and returns that part as a second candidate claim.
func (f *subsetFixture) duplicateUnsafePart(t *testing.T, ctx context.Context, part arbiter.CandidatePart) arbiter.CandidatePart {
	t.Helper()
	before := f.unsafePartNames(t, ctx)
	mustExecIntake(t, f.role.d.Conn, fmt.Sprintf("INSERT INTO %[1]s.%[2]s SELECT * FROM %[1]s.%[2]s WHERE _part = '%[3]s'",
		f.role.cfg.UnsafeDatabase, f.table(), part.PartName))
	for _, name := range f.unsafePartNames(t, ctx) {
		if !slices.Contains(before, name) {
			dup := part
			dup.PartName = name
			return dup
		}
	}
	t.Fatal("duplicate unsafe part not found")
	return arbiter.CandidatePart{}
}

// TestHandlePromote_RefusesDuplicatePartHashesBeforeReplace pins the
// controller ruling: a promotion whose published partition would hold two parts
// with the same row LtHash (duplicate candidates, or a candidate whose content
// is already in the safe base) is refused before BeginPromotion and REPLACE on
// both attach paths. Honest operation cannot produce such a command: every
// part's rows carry statement-unique _hg_row_ids. Before the ruling the
// closure gate accepted it, REPLACE published duplicated rows, and
// safeMappings then failed with the intent journaled, wedging the partition.
func TestHandlePromote_RefusesDuplicatePartHashesBeforeReplace(t *testing.T) {
	for _, tc := range []struct {
		name string
		// setup returns the command under test and the statements whose rows
		// hg_safe holds before it.
		setup   func(t *testing.T, ctx context.Context, f *subsetFixture) (arbiter.PromoteSafePartition, []int)
		subset  bool
		wantErr string
	}{
		{
			name: "whole partition: candidate already in base",
			setup: func(t *testing.T, ctx context.Context, f *subsetFixture) (arbiter.PromoteSafePartition, []int) {
				if err := f.promote(ctx, f.role, f.command(1, f.parts[0])); err != nil {
					t.Fatalf("first promotion: %v", err)
				}
				return f.command(2, f.parts[0]), []int{0}
			},
			wantErr: "duplicate part row LtHash",
		},
		{
			name: "subset: candidate already in base",
			setup: func(t *testing.T, ctx context.Context, f *subsetFixture) (arbiter.PromoteSafePartition, []int) {
				f.submit(t, ctx)
				if err := f.promote(ctx, f.role, f.command(1, f.parts[0])); err != nil {
					t.Fatalf("first promotion: %v", err)
				}
				return f.command(2, f.parts[0]), []int{0}
			},
			subset:  true,
			wantErr: "duplicate part row LtHash",
		},
		{
			name: "whole partition: duplicate candidates",
			setup: func(t *testing.T, ctx context.Context, f *subsetFixture) (arbiter.PromoteSafePartition, []int) {
				dup := f.duplicateUnsafePart(t, ctx, f.parts[0])
				return f.command(1, f.parts[0], dup), nil
			},
			wantErr: "duplicate candidate part_row_lthash",
		},
		{
			name: "subset: duplicate candidates",
			setup: func(t *testing.T, ctx context.Context, f *subsetFixture) (arbiter.PromoteSafePartition, []int) {
				f.submit(t, ctx)
				dup := f.duplicateUnsafePart(t, ctx, f.parts[0])
				return f.command(1, f.parts[0], dup), nil
			},
			subset:  true,
			wantErr: "duplicate candidate part_row_lthash",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			f := newSubsetFixture(t, ctx, promoteSchema(), 1)
			cmd, safeBefore := tc.setup(t, ctx, f)
			table := CHTableName(f.schema.TableID)
			covers, err := f.role.candidatesCoverUnsafePartition(ctx, cmd, f.schema, table)
			if err != nil || covers == tc.subset {
				t.Fatalf("fixture takes the wrong attach path: covers=%v err=%v", covers, err)
			}
			acksBefore := len(f.claims.promotionAcks())
			watermark := f.role.state.Watermark(f.partitionKey())
			unsafeBefore := f.unsafePartNames(t, ctx)
			hooks := &hookConn{Conn: f.role.d.Conn}
			f.role.d.Conn = hooks

			err = f.promote(ctx, f.role, cmd)
			f.role.d.Conn = hooks.Conn
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("promotion = %v, want error containing %q", err, tc.wantErr)
			}
			for _, q := range hooks.alters() {
				if strings.Contains(q, " REPLACE PARTITION ") {
					t.Fatalf("duplicate promotion reached REPLACE: %s", q)
				}
			}
			if got := f.role.state.Watermark(f.partitionKey()); got != watermark {
				t.Fatalf("watermark moved from %d to %d", watermark, got)
			}
			f.assertRejectedUntouched(t, ctx, acksBefore, unsafeBefore, safeBefore...)

			// The redelivered command stays refused.
			if err := f.promote(ctx, f.role, cmd); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("redelivery = %v, want error containing %q", err, tc.wantErr)
			}
			f.assertRejectedUntouched(t, ctx, acksBefore, unsafeBefore, safeBefore...)
		})
	}
}
