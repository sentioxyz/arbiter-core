package snode

import (
	"context"
	"slices"
	"sync"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// execRecorderS is a clickhouse.Conn that records every Exec and succeeds.
// Any other method is unimplemented (the embedded nil Conn panics).
type execRecorderS struct {
	clickhouse.Conn
	mu      sync.Mutex
	queries []string
}

func (c *execRecorderS) Exec(_ context.Context, query string, _ ...any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.queries = append(c.queries, query)
	return nil
}

func (c *execRecorderS) executed() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.queries)
}

// TestRejectedPartCleanupIsEchoedAndSigned: a rejected-part cleanup
// (promotion_seq 0, rejected_block_seq 12) drops exactly the parts it names,
// although no promotion ever covered them, and the acknowledgement echoes
// rejected_block_seq inside the body the indexer key signs (housegate spec
// 2026-10-10 §7.5; plan S2-A contract §2, clarification C4).
func TestRejectedPartCleanupIsEchoedAndSigned(t *testing.T) {
	fake := &claimsFakeS{}
	conn := &execRecorderS{}
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake), Conn: conn,
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	cleanup := arbiter.UnsafeCleanup{TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12, Parts: []arbiter.PartRef{
		{TableID: "db.t", PartitionID: "all", PartRowLtHash: "0xaa", PartName: "all_1_1_0"},
		{TableID: "db.t", PartitionID: "all", PartRowLtHash: "0xbb", PartName: "all_2_2_0"},
	}}
	jws, err := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex).SignCleanup(cleanup)
	if err != nil {
		t.Fatal(err)
	}
	if err := role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), jws); err != nil {
		t.Fatal(err)
	}
	if got, want := conn.executed(), []string{
		"ALTER TABLE hg_unsafe.db__t DROP PART 'all_1_1_0'",
		"ALTER TABLE hg_unsafe.db__t DROP PART 'all_2_2_0'",
	}; !slices.Equal(got, want) {
		t.Fatalf("executed %q, want %q", got, want)
	}
	_, _, cleanups := fake.sent()
	if len(cleanups) != 1 {
		t.Fatalf("cleanup acknowledgements = %v", cleanups)
	}
	ack := wire.CleanupAckFromPB(cleanups[0])
	if want := (arbiter.CleanupAck{NodeID: authoritytest.SNodeNodeID1, TableID: "db.t", PartitionID: "all", RejectedBlockSeq: 12}); ack != want {
		t.Fatalf("ack = %+v, want %+v", ack, want)
	}
	ctx := authoritytest.Context()
	if err := authority.VerifySNodeMessage(authority.SNodeMessageCleanupAck, ctx, ack, cleanups[0].GetSourceJws(), authoritytest.IndexerAddr1); err != nil {
		t.Fatal(err)
	}
	unechoed := ack
	unechoed.RejectedBlockSeq = 0
	if err := authority.VerifySNodeMessage(authority.SNodeMessageCleanupAck, ctx, unechoed, cleanups[0].GetSourceJws(), authoritytest.IndexerAddr1); err == nil {
		t.Fatal("the indexer signature does not cover rejected_block_seq")
	}
}

// TestRejectedPartCleanupFollowsTheOwnerCheck: requireOwned applies to a
// rejected-part cleanup unchanged: another owner's table is ignored without a
// drop or an acknowledgement; this SNode's own table is cleaned and
// acknowledged with rejected_block_seq.
func TestRejectedPartCleanupFollowsTheOwnerCheck(t *testing.T) {
	fake := &claimsFakeS{}
	conn := &execRecorderS{}
	cfg := signingConfigS(t)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	view := newFakeRegistryS()
	view.setSnapshot(ownedSnapshotS(t, cfg))
	role, err := New(cfg, Deps{Client: startClaimsFakeS(t, fake), Conn: conn, Registry: view,
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1)})
	if err != nil {
		t.Fatal(err)
	}
	authoritySigner := authoritytest.MustSigner(t, authoritytest.AuthorityKeyHex)
	for _, table := range []string{"db.theirs", "db.mine"} {
		cleanup := arbiter.UnsafeCleanup{TableID: table, PartitionID: "all", RejectedBlockSeq: 12,
			Parts: []arbiter.PartRef{{TableID: table, PartitionID: "all", PartRowLtHash: "0xaa", PartName: "all_1_1_0"}}}
		jws, err := authoritySigner.SignCleanup(cleanup)
		if err != nil {
			t.Fatal(err)
		}
		if err := role.handleCleanup(context.Background(), wire.CleanupToPB(cleanup), jws); err != nil {
			t.Fatalf("%s: %v", table, err)
		}
	}
	if got, want := conn.executed(), []string{"ALTER TABLE hg_unsafe.db__mine DROP PART 'all_1_1_0'"}; !slices.Equal(got, want) {
		t.Fatalf("executed %q, want %q", got, want)
	}
	_, _, cleanups := fake.sent()
	if len(cleanups) != 1 || cleanups[0].GetTableId() != "db.mine" || cleanups[0].GetRejectedBlockSeq() != 12 {
		t.Fatalf("cleanup acknowledgements = %v", cleanups)
	}
}

// TestHandleCleanup_RejectedPartsAreDroppedWithoutAPromotion (ClickHouse): the
// candidate part of a statement that was never promoted leaves hg_unsafe on a
// cleanup with promotion_seq 0, and the acknowledgement names the rejected
// block (spec §7.5).
func TestHandleCleanup_RejectedPartsAreDroppedWithoutAPromotion(t *testing.T) {
	ctx := context.Background()
	role, claims, signer, schema, _ := seedPromotableStatement(t, ctx)
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := cleanupCommand(schema.TableID, part)
	cmd.PromotionSeq, cmd.RejectedBlockSeq = 0, 12
	if err := role.handleCleanup(ctx, wire.CleanupToPB(cmd), mustSignCleanup(t, signer, cmd)); err != nil {
		t.Fatalf("handleCleanup: %v", err)
	}
	assertUnsafePartGone(t, ctx, role, schema.TableID, part.PartName)
	acks := claims.cleanupAcks()
	if want := (arbiter.CleanupAck{NodeID: role.cfg.NodeID, TableID: schema.TableID, PartitionID: part.PartitionID, RejectedBlockSeq: 12}); len(acks) != 1 || acks[0] != want {
		t.Fatalf("cleanup acknowledgements = %+v, want [%+v]", acks, want)
	}
}

// TestSNodeRegistrationAdvertisesTrackManifests: every SNode built from this
// release advertises track_manifests_v1, signed or not; the filter drops only
// signed_claims_v1, and only without a claim signer (plan S2-A contract §2).
func TestSNodeRegistrationAdvertisesTrackManifests(t *testing.T) {
	legacy, _, err := (&Role{cfg: Config{NodeID: "s1"}}).registrationRequests()
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{arbiter.ClientLanesFeature, arbiter.TrackManifestsFeature}; !slices.Equal(legacy.GetFeatures(), want) {
		t.Fatalf("unsigned SNode features = %v, want %v", legacy.GetFeatures(), want)
	}
	fake := &claimsFakeS{}
	now := fixtureMillis
	role, err := New(signingConfigS(t), Deps{Client: startClaimsFakeS(t, fake),
		ClaimSigner: authoritytest.MustSigner(t, authoritytest.IndexerKeyHex1), Now: clockS(&now)})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.Register(context.Background()); err != nil {
		t.Fatal(err)
	}
	regs, _ := fake.registrations()
	if len(regs) != 1 || !slices.Equal(regs[0].GetFeatures(), arbiter.LocalNodeFeatures()) {
		t.Fatalf("signing SNode registrations = %v, want features %v", regs, arbiter.LocalNodeFeatures())
	}
}
