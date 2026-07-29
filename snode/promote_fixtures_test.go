package snode

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"testing"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority"
	"github.com/sentioxyz/arbiter-core/wire"
)

const (
	promoteAuthorityKeyHex = "289c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"
	badAuthorityKeyHex     = "389c2857d4598e37fb9647507e47a309d6133539bf21a8b9cb6df88fd5232032"
)

type commandSigner interface {
	Address() string
	SignPromotion(arbiter.PromoteSafePartition) (string, error)
	SignCleanup(arbiter.UnsafeCleanup) (string, error)
}

func seedPromotableStatement(t *testing.T, ctx context.Context) (*Role, *sourceClaimsFake, commandSigner, payloadexec.TableSchema, arbiter.StatementEnvelope) {
	t.Helper()
	conn := requireCH(t)
	schema := promoteSchema()
	signer := mustPromoteSigner(t)
	cfg := testConfigS(t)
	cfg.Tables = []payloadexec.TableSchema{schema}
	cfg.SchemaRoot = payloadexec.SchemaRoot(cfg.NetworkID, cfg.Tables)
	cfg.AuthorityAddresses = []string{signer.Address()}
	setUniqueDatabases(t, &cfg)
	role, claims := newIntakeHarness(t, conn, cfg)
	createSNodeTables(t, conn, role.cfg, schema)
	payload := []byte("p,v\np0,1\np0,2\n")
	env := intakeEnvelope(payload)
	if err := role.SubmitLocalStatement(ctx, env, payload); err != nil {
		t.Fatalf("SubmitLocalStatement: %v", err)
	}
	rcs := claims.snapshot()
	if len(rcs) != 1 || len(rcs[0].CandidateParts) != 1 {
		t.Fatalf("seed rc: %+v", rcs)
	}
	return role, claims, signer, schema, env
}

func setUniqueDatabases(t *testing.T, cfg *Config) {
	t.Helper()
	sum := sha1.Sum([]byte(t.Name()))
	suffix := hex.EncodeToString(sum[:])[:10]
	cfg.UnsafeDatabase = "hg_unsafe_" + suffix
	cfg.SafeDatabase = "hg_safe_" + suffix
	cfg.PromoteDatabase = "hg_promote_" + suffix
}

func createSNodeTables(t *testing.T, conn clickhouse.Conn, cfg Config, schema payloadexec.TableSchema) {
	t.Helper()
	table := CHTableName(schema.TableID)
	for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
		mustExecIntake(t, conn, "CREATE DATABASE IF NOT EXISTS "+db)
		qualified := db + "." + table
		mustExecIntake(t, conn, "DROP TABLE IF EXISTS "+qualified)
		mustExecIntake(t, conn, fmt.Sprintf(`
			CREATE TABLE %s (
				_hg_row_id FixedString(32),
				p String,
				v UInt64
			) ENGINE = MergeTree
			PARTITION BY p
			ORDER BY tuple()`, qualified))
		mustExecIntake(t, conn, "SYSTEM STOP MERGES "+qualified)
	}
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db)
		}
	})
}

func mustPromoteSigner(t *testing.T) commandSigner {
	t.Helper()
	signer, err := authority.NewSignerFromHex(promoteAuthorityKeyHex)
	if err != nil {
		t.Fatalf("promote signer: %v", err)
	}
	return signer
}

func mustSignPromotion(t *testing.T, signer commandSigner, cmd arbiter.PromoteSafePartition) string {
	t.Helper()
	jws, err := signer.SignPromotion(cmd)
	if err != nil {
		t.Fatalf("sign promotion: %v", err)
	}
	return jws
}

func mustSignCleanup(t *testing.T, signer commandSigner, cmd arbiter.UnsafeCleanup) string {
	t.Helper()
	jws, err := signer.SignCleanup(cmd)
	if err != nil {
		t.Fatalf("sign cleanup: %v", err)
	}
	return jws
}

func promoteSeededPart(t *testing.T, ctx context.Context, role *Role, claims *sourceClaimsFake, signer commandSigner, schema payloadexec.TableSchema) arbiter.CandidatePart {
	t.Helper()
	part := claims.snapshot()[0].CandidateParts[0]
	cmd := arbiter.PromoteSafePartition{
		TableID:            schema.TableID,
		PartitionID:        part.PartitionID,
		PromotionSeq:       1,
		BaseSafeSnapshotID: "genesis",
		CandidateParts: []arbiter.PartRef{{
			TableID: schema.TableID, PartitionID: part.PartitionID,
			PartRowLtHash: part.PartRowLtHash, PartName: part.PartName,
		}},
	}
	if err := role.handlePromote(ctx, wire.PromoteToPB(cmd), mustSignPromotion(t, signer, cmd)); err != nil {
		t.Fatalf("promote seeded part: %v", err)
	}
	return part
}

func assertExactSafePartMappings(t *testing.T, ctx context.Context, role *Role, schema payloadexec.TableSchema, want arbiter.CandidatePart, maps []arbiter.SafePartMapping) {
	t.Helper()
	if len(maps) != 1 || maps[0].PartRowLtHash != want.PartRowLtHash {
		t.Fatalf("safe mappings: %+v want part hash %s", maps, want.PartRowLtHash)
	}
	info := activePartByName(t, ctx, role.d.Conn, role.cfg.SafeDatabase, CHTableName(schema.TableID), maps[0].SafePartName)
	if maps[0].PartPhysHash != info.PhysHash {
		t.Fatalf("safe mapping phys hash = %s want %s", maps[0].PartPhysHash, info.PhysHash)
	}
}

func assertNoPromotePartition(t *testing.T, ctx context.Context, role *Role, schema payloadexec.TableSchema, partitionID string) {
	t.Helper()
	parts := activePartsMust(t, ctx, role.d.Conn, role.cfg.PromoteDatabase, CHTableName(schema.TableID))
	for _, p := range parts {
		if logicalPartitionID(schema, p) == partitionID {
			t.Fatalf("promote partition still active: %+v", parts)
		}
	}
}

func activePartsMust(t *testing.T, ctx context.Context, conn clickhouse.Conn, db, table string) []partInfo {
	t.Helper()
	parts, err := activeParts(ctx, conn, db, table)
	if err != nil {
		t.Fatalf("active parts: %v", err)
	}
	return parts
}

func accumulatorHexZero() string {
	h, err := lthashCombineHexAll("", nil)
	if err != nil {
		panic(err)
	}
	return h
}
