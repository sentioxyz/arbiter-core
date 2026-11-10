package snode

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/fspayload"
	"github.com/sentioxyz/arbiter-core/wire"
)

// unsupportedTypeCandidatesS are declarations admission rules v2 or later may
// admit. The test keeps the ones the profile compiled into this binary still
// refuses (see verifier/unsupported_type_refusal_test.go for the rationale).
var unsupportedTypeCandidatesS = []string{
	"Date32", "Nullable(UInt64)", "Nullable(String)", "UUID", "Int128", "UInt256",
	"LowCardinality(String)", "Decimal(18, 4)", "Array(String)",
}

// TestUnsupportedTypeTableIsRefusedLocally pins what an SNode whose pinned
// housegate profile predates a column type does with an Active registry
// table it owns that declares it: the reconciler cannot build its protocol
// tables, so it never reports Ready; fresh intake is refused before anything
// is journaled or claimed; and the schema every promotion, cleanup and
// recorded-intake path resolves fails with ErrUnsupportedColumnType. Nothing
// on any of those paths signs a result for the table.
func TestUnsupportedTypeTableIsRefusedLocally(t *testing.T) {
	var declared []string
	for _, d := range unsupportedTypeCandidatesS {
		if !payloadexec.SupportedColumnType(d) {
			declared = append(declared, d)
		}
	}
	if len(declared) == 0 {
		t.Fatal("every candidate is admitted by the pinned profile; add one it still refuses")
	}
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	for i, typ := range declared {
		t.Run(typ, func(t *testing.T) {
			cfg := testConfigS(t)
			setUniqueDatabases(t, &cfg)
			cfg.SchemaSource = ddl.SchemaSourceNetworkState
			suffix := strings.TrimPrefix(cfg.UnsafeDatabase, "hg_unsafe_")
			cfg.NodeID = "snode-" + suffix
			t.Cleanup(func() {
				for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
					_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
				}
			})
			claims := &sourceClaimsFake{}
			client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startSourceClaimsFake(t, claims)}}})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(client.Close)
			payloads, err := fspayload.New(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			view := newFakeRegistryS()
			c := chainSchemaS("db.u" + string(rune('a'+i)) + "_" + suffix)
			c.Columns = append(append([]lthash.Column(nil), c.Columns...), lthash.Column{Name: "x", Type: typ})
			view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, wire.TableStatusActive))
			role, err := New(cfg, Deps{Client: client, Conn: conn, Payloads: payloads, Registry: view})
			if err != nil {
				t.Fatal(err)
			}
			_ = role.ensureProtocolTables(ctx) // the per-table failure backs off; it is not the assertion
			if role.TableReady(c.TableID) {
				t.Fatalf("table declaring %s is Ready", typ)
			}

			payload := nativePayload(t, pv{"p0", 1})
			env := intakeEnvelope(payload)
			env.TargetTableID = c.TableID
			env.SQL = "INSERT INTO " + c.TableID + " FORMAT Native"
			env.SQLHash = replay.DigestString(env.SQL)
			env.SchemaHash = payloadexec.TableSchemaHash("testnet", c)
			req := PrepareRequest{Envelope: env, PayloadEncoding: stagedNativeEncoding, Revision: testRevision}
			if _, err := role.PrepareLocalStatement(ctx, req, payload); !errors.Is(err, ErrTableNotReady) {
				t.Fatalf("PrepareLocalStatement = %v, want ErrTableNotReady", err)
			}
			if _, ok, _ := role.journal.load(env.StatementID.Flat()); ok {
				t.Fatal("a refused statement must write no journal record")
			}
			if _, err := role.schemaFor(c.TableID); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("schemaFor = %v, want ErrUnsupportedColumnType", err)
			}
			claims.mu.Lock()
			rcs, pas, cas := len(claims.rcs), len(claims.pas), len(claims.cas)
			claims.mu.Unlock()
			if rcs+pas+cas != 0 {
				t.Fatalf("signed submissions: %d result claims, %d promotion acks, %d cleanup acks; want none", rcs, pas, cas)
			}
		})
	}
}

// TestV2TypeTableIsServed is the upgraded half: on a profile that replays the
// admission rules v2 types, an Active registry table declaring them becomes
// Ready and fresh intake of a Native payload carrying them is prepared.
func TestV2TypeTableIsServed(t *testing.T) {
	v2 := []lthash.Column{
		{Name: "d32", Type: "Date32"}, {Name: "u", Type: "UUID"}, {Name: "i128", Type: "Int128"},
		{Name: "u256", Type: "UInt256"}, {Name: "lc", Type: "LowCardinality(String)"},
		{Name: "n", Type: "Nullable(UInt64)"}, {Name: "ns", Type: "Nullable(String)"},
	}
	for _, c := range v2 {
		if !payloadexec.SupportedColumnType(c.Type) {
			t.Skipf("the pinned profile does not replay %s", c.Type)
		}
	}
	ctx := context.Background()
	conn := requireCH(t)
	requireKeeperS(t, conn)
	cfg := testConfigS(t)
	setUniqueDatabases(t, &cfg)
	cfg.SchemaSource = ddl.SchemaSourceNetworkState
	suffix := strings.TrimPrefix(cfg.UnsafeDatabase, "hg_unsafe_")
	cfg.NodeID = "snode-" + suffix
	t.Cleanup(func() {
		for _, db := range []string{cfg.UnsafeDatabase, cfg.SafeDatabase, cfg.PromoteDatabase} {
			_ = conn.Exec(context.Background(), "DROP DATABASE IF EXISTS "+db+" SYNC")
		}
	})
	claims := &sourceClaimsFake{}
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: startSourceClaimsFake(t, claims)}}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	payloads, err := fspayload.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	view := newFakeRegistryS()
	c := chainSchemaS("db.v2_" + suffix)
	c.Columns = append(append([]lthash.Column(nil), c.Columns...), v2...)
	view.set(genesisIncarnationS(1, cfg.Tables[0]), chainIncarnationS(t, 2, c, wire.TableStatusActive))
	role, err := New(cfg, Deps{Client: client, Conn: conn, Payloads: payloads, Registry: view})
	if err != nil {
		t.Fatal(err)
	}
	if err := role.ensureProtocolTables(ctx); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !role.TableReady(c.TableID) {
		t.Fatal("the v2 table is not Ready")
	}

	p, v := new(proto.ColStr), new(proto.ColUInt64)
	p.Append("p0")
	v.Append(1)
	d32 := new(proto.ColDate32)
	d32.Append(time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC))
	u := new(proto.ColUUID)
	u.Append(uuid.MustParse("61f0c404-5cb3-11e7-907b-a6006ad3dba0"))
	lc := proto.NewLowCardinality(new(proto.ColStr))
	lc.Append("eu")
	n := new(proto.ColUInt64).Nullable()
	n.Append(proto.Null[uint64]())
	ns := new(proto.ColStr).Nullable()
	ns.Append(proto.NewNullable("x"))
	var buf proto.Buffer
	buf.PutUVarInt(uint64(proto.ClientCodeData))
	buf.PutString("")
	input := proto.Input{{Name: "p", Data: p}, {Name: "v", Data: v}, {Name: "d32", Data: d32}, {Name: "u", Data: u},
		{Name: "i128", Data: &proto.ColInt128{{Low: ^uint64(0), High: ^uint64(0)}}}, {Name: "u256", Data: &proto.ColUInt256{{Low: proto.UInt128{Low: 7}}}},
		{Name: "lc", Data: lc}, {Name: "n", Data: n}, {Name: "ns", Data: ns}}
	if err := (proto.Block{Rows: 1, Columns: len(input)}).EncodeBlock(&buf, testRevision, input); err != nil {
		t.Fatal(err)
	}
	payload := append([]byte(nil), buf.Buf...)
	env := intakeEnvelope(payload)
	env.TargetTableID = c.TableID
	env.SQL = "INSERT INTO " + c.TableID + " FORMAT Native"
	env.SQLHash = replay.DigestString(env.SQL)
	env.SchemaHash = payloadexec.TableSchemaHash("testnet", c)
	req := PrepareRequest{Envelope: env, PayloadEncoding: stagedNativeEncoding, Revision: testRevision}
	if _, err := role.PrepareLocalStatement(ctx, req, payload); err != nil {
		t.Fatalf("prepare on the v2 table: %v", err)
	}
}
