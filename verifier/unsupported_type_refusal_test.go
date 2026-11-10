package verifier

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"github.com/google/uuid"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

// A verifier whose pinned housegate profile predates a column type must never
// sign a result for a table that declares it. A later admission rules version
// can admit the type before every verifier has upgraded, so the old binary's
// only acceptable answer is a local refusal: no replay attestation at all,
// never a signed root computed without the column. These tests pin that for
// every candidate type the profile compiled into this binary still refuses;
// when a housegate bump admits one, it drops out of the set and the next
// unsupported candidate keeps the property covered.

// unsupportedTypeCandidates are declarations admission rules v2 or later may
// admit, each with a Native column that carries a value of that type.
var unsupportedTypeCandidates = []struct {
	declared string
	column   func() chproto.ColInput
}{
	{"Date32", func() chproto.ColInput {
		c := new(chproto.ColDate32)
		c.Append(time.Date(1950, time.January, 2, 0, 0, 0, 0, time.UTC))
		return c
	}},
	{"Nullable(UInt64)", func() chproto.ColInput {
		c := new(chproto.ColUInt64).Nullable()
		c.Append(chproto.Null[uint64]())
		return c
	}},
	{"Nullable(String)", func() chproto.ColInput {
		c := new(chproto.ColStr).Nullable()
		c.Append(chproto.NewNullable("x"))
		return c
	}},
	{"UUID", func() chproto.ColInput {
		c := new(chproto.ColUUID)
		c.Append(uuid.MustParse("61f0c404-5cb3-11e7-907b-a6006ad3dba0"))
		return c
	}},
	{"Int128", func() chproto.ColInput { c := new(chproto.ColInt128); c.Append(chproto.Int128{Low: 1}); return c }},
	{"UInt256", func() chproto.ColInput { c := new(chproto.ColUInt256); c.Append(chproto.UInt256{}); return c }},
	{"LowCardinality(String)", func() chproto.ColInput {
		c := chproto.NewLowCardinality(new(chproto.ColStr))
		c.Append("x")
		return c
	}},
	{"Decimal(18, 4)", func() chproto.ColInput { c := new(chproto.ColDecimal64); c.Append(chproto.Decimal64(12345)); return c }},
	{"Array(String)", func() chproto.ColInput { c := new(chproto.ColStr).Array(); c.Append([]string{"a"}); return c }},
}

// unsupportedCandidates returns the candidates this binary's profile refuses
// and fails the test when none is left, so the property is never vacuous.
func unsupportedCandidates(t *testing.T) []struct {
	declared string
	column   func() chproto.ColInput
} {
	t.Helper()
	var out []struct {
		declared string
		column   func() chproto.ColInput
	}
	for _, c := range unsupportedTypeCandidates {
		if !payloadexec.SupportedColumnType(c.declared) {
			out = append(out, c)
		}
	}
	if len(out) == 0 {
		t.Fatal("every candidate is admitted by the pinned profile; add one it still refuses")
	}
	return out
}

func unsupportedTypeSchema(tableID, declared string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: tableID, Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "c", Type: declared}}}
}

func schemaJSON(t *testing.T, s payloadexec.TableSchema) string {
	t.Helper()
	js, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	return string(js)
}

// TestReplayCoreRefusesUnsupportedTypesBeforeSigning drives the real replay
// core (the one NewReplayCore builds for production) past every gate in
// front of it: a table-set transition that adds the table, and a statement
// block that writes it with a real Native payload. Both must fail before the
// materializer creates anything and before the signer is asked for a
// receipt signature.
func TestReplayCoreRefusesUnsupportedTypesBeforeSigning(t *testing.T) {
	for _, tc := range unsupportedCandidates(t) {
		t.Run(tc.declared, func(t *testing.T) {
			f := newMemoFixture(t)
			chain := unsupportedTypeSchema("db.c", tc.declared)
			js := schemaJSON(t, chain)

			add := replay.ReplayJob{BlockSeq: 1, PrevSafeSnapshotID: f.job.PrevSafeSnapshotID, PrevStateRoot: f.job.PrevStateRoot,
				SchemaSnapshotID: f.cfg.SchemaSnapshotID, ExecutorProfileID: f.cfg.ExecutorProfileID,
				TableSetTransition: &replay.ReplayTableSetTransition{
					Adds:          []replay.ReplayTableSchema{{TableID: chain.TableID, SchemaJSON: js}},
					NewSchemaRoot: payloadexec.SchemaRoot(f.cfg.NetworkID, append(append([]payloadexec.TableSchema(nil), f.cfg.Tables...), chain)),
				}}
			if _, err := f.core.Verify(context.Background(), add); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("add transition: Verify = %v, want ErrUnsupportedColumnType", err)
			}

			p := new(chproto.ColStr)
			p.Append("p0")
			// Encoded with ch-go directly: housegate's own encoder refuses the
			// type, but a client on the wire does not ask it.
			var buf chproto.Buffer
			buf.PutUVarInt(uint64(chproto.ClientCodeData))
			buf.PutString("")
			if err := (chproto.Block{Rows: 1, Columns: 2}).EncodeBlock(&buf, 54460, chproto.Input{{Name: "p", Data: p}, {Name: "c", Data: tc.column()}}); err != nil {
				t.Fatal(err)
			}
			raw := append([]byte(nil), buf.Buf...)
			f.payloads.Put("u", raw)
			write := f.job
			write.TableSchemas = []replay.ReplayTableSchema{{TableID: chain.TableID, SchemaJSON: js}}
			sql := "INSERT INTO db.c FORMAT Native"
			write.Statements = []replay.Statement{{StatementID: "s2", StatementSeq: 1, SQL: sql, SQLHash: replay.DigestString(sql),
				SettingsHash: replay.DigestString("empty-settings"), PayloadRef: "u", PayloadHash: replay.DigestBytes(raw),
				PayloadLength: uint64(len(raw)), TargetTableID: chain.TableID, PayloadFormat: replay.PayloadFormatClickHouseNativeData,
				ClientRevision: 54460, SchemaHash: payloadexec.TableSchemaHash(f.cfg.NetworkID, chain)}}
			if _, err := f.core.Verify(context.Background(), write); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("statement block: Verify = %v, want ErrUnsupportedColumnType", err)
			}
			// The same payload against the old profile's Native decoder: the
			// refusal does not depend on the schema gate alone.
			if _, err := nativepayload.Decode(chain, 54460, raw); !errors.Is(err, payloadexec.ErrUnsupportedColumnType) {
				t.Fatalf("Native decode = %v, want ErrUnsupportedColumnType", err)
			}
			if f.signer.calls.Load() != 0 || f.ch.count() != 0 {
				t.Fatalf("signer calls = %d, scratch creates = %d; an unsupported type must refuse before both",
					f.signer.calls.Load(), f.ch.count())
			}
		})
	}
}

// TestUnsupportedTypeTableNeverBecomesReady runs the verifier role on real
// ClickHouse: the reconciler cannot build the table's protocol DDL, the
// table never reports Ready, and the add transition is refused with no
// attestation submitted, even though the replay core here would sign
// anything it is handed.
func TestUnsupportedTypeTableNeverBecomesReady(t *testing.T) {
	for _, tc := range unsupportedCandidates(t) {
		t.Run(tc.declared, func(t *testing.T) {
			view, core := newFakeRegistryView(), &fakeReplayCore{}
			role, server, cfg := newRegistryRoleV(t, view, core)
			chain := unsupportedTypeSchema("db.u_"+cfg.ReplicaID[len("verifier-"):], tc.declared)
			view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, chain, wire.TableStatusActive))

			err := role.tables.Reconcile(context.Background(), ddl.ModeVerifyOnly)
			if role.TableReady(chain.TableID) {
				t.Fatalf("table with %s is Ready after reconcile (err %v)", tc.declared, err)
			}
			if err := role.handleReplayJob(context.Background(), addTransitionJob(7, chain.TableID)); !errors.Is(err, ErrAddedTableNotReady) {
				t.Fatalf("handleReplayJob = %v, want ErrAddedTableNotReady", err)
			}
			if core.jobCount() != 0 {
				t.Fatal("the replay core ran for a table that is not ready")
			}
			if _, _, atts, _ := server.snapshot(); len(atts) != 0 {
				t.Fatalf("attestations = %d, want 0", len(atts))
			}
		})
	}
}
