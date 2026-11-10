package verifier

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	chproto "github.com/ClickHouse/ch-go/proto"
	"google.golang.org/protobuf/proto"

	"github.com/housegate/housegate/pkg/lthash"
	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/nativepayload"
	"github.com/housegate/housegate/pkg/replay/payloadexec"

	arbiter "github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/dataplane/evidence"
	"github.com/sentioxyz/arbiter-core/wire"
	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// arbiter-core#74: a verifier refuses, without a signature, any statement
// block that writes a table it does not hold Ready, not only the transition
// that adds one.

// unservableSchema is a chain table this binary's DDL refuses (the P1c
// partition freeze), so it can never become Ready here: the shape of a table
// a later admission rules version admits before this verifier upgrades.
func unservableSchema(tableID string) payloadexec.TableSchema {
	return payloadexec.TableSchema{TableID: tableID, PartitionBy: "c",
		Columns: []lthash.Column{{Name: "p", Type: "String"}, {Name: "c", Type: "UInt64"}}}
}

// writeJob is a one-statement Native write to schema; chain tables carry
// their schema in table_schemas, genesis tables do not.
func writeJob(t *testing.T, cfg Config, blockSeq uint64, prevSnapshot, prevRoot string, schema payloadexec.TableSchema, chain bool, payloadRef string, payload []byte) replay.ReplayJob {
	t.Helper()
	sql := "INSERT INTO " + schema.TableID + " FORMAT Native"
	job := replay.ReplayJob{BlockSeq: blockSeq, PrevSafeSnapshotID: prevSnapshot, PrevStateRoot: prevRoot,
		SchemaSnapshotID: cfg.SchemaSnapshotID, ExecutorProfileID: cfg.ExecutorProfileID,
		SourceClaimRoot: replay.DigestString("claim"),
		Statements: []replay.Statement{{StatementID: "s1", StatementSeq: 1, SQL: sql, SQLHash: replay.DigestString(sql),
			SettingsHash: replay.DigestString("empty-settings"), PayloadRef: payloadRef, PayloadHash: replay.DigestBytes(payload),
			PayloadLength: uint64(len(payload)), TargetTableID: schema.TableID, PayloadFormat: replay.PayloadFormatClickHouseNativeData,
			ClientRevision: 54460, SchemaHash: payloadexec.TableSchemaHash(cfg.NetworkID, schema)}}}
	if chain {
		job.TableSchemas = []replay.ReplayTableSchema{{TableID: schema.TableID, SchemaJSON: schemaJSON(t, schema)}}
	}
	return job
}

func pvPayload(t *testing.T, p string, v uint64) []byte {
	t.Helper()
	raw, err := nativepayload.EncodeClientDataPacket(54460, []chproto.InputColumn{
		{Name: "p", Data: &chproto.ColStr{Buf: []byte(p), Pos: []chproto.Position{{Start: 0, End: len(p)}}}},
		{Name: "v", Data: &chproto.ColUInt64{v}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestReplayJob_WriteToATableThatIsNotReadyIsRefused(t *testing.T) {
	for _, tc := range []struct {
		name   string
		schema func(id string) payloadexec.TableSchema
	}{
		// DDL refused: the table can never become Ready on this binary.
		{"unservable", unservableSchema},
		// Valid, but no reconcile pass has created it yet.
		{"not yet reconciled", func(id string) payloadexec.TableSchema { s := scanTableSchema(); s.TableID = id; return s }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			view, core := newFakeRegistryView(), &fakeReplayCore{}
			role, server, cfg := newRegistryRoleV(t, view, core)
			// A gate that waited would hold the subscription for this long.
			role.cfg.AddTransitionReadyWait = time.Minute
			chain := tc.schema("db.w_" + cfg.ReplicaID[len("verifier-"):])
			view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, chain, wire.TableStatusActive))
			if tc.name == "unservable" {
				_ = role.tables.Reconcile(context.Background(), ddl.ModeVerifyOnly)
			}
			if role.TableReady(chain.TableID) {
				t.Fatal("fixture: the table is Ready")
			}

			job := writeJob(t, cfg, 8, "0xprev", "0xroot", chain, true, "u", []byte("x"))
			start := time.Now()
			err := role.handleReplayJob(context.Background(), wire.ReplayJobToPB(job))
			if !errors.Is(err, ErrWrittenTableNotReady) || !errors.Is(err, ErrTableNotReady) {
				t.Fatalf("handleReplayJob = %v, want ErrWrittenTableNotReady", err)
			}
			if errors.Is(err, ErrAddedTableNotReady) {
				t.Fatalf("a write refusal must not read as an add refusal: %v", err)
			}
			if d := time.Since(start); d > 10*time.Second {
				t.Fatalf("the write gate waited %v; it must refuse at once", d)
			}
			if core.jobCount() != 0 {
				t.Fatal("the replay core ran for a table that is not ready")
			}
			if _, _, atts, _ := server.snapshot(); len(atts) != 0 {
				t.Fatalf("attestations = %d, want 0", len(atts))
			}

			if tc.name == "not yet reconciled" {
				if err := role.tables.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
					t.Fatal(err)
				}
				if err := role.handleReplayJob(context.Background(), wire.ReplayJobToPB(job)); err != nil {
					t.Fatalf("once the table is Ready: %v", err)
				}
				if _, _, atts, _ := server.snapshot(); len(atts) != 1 {
					t.Fatalf("attestations = %d, want 1 once Ready", len(atts))
				}
			}
		})
	}
}

// A refused write is one withheld vote: the subscription moves on to the
// next job at once, as it does for any refused or failed job, and the arbiter
// re-dispatches the refused one later. The test drives the role's dispatch
// through the real subscription loop without the protocol-table reconcile
// loop: Run itself currently ends on the first reconcile pass that meets a
// table whose DDL this binary refuses (#76), which is not what this test
// pins.
func TestRun_RefusedWriteDoesNotStallLaterJobs(t *testing.T) {
	view, core := newFakeRegistryView(), &fakeReplayCore{}
	role, server, cfg := newRegistryRoleV(t, view, core)
	role.cfg.AddTransitionReadyWait = time.Minute
	chain := unservableSchema("db.l_" + cfg.ReplicaID[len("verifier-"):])
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, chain, wire.TableStatusActive))
	if role.TableReady(chain.TableID) || !role.TableReady(cfg.Tables[0].TableID) {
		t.Fatal("fixture: want the chain table not Ready and the genesis table Ready")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- role.d.Client.RunVerifierSubscription(ctx, cfg.ReplicaID, func(d *pb.VerifierDispatch) error {
			return role.handleReplayJob(ctx, d.GetReplayJob())
		})
	}()
	t.Cleanup(func() { cancel(); <-done })

	refused := writeJob(t, cfg, 9, "0xprev", "0xroot", chain, true, "u", []byte("x"))
	served := writeJob(t, cfg, 10, "0xprev", "0xroot", cfg.Tables[0], false, "g", []byte("y"))
	for i := 0; i < 3; i++ { // the arbiter's re-dispatch of the refused job
		server.push(&pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: wire.ReplayJobToPB(refused)}})
	}
	server.push(&pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: wire.ReplayJobToPB(served)}})
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, _, atts, _ := server.snapshot(); len(atts) > 0 {
			jobs := core.jobsSnapshot()
			if len(jobs) != 1 || jobs[0].BlockSeq != 10 || len(atts) != 1 {
				t.Fatalf("replayed blocks %v with %d attestations, want only block 10", blockSeqs(jobs), len(atts))
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the job behind the refused write was never attested (replayed %v)", blockSeqs(core.jobsSnapshot()))
}

func blockSeqs(jobs []replay.ReplayJob) []uint64 {
	out := make([]uint64, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, j.BlockSeq)
	}
	return out
}

// The byte-side scan of a table this verifier never created already fails,
// so before the gate the verifier's evidence for such a block was incomplete
// anyway (an attestation without a scan never passes three-way quorum): the
// gate withholds no vote that could have counted.
func TestScan_OfATableThatIsNotReadyFails(t *testing.T) {
	view, core := newFakeRegistryView(), &fakeReplayCore{}
	_, _, cfg := newRegistryRoleV(t, view, core)
	chain := unservableSchema("db.s_" + cfg.ReplicaID[len("verifier-"):])
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, chain, wire.TableStatusActive))
	scanner := NewRegistryScanner(cfg, requireCH(t), view)
	if _, err := scanner.Scan(context.Background(), []arbiter.PartRef{{TableID: chain.TableID, PartitionID: "p_1", PartName: "p_1_1_1_0", PartRowLtHash: "0x00"}}); err == nil {
		t.Fatal("scan of a table this verifier never created succeeded")
	}
}

// For a table that is Ready the gate is a pass-through: the job reaches the
// replay core unchanged and its attestation is submitted unchanged.
func TestReplayJob_WriteToAReadyChainTableIsPassedThrough(t *testing.T) {
	view, core := newFakeRegistryView(), &fakeReplayCore{}
	core.results = []replay.ReplayAttestation{{ReplicaID: "v1", ReceiptHash: "0xr", Signature: "sig", MatchSourceRoot: true,
		Receipt: replay.ExecutionReceipt{BlockSeq: 8, ComputedStateRoot: "0xstate"}}}
	role, server, cfg := newRegistryRoleV(t, view, core)
	chain := scanTableSchema()
	chain.TableID = "db.r_" + cfg.ReplicaID[len("verifier-"):]
	view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, chain, wire.TableStatusActive))
	if err := role.tables.Reconcile(context.Background(), ddl.ModeVerifyOnly); err != nil {
		t.Fatal(err)
	}
	m := wire.ReplayJobToPB(writeJob(t, cfg, 8, "0xprev", "0xroot", chain, true, "u", []byte("x")))
	if err := role.handleReplayJob(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	jobs := core.jobsSnapshot()
	if len(jobs) != 1 || !reflect.DeepEqual(jobs[0], wire.ReplayJobFromPB(m)) {
		t.Fatalf("the replay core saw %+v, want the dispatched job unchanged", jobs)
	}
	_, _, atts, _ := server.snapshot()
	if len(atts) != 1 || !proto.Equal(atts[0], wire.AttestationToPB(core.results[0])) {
		t.Fatalf("submitted %v, want the core's attestation unchanged", atts)
	}
}

// With the real replay core, a write to a Ready table produces the same
// attestation bytes through the role (with the write gate) as through an
// evidence handler gated the way the role was before arbiter-core#74 (adds
// only).
func TestReplayJob_ReadyTableAttestationIsByteIdenticalToTheAddOnlyGate(t *testing.T) {
	view := newFakeRegistryView()
	role, server, cfg := newRegistryRoleV(t, view, &fakeReplayCore{})
	g := cfg.Tables[0]
	if !role.TableReady(g.TableID) {
		t.Fatal("fixture: the genesis table is not Ready")
	}
	snap, err := payloadexec.New(cfg.NetworkID, cfg.Tables...).GenesisSnapshot(0, cfg.SchemaSnapshotID, cfg.ExecutorProfileID)
	if err != nil {
		t.Fatal(err)
	}
	snapshots := payloadexec.NewMemSnapshotStore()
	snapshots.Put(snap)
	payloads := payloadexec.NewMemPayloadStore()
	raw := pvPayload(t, "p0", 7)
	payloads.Put("g", raw)
	core, err := NewReplayCore(cfg, &memoCH{}, snapshots, payloads)
	if err != nil {
		t.Fatal(err)
	}
	role.d.Replay = core
	m := wire.ReplayJobToPB(writeJob(t, cfg, 1, snap.SnapshotID, snap.StateRoot, g, false, "g", raw))

	addOnly, err := evidence.NewHandler(evidence.Config{ReplicaID: cfg.ReplicaID, Ed25519Seed: cfg.Ed25519Seed}, evidence.Deps{
		Client: role.d.Client, Replay: core, Scanner: role.d.Scanner,
		TablesReady: role.requireAddedTablesReady, NotReady: ErrAddedTableNotReady, Logger: role.d.Logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := addOnly.HandleReplayJob(context.Background(), m); err != nil {
		t.Fatalf("add-only gate: %v", err)
	}
	if err := role.handleReplayJob(context.Background(), m); err != nil {
		t.Fatalf("role with the write gate: %v", err)
	}
	_, _, atts, _ := server.snapshot()
	if len(atts) != 2 {
		t.Fatalf("attestations = %d, want 2", len(atts))
	}
	if atts[0].GetSignature() == "" {
		t.Fatal("fixture: the real core did not sign")
	}
	before, err := proto.MarshalOptions{Deterministic: true}.Marshal(atts[0])
	if err != nil {
		t.Fatal(err)
	}
	after, err := proto.MarshalOptions{Deterministic: true}.Marshal(atts[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatalf("attestation bytes differ:\nadd-only gate %x\nwrite gate    %x", before, after)
	}
}
