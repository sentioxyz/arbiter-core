package evidence

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/authority/authoritytest"
	"github.com/sentioxyz/arbiter-core/dataplane"
	"github.com/sentioxyz/arbiter-core/wire"
)

// The messages the network-pool verifier (package verifier at arbiter-core
// v0.15.0, before this package existed) submitted for frozenJob and
// frozenScanRequest as verifier-1 with authoritytest.VerifierKey(1), its
// replay core answering frozenAttestation and its scanner frozenScans:
// proto.MarshalOptions{Deterministic: true} of the SubmitAttestation and
// SubmitByteSideScan requests, hex. Captured once from that verifier and
// never regenerated; verifier's TestVerifierRoleDelegatesToTheEvidenceHandler
// pins the same two values.
const (
	frozenAttestationHex = "0a0a76657269666965722d3112001a0a3078726563656970743922063078736967392801"
	frozenScanHex        = "0a0a76657269666965722d3110091a340a0464622e7412063230323630371a0a3078636c61696d656431220a3078636c61696d6564312a0c3230323630375f315f315f301a340a0464622e7412063230323630381a0a3078636c61696d656432220a30787363616e6e6564322a0c3230323630385f325f325f3022423078323731336266636230353561366138333637323163386235613066656561333164323864663463336564313933306431336638393363616635656239306634342a80013963316437313533303932366662353665366436333236623561376433623739666530333635616662623863646266623531613333353931623530663335333334346265306661306134313034626666656463333634326237646130376435663933313463383966393437353931336338316534343165373531373830653033"
)

var (
	frozenJob = replay.ReplayJob{BlockSeq: 9, PrevSafeSnapshotID: "snap-8", PrevStateRoot: "0xprev",
		SchemaSnapshotID: "schema-genesis", ExecutorProfileID: "housegate-replay-mvp-v0", SourceClaimRoot: "0xsource"}
	frozenAttestation = replay.ReplayAttestation{ReplicaID: authoritytest.VerifierNodeID(1), ReceiptHash: "0xreceipt9",
		Signature: "0xsig9", MatchSourceRoot: true}
	frozenScanParts = []arbiter.PartRef{
		{TableID: "db.t", PartitionID: "202607", PartRowLtHash: "0xclaimed1", PartName: "202607_1_1_0"},
		{TableID: "db.t", PartitionID: "202608", PartRowLtHash: "0xclaimed2", PartName: "202608_2_2_0"},
	}
	frozenScans = []arbiter.PartScan{
		{TableID: "db.t", PartitionID: "202607", ClaimedPartRowLtHash: "0xclaimed1", ScannedPartRowLtHash: "0xclaimed1", LivePartName: "202607_1_1_0"},
		{TableID: "db.t", PartitionID: "202608", ClaimedPartRowLtHash: "0xclaimed2", ScannedPartRowLtHash: "0xscanned2", LivePartName: "202608_2_2_0"},
	}
)

func frozenScanRequest() *pb.ByteSideScanRequest {
	return wire.ByteSideScanDispatch(9, frozenScanParts).GetByteSideScan()
}

// newFrozenHandler is verifier-1's handler over fakes that answer the frozen
// inputs; deps edits its Deps before NewHandler.
func newFrozenHandler(t *testing.T, gw *gatewayFake, deps func(*Deps)) (*Handler, *replayFake, *scannerFake) {
	t.Helper()
	core := &replayFake{att: frozenAttestation}
	scanner := &scannerFake{scans: frozenScans}
	d := Deps{Client: startGatewayFake(t, gw), Replay: core, Scanner: scanner}
	if deps != nil {
		deps(&d)
	}
	h, err := NewHandler(Config{ReplicaID: authoritytest.VerifierNodeID(1), Ed25519Seed: authoritytest.VerifierKey(1).Seed()}, d)
	if err != nil {
		t.Fatal(err)
	}
	return h, core, scanner
}

func TestEvidenceHandlerSubmitsWhatTheVerifierSubmitted(t *testing.T) {
	gw := &gatewayFake{}
	h, _, scanner := newFrozenHandler(t, gw, nil)
	if err := h.HandleReplayJob(context.Background(), wire.ReplayJobToPB(frozenJob)); err != nil {
		t.Fatal(err)
	}
	if err := h.HandleScanRequest(context.Background(), frozenScanRequest()); err != nil {
		t.Fatal(err)
	}
	atts, scans := gw.submitted()
	if len(atts) != 1 || len(scans) != 1 {
		t.Fatalf("%d attestations and %d scans submitted, want one each", len(atts), len(scans))
	}
	if got := hex.EncodeToString(marshalDeterministic(t, atts[0])); got != frozenAttestationHex {
		t.Fatalf("attestation = %s, want the verifier's %s", got, frozenAttestationHex)
	}
	if got := hex.EncodeToString(marshalDeterministic(t, scans[0])); got != frozenScanHex {
		t.Fatalf("scan = %s, want the verifier's %s", got, frozenScanHex)
	}
	if len(scanner.parts) != 1 || fmt.Sprint(scanner.parts[0]) != fmt.Sprint(frozenScanParts) {
		t.Fatalf("scanner asked for %v, want %v", scanner.parts, frozenScanParts)
	}
}

func TestEvidenceHandlerRunsObserveFirstAndRefusesWhenItFails(t *testing.T) {
	var order []string
	observeErr := errors.New("test: ledger write failed")
	for _, tc := range []struct {
		name    string
		observe error
		want    []string
	}{
		{"observe succeeds", nil, []string{"observe", "ready", "replay"}},
		{"observe fails", observeErr, []string{"observe"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			order = nil
			logs := &syncBuffer{}
			gw := &gatewayFake{}
			h, core, _ := newFrozenHandler(t, gw, func(d *Deps) {
				d.Observe = func(job replay.ReplayJob) error {
					order = append(order, "observe")
					if job.BlockSeq != frozenJob.BlockSeq {
						t.Errorf("observed block %d, want %d", job.BlockSeq, frozenJob.BlockSeq)
					}
					return tc.observe
				}
				d.TablesReady = func(context.Context, replay.ReplayJob) error {
					order = append(order, "ready")
					return nil
				}
				d.NotReady = errNotReadyForTest
				d.Logger = textLogger(logs)
			})
			core.before = func(replay.ReplayJob) { order = append(order, "replay") }
			err := h.HandleReplayJob(context.Background(), wire.ReplayJobToPB(frozenJob))
			if !errors.Is(err, tc.observe) {
				t.Fatalf("HandleReplayJob = %v, want %v", err, tc.observe)
			}
			if strings.Join(order, ",") != strings.Join(tc.want, ",") {
				t.Fatalf("order = %v, want %v", order, tc.want)
			}
			atts, _ := gw.submitted()
			if tc.observe != nil {
				if len(atts) != 0 || core.count() != 0 {
					t.Fatalf("a failed observer still replayed (%d) or submitted (%d)", core.count(), len(atts))
				}
				if !strings.Contains(logs.String(), "evidence observer failed; refusing to attest") {
					t.Fatalf("logs = %s", logs.String())
				}
			} else if len(atts) != 1 {
				t.Fatalf("%d attestations, want 1", len(atts))
			}
		})
	}
}

func TestEvidenceHandlerGatesTheReplayOnTablesReady(t *testing.T) {
	for _, tc := range []struct {
		name    string
		ready   error
		log     string
		wantLog string
	}{
		{"not ready, the verifier's text", fmt.Errorf("%w: block 9 adds [db.n]", errNotReadyForTest), "",
			"table-set transition adds a table this verifier has not created; refusing to attest"},
		{"not ready, the caller's text", fmt.Errorf("%w: db.t", errNotReadyForTest), "member: no replica; refusing to attest",
			"member: no replica; refusing to attest"},
		{"the context ended", context.Canceled, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logs := &syncBuffer{}
			gw := &gatewayFake{}
			h, core, _ := newFrozenHandler(t, gw, func(d *Deps) {
				d.TablesReady = func(context.Context, replay.ReplayJob) error { return tc.ready }
				d.NotReady = errNotReadyForTest
				d.NotReadyLog = tc.log
				d.Logger = textLogger(logs)
			})
			if err := h.HandleReplayJob(context.Background(), wire.ReplayJobToPB(frozenJob)); !errors.Is(err, tc.ready) {
				t.Fatalf("HandleReplayJob = %v, want %v", err, tc.ready)
			}
			if atts, _ := gw.submitted(); core.count() != 0 || len(atts) != 0 {
				t.Fatalf("a refused job replayed (%d) or submitted (%d)", core.count(), len(atts))
			}
			switch {
			case tc.wantLog == "" && strings.Contains(logs.String(), "refusing to attest"):
				t.Fatalf("an ended context was logged as a refusal: %s", logs.String())
			case tc.wantLog != "" && !strings.Contains(logs.String(), `msg="`+tc.wantLog+`"`):
				t.Fatalf("logs = %s, want %q", logs.String(), tc.wantLog)
			}
		})
	}
}

func TestEvidenceHandlerLogsWhatItRefusesAndWhatTheArbiterRejects(t *testing.T) {
	logs := &syncBuffer{}
	gw := &gatewayFake{rejectErr: status.Error(codes.InvalidArgument, "scan_hash does not match the scan body")}
	h, _, _ := newFrozenHandler(t, gw, func(d *Deps) { d.Logger = textLogger(logs) })
	if err := h.HandleScanRequest(context.Background(), frozenScanRequest()); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("HandleScanRequest = %v, want the arbiter's InvalidArgument", err)
	}
	for _, want := range []string{`msg="arbiter did not accept verifier evidence"`, "kind=\"byte-side scan\"", "block=9", "replica=verifier-1", "code=InvalidArgument", "scan_hash=0x2713bfcb", "parts=2"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs = %s, want %s", logs.String(), want)
		}
	}

	logs = &syncBuffer{}
	h, _, _ = newFrozenHandler(t, &gatewayFake{}, func(d *Deps) {
		d.Replay = &replayFake{err: errors.New("test: replay failed")}
		d.Scanner = &scannerFake{err: errors.New("test: scan failed")}
		d.Logger = textLogger(logs)
	})
	if err := h.HandleReplayJob(context.Background(), wire.ReplayJobToPB(frozenJob)); err == nil {
		t.Fatal("a failed replay was attested")
	}
	if err := h.HandleScanRequest(context.Background(), frozenScanRequest()); err == nil {
		t.Fatal("a failed scan was submitted")
	}
	for _, want := range []string{`msg="replay verify failed; refusing to attest"`, `msg="byte-side scan failed; refusing to attest"`} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("logs = %s, want %s", logs.String(), want)
		}
	}
}

func TestNewHandlerRefusesAnIncompleteConfig(t *testing.T) {
	client, err := dataplane.New(dataplane.Config{Peers: []dataplane.Peer{{ID: "n1", GRPCAddr: "127.0.0.1:1"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	seed := authoritytest.VerifierKey(1).Seed()
	full := Deps{Client: client, Replay: &replayFake{}, Scanner: &scannerFake{}}
	for _, tc := range []struct {
		name string
		cfg  Config
		deps Deps
		want string
	}{
		{"no replica id", Config{Ed25519Seed: seed}, full, "evidence: replica id is required"},
		{"a short seed", Config{ReplicaID: "v1", Ed25519Seed: seed[:31]}, full, "evidence: ed25519 seed must be 32 bytes"},
		{"no client", Config{ReplicaID: "v1", Ed25519Seed: seed}, Deps{Replay: full.Replay, Scanner: full.Scanner}, "evidence: client, replay core, and scanner are required"},
		{"no scanner", Config{ReplicaID: "v1", Ed25519Seed: seed}, Deps{Client: client, Replay: full.Replay}, "evidence: client, replay core, and scanner are required"},
		{"a gate without its sentinel", Config{ReplicaID: "v1", Ed25519Seed: seed},
			Deps{Client: client, Replay: full.Replay, Scanner: full.Scanner, TablesReady: func(context.Context, replay.ReplayJob) error { return nil }},
			"evidence: TablesReady needs the NotReady sentinel it wraps"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewHandler(tc.cfg, tc.deps); err == nil || err.Error() != tc.want {
				t.Fatalf("NewHandler = %v, want %q", err, tc.want)
			}
		})
	}
}
