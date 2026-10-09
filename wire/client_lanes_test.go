package wire

import (
	"bytes"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/housegate/housegate/pkg/replay/payloadexec"
	"github.com/sentioxyz/arbiter-core"
)

func TestStatementIDLaneSurvivesEveryConverter(t *testing.T) {
	env := legacyGoldenEnvelope()
	env.StatementID.ClientLane = "5e1f0a2b7c9d3e4f"
	if got := EnvelopeFromPB(EnvelopeToPB(env)).StatementID; got != env.StatementID {
		t.Fatalf("envelope round trip = %+v", got)
	}
	rc := arbiter.RCRecord{StatementID: env.StatementID, SourceNode: "s1"}
	if got := RCFromPB(RCToPB(rc)).StatementID; got != env.StatementID || got.Flat() != "0x00000000000000000000000000000000000000a1:5e1f0a2b7c9d3e4f:42:9f1c" {
		t.Fatalf("RC round trip = %+v (%s): the SNode's claim must name the same laned flat id the FSM indexed", got, got.Flat())
	}
	b, err := Encode(Command{SubmitStatement: &SubmitStatement{Envelope: env}})
	if err != nil {
		t.Fatal(err)
	}
	cmd, err := Decode(b)
	if err != nil || cmd.SubmitStatement.Envelope.StatementID != env.StatementID {
		t.Fatalf("command round trip = %+v, %v", cmd.SubmitStatement, err)
	}
}

// TestDispatchedStatementIDIsOpaque pins the fact behind housegate spec
// 2026-10-09 §7 (activation gate rationale): a verifier receives each
// statement's flat id as an opaque string that the leader rendered with
// StatementID.Flat() (arbiter fsm/reads_dispatch.go), and RowID hashes those
// bytes. A verifier's row ids therefore follow the leader's rendering, not the
// verifier's own StatementID type; the lane-stripped rendering a lane-unaware
// leader would produce yields different row ids, which gate (1) of §5.6
// (every voter lane-aware) prevents.
func TestDispatchedStatementIDIsOpaque(t *testing.T) {
	laned := arbiter.StatementID{ClientAccount: "0x00000000000000000000000000000000000000a1", ClientLane: "5e1f0a2b7c9d3e4f", ClientSeq: 42, ClientNonce: "9f1c"}
	job := replay.ReplayJob{BlockSeq: 7, Statements: []replay.Statement{{StatementID: laned.Flat(), StatementSeq: 1}}}
	got := ReplayJobFromPB(ReplayJobToPB(job))
	if len(got.Statements) != 1 || got.Statements[0].StatementID != "0x00000000000000000000000000000000000000a1:5e1f0a2b7c9d3e4f:42:9f1c" {
		t.Fatalf("dispatched statements = %+v: the flat id must cross the wire byte for byte", got.Statements)
	}
	stripped := laned
	stripped.ClientLane = ""
	if bytes.Equal(payloadexec.RowID("net", "db.t", got.Statements[0].StatementID, 0), payloadexec.RowID("net", "db.t", stripped.Flat(), 0)) {
		t.Fatal("a lane-stripped flat id must derive different row ids")
	}
}

func TestClientLaneParamsWire(t *testing.T) {
	if ClientLaneParamsFromPB(nil) != nil || ClientLaneParamsToPB(nil) != nil {
		t.Fatal("absent params must stay absent")
	}
	u := legacyGoldenUpdate()
	u.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	got := ConsensusParamsUpdateFromPB(ConsensusParamsUpdateToPB(u))
	if got.ClientLanes == nil || *got.ClientLanes != *u.ClientLanes || got.ClientLanes == u.ClientLanes {
		t.Fatalf("update round trip = %+v", got.ClientLanes)
	}
}

func TestTableRegistrySnapshotCarriesClientLanes(t *testing.T) {
	s := TableRegistrySnapshot{Params: arbiter.TableRegistryParams{ChainID: 1, DatabasesContract: "0x00000000000000000000000000000000000000d1", ActivationBlock: 1, Confirmation: arbiter.TableRegistryConfirmationSafe}, Version: 3}
	got, err := TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || got.ClientLanes != nil {
		t.Fatalf("disabled lanes: %+v, %v", got.ClientLanes, err)
	}
	s.ClientLanes = &arbiter.ClientLaneParams{MaxLanesPerAccount: 256}
	got, err = TableRegistrySnapshotFromPB(TableRegistrySnapshotToPB(s))
	if err != nil || got.ClientLanes == nil || got.ClientLanes.MaxLanesPerAccount != 256 {
		t.Fatalf("enabled lanes: %+v, %v", got.ClientLanes, err)
	}
	m := TableRegistrySnapshotToPB(s)
	m.ClientLanes.MaxLanesPerAccount = 0
	if _, err := TableRegistrySnapshotFromPB(m); err == nil {
		t.Fatal("a snapshot carrying client_lanes with max_lanes_per_account 0 must be refused")
	}
}
