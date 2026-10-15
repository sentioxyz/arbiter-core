package snode

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// TestPlanOwnerAudit pins the repair rule (plan S3-A contract §3, clarification
// K2): of equal-hash twins keep the acknowledged name and drop the others, but
// only when that leaves exactly the reference; anything else is unclean and
// drops nothing.
func TestPlanOwnerAudit(t *testing.T) {
	part := func(name, hash string) partInfo {
		return partInfo{Name: name, PartitionID: "p0id", PartitionValue: "p0", PhysHash: hash, Rows: 2}
	}
	ref := func(parts ...ownerAuditPart) ownerAuditReference {
		return ownerAuditReference{Source: "local", Seq: 7, Parts: parts}
	}
	a, b := ownerAuditPart{Name: "p0_7_7_0", PhysHash: "hA"}, ownerAuditPart{Name: "p0_8_8_0", PhysHash: "hB"}
	acked := func(p ownerAuditPart) partInfo { return part(p.Name, p.PhysHash) }
	for _, tc := range []struct {
		name    string
		ref     ownerAuditReference
		haveRef bool
		active  []partInfo
		reason  OwnerAuditReason
		parts   []string
		drop    []string
	}{
		{"the acknowledged inventory exactly", ref(a, b), true, []partInfo{acked(a), acked(b)}, "", nil, nil},
		{"a resurrected twin", ref(a, b), true, []partInfo{part("p0_1_1_0", "hA"), acked(a), acked(b)},
			OwnerAuditUnacknowledgedPart, []string{"p0_1_1_0"}, []string{"p0_1_1_0"}},
		{"three-way twins from two REPLACEs inside old_parts_lifetime",
			ref(ownerAuditPart{"p0_9_9_0", "hA"}, ownerAuditPart{"p0_10_10_0", "hB"}, ownerAuditPart{"p0_11_11_0", "hC"}), true,
			[]partInfo{part("p0_1_1_0", "hA"), part("p0_5_5_0", "hA"), part("p0_9_9_0", "hA"), part("p0_6_6_0", "hB"), part("p0_10_10_0", "hB"), part("p0_11_11_0", "hC")},
			OwnerAuditUnacknowledgedPart, []string{"p0_1_1_0", "p0_5_5_0", "p0_6_6_0"}, []string{"p0_1_1_0", "p0_5_5_0", "p0_6_6_0"}},
		{"twins without an acknowledged name", ref(a), true, []partInfo{part("p0_1_1_0", "hA"), part("p0_3_3_0", "hA")},
			OwnerAuditUnacknowledgedPart, []string{"p0_1_1_0", "p0_3_3_0"}, nil},
		{"a foreign part without a twin", ref(a), true, []partInfo{acked(a), part("p0_99_99_0", "hF")},
			OwnerAuditUnacknowledgedPart, []string{"p0_99_99_0"}, nil},
		{"twins and a foreign part: nothing is dropped", ref(a), true, []partInfo{part("p0_1_1_0", "hA"), acked(a), part("p0_99_99_0", "hF")},
			OwnerAuditUnacknowledgedPart, []string{"p0_99_99_0"}, nil},
		{"an acknowledged part missing", ref(a, b), true, []partInfo{acked(a)}, OwnerAuditAcknowledgedPartMissing, []string{"p0_8_8_0"}, nil},
		{"twins and a missing part: nothing is dropped", ref(a, b), true, []partInfo{part("p0_1_1_0", "hA"), acked(a)},
			OwnerAuditAcknowledgedPartMissing, []string{"p0_8_8_0"}, nil},
		{"a content-neutral rename (an OPTIMIZE)", ref(a), true, []partInfo{part("p0_7_7_1", "hA")}, OwnerAuditUnacknowledgedPart, []string{"p0_7_7_1"}, nil},
		{"an acknowledged name with another hash", ref(a), true, []partInfo{part("p0_7_7_0", "hX")}, OwnerAuditUnacknowledgedPart, []string{"p0_7_7_0"}, nil},
		{"no reference and no part", ownerAuditReference{}, false, nil, "", nil, nil},
		{"no reference for a partition with parts", ownerAuditReference{}, false, []partInfo{part("p0_1_1_0", "hA")},
			OwnerAuditNoAcknowledgedInventory, []string{"p0_1_1_0"}, nil},
		{"the arbiter's empty inventory (it never promoted the partition)", ownerAuditReference{Source: "fsm"}, true, []partInfo{part("p0_1_1_0", "hA")},
			OwnerAuditUnacknowledgedPart, []string{"p0_1_1_0"}, nil},
		{"an empty covering part left by a DROP PART is not acknowledged", ref(a), true,
			[]partInfo{acked(a), {Name: "p0_1_1_1", PartitionID: "p0id", PartitionValue: "p0", PhysHash: "hEmpty", Rows: 0}},
			OwnerAuditUnacknowledgedPart, []string{"p0_1_1_1"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := planOwnerAudit(tc.ref, tc.haveRef, tc.active)
			if got.Reason != tc.reason || !slices.Equal(got.Parts, tc.parts) || !slices.Equal(got.Drop, tc.drop) {
				t.Fatalf("plan = reason %q parts %v drop %v, want %q %v %v", got.Reason, got.Parts, got.Drop, tc.reason, tc.parts, tc.drop)
			}
			if got.clean() != (tc.reason == "") || (tc.reason != "" && got.Detail == "") {
				t.Fatalf("plan = %+v: clean() and Detail disagree with the reason", got)
			}
		})
	}
}

// TestOwnerAuditReferenceFor pins the reference rule (contract §3, K3): the
// newer by publish seq of this SNode's acknowledged inventory and the arbiter's
// SafeParts; the arbiter ahead of this SNode's own watermark, or disagreeing at
// the same seq, is unclean; an unreadable arbiter leaves the local inventory.
func TestOwnerAuditReferenceFor(t *testing.T) {
	mapping := func(name, hash string) arbiter.SafePartMapping {
		return arbiter.SafePartMapping{PartRowLtHash: "0xrow-" + name, SafePartName: name, PartPhysHash: hash}
	}
	entry := func(name, hash string) replay.PartManifestEntry {
		return replay.PartManifestEntry{TableID: "db.t", PartitionID: "p_p0", PartName: name, PartPhysHash: hash, PartRowLtHash: "0xrow-" + name}
	}
	fsmAt := func(seq uint64, entries ...replay.PartManifestEntry) *wire.PartitionState {
		return &wire.PartitionState{TableID: "db.t", PartitionID: "p_p0", PublishSeq: seq, SafeParts: entries}
	}
	localAt := func(seq uint64) *ackedSafeInventory {
		return &ackedSafeInventory{PromotionSeq: seq, Parts: []arbiter.SafePartMapping{mapping("p0_8_8_0", "hA"), mapping("p0_9_9_0", "hB")}}
	}
	inventory := []ownerAuditPart{{"p0_8_8_0", "hA"}, {"p0_9_9_0", "hB"}}
	for _, tc := range []struct {
		name    string
		in      ownerAuditInputs
		ok      bool
		refusal OwnerAuditReason
		source  string
		seq     uint64
		parts   []ownerAuditPart
	}{
		{"the local inventory when the arbiter is unavailable", ownerAuditInputs{Watermark: 4, Local: localAt(4)}, true, "", "local", 4, inventory},
		{"the arbiter's when this SNode has none", ownerAuditInputs{Watermark: 5, FSMAvailable: true, FSM: fsmAt(4, entry("p0_8_8_0", "hA"), entry("p0_9_9_0", "hB"))},
			true, "", "fsm", 4, inventory},
		{"the local one while the arbiter has not applied the last acknowledgement", ownerAuditInputs{Watermark: 5, Local: localAt(5), FSMAvailable: true,
			FSM: fsmAt(4, entry("p0_1_1_0", "hA"))}, true, "", "local", 5, inventory},
		{"the arbiter's when it is newer than the local one", ownerAuditInputs{Watermark: 6, Local: localAt(4), FSMAvailable: true,
			FSM: fsmAt(5, entry("p0_10_10_0", "hA"))}, true, "", "fsm", 5, []ownerAuditPart{{"p0_10_10_0", "hA"}}},
		{"the same inventory at the same promotion", ownerAuditInputs{Watermark: 4, Local: localAt(4), FSMAvailable: true,
			FSM: fsmAt(4, entry("p0_9_9_0", "hB"), entry("p0_8_8_0", "hA"))}, true, "", "local", 4, inventory},
		{"the arbiter ahead of this SNode's watermark", ownerAuditInputs{Watermark: 4, Local: localAt(4), FSMAvailable: true,
			FSM: fsmAt(6, entry("p0_8_8_0", "hA"))}, false, OwnerAuditFSMAhead, "", 0, nil},
		{"another inventory at the same promotion", ownerAuditInputs{Watermark: 4, Local: localAt(4), FSMAvailable: true,
			FSM: fsmAt(4, entry("p0_8_8_0", "hA"), entry("p0_1_1_0", "hB"))}, false, OwnerAuditFSMDisagrees, "", 0, nil},
		{"nothing when neither is there", ownerAuditInputs{}, false, "", "", 0, nil},
		{"the arbiter's empty inventory when it never promoted the partition", ownerAuditInputs{FSMAvailable: true}, true, "", "fsm", 0, nil},
		{"the local one when the arbiter never promoted the partition", ownerAuditInputs{Watermark: 4, Local: localAt(4), FSMAvailable: true}, true, "", "local", 4, inventory},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ref, ok, refusal, detail := ownerAuditReferenceFor(tc.in)
			if ok != tc.ok || refusal != tc.refusal || (refusal != "" && detail == "") {
				t.Fatalf("reference = ok %v refusal %q detail %q, want ok %v refusal %q", ok, refusal, detail, tc.ok, tc.refusal)
			}
			if ok && (ref.Source != tc.source || ref.Seq != tc.seq || !samePartSet(ref.Parts, tc.parts)) {
				t.Fatalf("reference = %+v, want %s at %d with %v", ref, tc.source, tc.seq, tc.parts)
			}
		})
	}
}

// TestOwnerAuditStateHoldsATableUntilItsAuditCompletes: before the startup pass
// takes its table set every table is held, a table of the pass is held until it
// is audited, a finding holds its table, a restart holds every table again and
// wakes the worker, and a dropped table is forgotten. A nil state holds nothing.
func TestOwnerAuditStateHoldsATableUntilItsAuditCompletes(t *testing.T) {
	a := newOwnerAuditState(true)
	if !a.blocks("db.a") || !a.due() {
		t.Fatal("before the startup pass every table is held")
	}
	if trigger := a.begin([]string{"db.a", "db.b"}); trigger != OwnerAuditTriggerStartup {
		t.Fatalf("the first pass's trigger = %q, want startup", trigger)
	}
	if !a.blocks("db.a") || !a.blocks("db.b") || a.blocks("db.c") {
		t.Fatal("a pass holds exactly its own tables until they are audited")
	}
	a.tableDone("db.a")
	if a.blocks("db.a") || !a.blocks("db.b") || !a.due() {
		t.Fatal("an audited table is released, the others of the pass stay held")
	}
	a.tableDone("db.b")
	if a.blocks("db.b") || a.due() {
		t.Fatal("a completed pass holds nothing")
	}
	k := partitionKey{Table: "db.b", Partition: "all"}
	finding := OwnerAuditFinding{TableID: k.Table, PartitionID: k.Partition, Reason: OwnerAuditUnacknowledgedPart, Parts: []string{"all_1_1_0"}, Trigger: OwnerAuditTriggerStartup}
	if !a.set(finding) || a.set(finding) {
		t.Fatal("set must report a new finding as changed and the same finding again as unchanged")
	}
	if !a.blocks("db.b") || a.blocks("db.a") || !slices.Equal(a.findingPartitions("db.b"), []string{"all"}) || !slices.Equal(a.findingTables(), []string{"db.b"}) {
		t.Fatal("a finding holds exactly its table")
	}
	if got, ok := a.finding(k); !ok || got.Reason != OwnerAuditUnacknowledgedPart {
		t.Fatalf("finding = %+v, %v", got, ok)
	}
	a.clear(k)
	if a.blocks("db.b") {
		t.Fatal("a cleared finding still holds its table")
	}
	a.restarted()
	if !a.blocks("db.a") || !a.due() {
		t.Fatal("after a ClickHouse restart every table is held again")
	}
	select {
	case <-a.wake:
	default:
		t.Fatal("a restart must wake the worker")
	}
	if trigger := a.begin([]string{"db.a"}); trigger != OwnerAuditTriggerClickHouseRestart {
		t.Fatalf("the restart pass's trigger = %q", trigger)
	}
	a.set(OwnerAuditFinding{TableID: "db.a", PartitionID: "all", Reason: OwnerAuditUnresolvedIntent})
	a.forgetTable("db.a")
	if a.blocks("db.a") {
		t.Fatal("a dropped table is still held")
	}
	var none *ownerAuditState
	if none.blocks("db.a") || none.due() {
		t.Fatal("a nil audit state holds a table")
	}
	none.clear(k)
	none.forgetTable("db.a")
}

// TestOwnerAuditStatsCountsRunsFindingsAndDrops: the host's view of the audit is
// a detached copy with every trigger present.
func TestOwnerAuditStatsCountsRunsFindingsAndDrops(t *testing.T) {
	a := newOwnerAuditState(true)
	at := time.UnixMilli(fixtureMillis)
	a.ran(OwnerAuditTriggerStartup, at)
	a.ran(OwnerAuditTriggerPromotion, at.Add(time.Second))
	a.countDropped()
	a.countDropped()
	a.countFSMUnavailable()
	a.restarted()
	a.set(OwnerAuditFinding{TableID: "db.b", PartitionID: "p_2", Reason: OwnerAuditFSMAhead, Parts: []string{"p2_1_1_0"}})
	a.set(OwnerAuditFinding{TableID: "db.a", PartitionID: "p_1", Reason: OwnerAuditUnacknowledgedPart, Parts: []string{"p1_1_1_0"}})
	s := a.stats(OwnerAuditRepair)
	wantRuns := map[string]uint64{OwnerAuditTriggerStartup: 1, OwnerAuditTriggerPromotion: 1, OwnerAuditTriggerClickHouseRestart: 0}
	if s.Mode != OwnerAuditRepair || len(s.Runs) != 3 || s.Runs[OwnerAuditTriggerStartup] != 1 || s.Runs[OwnerAuditTriggerPromotion] != 1 ||
		s.Runs[OwnerAuditTriggerClickHouseRestart] != 0 || s.DroppedTwins != 2 || s.ClickHouseRestarts != 1 || s.FSMUnavailable != 1 ||
		!s.LastRun.Equal(at.Add(time.Second)) {
		t.Fatalf("stats = %+v, want runs %v, 2 drops, 1 restart, 1 FSM failure", s, wantRuns)
	}
	if len(s.Unclean) != 2 || s.Unclean[0].TableID != "db.a" || s.Unclean[1].TableID != "db.b" {
		t.Fatalf("unclean = %+v, want db.a then db.b", s.Unclean)
	}
	s.Unclean[0].Parts[0] = "mutated"
	s.Runs[OwnerAuditTriggerStartup] = 99
	if again := a.stats(OwnerAuditRepair); again.Unclean[0].Parts[0] != "p1_1_1_0" || again.Runs[OwnerAuditTriggerStartup] != 1 {
		t.Fatal("stats share memory with the audit state")
	}
	var none *ownerAuditState
	if s := none.stats(OwnerAuditReport); s.Mode != OwnerAuditReport || len(s.Runs) != 3 {
		t.Fatalf("stats of a nil state = %+v", s)
	}
}

// TestConfigOwnerAudit: report is the default, repair the only other mode; the
// probe interval and the FSM timeout default, and are refused when negative or,
// for the interval, below one second.
func TestConfigOwnerAudit(t *testing.T) {
	for _, tc := range []struct {
		name               string
		mode               OwnerAuditMode
		probe, fsm         time.Duration
		wantMode           OwnerAuditMode
		wantProbe, wantFSM time.Duration
		refusal            []string
	}{
		{"defaults", "", 0, 0, OwnerAuditReport, DefaultOwnerAuditProbeInterval, DefaultOwnerAuditFSMTimeout, nil},
		{"report", OwnerAuditReport, 0, 0, OwnerAuditReport, DefaultOwnerAuditProbeInterval, DefaultOwnerAuditFSMTimeout, nil},
		{"repair with the minimum interval", OwnerAuditRepair, MinOwnerAuditProbeInterval, 2 * time.Second, OwnerAuditRepair, MinOwnerAuditProbeInterval, 2 * time.Second, nil},
		{"an unknown mode", "drop", 0, 0, "", 0, 0, []string{"owner audit mode", `"drop"`, `"report"`, `"repair"`}},
		{"a negative probe interval", "", -time.Second, 0, "", 0, 0, []string{"owner audit probe interval", "negative"}},
		{"a probe interval below the minimum", "", 500 * time.Millisecond, 0, "", 0, 0, []string{"owner audit probe interval", "500ms", "1s"}},
		{"a negative fsm timeout", "", 0, -time.Second, "", 0, 0, []string{"owner audit fsm timeout", "negative"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfigS(t)
			cfg.OwnerAudit, cfg.OwnerAuditProbeInterval, cfg.OwnerAuditFSMTimeout = tc.mode, tc.probe, tc.fsm
			err := cfg.validate()
			if tc.refusal == nil {
				if err != nil || cfg.OwnerAudit != tc.wantMode || cfg.OwnerAuditProbeInterval != tc.wantProbe || cfg.OwnerAuditFSMTimeout != tc.wantFSM {
					t.Fatalf("validate: %v; mode %q interval %v timeout %v, want %q %v %v", err, cfg.OwnerAudit, cfg.OwnerAuditProbeInterval, cfg.OwnerAuditFSMTimeout,
						tc.wantMode, tc.wantProbe, tc.wantFSM)
				}
				return
			}
			if err == nil {
				t.Fatalf("mode %q, interval %v, timeout %v were accepted", tc.mode, tc.probe, tc.fsm)
			}
			for _, want := range tc.refusal {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("err = %v, want it to name %q", err, want)
				}
			}
		})
	}
}
