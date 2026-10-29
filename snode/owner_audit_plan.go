package snode

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core"
	"github.com/sentioxyz/arbiter-core/wire"
)

// ownerAuditPart is one hg_safe part as the owner audit compares it: its name
// and hash_of_all_files.
type ownerAuditPart struct {
	Name     string
	PhysHash string
}

// ownerAuditReference is a partition's reference inventory: this SNode's last
// acknowledged inventory ("local"; "rebuilt" when it is an inventory a rebuild
// wrote and the rebuilt rule of ownerAuditReferenceFor chose it) or the
// arbiter's SafeParts ("fsm"), with
// the promotion that acknowledged it (0 for the arbiter's answer that it never
// promoted the partition).
type ownerAuditReference struct {
	Source string
	Seq    uint64
	Parts  []ownerAuditPart
}

// ownerAuditInputs are what the reference is chosen from: this SNode's
// promotion watermark and acknowledged inventory of the partition, and the
// arbiter's state of it (FSMAvailable false: it could not be read; FSM nil:
// the arbiter knows no promotion of the partition).
type ownerAuditInputs struct {
	Watermark    uint64
	Local        *ackedSafeInventory
	FSMAvailable bool
	FSM          *wire.PartitionState
}

// ownerAuditReferenceFor picks the reference (plan S3-A contract §3,
// clarification K3): the newer, by publish seq, of the local inventory and the
// arbiter's SafeParts. The arbiter is "ahead" when its publish seq is past
// this SNode's own promotion watermark, which every journaled acknowledgement
// advances: then this SNode's state is behind, refusal is fsm_ahead and
// nothing may be dropped. Equal seqs with different inventories are
// fsm_disagrees. A rebuilt local inventory (plan S3-D clarification K52) at
// the arbiter's publish seq is compared by content instead: it is the
// reference ("rebuilt") exactly when its multiset of (row LtHash,
// hash_of_all_files) equals the arbiter's SafeParts, whatever the names, and
// fsm_disagrees otherwise; with the arbiter unavailable it is the local
// reference like any other. ok is false with no refusal when there is no
// reference.
func ownerAuditReferenceFor(in ownerAuditInputs) (ref ownerAuditReference, ok bool, refusal OwnerAuditReason, detail string) {
	var local, fsm *ownerAuditReference
	if in.Local != nil {
		l := ownerAuditReference{Source: "local", Seq: in.Local.PromotionSeq}
		for _, m := range in.Local.Parts {
			l.Parts = append(l.Parts, ownerAuditPart{Name: m.SafePartName, PhysHash: m.PartPhysHash})
		}
		local = &l
	}
	if in.FSMAvailable {
		f := ownerAuditReference{Source: "fsm"}
		if in.FSM != nil {
			if in.FSM.PublishSeq > in.Watermark {
				return ownerAuditReference{}, false, OwnerAuditFSMAhead, fmt.Sprintf(
					"the arbiter recorded applied promotion %d of the partition, past this SNode's promotion watermark %d: its local state is behind",
					in.FSM.PublishSeq, in.Watermark)
			}
			f.Seq = in.FSM.PublishSeq
			for _, e := range in.FSM.SafeParts {
				f.Parts = append(f.Parts, ownerAuditPart{Name: e.PartName, PhysHash: e.PartPhysHash})
			}
		}
		fsm = &f
	}
	if local != nil && fsm != nil && in.Local.Rebuilt && in.FSM != nil && in.FSM.PublishSeq == in.Local.PromotionSeq {
		if !sameContent(in.Local.Parts, in.FSM.SafeParts) {
			return ownerAuditReference{}, false, OwnerAuditFSMDisagrees, fmt.Sprintf(
				"the rebuilt inventory of promotion %d differs from the arbiter's by row hash or hash_of_all_files", in.Local.PromotionSeq)
		}
		rebuilt := *local
		rebuilt.Source = "rebuilt"
		return rebuilt, true, "", ""
	}
	switch {
	case local == nil && fsm == nil:
		return ownerAuditReference{}, false, "", ""
	case fsm == nil:
		return *local, true, "", ""
	case local == nil:
		return *fsm, true, "", ""
	case fsm.Seq > local.Seq:
		return *fsm, true, "", ""
	case local.Seq > fsm.Seq:
		return *local, true, "", ""
	case samePartSet(local.Parts, fsm.Parts):
		return *local, true, "", ""
	}
	return ownerAuditReference{}, false, OwnerAuditFSMDisagrees, fmt.Sprintf(
		"this SNode and the arbiter acknowledged different inventories for promotion %d", local.Seq)
}

// sameContent reports whether a rebuilt inventory and the arbiter's SafeParts
// hold the same multiset of (row LtHash, hash_of_all_files), names aside.
func sameContent(local []arbiter.SafePartMapping, fsm []replay.PartManifestEntry) bool {
	if len(local) != len(fsm) {
		return false
	}
	type content struct{ row, phys string }
	left := make(map[content]int, len(local))
	for _, m := range local {
		left[content{m.PartRowLtHash, m.PartPhysHash}]++
	}
	for _, e := range fsm {
		c := content{e.PartRowLtHash, e.PartPhysHash}
		if left[c] == 0 {
			return false
		}
		left[c]--
	}
	return true
}

func samePartSet(a, b []ownerAuditPart) bool {
	if len(a) != len(b) {
		return false
	}
	byNameHash := func(x, y ownerAuditPart) int {
		if c := strings.Compare(x.Name, y.Name); c != 0 {
			return c
		}
		return strings.Compare(x.PhysHash, y.PhysHash)
	}
	return slices.Equal(slices.SortedFunc(slices.Values(a), byNameHash), slices.SortedFunc(slices.Values(b), byNameHash))
}

// ownerAuditPlan is the audit's verdict on one partition: clean (no Reason),
// or unclean with the parts Reason names; Drop lists the resurrected twins
// repair mode drops, set only when dropping exactly them leaves the reference.
type ownerAuditPlan struct {
	Reason OwnerAuditReason
	Parts  []string
	Drop   []string
	Detail string
}

func (p ownerAuditPlan) clean() bool { return p.Reason == "" }

// planOwnerAudit compares one logical partition's active hg_safe parts with
// its reference (clarification K2). Parts are grouped by hash_of_all_files:
// the members whose name and hash the reference carries are kept, and every
// other member of a group with a kept member is a resurrected twin. Twins are
// dropped only when that leaves exactly the reference: a part without an
// acknowledged twin, or a missing acknowledged part, makes the partition
// unclean and drops nothing.
func planOwnerAudit(ref ownerAuditReference, haveRef bool, active []partInfo) ownerAuditPlan {
	if !haveRef {
		if len(active) == 0 {
			return ownerAuditPlan{}
		}
		return ownerAuditPlan{Reason: OwnerAuditNoAcknowledgedInventory, Parts: activePartNames(active),
			Detail: "this SNode has no acknowledged inventory of a partition that holds parts, and the arbiter's could not be read"}
	}
	acked := make(map[ownerAuditPart]bool, len(ref.Parts))
	for _, p := range ref.Parts {
		acked[p] = true
	}
	groups := map[string][]partInfo{}
	present := make(map[ownerAuditPart]bool, len(active))
	for _, p := range active {
		groups[p.PhysHash] = append(groups[p.PhysHash], p)
		present[ownerAuditPart{Name: p.Name, PhysHash: p.PhysHash}] = true
	}
	var unacknowledged, drop, missing []string
	for _, group := range groups {
		kept := slices.ContainsFunc(group, func(p partInfo) bool { return acked[ownerAuditPart{Name: p.Name, PhysHash: p.PhysHash}] })
		for _, p := range group {
			switch {
			case acked[ownerAuditPart{Name: p.Name, PhysHash: p.PhysHash}]:
			case kept:
				drop = append(drop, p.Name)
			default:
				unacknowledged = append(unacknowledged, p.Name)
			}
		}
	}
	for _, p := range ref.Parts {
		if !present[p] {
			missing = append(missing, p.Name)
		}
	}
	slices.Sort(unacknowledged)
	slices.Sort(drop)
	slices.Sort(missing)
	switch {
	case len(unacknowledged) > 0:
		return ownerAuditPlan{Reason: OwnerAuditUnacknowledgedPart, Parts: unacknowledged, Detail: fmt.Sprintf(
			"no active part with the same hash_of_all_files carries an acknowledged name; twins %v and missing acknowledged parts %v are left as they are", drop, missing)}
	case len(missing) > 0:
		return ownerAuditPlan{Reason: OwnerAuditAcknowledgedPartMissing, Parts: missing, Detail: fmt.Sprintf(
			"acknowledged parts are not active; twins %v are left as they are", drop)}
	case len(drop) > 0:
		return ownerAuditPlan{Reason: OwnerAuditUnacknowledgedPart, Parts: drop, Drop: drop,
			Detail: "equal hash_of_all_files twins of acknowledged parts: a ClickHouse restart resurrected the parts a REPLACE PARTITION retired"}
	}
	return ownerAuditPlan{}
}

// activePartNames returns the sorted names of parts. (The test-only
// partNames of converge_test.go takes candidate parts; this name keeps the
// two apart in the snode test binary.)
func activePartNames(parts []partInfo) []string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		out = append(out, p.Name)
	}
	slices.Sort(out)
	return out
}

// ownerAuditState is the audit's in-memory state: which tables are held until
// a full pass audits them, the current findings, and the counters. A restart
// of the SNode starts it afresh: the startup pass decides again.
type ownerAuditState struct {
	mu sync.Mutex
	// pendingAll: a full pass is due and has not yet taken its table set, so
	// every table is held (from New, and after each ClickHouse restart).
	pendingAll bool
	// trigger is the trigger of the full pass that is due or running.
	trigger string
	// pending are the tables of the current full pass not yet audited (not
	// reached, or failed on a ClickHouse or schema error).
	pending        map[string]bool
	findings       map[partitionKey]OwnerAuditFinding
	runs           map[string]uint64
	dropped        uint64
	restarts       uint64
	fsmUnavailable uint64
	lastRun        time.Time
	// wake is signalled when a full pass becomes due.
	wake chan struct{}
}

// newOwnerAuditState starts with the startup pass due when the SNode has a
// ClickHouse connection to audit.
func newOwnerAuditState(due bool) *ownerAuditState {
	return &ownerAuditState{
		pendingAll: due,
		trigger:    OwnerAuditTriggerStartup,
		pending:    map[string]bool{},
		findings:   map[partitionKey]OwnerAuditFinding{},
		runs:       map[string]uint64{OwnerAuditTriggerStartup: 0, OwnerAuditTriggerPromotion: 0, OwnerAuditTriggerClickHouseRestart: 0},
		wake:       make(chan struct{}, 1),
	}
}

// blocks reports whether table may not be served yet: a full pass is due that
// has not taken its table set, table is in the current pass and not audited,
// or one of its partitions has a finding.
func (a *ownerAuditState) blocks(table string) bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.pendingAll || a.pending[table] {
		return true
	}
	for k := range a.findings {
		if k.Table == table {
			return true
		}
	}
	return false
}

// require makes a full pass with trigger due and wakes the worker.
func (a *ownerAuditState) require(trigger string) {
	a.mu.Lock()
	a.pendingAll, a.trigger = true, trigger
	a.mu.Unlock()
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// restarted counts a detected ClickHouse restart and makes its pass due.
func (a *ownerAuditState) restarted() {
	a.mu.Lock()
	a.restarts++
	a.mu.Unlock()
	a.require(OwnerAuditTriggerClickHouseRestart)
}

// begin starts a full pass over tables and returns its trigger; each table is
// held until tableDone.
func (a *ownerAuditState) begin(tables []string) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.pendingAll = false
	a.pending = make(map[string]bool, len(tables))
	for _, t := range tables {
		a.pending[t] = true
	}
	return a.trigger
}

func (a *ownerAuditState) tableDone(table string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.pending, table)
}

// due reports whether a full pass is due or did not complete.
func (a *ownerAuditState) due() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.pendingAll || len(a.pending) > 0
}

func (a *ownerAuditState) ran(trigger string, at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.runs[trigger]++
	a.lastRun = at
}

// set makes f its partition's finding and reports whether it differs from the
// finding it replaces (none, or another reason, parts or trigger), so an
// unchanged verdict is not logged again on every re-audit.
func (a *ownerAuditState) set(f OwnerAuditFinding) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	k := partitionKey{Table: f.TableID, Partition: f.PartitionID}
	old, ok := a.findings[k]
	a.findings[k] = f
	return !ok || old.Reason != f.Reason || old.Trigger != f.Trigger || !slices.Equal(old.Parts, f.Parts)
}

// finding returns k's current finding.
func (a *ownerAuditState) finding(k partitionKey) (OwnerAuditFinding, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	f, ok := a.findings[k]
	return f, ok
}

// findingTables returns, sorted, the tables with a finding.
func (a *ownerAuditState) findingTables() []string {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	seen := map[string]bool{}
	for k := range a.findings {
		seen[k.Table] = true
	}
	return slices.Sorted(maps.Keys(seen))
}

func (a *ownerAuditState) clear(k partitionKey) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.findings, k)
}

// forgetTable drops a dropped table's findings and its place in the pass.
func (a *ownerAuditState) forgetTable(table string) {
	if a == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.pending, table)
	for k := range a.findings {
		if k.Table == table {
			delete(a.findings, k)
		}
	}
}

// findingPartitions returns, sorted, the partitions of table with a finding.
func (a *ownerAuditState) findingPartitions(table string) []string {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for k := range a.findings {
		if k.Table == table {
			out = append(out, k.Partition)
		}
	}
	slices.Sort(out)
	return out
}

func (a *ownerAuditState) countDropped() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.dropped++
}

func (a *ownerAuditState) countFSMUnavailable() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.fsmUnavailable++
}

// stats returns a detached copy for OwnerAuditStats.
func (a *ownerAuditState) stats(mode OwnerAuditMode) OwnerAuditStats {
	if a == nil {
		return OwnerAuditStats{Mode: mode, Runs: map[string]uint64{OwnerAuditTriggerStartup: 0, OwnerAuditTriggerPromotion: 0, OwnerAuditTriggerClickHouseRestart: 0}}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	out := OwnerAuditStats{Mode: mode, Runs: maps.Clone(a.runs), DroppedTwins: a.dropped, ClickHouseRestarts: a.restarts,
		FSMUnavailable: a.fsmUnavailable, LastRun: a.lastRun}
	for _, f := range a.findings {
		f.Parts = slices.Clone(f.Parts)
		out.Unclean = append(out.Unclean, f)
	}
	slices.SortFunc(out.Unclean, func(x, y OwnerAuditFinding) int {
		if c := strings.Compare(x.TableID, y.TableID); c != 0 {
			return c
		}
		return strings.Compare(x.PartitionID, y.PartitionID)
	})
	return out
}
