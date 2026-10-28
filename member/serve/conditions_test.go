package serve

import (
	"net/http"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sentioxyz/arbiter-core/wire"
)

// TestServingRequiresTheEntryUnchangedOverTwoPolls: a database serves only
// once two consecutive polls read the same owner entry (spec §11).
func TestServingRequiresTheEntryUnchangedOverTwoPolls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		h := newHarness(t, testConfig())
		// Poll 1 (at start) is the first to read entry40.
		h.requireReason(ReasonEntryUnconfirmed)
		if d := h.database(); d.FrozenSinceUnixMs != start.UnixMilli() || len(d.Tables) != 0 || d.Lease != nil {
			t.Fatalf("after poll 1: %+v", d)
		}
		// The entry moves before poll 2, and the copy follows it.
		h.pub.set(manifestAt("0xs41", entry41), nil)
		h.copies.set(testOwner, testDatabase, copyAt("0xs41", entry41))
		advance(time.Second)
		h.requireReason(ReasonEntryUnconfirmed)
		if d := h.database(); d.FrozenSinceUnixMs != start.Add(time.Second).UnixMilli() {
			t.Fatalf("frozen_since_unix_ms %d, want poll 2's %d", d.FrozenSinceUnixMs, start.Add(time.Second).UnixMilli())
		}
		// Poll 3 reads entry41 a second time.
		advance(time.Second)
		h.requireServing()
		want := DatabaseView{
			Database: "dex", Owner: 0, Serving: true, SnapshotID: "0xs41", Entry: entry41,
			FrozenSinceUnixMs: start.Add(time.Second).UnixMilli(), OwnerQuarantined: true,
			Tables: []ServedTable{
				{TableID: "dex.pools", IncarnationSeq: 8, CopyTable: "dex__pools__i8", SchemaHash: "0xpools"},
				{TableID: "dex.swaps", IncarnationSeq: 7, CopyTable: "dex__swaps__i7", SchemaHash: "0xswaps"},
			},
		}
		if got := h.srv.Serving(); got.NodeID != "member-b" || len(got.Databases) != 1 || !reflect.DeepEqual(got.Databases[0], want) {
			t.Fatalf("serving view %+v, want node member-b and %+v", got, want)
		}
		requireMetrics(t, h.metrics, map[string]float64{
			"sentio_node_si_member_serve_databases{state=serving}":     1,
			"sentio_node_si_member_serve_databases{state=not_serving}": 0,
		})
	})
}

// TestServingStopsWhenTheOwnerEntryAdvances: a late acknowledgement that
// advances a quarantined owner's entry stops serving and revokes the lease;
// the install runs after the release, and serving resumes only after two
// polls at the new entry.
func TestServingStopsWhenTheOwnerEntryAdvances(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		h.requireServing()
		first := h.acquire("0xs40")

		// A late acknowledgement advances the quarantined owner's entry.
		h.pub.set(manifestAt("0xs41", entry41), nil)
		advance(time.Second)
		h.requireReason(ReasonEntryUnconfirmed)
		if l := h.database().Lease; l == nil || l.LeaseID != first || l.State != "revoking" {
			t.Fatalf("lease %+v, want %s revoking", l, first)
		}
		status, out := h.renew(first)
		requireError(t, status, out, http.StatusConflict, "member serving: lease "+first+" is revoked")
		advance(time.Second)
		h.requireReason(ReasonCopyBehind)

		// The install waits for the release, then the copy reaches entry41.
		done := make(chan error, 1)
		go func() { done <- h.srv.BeforeInstall(t.Context(), testOwner, testDatabase) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("BeforeInstall returned %v while the revoked lease is still held", err)
		default:
		}
		h.release(first)
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatalf("BeforeInstall: %v", err)
		}
		h.copies.set(testOwner, testDatabase, copyAt("0xs41", entry41))
		h.srv.AfterInstall(testOwner, testDatabase)

		// Serving resumes only after two polls at entry41 since the install.
		advance(time.Second)
		h.requireReason(ReasonEntryUnconfirmed)
		advance(time.Second)
		h.requireServing()
		status, out = h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs40"}`)
		requireError(t, status, out, http.StatusConflict, "member serving: database dex moved to snapshot 0xs41; acquire again")
		if second := h.acquire("0xs41"); second == first {
			t.Fatalf("the new lease reuses the id %s", first)
		}
	})
}

// TestServingNeverStartsWithACopyBehindTheEntry: a copy behind the published
// entry never serves, however long the entry is stable.
func TestServingNeverStartsWithACopyBehindTheEntry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig(), func(h *harness) {
			h.pub.set(manifestAt("0xs41", entry41), nil)
		})
		h.requireReason(ReasonEntryUnconfirmed)
		for range 20 {
			advance(time.Second)
			h.requireReason(ReasonCopyBehind)
		}
		status, out := h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs40"}`)
		requireError(t, status, out, http.StatusConflict, "member serving: database dex is not served: copy_behind")
		requireMetrics(t, h.metrics, map[string]float64{
			"sentio_node_si_member_serve_databases{state=not_serving}":    1,
			"sentio_node_si_member_serve_not_serving{reason=copy_behind}": 1,
		})
	})
}

// TestServingRefusesAForeignDivergedExcludedOrMissingTable: a Foreign,
// Diverged, excluded or missing Active table, or one that disagrees with the
// registry, serves nothing of the database.
func TestServingRefusesAForeignDivergedExcludedOrMissingTable(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(c *DatabaseCopy, reg *wire.TableRegistrySnapshot, on *bool)
		want   Reason
	}{
		{"foreign", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) {
			c.Tables[1].Condition = ConditionForeign
		}, ReasonTableForeign},
		{"diverged", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) {
			c.Tables[0].Condition = ConditionDiverged
		}, ReasonTableDiverged},
		{"excluded", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) {
			c.Tables[0].Condition = ConditionExcluded
		}, ReasonTableExcluded},
		{"foreign before diverged", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) {
			c.Tables[0].Condition, c.Tables[1].Condition = ConditionDiverged, ConditionForeign
		}, ReasonTableForeign},
		{"a registry table the copy lacks", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) { c.Tables = c.Tables[1:] }, ReasonTableMissing},
		{"another incarnation", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) {
			c.Tables[1].IncarnationSeq, c.Tables[1].CopyTable = 6, "dex__swaps__i6"
		}, ReasonTableMissing},
		{"another schema hash", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) { c.Tables[1].SchemaHash = "0xold" }, ReasonTableMissing},
		{"a copy name that is not sitable.CopyTable", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) { c.Tables[1].CopyTable = "dex__swaps" }, ReasonTableMissing},
		{"a table of another owner", func(_ *DatabaseCopy, reg *wire.TableRegistrySnapshot, _ *bool) {
			reg.Incarnations[1].OwnerIndexerID = ownerID(1)
		}, ReasonTableMissing},
		{"no table", func(c *DatabaseCopy, _ *wire.TableRegistrySnapshot, _ *bool) { c.Tables = nil }, ReasonTableMissing},
		{"no Active incarnation", func(_ *DatabaseCopy, reg *wire.TableRegistrySnapshot, _ *bool) {
			reg.Incarnations[0].Status, reg.Incarnations[1].Status = wire.TableStatusRetiring, wire.TableStatusRetiring
		}, ReasonTableMissing},
		{"a disabled registry", func(_ *DatabaseCopy, _ *wire.TableRegistrySnapshot, on *bool) { *on = false }, ReasonOwnerAvailable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				h := newHarness(t, testConfig(), func(h *harness) {
					c := copyAt("0xs40", entry40)
					reg := registrySnapshot(testOwner)
					on := true
					tc.mutate(&c, &reg, &on)
					h.copies.set(testOwner, testDatabase, c)
					h.reg.mu.Lock()
					h.reg.snap, h.reg.on = reg, on
					h.reg.mu.Unlock()
				})
				for range 3 {
					advance(time.Second)
					h.requireReason(tc.want)
				}
				status, out := h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs40"}`)
				requireError(t, status, out, http.StatusConflict, "member serving: database dex is not served: "+string(tc.want))
			})
		})
	}
}

// TestServingListsOnlyTheActiveIncarnations: copies of incarnations that are
// not Active (a Pending table never added, a Retiring one, an older seq of an
// Active table) are not served and do not block the database, whatever their
// condition; the view lists exactly the owner's Active incarnations.
func TestServingListsOnlyTheActiveIncarnations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig(), func(h *harness) {
			reg := registrySnapshot(testOwner)
			reg.Incarnations = append(reg.Incarnations,
				wire.TableIncarnation{Seq: 10, DatabaseID: "dex", TableID: "trades", Origin: wire.TableOriginChain, Status: wire.TableStatusPending, SchemaHash: "0xtrades", OwnerIndexerID: ownerID(0)},
				wire.TableIncarnation{Seq: 11, DatabaseID: "dex", TableID: "quotes", Origin: wire.TableOriginChain, Status: wire.TableStatusRetiring, SchemaHash: "0xquotes", AddBlockSeq: 33, OwnerIndexerID: ownerID(0)},
			)
			h.reg.set(reg)
			c := copyAt("0xs40", entry40)
			c.Tables = append(c.Tables,
				TableCopy{TableID: "dex.trades", IncarnationSeq: 10, CopyTable: "dex__trades__i10", SchemaHash: "0xtrades", Condition: ConditionExcluded},
				TableCopy{TableID: "dex.quotes", IncarnationSeq: 11, CopyTable: "dex__quotes__i11", SchemaHash: "0xquotes", Condition: ConditionForeign},
				TableCopy{TableID: "dex.swaps", IncarnationSeq: 5, CopyTable: "dex__swaps__i5", SchemaHash: "0xswaps0", Condition: ConditionDiverged},
			)
			h.copies.set(testOwner, testDatabase, c)
		})
		advance(time.Second)
		h.requireServing()
		want := []ServedTable{
			{TableID: "dex.pools", IncarnationSeq: 8, CopyTable: "dex__pools__i8", SchemaHash: "0xpools"},
			{TableID: "dex.swaps", IncarnationSeq: 7, CopyTable: "dex__swaps__i7", SchemaHash: "0xswaps"},
		}
		if got := h.database().Tables; !reflect.DeepEqual(got, want) {
			t.Fatalf("served tables %+v, want %+v", got, want)
		}
	})
}

// TestServingRequiresACleanAudit: an unclean audit serves nothing.
func TestServingRequiresACleanAudit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig(), func(h *harness) {
			c := copyAt("0xs40", entry40)
			c.AuditClean = false
			h.copies.set(testOwner, testDatabase, c)
		})
		advance(time.Second)
		h.requireReason(ReasonAuditUnclean)
		h.copies.set(testOwner, testDatabase, copyAt("0xs40", entry40))
		advance(time.Second)
		h.requireServing()
	})
}

// TestServingIsOffWhenDisabled: without Config.Enabled nothing serves and the
// arbiter is never read.
func TestServingIsOffWhenDisabled(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		cfg.Enabled = false
		h := newHarness(t, cfg)
		for range 5 {
			advance(time.Second)
			h.requireReason(ReasonServingDisabled)
		}
		status, out := h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs40"}`)
		requireError(t, status, out, http.StatusConflict, "member serving: database dex is not served: serving_disabled")
		before := time.Now()
		if err := h.srv.BeforeInstall(t.Context(), testOwner, testDatabase); err != nil || time.Since(before) != 0 {
			t.Fatalf("BeforeInstall without a lease: %v after %s", err, time.Since(before))
		}
		h.srv.AfterInstall(testOwner, testDatabase)
		advance(time.Second)
		h.requireReason(ReasonServingDisabled)
		if n := h.pub.readCount(); n != 0 {
			t.Fatalf("serving off read the published manifest %d times", n)
		}
		requireMetrics(t, h.metrics, map[string]float64{
			"sentio_node_si_member_serve_not_serving{reason=serving_disabled}": 1,
			"sentio_node_si_member_serve_databases{state=serving}":             0,
		})
	})
}

// TestServingRequiresTheOwnerQuarantinedOrUnreachable: an available owner's
// database is never served; the quarantine starts and ends serving.
func TestServingRequiresTheOwnerQuarantinedOrUnreachable(t *testing.T) {
	t.Run("quarantine", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newHarness(t, testConfig(), func(h *harness) { h.reg.set(registrySnapshot()) })
			for range 3 {
				advance(time.Second)
				h.requireReason(ReasonOwnerAvailable)
			}
			// The entry has been stable for three polls: serving starts at the
			// first poll that sees the quarantine.
			h.reg.set(registrySnapshot(testOwner))
			advance(time.Second)
			h.requireServing()
			if d := h.database(); !d.OwnerQuarantined || d.OwnerUnreachable {
				t.Fatalf("owner flags %+v", d)
			}
			id := h.acquire("0xs40")
			// The release of the quarantine revokes the lease at the next poll.
			h.reg.set(registrySnapshot())
			advance(time.Second)
			h.requireReason(ReasonOwnerAvailable)
			status, out := h.renew(id)
			requireError(t, status, out, http.StatusConflict, "member serving: lease "+id+" is revoked")
		})
	})
	t.Run("a reachable owner", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			cfg := testConfig()
			cfg.OwnerHouseGate = map[uint64]string{testOwner: "sentio-node-devnet2-indexer-a:32001"}
			h := newHarness(t, cfg, func(h *harness) { h.reg.set(registrySnapshot()) })
			for range 120 {
				advance(time.Second)
				h.requireReason(ReasonOwnerAvailable)
			}
			if n := h.dialer.count(); n < 24 {
				t.Fatalf("%d probes in 120s, want one every 5s", n)
			}
		})
	})
}

// TestServeStartsAfterTheOwnerIsUnreachableForTUnreach: an owner whose every
// probe failed for t_unreach triggers serving; one successful probe restarts
// the count.
func TestServeStartsAfterTheOwnerIsUnreachableForTUnreach(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cfg := testConfig()
		cfg.OwnerUnreachableAfter = MinOwnerUnreachableAfter
		cfg.OwnerProbeInterval = time.Second
		cfg.OwnerHouseGate = map[uint64]string{testOwner: "sentio-node-devnet2-indexer-a:32001"}
		h := newHarness(t, cfg, func(h *harness) {
			h.reg.set(registrySnapshot())
			h.dialer.set(errRefused)
		})
		// Probes and polls run on whole seconds; observe between them.
		advance(500 * time.Millisecond)
		// Every probe fails from t=0; t_unreach is 15s.
		for range 14 {
			advance(time.Second)
			h.requireReason(ReasonOwnerAvailable)
		}
		requireMetrics(t, h.metrics, map[string]float64{"sentio_node_si_member_serve_owner_unreachable{owner=0}": 0})
		advance(time.Second) // t=15.5
		h.requireServing()
		if d := h.database(); d.OwnerQuarantined || !d.OwnerUnreachable {
			t.Fatalf("owner flags %+v", d)
		}
		requireMetrics(t, h.metrics, map[string]float64{"sentio_node_si_member_serve_owner_unreachable{owner=0}": 1})

		// One successful probe ends the streak: t_unreach counts again from
		// the next failure.
		// (The poll at 16 may run before or after the probe at 16, so the
		// first observation is at 17.5.)
		h.dialer.set(nil)
		advance(2 * time.Second) // t=17.5
		h.requireReason(ReasonOwnerAvailable)
		h.dialer.set(errRefused)
		// The streak starts with the probe at 18: unreachable from 33.
		for range 15 {
			advance(time.Second)
			h.requireReason(ReasonOwnerAvailable)
		}
		advance(time.Second) // t=33.5
		h.requireServing()
	})
}
