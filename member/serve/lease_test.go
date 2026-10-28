package serve

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// TestAcquireRefusesAStaleSnapshot: a host acquires a served database only at
// the snapshot the view names; other databases and bad bodies are refused.
func TestAcquireRefusesAStaleSnapshot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		h.requireServing()
		status, out := h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs39"}`)
		requireError(t, status, out, http.StatusConflict, "member serving: database dex moved to snapshot 0xs40; acquire again")
		status, out = h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"nft","snapshot_id":"0xs40"}`)
		requireError(t, status, out, http.StatusConflict, "member serving: database nft is not served: table_missing")
		status, out = h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex"}`)
		requireError(t, status, out, http.StatusBadRequest, "member serving: acquire needs a database and a snapshot_id")
		status, out = h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs40","owner":0}`)
		requireError(t, status, out, http.StatusBadRequest, `member serving: json: unknown field "owner"`)
		if d := h.database(); d.Lease != nil {
			t.Fatalf("a refused acquire left lease %+v", d.Lease)
		}
		id := h.acquire("0xs40")
		if l := h.database().Lease; l == nil || *l != (LeaseView{LeaseID: id, State: "held", RemainingMs: 30000}) {
			t.Fatalf("lease view %+v", l)
		}
	})
}

// TestRenewAfterRevokeIsRefused: a database that stops serving revokes its
// lease; renew then answers 409, and the lease still expires LeaseTTL after
// its last renew (404 from then on).
func TestRenewAfterRevokeIsRefused(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		id := h.acquire("0xs40")
		advance(10 * time.Second)
		status, out := h.renew(id)
		if status != http.StatusOK || out["ttl_ms"] != float64(30000) || len(out) != 1 {
			t.Fatalf("renew: %d %v", status, out)
		}
		renewed := time.Now()
		// The owner leaves quarantine: the next poll revokes the lease.
		h.reg.set(registrySnapshot())
		advance(time.Second)
		h.requireReason(ReasonOwnerAvailable)
		if l := h.database().Lease; l == nil || l.State != "revoking" || l.RemainingMs != 29000 {
			t.Fatalf("lease view %+v, want revoking with 29000ms left", l)
		}
		status, out = h.renew(id)
		requireError(t, status, out, http.StatusConflict, "member serving: lease "+id+" is revoked")
		// A revoked lease still expires LeaseTTL after its last renew.
		advance(renewed.Add(DefaultLeaseTTL).Sub(time.Now()))
		status, out = h.renew(id)
		requireError(t, status, out, http.StatusNotFound, "member serving: unknown lease "+id)
		if l := h.database().Lease; l != nil {
			t.Fatalf("an expired lease is still listed: %+v", l)
		}
	})
}

// TestReleaseIsIdempotent: release answers 200 for a held, a released and an
// unknown lease.
func TestReleaseIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		id := h.acquire("0xs40")
		h.release(id)
		h.release(id)
		h.release("00000000000000000000000000000000")
		status, out := h.renew(id)
		requireError(t, status, out, http.StatusNotFound, "member serving: unknown lease "+id)
		h.requireServing()
		if l := h.database().Lease; l != nil {
			t.Fatalf("a released lease is still listed: %+v", l)
		}
		if again := h.acquire("0xs40"); again == id {
			t.Fatalf("a new lease reuses the released id %s", id)
		}
	})
}

// TestBeforeInstallRevokesAndWaitsForTheRelease: BeforeInstall stops serving,
// revokes the lease and returns only once the host releases it (K44).
func TestBeforeInstallRevokesAndWaitsForTheRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		id := h.acquire("0xs40")
		done := make(chan error, 1)
		go func() { done <- h.srv.BeforeInstall(t.Context(), testOwner, testDatabase) }()
		synctest.Wait()
		h.requireReason(ReasonInstalling)
		if l := h.database().Lease; l == nil || l.LeaseID != id || l.State != "revoking" {
			t.Fatalf("lease view %+v, want %s revoking", l, id)
		}
		status, out := h.renew(id)
		requireError(t, status, out, http.StatusConflict, "member serving: lease "+id+" is revoked")
		status, out = h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs40"}`)
		requireError(t, status, out, http.StatusConflict, "member serving: database dex is not served: installing")
		advance(5 * time.Second)
		select {
		case err := <-done:
			t.Fatalf("BeforeInstall returned %v while the lease is held", err)
		default:
		}
		h.requireReason(ReasonInstalling)
		requireMetrics(t, h.metrics, map[string]float64{
			"sentio_node_si_member_serve_databases{state=revoking}":           1,
			"sentio_node_si_member_serve_not_serving{reason=installing}":      1,
			"sentio_node_si_member_serve_revoking_seconds":                    5,
			"sentio_node_si_member_serve_lease_waits_total{outcome=released}": 0,
		})
		h.release(id)
		synctest.Wait()
		if err := <-done; err != nil {
			t.Fatalf("BeforeInstall: %v", err)
		}
		requireMetrics(t, h.metrics, map[string]float64{
			"sentio_node_si_member_serve_lease_waits_total{outcome=released}": 1,
			"sentio_node_si_member_serve_lease_waits_total{outcome=expired}":  0,
			"sentio_node_si_member_serve_revoking_seconds":                    0,
		})
		// Installing holds until AfterInstall, then two polls confirm the entry.
		advance(3 * time.Second)
		h.requireReason(ReasonInstalling)
		h.srv.AfterInstall(testOwner, testDatabase)
		advance(time.Second)
		h.requireReason(ReasonEntryUnconfirmed)
		advance(time.Second)
		h.requireServing()
	})
}

// TestBeforeInstallWaitsForExpiryWhenTheHostIsSilent: a host that neither
// renews nor releases holds an install for exactly one LeaseTTL after its
// last renew.
func TestBeforeInstallWaitsForExpiryWhenTheHostIsSilent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		id := h.acquire("0xs40")
		advance(4 * time.Second)
		if status, _ := h.renew(id); status != http.StatusOK {
			t.Fatalf("renew: %d", status)
		}
		renewed := time.Now()
		done := make(chan time.Time, 1)
		go func() {
			if err := h.srv.BeforeInstall(t.Context(), testOwner, testDatabase); err != nil {
				t.Errorf("BeforeInstall: %v", err)
			}
			done <- time.Now()
		}()
		// The host neither renews nor releases.
		advance(DefaultLeaseTTL - 500*time.Millisecond)
		select {
		case at := <-done:
			t.Fatalf("BeforeInstall returned at %s, before the lease expired", at.Sub(renewed))
		default:
		}
		advance(time.Second)
		select {
		case at := <-done:
			if at.Sub(renewed) != DefaultLeaseTTL {
				t.Fatalf("BeforeInstall returned %s after the last renew, want exactly the lease ttl %s", at.Sub(renewed), DefaultLeaseTTL)
			}
		default:
			t.Fatal("BeforeInstall still waits after the lease expired")
		}
		status, out := h.renew(id)
		requireError(t, status, out, http.StatusNotFound, "member serving: unknown lease "+id)
		requireMetrics(t, h.metrics, map[string]float64{
			"sentio_node_si_member_serve_lease_waits_total{outcome=expired}":  1,
			"sentio_node_si_member_serve_lease_waits_total{outcome=released}": 0,
		})
		h.srv.AfterInstall(testOwner, testDatabase)
	})
}

// TestBeforeInstallReturnsAtOnceWithoutALease: without a lease BeforeInstall
// only marks the database installing.
func TestBeforeInstallReturnsAtOnceWithoutALease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		h.requireServing()
		before := time.Now()
		if err := h.srv.BeforeInstall(t.Context(), testOwner, testDatabase); err != nil || time.Since(before) != 0 {
			t.Fatalf("BeforeInstall: %v after %s", err, time.Since(before))
		}
		h.requireReason(ReasonInstalling)
		// A database the member does not list yet (a first install) too.
		if err := h.srv.BeforeInstall(t.Context(), 1, "nft"); err != nil || time.Since(before) != 0 {
			t.Fatalf("BeforeInstall of a new database: %v after %s", err, time.Since(before))
		}
		h.srv.AfterInstall(1, "nft")
		h.srv.AfterInstall(testOwner, testDatabase)
		advance(time.Second)
		h.requireReason(ReasonEntryUnconfirmed)
		advance(time.Second)
		h.requireServing()
		requireMetrics(t, h.metrics, map[string]float64{
			"sentio_node_si_member_serve_lease_waits_total{outcome=released}": 0,
			"sentio_node_si_member_serve_lease_waits_total{outcome=expired}":  0,
		})
	})
}

// TestBeforeInstallGivesUpWithItsContext: a cancelled install clears
// installing again; the revoked lease stays revoked.
func TestBeforeInstallGivesUpWithItsContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		id := h.acquire("0xs40")
		// The deadline falls between two polls (t=3.5).
		ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
		defer cancel()
		if err := h.srv.BeforeInstall(ctx, testOwner, testDatabase); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("BeforeInstall: %v, want the context's deadline", err)
		}
		// Installing is cleared, so the database serves again after two
		// polls; the revoked lease stays revoked and a new acquire replaces it.
		advance(time.Second)
		h.requireReason(ReasonEntryUnconfirmed)
		advance(time.Second)
		h.requireServing()
		status, out := h.renew(id)
		requireError(t, status, out, http.StatusConflict, "member serving: lease "+id+" is revoked")
		next := h.acquire("0xs40")
		status, out = h.renew(id)
		requireError(t, status, out, http.StatusNotFound, "member serving: unknown lease "+id)
		if l := h.database().Lease; l == nil || l.LeaseID != next || l.State != "held" {
			t.Fatalf("lease view %+v, want %s held", l, next)
		}
	})
}

// TestARestartedMemberWaitsForTheLeasesItsPreviousProcessGranted is
// clarification K44 across a restart of the si-member: its host keeps the
// lease the previous process granted and may still answer the database
// Replica. The new process reads the lease back from its state directory,
// revoked, so an install of the database (the startup audit's included)
// waits for the host's release or the lease's expiry instead of returning at
// once, and a renew of the restored lease answers 409.
func TestARestartedMemberWaitsForTheLeasesItsPreviousProcessGranted(t *testing.T) {
	t.Run("the host releases", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newHarness(t, testConfig())
			advance(time.Second)
			id := h.acquire("0xs40")
			advance(2 * time.Second)
			h.restart(testConfig())
			h.requireReason(ReasonEntryUnconfirmed)
			if l := h.database().Lease; l == nil || *l != (LeaseView{LeaseID: id, State: "revoking", RemainingMs: 28000}) {
				t.Fatalf("lease view after the restart %+v, want %s revoking with 28000ms left", l, id)
			}
			status, out := h.renew(id)
			requireError(t, status, out, http.StatusConflict, "member serving: lease "+id+" is revoked")
			done := make(chan error, 1)
			go func() { done <- h.srv.BeforeInstall(t.Context(), testOwner, testDatabase) }()
			advance(5 * time.Second)
			select {
			case err := <-done:
				t.Fatalf("BeforeInstall returned %v after the restart while the host holds the lease", err)
			default:
			}
			h.release(id)
			synctest.Wait()
			if err := <-done; err != nil {
				t.Fatalf("BeforeInstall: %v", err)
			}
			requireMetrics(t, h.metrics, map[string]float64{
				"sentio_node_si_member_serve_lease_waits_total{outcome=released}": 1,
				"sentio_node_si_member_serve_lease_waits_total{outcome=expired}":  0,
			})
			h.srv.AfterInstall(testOwner, testDatabase)
		})
	})
	t.Run("the host is silent", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			h := newHarness(t, testConfig())
			advance(time.Second)
			id := h.acquire("0xs40")
			advance(4 * time.Second)
			if status, _ := h.renew(id); status != http.StatusOK {
				t.Fatalf("renew: %d", status)
			}
			renewed := time.Now()
			advance(3 * time.Second)
			h.restart(testConfig())
			done := make(chan time.Time, 1)
			go func() {
				if err := h.srv.BeforeInstall(t.Context(), testOwner, testDatabase); err != nil {
					t.Errorf("BeforeInstall: %v", err)
				}
				done <- time.Now()
			}()
			advance(renewed.Add(DefaultLeaseTTL).Sub(time.Now()) - 500*time.Millisecond)
			select {
			case at := <-done:
				t.Fatalf("BeforeInstall returned %s after the last renew, before the restored lease expired", at.Sub(renewed))
			default:
			}
			advance(time.Second)
			select {
			case at := <-done:
				if at.Sub(renewed) != DefaultLeaseTTL {
					t.Fatalf("BeforeInstall returned %s after the last renew, want exactly the lease ttl %s", at.Sub(renewed), DefaultLeaseTTL)
				}
			default:
				t.Fatal("BeforeInstall still waits after the restored lease expired")
			}
			requireMetrics(t, h.metrics, map[string]float64{
				"sentio_node_si_member_serve_lease_waits_total{outcome=expired}": 1,
			})
			h.srv.AfterInstall(testOwner, testDatabase)
			// A lease past its expiry is not restored again.
			h.restart(testConfig())
			before := time.Now()
			if err := h.srv.BeforeInstall(t.Context(), testOwner, testDatabase); err != nil || time.Since(before) != 0 {
				t.Fatalf("BeforeInstall after the lease expired: %v after %s", err, time.Since(before))
			}
			h.srv.AfterInstall(testOwner, testDatabase)
		})
	})
}

// TestLeasesAreRecordedBeforeTheyAreGrantedOrRenewed: the server writes every
// lease to LeaseFile in its state directory before it answers a grant or a
// renew, and drops it there on release. A grant or a renew it cannot record
// is refused with 500 and changes nothing, and New refuses a lease file it
// cannot read, since it could not tell which leases a host still holds.
func TestLeasesAreRecordedBeforeTheyAreGrantedOrRenewed(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		path := filepath.Join(h.stateDir, LeaseFile)
		recorded := func() []leaseRecord {
			t.Helper()
			b, err := os.ReadFile(path)
			if err != nil {
				t.Fatalf("read the lease file: %v", err)
			}
			var f leaseFileContent
			if err := json.Unmarshal(b, &f); err != nil || f.Version != 1 {
				t.Fatalf("lease file %s: %v", b, err)
			}
			return f.Leases
		}
		id := h.acquire("0xs40")
		if got := recorded(); len(got) != 1 || got[0] != (leaseRecord{LeaseID: id, Database: testDatabase, Owner: testOwner, RenewedUnixNano: time.Now().UnixNano(), TTLMs: 30000}) {
			t.Fatalf("recorded %+v, want the granted lease", got)
		}
		advance(10 * time.Second)
		if status, _ := h.renew(id); status != http.StatusOK {
			t.Fatalf("renew: %d", status)
		}
		if got := recorded(); len(got) != 1 || got[0].RenewedUnixNano != time.Now().UnixNano() {
			t.Fatalf("recorded %+v, want the renew's time", got)
		}

		// A lease file the server cannot replace refuses the renew and the
		// grant, and changes neither.
		if err := os.Remove(path); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(filepath.Join(path, "blocked"), 0o700); err != nil {
			t.Fatal(err)
		}
		cannotRecord := "serve: record leases in " + path + ": "
		advance(5 * time.Second)
		status, out := h.renew(id)
		if msg, _ := out["error"].(string); status != http.StatusInternalServerError || !strings.HasPrefix(msg, cannotRecord) {
			t.Fatalf("renew with an unwritable lease file: %d %v, want 500 %q...", status, out, cannotRecord)
		}
		if l := h.database().Lease; l == nil || l.RemainingMs != 25000 {
			t.Fatalf("lease view %+v, want the renew not applied (25000ms left)", l)
		}
		h.release(id)
		status, out = h.call(http.MethodPost, "/v1/serving/acquire", `{"database":"dex","snapshot_id":"0xs40"}`)
		if msg, _ := out["error"].(string); status != http.StatusInternalServerError || !strings.HasPrefix(msg, cannotRecord) {
			t.Fatalf("acquire with an unwritable lease file: %d %v, want 500 %q...", status, out, cannotRecord)
		}
		if l := h.database().Lease; l != nil {
			t.Fatalf("a grant that was not recorded left lease %+v", l)
		}
		if err := os.RemoveAll(path); err != nil {
			t.Fatal(err)
		}
		next := h.acquire("0xs40")
		if got := recorded(); len(got) != 1 || got[0].LeaseID != next {
			t.Fatalf("recorded %+v, want only %s", got, next)
		}
		h.release(next)
		if got := recorded(); len(got) != 0 {
			t.Fatalf("recorded %+v after the release, want none", got)
		}

		// New refuses a lease file it cannot read.
		for content, want := range map[string]string{
			"{":                           "serve: lease file %s: unexpected EOF",
			`{"version":2,"leases":[]}`:   "serve: lease file %s has version 2, want 1",
			`{"version":1,"leases":[{}]}`: "serve: lease file %s has a lease record without an id, a database or a ttl",
		} {
			dir := t.TempDir()
			bad := filepath.Join(dir, LeaseFile)
			if err := os.WriteFile(bad, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := New(testConfig(), Deps{Registry: h.reg, Published: h.pub, Copies: h.copies, StateDir: dir})
			if err == nil || err.Error() != strings.ReplaceAll(want, "%s", bad) {
				t.Fatalf("New with lease file %q: %v, want %s", content, err, strings.ReplaceAll(want, "%s", bad))
			}
		}
	})
}

// TestLeaseRestoreRejectsTrailingAndOverflowingRecords fails startup instead
// of discarding a lease after an integer conversion or a partial JSON read.
func TestLeaseRestoreRejectsTrailingAndOverflowingRecords(t *testing.T) {
	for name, content := range map[string]string{
		"trailing document": `{"version":1,"leases":[]} {"version":1,"leases":[{}]}`,
		"overflowing ttl":   `{"version":1,"leases":[{"lease_id":"0011","database":"dex","renewed_unix_nano":0,"ttl_ms":9223372036854775807}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.WriteFile(filepath.Join(dir, LeaseFile), []byte(content), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := New(testConfig(), Deps{Registry: &fakeRegistry{}, Published: &fakePublished{}, Copies: &fakeCopies{}, StateDir: dir})
			if err == nil {
				t.Fatal("New accepted an invalid lease record")
			}
		})
	}
}
