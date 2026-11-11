package verifier

import (
	"context"
	"testing"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core/dataplane/tableset"
	"github.com/sentioxyz/arbiter-core/wire"
)

// arbiter-core#76: an Active registry table this binary cannot build makes
// that one table unservable. Run keeps serving every other table, both when
// the table is in the registry at startup (a restart) and when it appears
// while Run is active; the table stays not Ready, so the attestation gate
// refuses it.
func TestRun_VerifierKeepsServingWithAnUnservableRegistryTable(t *testing.T) {
	for _, phase := range []string{"at startup", "after startup"} {
		t.Run(phase, func(t *testing.T) {
			view, core := newFakeRegistryView(), &fakeReplayCore{}
			role, server, cfg := newRegistryRoleV(t, view, core)
			role.cfg.ProtocolTablesReconcile = 20 * time.Millisecond
			bad := unservableSchema("db.u_" + cfg.ReplicaID[len("verifier-"):])
			install := func() {
				view.set(genesisIncarnationV(1, cfg.NetworkID, cfg.Tables[0]), chainIncarnationV(t, 2, cfg.NetworkID, bad, wire.TableStatusActive))
			}
			if phase == "at startup" {
				install()
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- role.Run(ctx) }()
			t.Cleanup(func() {
				cancel()
				select {
				case <-done:
				case <-time.After(10 * time.Second):
				}
			})
			for start := time.Now(); ; time.Sleep(10 * time.Millisecond) {
				if _, active := server.subscriptionSnapshot(); active > 0 {
					break
				}
				select {
				case err := <-done:
					t.Fatalf("Run returned %v before subscribing", err)
				default:
				}
				if time.Since(start) > 10*time.Second {
					t.Fatal("the verifier never subscribed")
				}
			}
			if phase == "after startup" {
				install()
			}
			select {
			case err := <-done:
				t.Fatalf("Run returned %v; an unservable table must not end it", err)
			case <-time.After(time.Second):
			}
			if !role.TableReady(cfg.Tables[0].TableID) || role.TableReady(bad.TableID) {
				t.Fatalf("ready: genesis=%v unservable=%v, want true/false", role.TableReady(cfg.Tables[0].TableID), role.TableReady(bad.TableID))
			}
			st := role.TableSetStats()
			if len(st.Unservable) != 1 || st.Unservable[0].TableID != bad.TableID || st.Unservable[0].Reason != tableset.UnservablePartitionFreeze {
				t.Fatalf("Unservable = %+v", st.Unservable)
			}
			// The other tables are still served, and the unservable one is
			// still refused.
			refused := writeJob(t, cfg, 9, "0xprev", "0xroot", bad, true, "u", []byte("x"))
			served := writeJob(t, cfg, 10, "0xprev", "0xroot", cfg.Tables[0], false, "g", []byte("y"))
			server.push(&pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: wire.ReplayJobToPB(refused)}})
			server.push(&pb.VerifierDispatch{Dispatch: &pb.VerifierDispatch_ReplayJob{ReplayJob: wire.ReplayJobToPB(served)}})
			for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(20 * time.Millisecond) {
				if _, _, atts, _ := server.snapshot(); len(atts) > 0 {
					if jobs := core.jobsSnapshot(); len(jobs) != 1 || jobs[0].BlockSeq != 10 {
						t.Fatalf("replayed blocks %v, want only 10", blockSeqs(jobs))
					}
					return
				}
			}
			t.Fatalf("the genesis write was never attested (replayed %v)", blockSeqs(core.jobsSnapshot()))
		})
	}
}
