package consensus

import (
	"context"
	"errors"
	"testing"

	"github.com/housegate/housegate/pkg/replay"
	"github.com/sentioxyz/arbiter-core/dataplane/ddl"
	"github.com/sentioxyz/arbiter-core/wire"
)

func TestMemberReadinessChecksTheConfiguredStoragePolicy(t *testing.T) {
	for _, policy := range []string{"member_policy", "other_policy"} {
		t.Run(policy, func(t *testing.T) {
			cfg := testConfig(t)
			cfg.StoragePolicy = "member_policy"
			inc := chainIncarnation(t, 3, swapsSchema, wire.TableStatusActive, true)
			pin := ddl.DefaultMemberPinned(testNodeID, 0, "")
			pin.StoragePolicy = policy
			intent, err := ddl.MemberUnsafeIntent(pin, swapsSchema, 3, "/sentio/0/unsafe_inc/dex__swaps/3")
			if err != nil {
				t.Fatal(err)
			}
			conn := newCHFake()
			conn.addIntent(intent)
			gw := newGatewayFake()
			core := &replayFake{att: replay.ReplayAttestation{ReplicaID: testNodeID}}
			v := newTestRole(t, cfg, VerifyDeps{Client: startGatewayFake(t, gw), Registry: newViewFake(groupSnapshot(true, []string{testNodeID}, nil, inc)), Conn: conn}, core)
			err = v.handler.HandleReplayJob(context.Background(), wire.ReplayJobToPB(swapsJob(5)))
			_, _, _, atts, _ := gw.snapshot()
			if policy == cfg.StoragePolicy {
				if err != nil || core.count() != 1 || len(atts) != 1 {
					t.Fatalf("valid policy rejected: err=%v replays=%d submits=%d", err, core.count(), len(atts))
				}
			} else if !errors.Is(err, ErrMemberTableNotReady) || core.count() != 0 || len(atts) != 0 {
				t.Fatalf("wrong policy attested: err=%v replays=%d submits=%d", err, core.count(), len(atts))
			}
		})
	}
}
