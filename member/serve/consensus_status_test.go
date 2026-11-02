package serve

import (
	"log/slog"
	"net/http"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	"github.com/sentioxyz/arbiter-core/member/consensus"
)

// TestServingViewCarriesTheConsensusStatus (plan S3-E1 contract §42.3): the
// serving view and GET /v1/serving carry Deps.Consensus's status at each
// view, and the mode off without it.
func TestServingViewCarriesTheConsensusStatus(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		h := newHarness(t, testConfig())
		advance(time.Second)
		if got := h.srv.Serving().Consensus; !reflect.DeepEqual(got, consensus.OffStatus()) {
			t.Fatalf("without Deps.Consensus the view's consensus is %+v, want %+v", got, consensus.OffStatus())
		}
		status := consensus.Status{Mode: consensus.ModeConsensus, Registered: true, Groups: []consensus.GroupStatus{
			{OwnerIndexerID: 0, DatabaseID: "dex", Generation: 3, Ready: true, Class: "caught_up", CheckpointSeq: 7},
		}}
		srv, err := New(testConfig(), Deps{
			Registry: h.reg, Published: h.pub, Copies: h.copies, Dial: h.dialer.Dial,
			Logger: slog.New(slog.DiscardHandler), NodeID: "member-b",
			StateDir:  h.stateDir,
			Consensus: func() consensus.Status { return status },
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := srv.Serving().Consensus; !reflect.DeepEqual(got, status) {
			t.Fatalf("the view's consensus is %+v, want %+v", got, status)
		}
		h.srv = srv
		code, out := h.call(http.MethodGet, "/v1/serving", "")
		want := map[string]any{"mode": "consensus", "registered": true, "groups": []any{map[string]any{
			"owner_indexer_id": float64(0), "database_id": "dex", "generation": float64(3), "ready": true, "class": "caught_up", "checkpoint_seq": float64(7),
		}}}
		if code != http.StatusOK || !reflect.DeepEqual(out["consensus"], want) {
			t.Fatalf("GET /v1/serving: %d consensus %v, want %v", code, out["consensus"], want)
		}
	})
}
