package dastore

import (
	"context"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/dataplane/dastore/dastoretest"
)

func TestClient_ClosePreventsReconnect(t *testing.T) {
	store := dastoretest.New(t)
	client, err := New(Config{DataAddr: store.Addr(), ControlAddr: store.Addr()})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	payload := []byte("close")
	ref := "fake://" + replay.DigestBytes(payload)
	if err := client.Put(context.Background(), ref, payload); err != nil {
		t.Fatalf("put: %v", err)
	}
	if err := client.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_SEQUENCED, "1", []string{ref}); err != nil {
		t.Fatalf("pin: %v", err)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	callsBefore := len(store.Calls())

	if err := client.Put(context.Background(), ref, payload); err == nil ||
		!strings.Contains(err.Error(), "closed") {
		t.Fatalf("put after close must fail without reconnecting, got %v", err)
	}
	if err := client.Release(context.Background(), pb.PinPurpose_PIN_PURPOSE_SEQUENCED, "1"); err == nil ||
		!strings.Contains(err.Error(), "closed") {
		t.Fatalf("release after close must fail without reconnecting, got %v", err)
	}
	if callsAfter := len(store.Calls()); callsAfter != callsBefore {
		t.Fatalf("closed client issued new RPCs: before=%d after=%d", callsBefore, callsAfter)
	}
	if err := client.Close(); err != nil {
		t.Fatalf("second close: %v", err)
	}
}
