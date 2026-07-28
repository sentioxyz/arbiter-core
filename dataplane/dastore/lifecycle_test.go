package dastore

import (
	"context"
	"fmt"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/sentioxyz/arbiter-core/dataplane/dastore/dastoretest"
)

func TestPin_UnionsAndRelease(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	ref := putOne(t, c, []byte("pinme"))
	if err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7", []string{ref}); err != nil {
		t.Fatalf("pin: %v", err)
	}
	key := "arbiter|PIN_PURPOSE_REPLAY|7"
	if got := s.Pins()[key]; len(got) != 1 || got[0] != ref {
		t.Fatalf("pin bookkeeping: %v", s.Pins())
	}
	if err := c.Release(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if _, ok := s.Pins()[key]; ok {
		t.Fatal("release must drop the whole pin")
	}
	if err := c.Release(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "404"); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}

func TestPin_EmptyRefsIsNoOp(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	if err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7", nil); err != nil {
		t.Fatalf("empty pin: %v", err)
	}
	for _, call := range s.Calls() {
		if call.Method == "PinPayloads" {
			t.Fatal("empty refs must not issue an RPC")
		}
	}
}

func TestPin_SplitsAtMaxBatchRefs(t *testing.T) {
	s := dastoretest.New(t)
	s.SetLimits(&pb.StoreLimits{MaxInlineBytes: 1 << 20, MaxChunkBytes: 1 << 18, MaxBatchRefs: 2, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000})
	c := newClient(t, s)
	var refs []string
	for i := range 5 {
		refs = append(refs, putOne(t, c, []byte(fmt.Sprintf("payload-%d", i))))
	}
	if err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "9", refs); err != nil {
		t.Fatalf("pin: %v", err)
	}
	var pinCalls int
	for _, call := range s.Calls() {
		if call.Method == "PinPayloads" {
			pinCalls++
			if len(call.Refs) > 2 {
				t.Fatalf("batch over max_batch_refs: %v", call.Refs)
			}
		}
	}
	if pinCalls != 3 {
		t.Fatalf("want 3 pin batches, got %d", pinCalls)
	}
	if got := s.Pins()["arbiter|PIN_PURPOSE_REPLAY|9"]; len(got) != 5 {
		t.Fatalf("union across batches: %v", got)
	}
}

func TestPin_NotFoundIsCustodyBroken(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7", []string{"fake://ghost"})
	if err == nil || !strings.Contains(err.Error(), "PIN_CODE_NOT_FOUND") ||
		!strings.Contains(err.Error(), "custody chain broken") {
		t.Fatalf("want NOT_FOUND custody error, got %v", err)
	}
}

func TestPin_ResponseRefMismatchFailsClosed(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	ref := putOne(t, c, []byte("pinme"))
	s.SetPinRef(ref, "fake://different")
	err := c.Pin(context.Background(), pb.PinPurpose_PIN_PURPOSE_REPLAY, "7", []string{ref})
	if err == nil || !strings.Contains(err.Error(), "response ref") {
		t.Fatalf("want response ref mismatch, got %v", err)
	}
}

func TestRelease_LeavesAuthorityJWSEmpty(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	if err := c.Release(context.Background(), pb.PinPurpose_PIN_PURPOSE_AUDIT, "1"); err != nil {
		t.Fatalf("release: %v", err)
	}
	if jws := s.LastReleaseJWS(); jws != "" {
		t.Fatalf("authority_jws must stay empty in v1, got %q", jws)
	}
}
