package dastore

import (
	"context"
	"slices"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/dataplane/dastore/dastoretest"
)

func newClient(t *testing.T, s *dastoretest.Store) *Client {
	t.Helper()
	c, err := New(Config{DataAddr: s.Addr(), ControlAddr: s.Addr()})
	if err != nil {
		t.Fatalf("new client: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestPut_InlineSmallPayload(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	payload := []byte("a,b\n1,2\n")
	ref := "fake://" + replay.DigestBytes(payload)
	if err := c.Put(context.Background(), ref, payload); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, ok := s.Payload(ref)
	if !ok || string(got) != string(payload) {
		t.Fatalf("stored payload: %q ok=%v", got, ok)
	}
	for _, call := range s.Calls() {
		if call.Method == "PutPayload" {
			t.Fatal("small payload must use PutPayloadInline, not the stream")
		}
	}
}

func TestPut_ChunkedLargePayload(t *testing.T) {
	s := dastoretest.New(t)
	s.SetLimits(&pb.StoreLimits{MaxInlineBytes: 8, MaxChunkBytes: 4, MaxBatchRefs: 1024, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000})
	c := newClient(t, s)
	payload := []byte("0123456789abcdef")
	ref := "fake://" + replay.DigestBytes(payload)
	if err := c.Put(context.Background(), ref, payload); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, ok := s.Payload(ref)
	if !ok || string(got) != string(payload) {
		t.Fatalf("stored payload: %q ok=%v", got, ok)
	}
	var sawStream bool
	for _, call := range s.Calls() {
		if call.Method == "PutPayload" {
			sawStream = true
			wantChunks := []int{4, 4, 4, 4}
			if !slices.Equal(call.ChunkSizes, wantChunks) {
				t.Fatalf("stream chunks: got %v want %v", call.ChunkSizes, wantChunks)
			}
		}
	}
	if !sawStream {
		t.Fatal("large payload must use the PutPayload stream")
	}
}

func TestPut_BoundaryExactlyInline(t *testing.T) {
	s := dastoretest.New(t)
	s.SetLimits(&pb.StoreLimits{MaxInlineBytes: 8, MaxChunkBytes: 4, MaxBatchRefs: 1024, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000})
	c := newClient(t, s)
	payload := []byte("01234567")
	if err := c.Put(context.Background(), "fake://"+replay.DigestBytes(payload), payload); err != nil {
		t.Fatalf("put: %v", err)
	}
	for _, call := range s.Calls() {
		if call.Method == "PutPayload" {
			t.Fatal("payload at the inline boundary must go inline")
		}
	}
}

func TestPut_RefMismatchFailsClosed(t *testing.T) {
	s := dastoretest.New(t)
	s.SetMintRefSuffix("-divergent")
	c := newClient(t, s)
	payload := []byte("x")
	err := c.Put(context.Background(), "fake://"+replay.DigestBytes(payload), payload)
	if err == nil || !strings.Contains(err.Error(), "divergence") {
		t.Fatalf("want ref-minting divergence error, got %v", err)
	}
}

func TestPut_TerminalCodeSurfaces(t *testing.T) {
	s := dastoretest.New(t)
	s.SetPutCode(pb.PutCode_PUT_CODE_TOO_LARGE)
	c := newClient(t, s)
	err := c.Put(context.Background(), "fake://whatever", []byte("x"))
	if err == nil || !strings.Contains(err.Error(), "PUT_CODE_TOO_LARGE") {
		t.Fatalf("want TOO_LARGE error, got %v", err)
	}
}

func TestNew_RequiresAnAddr(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("want error when both addrs are empty")
	}
}
