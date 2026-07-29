package dastore

import (
	"context"
	"strings"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"

	"github.com/sentioxyz/arbiter-core/dataplane/dastore/dastoretest"
)

var _ replay.PayloadStore = (*Client)(nil)

func putOne(t *testing.T, c *Client, payload []byte) string {
	t.Helper()
	ref := "fake://" + replay.DigestBytes(payload)
	if err := c.Put(context.Background(), ref, payload); err != nil {
		t.Fatalf("seed put: %v", err)
	}
	return ref
}

func TestGetPayload_RoundTrip(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	payload := []byte("a,b\n1,2\n3,4\n")
	ref := putOne(t, c, payload)
	got, err := c.GetPayload(context.Background(), ref)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("get: %q %v", got, err)
	}
}

func TestGetPayload_MultiChunkReassembly(t *testing.T) {
	s := dastoretest.New(t)
	s.SetLimits(&pb.StoreLimits{MaxInlineBytes: 1 << 20, MaxChunkBytes: 3, MaxBatchRefs: 1024, MaxPayloadBytes: 1 << 20, IngestLeaseMs: 60000})
	c := newClient(t, s)
	payload := []byte("0123456789")
	ref := putOne(t, c, payload)
	got, err := c.GetPayload(context.Background(), ref)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("get: %q %v", got, err)
	}
}

func TestGetPayload_NotFoundIsAvailabilityIncident(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	_, err := c.GetPayload(context.Background(), "fake://missing")
	if err == nil || !strings.Contains(err.Error(), "FETCH_CODE_NOT_FOUND") ||
		!strings.Contains(err.Error(), "availability incident") {
		t.Fatalf("want NOT_FOUND availability incident, got %v", err)
	}
}

func TestGetPayload_PendingRetriesThenSucceeds(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	payload := []byte("late")
	ref := putOne(t, c, payload)
	s.SetPending(ref, 2)
	got, err := c.GetPayload(context.Background(), ref)
	if err != nil || string(got) != string(payload) {
		t.Fatalf("get after pending: %q %v", got, err)
	}
}

func TestGetPayload_PendingExhaustsRetries(t *testing.T) {
	s := dastoretest.New(t)
	c := newClient(t, s)
	payload := []byte("never")
	ref := putOne(t, c, payload)
	s.SetPending(ref, 99)
	_, err := c.GetPayload(context.Background(), ref)
	if err == nil || !strings.Contains(err.Error(), "FETCH_CODE_PENDING") {
		t.Fatalf("want PENDING exhaustion error, got %v", err)
	}
}

func TestGetPayload_TerminalFetchCodes(t *testing.T) {
	for _, code := range []pb.FetchCode{
		pb.FetchCode_FETCH_CODE_RELEASED,
		pb.FetchCode_FETCH_CODE_OFFSET_OUT_OF_RANGE,
		pb.FetchCode_FETCH_CODE_INTERNAL,
	} {
		t.Run(code.String(), func(t *testing.T) {
			s := dastoretest.New(t)
			c := newClient(t, s)
			ref := putOne(t, c, []byte(code.String()))
			s.SetFetchCode(ref, code)
			_, err := c.GetPayload(context.Background(), ref)
			if err == nil || !strings.Contains(err.Error(), code.String()) {
				t.Fatalf("want %s error, got %v", code, err)
			}
			if code == pb.FetchCode_FETCH_CODE_RELEASED &&
				!strings.Contains(err.Error(), "availability incident") {
				t.Fatalf("RELEASED must be an availability incident, got %v", err)
			}
		})
	}
}

func TestGetPayload_RejectsMalformedFrameGrammar(t *testing.T) {
	const ref = "fake://scripted"
	begin := func() *pb.FetchFrame {
		return &pb.FetchFrame{Frame: &pb.FetchFrame_Begin{Begin: &pb.FetchBegin{
			PayloadRef: ref,
			SpecIndex:  0,
		}}}
	}
	data := func(offset uint64) *pb.FetchFrame {
		return &pb.FetchFrame{Frame: &pb.FetchFrame_Data{Data: &pb.FetchData{
			PayloadRef: ref,
			SpecIndex:  0,
			Offset:     offset,
			Chunk:      []byte("x"),
		}}}
	}
	end := func() *pb.FetchFrame {
		return &pb.FetchFrame{Frame: &pb.FetchFrame_End{End: &pb.FetchEnd{
			PayloadRef:   ref,
			SpecIndex:    0,
			Code:         pb.FetchCode_FETCH_CODE_OK,
			ServedLength: 1,
		}}}
	}

	cases := []struct {
		name   string
		frames []*pb.FetchFrame
		want   string
	}{
		{name: "data before begin", frames: []*pb.FetchFrame{data(0)}, want: "before FetchBegin"},
		{name: "duplicate begin", frames: []*pb.FetchFrame{begin(), begin()}, want: "duplicate FetchBegin"},
		{name: "non-contiguous offset", frames: []*pb.FetchFrame{begin(), data(1)}, want: "non-contiguous"},
		{name: "missing end", frames: []*pb.FetchFrame{begin(), data(0)}, want: "without FetchEnd"},
		{name: "frame after end", frames: []*pb.FetchFrame{begin(), data(0), end(), begin()}, want: "after FetchEnd"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := dastoretest.New(t)
			s.SetFetchFrames(ref, tc.frames)
			c := newClient(t, s)
			_, err := c.GetPayload(context.Background(), ref)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want error containing %q, got %v", tc.want, err)
			}
		})
	}
}
