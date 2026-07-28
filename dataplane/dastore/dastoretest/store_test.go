package dastoretest

import (
	"context"
	"testing"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func dial(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

func TestFake_PutFetchPinReleaseRoundTrip(t *testing.T) {
	s := New(t)
	conn := dial(t, s.Addr())
	store := pb.NewPayloadStoreClient(conn)
	lifecycle := pb.NewPayloadLifecycleClient(conn)
	ctx := context.Background()

	lim, err := store.GetStoreLimits(ctx, &pb.GetStoreLimitsRequest{})
	if err != nil || lim.GetMaxInlineBytes() == 0 {
		t.Fatalf("limits: %v %v", lim, err)
	}

	payload := []byte("a,b\n1,2\n")
	put, err := store.PutPayloadInline(ctx, &pb.PutPayloadInlineRequest{
		Header:  &pb.PutPayloadHeader{PayloadHash: digest(payload), PayloadLength: uint64(len(payload))},
		Payload: payload,
	})
	if err != nil || put.GetCode() != pb.PutCode_PUT_CODE_OK {
		t.Fatalf("put: %v %v", put, err)
	}
	ref := put.GetPayloadRef()

	put2, err := store.PutPayloadInline(ctx, &pb.PutPayloadInlineRequest{
		Header:  &pb.PutPayloadHeader{PayloadHash: digest(payload), PayloadLength: uint64(len(payload))},
		Payload: payload,
	})
	if err != nil || put2.GetPayloadRef() != ref || !put2.GetDeduplicated() {
		t.Fatalf("dedupe: %v %v", put2, err)
	}

	fs, err := store.FetchPayloads(ctx, &pb.FetchPayloadsRequest{Specs: []*pb.FetchSpec{{PayloadRef: ref}}})
	if err != nil {
		t.Fatalf("fetch open: %v", err)
	}
	var got []byte
	for {
		fr, err := fs.Recv()
		if err != nil {
			t.Fatalf("fetch recv: %v", err)
		}
		if d := fr.GetData(); d != nil {
			got = append(got, d.GetChunk()...)
		}
		if e := fr.GetEnd(); e != nil {
			if e.GetCode() != pb.FetchCode_FETCH_CODE_OK {
				t.Fatalf("fetch end: %v", e)
			}
			break
		}
	}
	if string(got) != string(payload) {
		t.Fatalf("fetch bytes: %q", got)
	}

	pin, err := lifecycle.PinPayloads(ctx, &pb.PinPayloadsRequest{
		Key:         &pb.PinKey{HolderId: "arbiter", Purpose: pb.PinPurpose_PIN_PURPOSE_SEQUENCED, ScopeKey: "1"},
		PayloadRefs: []string{ref},
	})
	if err != nil || pin.GetResults()[0].GetCode() != pb.PinCode_PIN_CODE_OK {
		t.Fatalf("pin: %v %v", pin, err)
	}
	if got := s.Pins()["arbiter|PIN_PURPOSE_SEQUENCED|1"]; len(got) != 1 || got[0] != ref {
		t.Fatalf("pin bookkeeping: %v", s.Pins())
	}
	rel, err := lifecycle.ReleasePins(ctx, &pb.ReleasePinsRequest{
		Key: &pb.PinKey{HolderId: "arbiter", Purpose: pb.PinPurpose_PIN_PURPOSE_SEQUENCED, ScopeKey: "1"},
	})
	if err != nil || rel.GetCode() != pb.ReleaseCode_RELEASE_CODE_OK {
		t.Fatalf("release: %v %v", rel, err)
	}
	rel2, err := lifecycle.ReleasePins(ctx, &pb.ReleasePinsRequest{
		Key: &pb.PinKey{HolderId: "arbiter", Purpose: pb.PinPurpose_PIN_PURPOSE_SEQUENCED, ScopeKey: "999"},
	})
	if err != nil || rel2.GetCode() != pb.ReleaseCode_RELEASE_CODE_OK {
		t.Fatalf("idempotent release: %v %v", rel2, err)
	}
}

func TestFake_PutCommitmentMismatch(t *testing.T) {
	s := New(t)
	conn := dial(t, s.Addr())
	store := pb.NewPayloadStoreClient(conn)
	put, err := store.PutPayloadInline(context.Background(), &pb.PutPayloadInlineRequest{
		Header:  &pb.PutPayloadHeader{PayloadHash: digest([]byte("other")), PayloadLength: 5},
		Payload: []byte("bytes"),
	})
	if err != nil || put.GetCode() != pb.PutCode_PUT_CODE_COMMITMENT_MISMATCH {
		t.Fatalf("want COMMITMENT_MISMATCH, got %v %v", put, err)
	}
	if _, ok := s.RefFor(digest([]byte("other")), 5); ok {
		t.Fatal("mismatched put must store nothing")
	}
}

func TestFake_RejectsAdvertisedLimitViolations(t *testing.T) {
	s := New(t)
	s.SetLimits(&pb.StoreLimits{
		MaxInlineBytes:  4,
		MaxChunkBytes:   4,
		MaxBatchRefs:    1,
		MaxPayloadBytes: 1 << 20,
		IngestLeaseMs:   60_000,
	})
	conn := dial(t, s.Addr())
	store := pb.NewPayloadStoreClient(conn)
	lifecycle := pb.NewPayloadLifecycleClient(conn)
	ctx := context.Background()
	payload := []byte("12345")
	header := &pb.PutPayloadHeader{
		PayloadHash:   digest(payload),
		PayloadLength: uint64(len(payload)),
	}

	inline, err := store.PutPayloadInline(ctx, &pb.PutPayloadInlineRequest{
		Header:  header,
		Payload: payload,
	})
	if err != nil || inline.GetCode() != pb.PutCode_PUT_CODE_INLINE_LIMIT_EXCEEDED {
		t.Fatalf("oversized inline: got %v %v", inline, err)
	}

	stream, err := store.PutPayload(ctx)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	if err := stream.Send(&pb.PutPayloadFrame{
		Frame: &pb.PutPayloadFrame_Header{Header: header},
	}); err != nil {
		t.Fatalf("send header: %v", err)
	}
	if err := stream.Send(&pb.PutPayloadFrame{
		Frame: &pb.PutPayloadFrame_Chunk{Chunk: payload},
	}); err != nil {
		t.Fatalf("send oversized chunk: %v", err)
	}
	chunked, err := stream.CloseAndRecv()
	if err != nil || chunked.GetCode() != pb.PutCode_PUT_CODE_MALFORMED {
		t.Fatalf("oversized chunk: got %v %v", chunked, err)
	}

	refs := []string{"ref-1", "ref-2"}
	_, err = lifecycle.PinPayloads(ctx, &pb.PinPayloadsRequest{
		Key: &pb.PinKey{
			HolderId: "arbiter",
			Purpose:  pb.PinPurpose_PIN_PURPOSE_REPLAY,
			ScopeKey: "1",
		},
		PayloadRefs: refs,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized pin batch: got %v", err)
	}

	_, err = store.StatPayloads(ctx, &pb.StatPayloadsRequest{PayloadRefs: refs})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized stat batch: got %v", err)
	}

	fetch, err := store.FetchPayloads(ctx, &pb.FetchPayloadsRequest{
		Specs: []*pb.FetchSpec{{PayloadRef: refs[0]}, {PayloadRef: refs[1]}},
	})
	if err != nil {
		t.Fatalf("open oversized fetch: %v", err)
	}
	_, err = fetch.Recv()
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized fetch batch: got %v", err)
	}
}
