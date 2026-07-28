package dastore

import (
	"context"
	"fmt"
	"io"
	"time"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

const (
	pendingAttempts = 3
	pendingBackoff  = 200 * time.Millisecond
)

// GetPayload fetches one whole payload without trusting any store-side
// content assertion. replay.Verifier validates the returned bytes.
func (c *Client) GetPayload(ctx context.Context, payloadRef string) ([]byte, error) {
	backoff := pendingBackoff
	for attempt := 1; ; attempt++ {
		payload, pending, err := c.fetchOnce(ctx, payloadRef)
		if err == nil {
			return payload, nil
		}
		if !pending || attempt >= pendingAttempts {
			return nil, err
		}
		timer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return nil, ctx.Err()
		case <-timer.C:
		}
		backoff *= 2
	}
}

func (c *Client) fetchOnce(ctx context.Context, payloadRef string) ([]byte, bool, error) {
	conn, err := c.dataConnection()
	if err != nil {
		return nil, false, err
	}
	stream, err := pb.NewPayloadStoreClient(conn).FetchPayloads(ctx, &pb.FetchPayloadsRequest{
		Specs: []*pb.FetchSpec{{PayloadRef: payloadRef}},
	})
	if err != nil {
		return nil, false, fmt.Errorf("dastore: fetch %s: %w", payloadRef, err)
	}

	var (
		payload []byte
		begun   bool
		end     *pb.FetchEnd
	)
	for {
		frame, recvErr := stream.Recv()
		if recvErr == io.EOF {
			if end == nil {
				return nil, false, fmt.Errorf("dastore: fetch %s: stream ended without FetchEnd", payloadRef)
			}
			return finishFetch(payloadRef, begun, payload, end)
		}
		if recvErr != nil {
			return nil, false, fmt.Errorf("dastore: fetch %s stream: %w", payloadRef, recvErr)
		}
		if end != nil {
			return nil, false, fmt.Errorf("dastore: fetch %s: frame arrived after FetchEnd", payloadRef)
		}
		switch typed := frame.GetFrame().(type) {
		case *pb.FetchFrame_Begin:
			begin := typed.Begin
			if begun {
				return nil, false, fmt.Errorf("dastore: fetch %s: duplicate FetchBegin", payloadRef)
			}
			if begin == nil || begin.GetSpecIndex() != 0 || begin.GetPayloadRef() != payloadRef {
				return nil, false, fmt.Errorf("dastore: fetch %s: invalid FetchBegin", payloadRef)
			}
			begun = true
		case *pb.FetchFrame_Data:
			data := typed.Data
			if !begun || data == nil {
				return nil, false, fmt.Errorf("dastore: fetch %s: FetchData before FetchBegin", payloadRef)
			}
			if data.GetSpecIndex() != 0 || data.GetPayloadRef() != payloadRef {
				return nil, false, fmt.Errorf("dastore: fetch %s: FetchData attributed to another spec", payloadRef)
			}
			if data.GetOffset() != uint64(len(payload)) {
				return nil, false, fmt.Errorf(
					"dastore: fetch %s: non-contiguous data offset %d after %d bytes",
					payloadRef,
					data.GetOffset(),
					len(payload),
				)
			}
			if len(data.GetChunk()) == 0 {
				return nil, false, fmt.Errorf("dastore: fetch %s: empty FetchData chunk", payloadRef)
			}
			payload = append(payload, data.GetChunk()...)
		case *pb.FetchFrame_End:
			if typed.End == nil || typed.End.GetSpecIndex() != 0 || typed.End.GetPayloadRef() != payloadRef {
				return nil, false, fmt.Errorf("dastore: fetch %s: invalid FetchEnd", payloadRef)
			}
			end = typed.End
		default:
			return nil, false, fmt.Errorf("dastore: fetch %s: empty or unknown frame", payloadRef)
		}
	}
}

func finishFetch(payloadRef string, begun bool, payload []byte, end *pb.FetchEnd) ([]byte, bool, error) {
	switch end.GetCode() {
	case pb.FetchCode_FETCH_CODE_OK:
		if !begun {
			return nil, false, fmt.Errorf("dastore: fetch %s: OK FetchEnd without FetchBegin", payloadRef)
		}
		if end.GetServedLength() != uint64(len(payload)) {
			return nil, false, fmt.Errorf(
				"dastore: fetch %s: served length %d differs from received %d",
				payloadRef,
				end.GetServedLength(),
				len(payload),
			)
		}
		return payload, false, nil
	case pb.FetchCode_FETCH_CODE_PENDING:
		return nil, true, fmt.Errorf(
			"dastore: fetch %s: %s: %s",
			payloadRef,
			end.GetCode(),
			end.GetMessage(),
		)
	case pb.FetchCode_FETCH_CODE_NOT_FOUND, pb.FetchCode_FETCH_CODE_RELEASED:
		return nil, false, fmt.Errorf(
			"dastore: fetch %s: availability incident: %s: %s",
			payloadRef,
			end.GetCode(),
			end.GetMessage(),
		)
	default:
		return nil, false, fmt.Errorf(
			"dastore: fetch %s: %s: %s",
			payloadRef,
			end.GetCode(),
			end.GetMessage(),
		)
	}
}
