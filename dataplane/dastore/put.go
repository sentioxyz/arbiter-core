package dastore

import (
	"context"
	"fmt"
	"io"
	"log/slog"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"

	"github.com/housegate/housegate/pkg/replay"
)

func (c *Client) storeLimits(ctx context.Context) (*pb.StoreLimits, error) {
	c.limitsMu.Lock()
	defer c.limitsMu.Unlock()
	if c.limits != nil {
		return c.limits, nil
	}
	conn, err := c.dataConnection()
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	defer cancel()
	limits, err := pb.NewPayloadStoreClient(conn).GetStoreLimits(callCtx, &pb.GetStoreLimitsRequest{})
	if err != nil {
		return nil, fmt.Errorf("get store limits: %w", err)
	}
	c.limits = limits
	return limits, nil
}

// Put stores payload and asserts that the store minted expectedRef.
func (c *Client) Put(ctx context.Context, expectedRef string, payload []byte) error {
	limits, err := c.storeLimits(ctx)
	if err != nil {
		return fmt.Errorf("dastore: store limits: %w", err)
	}
	header := &pb.PutPayloadHeader{
		PayloadHash:   replay.DigestBytes(payload),
		PayloadLength: uint64(len(payload)),
	}
	var result *pb.PutPayloadResult
	if uint64(len(payload)) <= limits.GetMaxInlineBytes() {
		result, err = c.putInline(ctx, header, payload)
	} else {
		result, err = c.putStream(ctx, limits, header, payload)
	}
	if err != nil {
		return fmt.Errorf("dastore: put %s: %w", expectedRef, err)
	}
	if result.GetCode() != pb.PutCode_PUT_CODE_OK {
		return fmt.Errorf("dastore: put %s: %s: %s", expectedRef, result.GetCode(), result.GetMessage())
	}
	if result.GetPayloadRef() != expectedRef {
		return fmt.Errorf(
			"dastore: put ref-minting divergence: store minted %q, envelope carries %q",
			result.GetPayloadRef(),
			expectedRef,
		)
	}
	if result.GetDeduplicated() {
		slog.DebugContext(ctx, "payload store deduplicated put", "payload_ref", expectedRef)
	}
	return nil
}

func (c *Client) putInline(ctx context.Context, header *pb.PutPayloadHeader, payload []byte) (*pb.PutPayloadResult, error) {
	conn, err := c.dataConnection()
	if err != nil {
		return nil, err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	defer cancel()
	return pb.NewPayloadStoreClient(conn).PutPayloadInline(callCtx, &pb.PutPayloadInlineRequest{
		Header:  header,
		Payload: payload,
	})
}

func (c *Client) putStream(
	ctx context.Context,
	limits *pb.StoreLimits,
	header *pb.PutPayloadHeader,
	payload []byte,
) (*pb.PutPayloadResult, error) {
	conn, err := c.dataConnection()
	if err != nil {
		return nil, err
	}
	chunkSize := int(limits.GetMaxChunkBytes())
	if chunkSize <= 0 {
		return nil, fmt.Errorf("store advertised invalid max_chunk_bytes %d", limits.GetMaxChunkBytes())
	}
	stream, err := pb.NewPayloadStoreClient(conn).PutPayload(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&pb.PutPayloadFrame{Frame: &pb.PutPayloadFrame_Header{Header: header}}); err != nil {
		if err != io.EOF {
			return nil, err
		}
		return stream.CloseAndRecv()
	}
	for start := 0; start < len(payload); start += chunkSize {
		end := min(start+chunkSize, len(payload))
		err := stream.Send(&pb.PutPayloadFrame{
			Frame: &pb.PutPayloadFrame_Chunk{Chunk: payload[start:end]},
		})
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}
