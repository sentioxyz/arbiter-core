package dastore

import (
	"context"
	"fmt"

	pb "github.com/sentioxyz/arbiter-proto/gen/pb"
)

// Pin idempotently unions refs into the cluster-stable custody key.
func (c *Client) Pin(ctx context.Context, purpose pb.PinPurpose, scopeKey string, refs []string) error {
	if len(refs) == 0 {
		return nil
	}
	limits, err := c.storeLimits(ctx)
	if err != nil {
		return fmt.Errorf("dastore: store limits: %w", err)
	}
	batchSize := int(limits.GetMaxBatchRefs())
	if batchSize <= 0 {
		batchSize = len(refs)
	}
	conn, err := c.controlConnection()
	if err != nil {
		return err
	}
	client := pb.NewPayloadLifecycleClient(conn)
	for start := 0; start < len(refs); start += batchSize {
		end := min(start+batchSize, len(refs))
		callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
		result, callErr := client.PinPayloads(callCtx, &pb.PinPayloadsRequest{
			Key: &pb.PinKey{
				HolderId: holderID,
				Purpose:  purpose,
				ScopeKey: scopeKey,
			},
			PayloadRefs: refs[start:end],
		})
		cancel()
		if callErr != nil {
			return fmt.Errorf("dastore: pin %s/%s: %w", purpose, scopeKey, callErr)
		}
		if len(result.GetResults()) != end-start {
			return fmt.Errorf(
				"dastore: pin %s/%s: result count %d differs from request count %d",
				purpose,
				scopeKey,
				len(result.GetResults()),
				end-start,
			)
		}
		// da.proto guarantees results[i] corresponds to payload_refs[i].
		for i, refResult := range result.GetResults() {
			expectedRef := refs[start+i]
			if refResult.GetPayloadRef() != expectedRef {
				return fmt.Errorf(
					"dastore: pin %s/%s: response ref %q differs from request ref %q",
					purpose,
					scopeKey,
					refResult.GetPayloadRef(),
					expectedRef,
				)
			}
			if refResult.GetCode() != pb.PinCode_PIN_CODE_OK {
				return fmt.Errorf(
					"dastore: pin %s/%s ref %s: custody chain broken: %s: %s",
					purpose,
					scopeKey,
					refResult.GetPayloadRef(),
					refResult.GetCode(),
					refResult.GetMessage(),
				)
			}
		}
	}
	return nil
}

// Release idempotently releases the exact custody key. authority_jws is
// deliberately empty while the v1 store operates in channel-trust mode.
func (c *Client) Release(ctx context.Context, purpose pb.PinPurpose, scopeKey string) error {
	conn, err := c.controlConnection()
	if err != nil {
		return err
	}
	callCtx, cancel := context.WithTimeout(ctx, c.cfg.CallTimeout)
	defer cancel()
	result, err := pb.NewPayloadLifecycleClient(conn).ReleasePins(callCtx, &pb.ReleasePinsRequest{
		Key: &pb.PinKey{
			HolderId: holderID,
			Purpose:  purpose,
			ScopeKey: scopeKey,
		},
	})
	if err != nil {
		return fmt.Errorf("dastore: release %s/%s: %w", purpose, scopeKey, err)
	}
	if result.GetCode() != pb.ReleaseCode_RELEASE_CODE_OK {
		return fmt.Errorf(
			"dastore: release %s/%s: %s: %s",
			purpose,
			scopeKey,
			result.GetCode(),
			result.GetMessage(),
		)
	}
	return nil
}
