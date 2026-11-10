package dastore

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"google.golang.org/grpc/metadata"
)

// PutAuthMetadataKey is the gRPC metadata key that carries a put token to
// the payload store.
const PutAuthMetadataKey = "sentio-da-put-auth"

// PutAuthorizer attaches the write authorisation of one put to ctx. The
// payload store attributes and rate-limits puts by it; a nil authorizer
// sends puts without a token.
type PutAuthorizer func(ctx context.Context, payloadHash string, payloadLength uint64) (context.Context, error)

// PutSigner signs a put token (authority.Signer implements it with the
// indexer's enrolled key).
type PutSigner interface {
	SignDAPut(networkID string, indexerID uint64, snodeNodeID, payloadHash string, payloadLength uint64) (string, error)
}

// NewPutAuthorizer returns an authorizer that signs a fresh put token per put
// with signer, as the si_indexers entry (indexerID, snodeNodeID) of
// networkID.
func NewPutAuthorizer(signer PutSigner, networkID string, indexerID uint64, snodeNodeID string) (PutAuthorizer, error) {
	if signer == nil {
		return nil, errors.New("dastore: put authorizer needs a signer")
	}
	if strings.TrimSpace(networkID) == "" || strings.TrimSpace(snodeNodeID) == "" {
		return nil, errors.New("dastore: put authorizer needs a network ID and an snode node ID")
	}
	return func(ctx context.Context, payloadHash string, payloadLength uint64) (context.Context, error) {
		token, err := signer.SignDAPut(networkID, indexerID, snodeNodeID, payloadHash, payloadLength)
		if err != nil {
			return nil, fmt.Errorf("dastore: sign put token: %w", err)
		}
		return metadata.AppendToOutgoingContext(ctx, PutAuthMetadataKey, token), nil
	}, nil
}
