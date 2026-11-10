package authority

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// DAPutPurpose is the purpose of a payload-store put token: the indexer's
// enrolled signer authorises one put of one content address to the network's
// payload store. The store recovers the signer and checks it against its
// allowlist of si_indexers entries.
const DAPutPurpose = "da-put-v1"

// DAPutClaims is the JWS payload of a put token. Field order is the wire
// order; the payload store parses it as JSON, so the order only matters for
// reproducing test vectors.
type DAPutClaims struct {
	Purpose       string `json:"purpose"`
	NetworkID     string `json:"network_id"`
	IndexerID     uint64 `json:"indexer_id"`
	SNodeNodeID   string `json:"snode_node_id"`
	PayloadHash   string `json:"payload_hash"`
	PayloadLength uint64 `json:"payload_length"`
	// IssuedAt is unix seconds; the store refuses tokens outside its clock
	// skew window.
	IssuedAt int64 `json:"iat"`
}

// SignDAPut signs a put token for one payload at the current time.
func (s *Signer) SignDAPut(networkID string, indexerID uint64, snodeNodeID, payloadHash string, payloadLength uint64) (string, error) {
	return s.SignDAPutAt(DAPutClaims{
		NetworkID:     networkID,
		IndexerID:     indexerID,
		SNodeNodeID:   snodeNodeID,
		PayloadHash:   payloadHash,
		PayloadLength: payloadLength,
		IssuedAt:      time.Now().Unix(),
	})
}

// SignDAPutAt signs claims as given (Purpose is set to DAPutPurpose), for
// fixtures that need reproducible output.
func (s *Signer) SignDAPutAt(claims DAPutClaims) (string, error) {
	if strings.TrimSpace(claims.NetworkID) == "" || strings.TrimSpace(claims.SNodeNodeID) == "" {
		return "", errors.New("da put token: network ID and snode node ID must be non-empty")
	}
	if claims.PayloadHash == "" || claims.PayloadLength == 0 {
		return "", errors.New("da put token: payload hash and length are required")
	}
	claims.Purpose = DAPutPurpose
	token, err := s.signJSON(claims)
	if err != nil {
		return "", fmt.Errorf("da put token: %w", err)
	}
	return token, nil
}
