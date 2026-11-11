package authority

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// DAArchivePutPurpose is the purpose of a payload-store archive put token:
// an Arbiter authority key (the committed operational set the store also
// verifies ReleasePins against) authorises one put of one consensus-history
// archive object. Unlike a da-put-v1 token it names no indexer: the store
// attributes it to the Arbiter's archive, not to an si_indexers writer, and
// accepts it only for content destined to an ARCHIVE pin under the
// network's history scope.
const DAArchivePutPurpose = "da-archive-put-v1"

// DAArchiveHolderID is the pin holder an archive put token names.
const DAArchiveHolderID = "arbiter"

// DAArchivePinPurpose is the PinPurpose number an archive put token names
// (arbiter-proto da.proto PIN_PURPOSE_ARCHIVE).
const DAArchivePinPurpose int32 = 3

// DAArchiveScopeKey is the pin scope of the history archive that covers the
// blocks through throughBlockSeq: "history:<network_id>:<through_block_seq>".
func DAArchiveScopeKey(networkID string, throughBlockSeq uint64) string {
	return "history:" + networkID + ":" + strconv.FormatUint(throughBlockSeq, 10)
}

// DAArchivePutClaims is the JWS payload of an archive put token. Field order
// is the wire order; the payload store parses it as JSON and refuses unknown
// fields, so the order only matters for reproducing test vectors.
type DAArchivePutClaims struct {
	Purpose       string `json:"purpose"`
	NetworkID     string `json:"network_id"`
	HolderID      string `json:"holder_id"`
	PinPurpose    int32  `json:"pin_purpose"`
	ScopeKey      string `json:"scope_key"`
	PayloadHash   string `json:"payload_hash"`
	PayloadLength uint64 `json:"payload_length"`
	// IssuedAt is unix seconds; the store refuses tokens outside its clock
	// skew window.
	IssuedAt int64 `json:"iat"`
}

// SignDAArchivePut signs an archive put token for the history archive
// through throughBlockSeq of networkID at the current time.
func (s *Signer) SignDAArchivePut(networkID string, throughBlockSeq uint64, payloadHash string, payloadLength uint64) (string, error) {
	return s.SignDAArchivePutAt(DAArchivePutClaims{
		NetworkID:     networkID,
		ScopeKey:      DAArchiveScopeKey(networkID, throughBlockSeq),
		PayloadHash:   payloadHash,
		PayloadLength: payloadLength,
		IssuedAt:      time.Now().Unix(),
	})
}

// SignDAArchivePutAt signs claims as given, with Purpose, HolderID and
// PinPurpose set to their only values, for fixtures that need reproducible
// output. ScopeKey must be the network's history scope with a positive
// block sequence.
func (s *Signer) SignDAArchivePutAt(claims DAArchivePutClaims) (string, error) {
	if strings.TrimSpace(claims.NetworkID) == "" {
		return "", errors.New("da archive put token: network ID must be non-empty")
	}
	prefix := "history:" + claims.NetworkID + ":"
	through, ok := strings.CutPrefix(claims.ScopeKey, prefix)
	if n, err := strconv.ParseUint(through, 10, 64); !ok || err != nil || n == 0 || strconv.FormatUint(n, 10) != through {
		return "", fmt.Errorf("da archive put token: scope key %q is not %s<positive block seq>", claims.ScopeKey, prefix)
	}
	if claims.PayloadHash == "" || claims.PayloadLength == 0 {
		return "", errors.New("da archive put token: payload hash and length are required")
	}
	claims.Purpose, claims.HolderID, claims.PinPurpose = DAArchivePutPurpose, DAArchiveHolderID, DAArchivePinPurpose
	token, err := s.signJSON(claims)
	if err != nil {
		return "", fmt.Errorf("da archive put token: %w", err)
	}
	return token, nil
}
