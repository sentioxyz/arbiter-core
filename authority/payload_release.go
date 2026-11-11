package authority

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/housegate/housegate/pkg/replay"
)

// PayloadReleasePurpose is the purpose of a payload-store release token
// (arbiter-proto da.proto ReleasePinsRequest.authority_jws): the Arbiter
// authority authorises the release of one pin key. The store recovers the
// signer against its authority list and keeps a per-key release_seq
// watermark.
const PayloadReleasePurpose = "arbiter-payload-release"

// payloadReleaseDomain is the CanonicalDigest domain of the released key.
const payloadReleaseDomain = "arbiter-payload-release-v1"

// PayloadReleaseKey is the canonical form of a da.proto PinKey that a
// release token binds. Purpose is the PinPurpose enum number. The field order
// is the canonical order; the payload store hashes the same shape.
type PayloadReleaseKey struct {
	HolderID string `json:"holder_id"`
	Purpose  int32  `json:"purpose"`
	ScopeKey string `json:"scope_key"`
}

// PayloadReleaseKeyHash is the cmd_hash a release token for key carries.
func PayloadReleaseKeyHash(key PayloadReleaseKey) (string, error) {
	h, err := replay.CanonicalDigest(payloadReleaseDomain, key)
	if err != nil {
		return "", fmt.Errorf("hash payload release key: %w", err)
	}
	return h, nil
}

// PayloadReleaseClaims is the JWS payload of a release token. Field order is
// the wire order; the store parses JSON and refuses unknown fields, so the
// order only matters for reproducing vectors.
type PayloadReleaseClaims struct {
	Purpose    string `json:"purpose"`
	CmdHash    string `json:"cmd_hash"`
	IssuedAt   int64  `json:"iat"`
	ReleaseSeq uint64 `json:"release_seq"`

	NetworkID         string `json:"network_id"`
	GenesisSnapshotID string `json:"genesis_snapshot_id"`
	AuthorityEpoch    uint64 `json:"authority_epoch"`
}

// SignPayloadRelease signs the release of key at releaseSeq under context, at
// the current time.
func (s *Signer) SignPayloadRelease(key PayloadReleaseKey, releaseSeq uint64, context ConsensusContext) (string, error) {
	return s.SignPayloadReleaseAt(key, releaseSeq, context, time.Now().Unix())
}

// SignPayloadReleaseAt is SignPayloadRelease with an explicit issue time, for
// fixtures that need reproducible output.
func (s *Signer) SignPayloadReleaseAt(key PayloadReleaseKey, releaseSeq uint64, context ConsensusContext, iat int64) (string, error) {
	if strings.TrimSpace(key.HolderID) == "" || key.Purpose <= 0 || key.ScopeKey == "" {
		return "", errors.New("payload release token: holder ID, purpose and scope key are required")
	}
	if releaseSeq == 0 {
		return "", errors.New("payload release token: release_seq must be positive")
	}
	if strings.TrimSpace(context.NetworkID) == "" || strings.TrimSpace(context.GenesisSnapshotID) == "" {
		return "", errors.New("payload release token: network ID and genesis snapshot ID must be non-empty")
	}
	hash, err := PayloadReleaseKeyHash(key)
	if err != nil {
		return "", err
	}
	token, err := s.signJSON(PayloadReleaseClaims{
		Purpose:           PayloadReleasePurpose,
		CmdHash:           hash,
		IssuedAt:          iat,
		ReleaseSeq:        releaseSeq,
		NetworkID:         context.NetworkID,
		GenesisSnapshotID: context.GenesisSnapshotID,
		AuthorityEpoch:    context.AuthorityEpoch,
	})
	if err != nil {
		return "", fmt.Errorf("payload release token: %w", err)
	}
	return token, nil
}

// ReleaseSeqTermShift is the bit position of the Raft term in the Arbiter
// leader's release_seq: release_seq = term << 32 | counter, counter ≥ 1 and
// restarting per term (informative convention in da.proto). A later leader
// has a larger term, so it always signs above every earlier leader.
const ReleaseSeqTermShift = 32

// PackReleaseSeq returns term << 32 | counter. term must be in [1, 2^32) and
// counter in [1, 2^32).
func PackReleaseSeq(term, counter uint64) (uint64, error) {
	if term == 0 || term >= 1<<ReleaseSeqTermShift {
		return 0, fmt.Errorf("release seq: term %d out of range", term)
	}
	if counter == 0 || counter >= 1<<ReleaseSeqTermShift {
		return 0, fmt.Errorf("release seq: counter %d out of range", counter)
	}
	return term<<ReleaseSeqTermShift | counter, nil
}
