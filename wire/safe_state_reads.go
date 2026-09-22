package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/housegate/housegate/pkg/replay"
)

const (
	// SafeStateReadProofVersion is the version of both SafeState read proofs.
	SafeStateReadProofVersion uint32 = 1
	// PublishedSnapshotReplyDomain and QueryPolicyReplyDomain are the canonical
	// digest domains of the two authenticated SafeState reads. Like
	// artifact-disposition-reply-v1 the root is CanonicalDigest over the reply
	// body and excludes the proof; the proof is integrity and correlation
	// evidence for a leader-barrier read, not a signature.
	PublishedSnapshotReplyDomain = "published-snapshot-reply-v1"
	QueryPolicyReplyDomain       = "query-policy-reply-v1"
)

// PublishedSnapshotReplyBodyV1 is the committed state a leader captured under
// one FSM read lock for GetPublishedSnapshot. The records are present only when
// snapshot_id names a published candidate; activation is present only when
// that candidate's publication activated the policy. read_index is the
// artifact-disposition lane's last applied index at capture.
type PublishedSnapshotReplyBodyV1 struct {
	Version               uint32                                  `json:"version"`
	NetworkID             string                                  `json:"network_id"`
	KeeperShardID         uint32                                  `json:"keeper_shard_id"`
	SnapshotID            string                                  `json:"snapshot_id"`
	Found                 bool                                    `json:"found"`
	ReadIndex             uint64                                  `json:"read_index"`
	CandidateSeq          uint64                                  `json:"candidate_seq"`
	PublishedIndex        uint64                                  `json:"published_index"`
	Manifest              *replay.SafeSnapshotManifest            `json:"manifest,omitempty"`
	ArtifactReady         *replay.SnapshotArtifactReadySubmission `json:"artifact_ready,omitempty"`
	Activation            *replay.ActiveQueryPolicy               `json:"activation,omitempty"`
	ActivationCommitIndex uint64                                  `json:"activation_commit_index"`
}

// QueryPolicyReplyBodyV1 is the committed activation record for GetQueryPolicy.
// activation_kind is "publication" (installed by a candidate publication and
// bound to candidate_seq / commit_index / transition_root) or "authority"
// (installed by an authority-signed ActivateQueryProfile command).
type QueryPolicyReplyBodyV1 struct {
	Version        uint32                    `json:"version"`
	NetworkID      string                    `json:"network_id"`
	KeeperShardID  uint32                    `json:"keeper_shard_id"`
	ActivationID   string                    `json:"activation_id"`
	BlockSeq       uint64                    `json:"block_seq"`
	Found          bool                      `json:"found"`
	ReadIndex      uint64                    `json:"read_index"`
	ActivationKind string                    `json:"activation_kind"`
	CandidateSeq   uint64                    `json:"candidate_seq"`
	CommitIndex    uint64                    `json:"commit_index"`
	TransitionRoot string                    `json:"transition_root"`
	Activation     *replay.ActiveQueryPolicy `json:"activation,omitempty"`
}

type PublishedSnapshotReadProofV1 struct {
	Version   uint32                       `json:"version"`
	ReplyRoot string                       `json:"reply_root"`
	Body      PublishedSnapshotReplyBodyV1 `json:"body"`
}

type QueryPolicyReadProofV1 struct {
	Version   uint32                 `json:"version"`
	ReplyRoot string                 `json:"reply_root"`
	Body      QueryPolicyReplyBodyV1 `json:"body"`
}

func PublishedSnapshotReplyRoot(body PublishedSnapshotReplyBodyV1) (string, error) {
	return replay.CanonicalDigest(PublishedSnapshotReplyDomain, body)
}

func QueryPolicyReplyRoot(body QueryPolicyReplyBodyV1) (string, error) {
	return replay.CanonicalDigest(QueryPolicyReplyDomain, body)
}

func EncodePublishedSnapshotReadProof(body PublishedSnapshotReplyBodyV1) ([]byte, error) {
	root, err := PublishedSnapshotReplyRoot(body)
	if err != nil {
		return nil, err
	}
	return json.Marshal(PublishedSnapshotReadProofV1{Version: SafeStateReadProofVersion, ReplyRoot: root, Body: body})
}

// DecodePublishedSnapshotReadProof parses a proof strictly (no unknown fields,
// no trailing data) and re-derives the reply root before returning it.
func DecodePublishedSnapshotReadProof(proof []byte) (PublishedSnapshotReadProofV1, error) {
	var p PublishedSnapshotReadProofV1
	if err := decodeStrictJSON(proof, &p); err != nil {
		return PublishedSnapshotReadProofV1{}, fmt.Errorf("published snapshot read proof: %w", err)
	}
	if p.Version != SafeStateReadProofVersion {
		return PublishedSnapshotReadProofV1{}, fmt.Errorf("published snapshot read proof: version %d is unsupported", p.Version)
	}
	root, err := PublishedSnapshotReplyRoot(p.Body)
	if err != nil || root != p.ReplyRoot {
		return PublishedSnapshotReadProofV1{}, errors.New("published snapshot read proof: reply root mismatch")
	}
	return p, nil
}

func EncodeQueryPolicyReadProof(body QueryPolicyReplyBodyV1) ([]byte, error) {
	root, err := QueryPolicyReplyRoot(body)
	if err != nil {
		return nil, err
	}
	return json.Marshal(QueryPolicyReadProofV1{Version: SafeStateReadProofVersion, ReplyRoot: root, Body: body})
}

func DecodeQueryPolicyReadProof(proof []byte) (QueryPolicyReadProofV1, error) {
	var p QueryPolicyReadProofV1
	if err := decodeStrictJSON(proof, &p); err != nil {
		return QueryPolicyReadProofV1{}, fmt.Errorf("query policy read proof: %w", err)
	}
	if p.Version != SafeStateReadProofVersion {
		return QueryPolicyReadProofV1{}, fmt.Errorf("query policy read proof: version %d is unsupported", p.Version)
	}
	root, err := QueryPolicyReplyRoot(p.Body)
	if err != nil || root != p.ReplyRoot {
		return QueryPolicyReadProofV1{}, errors.New("query policy read proof: reply root mismatch")
	}
	return p, nil
}

func decodeStrictJSON(data []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if dec.More() {
		return errors.New("trailing data")
	}
	return nil
}
