# Canonical consensus parameter update contract

Tracking: [Arbiter #39](https://github.com/sentioxyz/arbiter/issues/39).
The runtime policy and migration specification is maintained in Arbiter at
`docs/specs/2026-09-17-consensus-parameter-updates.md`. The transport definition
is arbiter-proto `proto/consensus.proto` and Raft command slot 18.

`arbiter.ConsensusParamsUpdate` is the signing form. It binds network ID,
genesis snapshot ID, expected parameter epoch, previous full-parameter digest,
the complete target authority set, target MaxWriters, and expected promotion
sequence. Identity strings must be non-empty; target MaxWriters must be
positive. Target authorities must be non-empty, non-zero 20-byte hex addresses
with a `0x` prefix. Hashing copies, lowercases, sorts and deduplicates the set.
No caller-owned slice is mutated.

The JWS purpose is `arbiter-consensus-params-update-v1`; its command hash is
`CanonicalDigest("arbiter-consensus-params-update-command-v1", normalizedUpdate)`.
Both constants are versioned protocol values. Promotion and cleanup tokens
cannot authorize an update, and update tokens cannot authorize either existing
command family. Protobuf bytes remain transport only.

`Validator.VerifyConsensusParamsUpdate` is deterministic: it verifies command
structure, purpose, signature and the supplied authority allowlist without
reading wall time. Raft Apply and snapshot replay use this entry point. The
FSM additionally validates identity, epoch, previous digest, promotion-sequence
boundary, effective change, writer membership capacity and drained authority
work. The allowlist must be the authority set that applies before the update.

`AuthorizeConsensusParamsUpdate` additionally enforces positive MaxTokenAge
and issue-time bounds for API use; it is not suitable for deterministic replay.
`SignConsensusParamsUpdateAt` exposes the explicit Unix issue time for durable
audit fixtures. The signed CAS fields, rather than token age, prevent replay.

Promotion and cleanup audit tokens can additionally bind a `ConsensusContext`
with network ID, genesis snapshot ID and authority epoch. The new
`SignPromotionWithContext` / `SignCleanupWithContext` methods retain the existing
purpose and command hash, so SNode validators still authenticate the complete
JWS. Context fields are additional signed payload claims; the FSM verifies
their identity and epoch against history. Cleanup may refer to an earlier
promotion sequence, so that sequence alone does not identify its authority
epoch. The pointer-valued `JWSCommandPayload.AuthorityEpoch` preserves explicit
epoch zero and distinguishes it from a legacy token with no context. Original
signing methods retain their exact three-field payload for legacy callers.

`wire.Command.UpdateConsensusParams` encodes through the normal protobuf Raft
oneof and preserves all signed fields and the token. Address normalization is
owned by the authority hash, not by transport conversion. The admin protocol
version is 1. All voters must support it before enabling mutable updates.
