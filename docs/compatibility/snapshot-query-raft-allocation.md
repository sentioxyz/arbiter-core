# Snapshot-query transport and merged consensus compatibility

The Raft command union retains main commands 1–18, with
`UpdateConsensusParams` at 18. Snapshot commands use the explicit allocation
`BeginSnapshotQuery=30`, followed by unchanged 19–27 for Grant through
RecordArtifactReady. Tag 28 belongs to the separately adopted disposition
contract but is not implemented here; tag 29 is not adopted or allocated here.
`PromotionAck.safe_partition_parts=9` retains the complete active partition
inventory, including previously safe parts; `parts` remains candidate-only.

Begin30 is an incompatible correction to the pre-integration Begin18 contract.
Field 18 now has only the main consensus meaning. There is no alias,
payload-shape guess, automatic import, replay migration, or archive rewriting.
Known old Begin18 or unknown-origin archives must retain their original bytes
and provenance pending a separately authorized version/provenance-aware
disposition. A protobuf decoder cannot determine the historical producer of
ambiguous field18 bytes. Task-local absence of admission implementation does
not establish absence of external consumers or archives.

The outer `wire.Command.UpdateConsensusParams` alone gains `omitempty`.
This preserves the older nil-command JSON golden but deliberately changes
main's newly added nil-field rendering. Nonnil consensus JSON, typed consensus
signing fields/domains and protobuf encoding are unchanged. Original command
field order, v2/JWS/canonical/receipt vectors and legacy nil-array semantics
remain intact. External nil-JSON consumers remain unknown.

Encode requires exactly one command. Decode retains the descriptor-driven
precheck rejecting unknown fields, mixed variants, duplicate singular fields,
and incorrect wire types before protobuf can discard their presence. Repeated
scalars remain supported. These transport checks do not establish admission or
execution validity. Snapshot-query remains default-off; this union authorizes
no production activation and closes no C1, B1, C3, or full-feature acceptance
gate.

## artifact-disposition-command-v1 canonical change (pre-release)

2026-09-20 (PR: this branch). The canonical command DTO no longer carries part storage locations. `canonicalPartManifestEntry` dropped its `storage_refs` member, so the field leaves the canonical JSON and the root of every command that projects parts: the `RegisterCandidate` and `PublishCandidate` manifests, and the `OpenChallenge` attestation receipt's affected parts. Design D5 treats `StorageRefs` as fetch hints rather than identity, and acceptance A5 requires a changed location serving the same authenticated bytes to stay usable; binding locations into the root made relocating an artifact change the identity of an otherwise identical registration. Transport is unchanged: `replay.PartManifestEntry` still carries `StorageRefs` across protobuf and JSON, and `replay.SnapshotQueryReceipt.Hash()` already excluded them, so the command projection now agrees with the receipt commitment instead of contradicting it.

This is a one-time re-freeze of a pre-release vector, not a migration. The `TestArtifactDispositionManifestCanonicalJSONAndRootGolden` golden moved from `0x9aef7ff31b70484db4c35a288ea58acad926e1fe9e380fedc8a7810a6de19f81` to `0x1741594d3e441ad9718be1fbd0928775614978b0aff9226a14f4e610d9de4aca`. It is allowed only because nothing consumes `artifact-disposition-command-v1` in production yet: the Arbiter disposition `Capability` has no setter, so no command path is wired and no stored administrator JWS commits to the old root. A later change to this domain needs a version bump rather than another re-freeze. No other signing vector moves: the v2 statement, `clickhouse-native-data-v1`, `safe-snapshot-data-v2`, `replay-statement-root`, `replay-execution-receipt` and `housegate-row-id-v1` domains are untouched, and `arbiter-consensus-params-update-command-v1` keeps its own golden.

`rejectNilSlices` still walks the raw command, so `StorageRefs` must remain non-nil for a command root to be computed even though it no longer reaches the root. That residue is deliberate for now: relaxing it is a separate change to the nil-array contract, not part of this one.
