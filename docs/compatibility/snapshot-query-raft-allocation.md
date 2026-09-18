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
