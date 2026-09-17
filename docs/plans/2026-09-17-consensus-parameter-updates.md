# Consensus update dependency implementation plan

1. Pin the canonical update and protobuf contract with the Arbiter runtime
   implementer; add no alternate Raft serialization.
2. Implement canonical address normalization, dedicated hash/purpose, signing,
   deterministic verification and optional API age checks. Add signed network,
   genesis and authority-epoch context to promotion/cleanup audit tokens through
   new signing methods, retaining legacy payload and SNode compatibility.
3. Extend the wire command union, converters and mirror conformance tests.
4. Test every signed-field mutation, cross-purpose and cross-domain rejection,
   malformed targets/tokens, empty or wrong authority sets, deterministic
   historical replay, and round trips through Raft command slot 18.
5. Run public-boundary, Go, Bazel build and race test gates. During development,
   use only an external local workspace for unreleased arbiter-proto. Commit a
   real released dependency after the coordinator merges/releases its PR.
6. Publish a ready PR for coordinator review/merge/release. Do not tag, deploy,
   rotate production authorities or modify live Raft storage in this task.
