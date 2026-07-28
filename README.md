# arbiter-core

Public Go types and runtime modules shared by Sentio Arbiter control-plane
nodes and storage nodes.

The module path is `github.com/sentioxyz/arbiter-core`; its root Go package is
named `arbiter` so domain types retain concise names such as
`arbiter.StatementEnvelope`.

## Packages

| Package | Purpose |
|---|---|
| root | Canonical statement, result-claim, promotion, cleanup, and node types. |
| `authority` | Domain-separated promotion and cleanup signing and validation. |
| `wire` | The canonical Go ↔ `arbiter-proto` conversion and Raft command encoding. |
| `dataplane` | Leader-aware Arbiter clients, subscriptions, manifests, and payload stores. |
| `snode` | Durable storage-node intake, crash convergence, promotion, and cleanup runtime. |
| `verifier` | Replay and byte-side verification runtime shared by storage-node hosts. |
| `conformance` | Field and enum compatibility gates against `arbiter-proto` and Housegate replay types. |

The private `github.com/sentioxyz/arbiter` repository owns the Raft FSM,
orchestrator, servers, anchoring backend, and operator binaries. It consumes
this module; this module never imports the private control-plane repository.

## Build and test

```bash
bazel build //...
bazel test //...
```

Bazel 8.5.1 with Bzlmod is the supported build and dependency contract.
`arbiter-core` consumes Housegate as a first-class Bazel module, so downstream
repositories must declare both modules and pin their source revisions:

```starlark
bazel_dep(name = "arbiter_core", version = "0.0.0")
bazel_dep(name = "housegate", version = "1.0.0")

git_override(
    module_name = "arbiter_core",
    commit = "<arbiter-core commit>",
    remote = "https://github.com/sentioxyz/arbiter-core",
)
git_override(
    module_name = "housegate",
    commit = "06936750928be7e487851d56fb6c862a19408c3f",
    remote = "https://github.com/housegate/housegate",
)
```

The `go.mod` file is retained as Gazelle's dependency manifest and for editor
metadata. Direct `go get github.com/sentioxyz/arbiter-core` is not a supported
installation path while Housegate keeps the non-network-resolvable module path
`housegate/housegate`.

ClickHouse-backed SNode tests are opt-in:

```bash
docker run -d --rm --name arbiter-core-ch \
  -p 9000:9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:25.8

ARBITER_CH_INTEGRATION=1 \
  CH_ADDR=127.0.0.1:9000 \
  bazel test //snode:snode_test //verifier:verifier_test \
    --test_env=ARBITER_CH_INTEGRATION \
    --test_env=CH_ADDR \
    --test_timeout=900
```

## Releases

Run the **Cut Release** workflow from `main`. It validates the Bazel module and
the ClickHouse-backed SNode and verifier paths, then creates an annotated tag
and GitHub Release.
Versions follow the same UTC calendar scheme as Arbiter:

- the first cut is `v0.0.0`;
- another cut on the same UTC day increments patch;
- the first cut on a later UTC day increments minor and resets patch to zero.

Tags are the version ledger; no version file is maintained in the repository.

## Compatibility

`arbiter-proto` is the language-neutral wire contract. During the `v0.x`
series, minor releases of this module may evolve its Go interface; protocol
field numbering and canonical signing forms remain guarded by conformance
tests.
