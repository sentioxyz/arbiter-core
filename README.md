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

Bazel 9.1.0 with Bzlmod is the supported build and dependency contract.
`arbiter-core` consumes Housegate as a first-class Bazel module, so downstream
repositories must declare both modules and pin their source revisions:

```starlark
bazel_dep(name = "arbiter_core")
bazel_dep(
    name = "housegate",
    version = "0.8.1",
)

git_override(
    module_name = "arbiter_core",
    commit = "<arbiter-core commit>",
    remote = "https://github.com/sentioxyz/arbiter-core",
)
git_override(
    module_name = "housegate",
    # Resolved Housegate v0.8.1; source is pinned by the commit below.
    commit = "58fa6decadd4e86d9368853433b9640071f6b05b",
    remote = "https://github.com/housegate/housegate",
)
```

The `go.mod` file is retained as Gazelle's dependency manifest and for editor
metadata. Housegate now uses the canonical
`github.com/housegate/housegate` module path, so standard Go consumers no
longer need a downstream `replace` directive to resolve it.

The source `MODULE.bazel` intentionally leaves the arbiter-core module version
unset. Until arbiter-core is published in a Bazel registry, downstream
repositories select its exact source revision through `git_override`.

Update both the Go and Bzlmod Housegate pins from a release tag or commit SHA:

```bash
bash scripts/update-housegate.sh v0.7.1
bash scripts/update-housegate.sh 4dd088f4fe17d7bf13ba2c2e2311d72d0b97cd54
```

The script resolves the canonical Go version and full commit, updates
`go.mod`, `go.sum`, `MODULE.bazel`, this example, and the Bzlmod lockfile.

ClickHouse-backed SNode tests are opt-in:

```bash
docker run -d --rm --name arbiter-core-ch \
  -p 9000:9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  clickhouse/clickhouse-server:25.8

ARBITER_CH_INTEGRATION=1 \
  ARBITER_CH_KEEPER=1 \
  CH_ADDR=127.0.0.1:9000 \
  bazel test //dataplane/ddl:ddl_test //snode:snode_test //verifier:verifier_test \
    --test_env=ARBITER_CH_INTEGRATION \
    --test_env=ARBITER_CH_KEEPER \
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
