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
| `conformance` | Field and enum compatibility gates against `arbiter-proto` and Housegate replay types. |

The private `github.com/sentioxyz/arbiter` repository owns the Raft FSM,
orchestrator, servers, anchoring backend, and operator binaries. It consumes
this module; this module never imports the private control-plane repository.

## Build and test

```bash
go build ./...
go vet ./...
go test ./...
```

ClickHouse-backed SNode tests are opt-in:

```bash
docker run -d --rm --name arbiter-core-ch \
  -p 9000:9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  clickhouse/clickhouse-server:25.8

ARBITER_CH_INTEGRATION=1 \
  CH_ADDR=127.0.0.1:9000 \
  go test ./snode -count=1 -timeout=900s
```

## Releases

Run the **Cut Release** workflow from `main`. It validates the Go module and
ClickHouse-backed SNode path, then creates an annotated tag and GitHub Release.
Versions follow the same UTC calendar scheme as Arbiter:

- the first cut is `v0.0.0`;
- another cut on the same UTC day increments patch;
- the first cut on a later UTC day increments minor and resets patch to zero.

Tags are the version ledger; no version file is maintained in the repository.

Housegate currently declares the module path `housegate/housegate`. Go
consumers of packages that reach Housegate replay types must carry the same
replacement used here until Housegate publishes a canonical GitHub module
path:

```go
replace housegate/housegate => github.com/housegate/housegate <version>
```

## Compatibility

`arbiter-proto` is the language-neutral wire contract. During the `v0.x`
series, minor releases of this module may evolve its Go interface; protocol
field numbering and canonical signing forms remain guarded by conformance
tests.
