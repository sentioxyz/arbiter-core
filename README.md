# arbiter-core

Public Go types and runtime modules shared by Sentio Arbiter control-plane
nodes and storage nodes.

The module path is `github.com/sentioxyz/arbiter-core`; its root Go package is
named `arbiter` so domain types retain concise names such as
`arbiter.StatementEnvelope`.

## Packages

| Package | Purpose |
|---|---|
| root | Canonical statement, result-claim, promotion, cleanup, consensus-update, and node types. |
| `authority` | Domain-separated promotion, cleanup, consensus-update and snapshot-query-abort signing and validation. |
| `wire` | The canonical Go ↔ `arbiter-proto` conversion and Raft command encoding. |
| `dataplane` | Leader-aware Arbiter clients, subscriptions, manifests, payload stores, the table-registry follower, and purge reports. |
| `dataplane/tableset` | The level-triggered table-set reconciler: creates, verifies, and purges the `hg_*` tables the table registry asks for. |
| `dataplane/chtest` | The credentialed ClickHouse opener of every ClickHouse-gated test (`CH_USER`, `CH_PASSWORD`). |
| `dataplane/chrestart` | ClickHouse restart detection from the server start time (`uptime()`), for the `hg_safe` audits. |
| `dataplane/chmechanics` | Test-only ClickHouse mechanics suite: pins the engine behaviour the protocol relies on (§ ClickHouse mechanics). |
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
    version = "0.21.0",
)

git_override(
    module_name = "arbiter_core",
    commit = "<arbiter-core commit>",
    remote = "https://github.com/sentioxyz/arbiter-core",
)
git_override(
    module_name = "housegate",
    # Resolved Housegate v0.21.0; source is pinned by the commit below.
    commit = "c77fd4d2d9f8a78032c1350ef322183ffc1fb641",
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

Storage-integrity protocol-table construction validates every declared column type against the storage-integrity whitelist before emitting any DDL. A declaration outside that profile fails role construction, so an invalid schema cannot create a partial set of protocol tables.

The protocol-table mode is derived from `schema_source`: `network_state` and `chain` create and reconcile the tables, while `clickhouse` verifies an existing deployment without creating it. There is no separately configurable mode that can disagree with the authoritative schema source.

`hg_promote` is created and verified together with `hg_unsafe` and `hg_safe`. A verify-only node whose protocol tables were never bootstrapped in create mode, or whose `hg_promote` table has drifted, now fails at startup instead of discovering the problem during the first promotion.

SNode promotion reaches ClickHouse only through its TCP connection and needs no access to ClickHouse's filesystem, so the SNode may run in a different pod or on a different node. When a promotion's candidates are a strict subset of the active parts in the `hg_unsafe` partition, the SNode attaches the whole partition into `hg_promote`, matches the attached parts against the candidates by row LtHash (as a multiset), and drops every part that is not a candidate before the shadow closure gate and `REPLACE PARTITION`. An unmatched candidate or a scan failure aborts the promotion before `REPLACE`, leaving `hg_safe` untouched, and the command is redelivered. On both attach paths a promotion whose published partition would hold two parts with the same row LtHash (duplicate candidates, or a candidate already in the safe base) is refused before `REPLACE`; honest operation cannot produce one, because every row's `_hg_row_id` is unique to its statement and ordinal.

ClickHouse-backed SNode tests are opt-in. The full DDL acceptance uses two
ClickHouse 25.8 nodes sharing one Keeper:

```bash
docker network create arbiter-core-ch
docker run -d --rm --name arbiter-core-ch-a --hostname arbiter-core-ch-a \
  --network arbiter-core-ch --network-alias arbiter-core-clickhouse-a \
  -p 9000:9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-shared-keeper-server.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  clickhouse/clickhouse-server:25.8
docker run -d --rm --name arbiter-core-ch-b --hostname arbiter-core-ch-b \
  --network arbiter-core-ch \
  -p 9001:9000 \
  -e CLICKHOUSE_SKIP_USER_SETUP=1 \
  -v "$PWD/scripts/ci/clickhouse-shared-keeper-client.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  clickhouse/clickhouse-server:25.8

ARBITER_CH_INTEGRATION=1 \
  ARBITER_CH_KEEPER=1 \
  ARBITER_CH_REPLICA=1 \
  CH_ADDR=127.0.0.1:9000 \
  CH_REPLICA_ADDR=127.0.0.1:9001 \
  bazel test //dataplane/ddl:ddl_test //snode:snode_test //verifier:verifier_test \
    --test_env=ARBITER_CH_INTEGRATION \
    --test_env=ARBITER_CH_KEEPER \
    --test_env=ARBITER_CH_REPLICA \
    --test_env=CH_ADDR \
    --test_env=CH_REPLICA_ADDR \
    --test_timeout=900
```

Every ClickHouse-gated test connects through `dataplane/chtest`. Against a ClickHouse whose user has a password, set `CH_USER` and `CH_PASSWORD` and add `--test_env=CH_USER --test_env=CH_PASSWORD`; with neither set the tests connect as the passwordless `default` user, as in the recipe above. CI's `integration-clickhouse` job gives `default` a password on both nodes (`scripts/ci/clickhouse-default-password.xml`), so a test opener that bypasses `chtest` fails there; Cut Release keeps the passwordless pair.

### ClickHouse mechanics

`dataplane/chmechanics` is a test-only suite that pins, on a real ClickHouse pair, the engine behaviour the storage-integrity protocol relies on (housegate spec 2026-10-14 §2): `REPLACE PARTITION` and `ATTACH PARTITION FROM` keep `hash_of_all_files`, and the latter is not throttled by `parts_to_throw_insert`; `old_parts_lifetime` is 480 s, the protocol DDL keeps it, and a restart inside it, abrupt or graceful, resurrects the parts a `REPLACE PARTITION` retired, while `DROP PARTITION` followed by `ATTACH PARTITION FROM` does not; `DROP PART` (or `DETACH PART` and `DROP DETACHED PART`) of each resurrected twin leaves exactly the replacements and stays dropped; partition backups with `base_backup` are incremental and restore every part's `hash_of_all_files`; a pull from `remote()` with the no-split settings writes one part; `READ ON REMOTE` refuses writes; a `NOT OVERRIDABLE` named collection refuses overrides; an `OPTIMIZE` on one replica merges another replica's parts; and dropping a table's last replica removes its whole Keeper subtree. Run it with:

```bash
bash scripts/ci/clickhouse-mechanics.sh clickhouse/clickhouse-server:26.3.37.3 26.3.37.3
```

The script starts two servers of the image on one Docker network (node A hosts the shared Keeper; node B runs with `--restart always` on a fixed host port, so the suite restarts it with `SYSTEM KILL` and `SYSTEM SHUTDOWN`), mounts `scripts/ci/clickhouse-mechanics-backups.xml` and `scripts/ci/clickhouse-mechanics-users.xml`, runs `//dataplane/chmechanics:chmechanics_test` and then, against node B, the SNode's owner-audit restart test (`//snode:snode_test`, `TestOwnerAuditRepairsResurrectedPartsAcrossAClickHouseRestart`), fails if any of these tests is skipped or does not pass, and removes the containers. The second argument is the version prefix every server must report (M0). CI's `clickhouse-mechanics` job and Cut Release run it exactly as above, on the upstream patch image `clickhouse/clickhouse-server:26.3.37.3` with its exact version as the prefix, never on the floating `26.3` tag, so a gate result changes only with a commit; moving to another patch changes the image and the prefix in `.github/workflows/ci.yml`, `.github/workflows/cut-release.yml` and this command together. The suite is opt-in (`ARBITER_CH_MECHANICS=1`); once opted in, a missing `ARBITER_CH_INTEGRATION=1`, `ARBITER_CH_KEEPER=1`, `CH_ADDR`, `CH_REPLICA_ADDR`, `CH_PEER_ADDR`, `ARBITER_CH_EXPECT_VERSION_PREFIX` or `ARBITER_CH_IMAGE` fails it, and the restart tests also need `ARBITER_CH_RESTART=1` and `CH_RESTART_ADDR`. `ARBITER_CH_MECHANICS_FILTER` limits a run to the tests it matches; `ARBITER_CH_PLATFORM` (for example `linux/amd64`) selects the image platform.

The storage-integrity ClickHouses run a private Sentio build of 26.3 (the image the production `charts/storage-integrity` chart deploys). CI cannot pull it, so a maintainer with read access to that registry runs the same script by hand with that image and the prefix `26.3.` (`bash scripts/ci/clickhouse-mechanics.sh <the chart's image> 26.3.`, with `ARBITER_CH_PLATFORM=linux/amd64` on a host of another architecture), after the release's CI run and before any storage-integrity rollout of the release, and records the date, the version M0 logs and the result with the release. Running it in CI needs a read-only registry secret for this repository.

## Releases

Run the **Cut Release** workflow from `main`. It validates the Bazel module, the ClickHouse-backed SNode and verifier paths and the ClickHouse mechanics suite on upstream ClickHouse 26.3.37.3 (§ ClickHouse mechanics), then creates an annotated tag and GitHub Release.
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

Consensus-parameter updates use the canonical `arbiter.ConsensusParamsUpdate`
and Raft command slot 18. `(*authority.Validator).VerifyConsensusParamsUpdate`
supports deterministic replay without clock checks; use
`AuthorizeConsensusParamsUpdate` only at live API boundaries. Both fail closed
for an empty authority allowlist. See the [protocol specification](docs/specs/2026-09-17-consensus-parameter-updates.md)
for signed preconditions, address normalization, and the all-voters upgrade gate.

Client sequence lanes add `StatementID.ClientLane`, `ConsensusParamsUpdate.ClientLanes` and the request-only `NodeRegistration.features`. Legacy statement ids, consensus-update digests and Raft command bytes are unchanged when no lane is in use, pinned by `wire/legacy_command_golden_test.go` and `authority/consensus_golden_test.go`. `NodeRegistration.features` is never copied into a Raft command or replicated state, and `wire.Decode` refuses a `RegisterNode` command that carries it. SNode and verifier registrations advertise `client_lanes_v1`.

Multi-source storage integrity, stage 1 (housegate spec 2026-10-10), adds `ConsensusParamsUpdate.SIIndexers` / `Verifiers`, `NodeRegistration.RegistrationSeq`, the table registry's owner fields (`AddTable.OwnerIndexerID`, `SeedLegacyTables.IndexerID`, incarnation `OwnerIndexerID`, snapshot `si_indexers` / `seeded_indexers`), `AdmissionCodeSourceUnavailable` and the signature fields of `RegisterRC`, `RecordPromotionAck`, `RecordCleanupAck`, `RegisterNode`, `MarkActive`, `EvictNode` and `RecordTablePurged`. All are absent before the signed-claims activation, so earlier digests and Raft command bytes are unchanged (pinned by `wire/signed_claims_golden_test.go` and `authority/consensus_golden_test.go`); `wire.StripSignedClaims` is the pre-activation proposal filter, and `wire.Decode` refuses a signature in a request-only copy. `authority` signs and verifies SNode enrolment statements and messages (ES256K, purposes `arbiter-snode-enrollment-v1` / `arbiter-snode-message-v1`), verifier messages (ed25519) and authority evictions (`arbiter-evict-node-v1`); `authority/authoritytest` pins their vectors. An SNode given `Deps.ClaimSigner` signs what it sends, keeps a durable `registration_seq`, advertises `signed_claims_v1` and serves only `Config.IndexerID`'s tables; without one it is unchanged. Verifiers always sign with their evidence key and keep `registration_seq` in `Config.StateDir`.

Multi-source storage integrity, stage 2 (housegate spec 2026-10-10 §7), adds `ConsensusParamsUpdate.TracksEnabled`, `UnsafeCleanup.RejectedBlockSeq` and `CleanupAck.RejectedBlockSeq`, `AdmissionCodeIndexerQuarantined`, the authority-signed `QuarantineIndexer` and `ReleaseIndexer` commands (Raft slots 36 and 37; `wire.QuarantineIndexer` / `wire.ReleaseIndexer` with their request converters), `TableRegistrySnapshot.QuarantinedIndexers`, and the safe-snapshot manifest's `track_frontiers` from housegate `pkg/replay`, which `wire.ManifestFromPB` / `ManifestToPB` carry and the C1 artifact-disposition command root omits. All are absent before the tracks activation, so earlier digests and Raft command bytes are unchanged (pinned by the earlier goldens and by `wire/tracks_golden_test.go` and `authority/tracks_golden_test.go`). `authority` signs and verifies quarantine and release tokens (purposes `arbiter-quarantine-indexer-v1` / `arbiter-release-indexer-v1`) bound to the network, genesis snapshot and authority epoch like evictions; `authority/authoritytest` pins their vectors. Every binary built from this release advertises `track_manifests_v1` (`arbiter.LocalNodeFeatures`): a verifier that drops `track_frontiers` fails the manifest root of every track-aware base manifest and an older SNode refuses a rejected-part cleanup, so every voter and data-plane node must run this release before the tracks activation. The SNode echoes `rejected_block_seq` in the cleanup acknowledgement it signs. SNodes authorize promotions and cleanups against their configured `AuthorityAddresses` with no epoch, so work that a quarantine suspended and that a release re-dispatches after an authority rotation is accepted only if every SNode already lists the new authority.

An SNode records each cleanup acknowledgement it owes the arbiter in the same `state.json` write that journals the executed cleanup (`owed_cleanup_acks`, omitted while nothing is owed: a v0.14.0 state file loads unchanged and a v0.14.0 binary ignores the field), clears it only once `AckCleanup` succeeds, and re-sends it in the background from startup on: one pass at startup, then one pass `Config.CleanupAckResendInterval` (default 10s, at least 1s) after the previous pass ended, each pass bounded by that interval, so while the arbiter is unreachable an owed acknowledgement is retried about every two intervals. While one is owed, the purge gate and `ForgetTable` hold its table, so the SNode never reports a table purged while it owes an acknowledgement for it. Downgrading to v0.14.0 forgets the owed entries at its next state write; after a re-upgrade the arbiter re-delivers any cleanup it still holds. An entry the arbiter keeps refusing (a warn line every pass) holds that table's purge; to recover, stop the SNode and remove the entry from `state.json`, only after confirming at the arbiter that the cleanup is no longer pending. A cleanup whose `DROP PART` meets `UNKNOWN_TABLE` counts as dropped, so an SNode that purged a table under an earlier release acknowledges the redelivered cleanup and recovers. The acknowledgement's bytes and signature are unchanged.

The owner SNode audits its `hg_safe` (housegate spec 2026-10-14 §14): before building each promotion's shadow, at startup and after every ClickHouse restart (`dataplane/chrestart`, from `uptime()`), it compares each partition's active parts, by name and `hash_of_all_files`, with the newer of its own last acknowledged inventory and the arbiter's `SafeParts` (`GetPartitionStates`). The local inventory is `state.json`'s `acked_safe_inventories`, omitted while empty: a v0.14.1 state derives it from `last_acks` and a v0.14.1 binary ignores it (a downgrade to v0.14.1 forgets it at its next state write; after a re-upgrade it is derived again from an applied `last_acks` entry, else the arbiter's inventory is the reference). `Config.OwnerAudit` is `report` (the default) or `repair`. Report mode logs each unclean partition at error level and counts it in `OwnerAuditStats` but never drops a part and never holds a table. Repair mode drops, with `ALTER TABLE ... DROP PART`, each equal-hash twin whose name the reference does not carry when another member of its group carries one and dropping the twins leaves exactly the reference; otherwise it drops nothing and holds the partition: `TableReady` and `OwnerAuditClean` are false for its table (the host answers it Pending) and its promotions fail with `ErrSafePartitionUnclean` before the shadow until an audit finds it clean (repair mode re-audits each unclean partition every `Config.OwnerAuditProbeInterval`); a table is also held until its first audit after startup or a restart completes. An arbiter that does not serve `GetPartitionStates`, or does not answer within `Config.OwnerAuditFSMTimeout` (default 5s), leaves the audit on the local inventory and counts `FSMUnavailable` (after one failed read a full pass asks no more for its other tables); the arbiter past the SNode's own promotion watermark is `fsm_ahead` and drops nothing. A partition with an unresolved promotion intent is skipped and reported `unresolved_intent`. The restart probe runs every `Config.OwnerAuditProbeInterval` (default 5s, at least 1s); the SNode's ClickHouse address must reach a single server.

Multi-source storage integrity, stage 3 P0 (housegate spec 2026-10-14 §13.1, §14, §15), changes no consensus format: no Raft command, snapshot version or feature string is added and `arbiter.LocalNodeFeatures()` is unchanged, so every binary built from this release can be rolled back to the previous one (the SNode's `acked_safe_inventories` on a downgrade and re-upgrade behaves as the paragraph above says). The root package adds `SafeWatermark`, the watermark a `PartitionStates` answer carries; `wire` adds the canonical `PartitionStates` and `PromotionOutcomes` read types with their converters and exports `PartManifestEntryToPB` / `PartManifestEntryFromPB`; `dataplane.Client` gains `GetPartitionStates` and `GetPromotionOutcomes`, which answer `ErrStage3ReadsUnavailable` from an arbiter that predates them; the owner `hg_safe` audit of the paragraph above reads the first. ClickHouse-gated tests open their connections through `dataplane/chtest`, which takes a user and password from `CH_USER` / `CH_PASSWORD` and keeps the passwordless `default` when they are unset. The ClickHouse mechanics suite (§ ClickHouse mechanics) pins the engine behaviour the audit and stage 3 rely on, including the restart resurrection the audit repairs (M3) and the `DROP PART` that repair mode issues (M11); run it on an image before any storage-integrity ClickHouse moves to it.
