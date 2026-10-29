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
| `dataplane/chparts` | Active parts, logical partition ids, partition quoting, `DROP PARTITION` and partition content scans of one table, through ClickHouse statements only; shared by the SNode and the stage-3 restore-and-gate library. |
| `dataplane/objstore` | Storage-integrity backups in an S3-compatible bucket through ClickHouse statements only (`BACKUP` / `RESTORE ... S3()`, `s3()`; no S3 client): owner prefixes and backup names, the backup index object, the chained full and incremental `Backuper`, `RestoreAndGate` against a published manifest, and the restore `Drill`. |
| `snode` | Durable storage-node intake, crash convergence, promotion, and cleanup runtime. |
| `snode/rebuild` | The same-identity owner rebuild of a lost source ClickHouse: Keeper fence, protocol tables, `hg_safe` restored to the arbiter's `SafeParts` through the restore gate, and the rebuilt journal entries. |
| `verifier` | Replay and byte-side verification runtime shared by storage-node hosts. |
| `member/gate` | The pure stage-3 install gate (housegate spec 2026-10-14 §8.5): the partition plan against a manifest, the scratch classifier, the arithmetic closure and the partition-set check. |
| `member/manifestfeed` | The si-member's verifying manifest feed: the safe watermark followed along `ParentSnapshotID`, demultiplexed per owner, with a bounded LRU manifest store. |
| `member/copyset` | The si-member's copy reconciler: one incarnation-scoped `hg_replica` copy of every Pending or Active table of every peer owner. |
| `member/sources` | The install gate's byte sources (owner pull, peer unsafe pull, object-store restore) and the si-member's startup posture probes. |
| `member/install` | The install gate executor around `member/gate`: scratch tables, the per-owner journal, startup audit and self-audit, the member disk budget. |
| `member/shadow` | Off-consensus shadow verification of every sealed block of a peer owner. |
| `member` | The stage-3 si-member role: composes the packages above in its startup order; no consensus identity, read RPCs only (§ Member role (stage 3 P2)). |
| `member/serve` | The si-member's serving side: the serving conditions, the owner probe, serving leases that hold the installs of a served database, and the bearer-token endpoint with its client (§ Member serving (stage 3 P3)). |
| `member/confinement` | Test-only ClickHouse proof that the member users are confined (§ Member role (stage 3 P2)). |
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
    version = "0.23.0",
)

git_override(
    module_name = "arbiter_core",
    commit = "<arbiter-core commit>",
    remote = "https://github.com/sentioxyz/arbiter-core",
)
git_override(
    module_name = "housegate",
    # Resolved Housegate v0.23.0; source is pinned by the commit below.
    commit = "74a100b38348e3532cbdc3fafdd012069ef10fe6",
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

Since stage 3 P1 the script also starts a pinned MinIO (MinIO RELEASE.2024-12-18T13-15-44Z as Bitnami builds it, `bitnamilegacy/minio:2024.12.18-debian-12-r1` pinned by digest, because MinIO's own images can no longer be pulled; network alias `arbiter-core-minio`) with the bucket `si-mechanics` and three keys, fixed CI-only literals of that ephemeral container: `mech-nodelete` may read, write and list but delete nothing, `mech-lockdelete` may also delete objects named `*/.lock`, and `mech-readonly` may only read and list. It passes them to the tests as `ARBITER_CH_S3=1`, `CH_S3_ENDPOINT`, `CH_S3_BUCKET` and `CH_S3_{NODELETE,LOCKDELETE,READONLY}_{ACCESS,SECRET}_KEY`. M12 re-pins M5 on an S3 endpoint (an incremental `BACKUP DATABASE ... TO S3()` stores fewer objects than the full one, and `RESTORE TABLE ... AS ... PARTITIONS` from it keeps every `hash_of_all_files` and row); M13 runs `objstore.Backuper` as a user holding exactly the `si_backup` grants with each writing key, prints what a backup key must be allowed to delete (`mechanics: S3 backup delete right: none` when the key that cannot delete completes, `lock` when only the one that may delete `*/.lock` does), restores as a user holding exactly the `si_restore` grants and checks their refusals; M14 checks that no row of `system.query_log`, `system.processes` or `system.backups` carries a secret and that the statements show it as `[HIDDEN]`, and the script then greps both servers' log files for the three secrets (`mechanics: no S3 secret in the server logs on <image>`). Unfiltered runs also run `//dataplane/objstore:objstore_test` against the same MinIO and require every `TestS3_` test to pass (`objstore: <n> S3 test(s) passed on <image>`).

Since stage 3 P4, M15 (`TestMechanics_RestoreAndAttachIntoAFreshTableKeepsEveryPartHash`) pins the restore path of the owner rebuild (`snode/rebuild`): `RESTORE ... AS <scratch> PARTITIONS` from a disk backup and `ATTACH PARTITION ... FROM` the scratch into a fresh table keep every part's row LtHash and `hash_of_all_files`; the new part names are logged, not asserted. M16 (`TestMechanics_InterserverAllowEmptyAcceptsAnUncredentialedFetcher`, `TestMechanics_InterserverUncredentialedServerRefusesACredentialedFetcher`) pins the mixed state of a rolling interserver-credential change on two more servers the script starts beside the pair, `ch-iscred` with `interserver_http_credentials` (`scripts/ci/clickhouse-mechanics-interserver.xml`: user `si_interserver`, a fixed CI-only password, `allow_empty` true) and `ch-isplain` without: a part inserted on the credentialed server reaches the other one, while the other way the fetch is refused with HTTP 401 and the part does not arrive within 60 s; the script prints the refusal text the image answers (`M16 interserver refusal: ...`). M16 is a further opt-in, `ARBITER_CH_INTERSERVER=1` with `CH_ISCRED_ADDR` and `CH_ISPLAIN_ADDR`. In unfiltered runs the script also runs `//snode/rebuild:rebuild_test` with `ARBITER_CH_REBUILD=1` after the objstore tests (node A is the rebuilt server, holding the shared Keeper and the backups disk; node B holds the pool's and an old server's replicas) and fails unless every `Test` function of `snode/rebuild/rebuild_ch_test.go` passes (`rebuild: <n> test(s) passed on <image>`).

### Member role (stage 3 P2)

`member.Role` is the stage-3 si-member (housegate spec 2026-10-14 §8): a process separate from the SNode, run by `sentio-node si-member`, that keeps one incarnation-scoped copy in `hg_replica` of every SI table of every peer owner and fills it only with parts a verified manifest lists, through the install gate. It has a node id and no keys: it never registers, and it calls only `SafeState.GetSafeWatermark`, `GetManifest` and `GetBlockVerification`, `TableRegistry.GetTableRegistry`, `WatchTableRegistry` and `GetPurgeNodeSet`, and the DA's `PayloadStore.FetchPayloads`. At startup, in this order, it locks `<state dir>/LOCK`, refuses member databases that meet the host SNode's and a peer that is its host, creates `hg_replica` and `hg_rstage` without `ENGINE`, refuses to start unless ClickHouse refuses it an ungranted table engine (`table_engines_require_grant`) and refuses `INSERT INTO FUNCTION remote` through each of its named collections, refuses a node id an SNode or verifier uses, a peer that is not enrolled in `si_indexers` and genesis tables that differ from the genesis manifest, drops its own stale `hg_rstage` tables, runs the copy reconciler's first pass and audits the copies it made ready. It then runs the copy reconciler (`member/copyset`), the manifest feed (`member/manifestfeed`) into the installer (`member/install`, fed by `member/sources`; while an owner bootstraps, each copy's first part tries the object-store restore first), a copy watch that re-applies the newest update the follower offered when the copies change or an audit found something to repair, an audit after every ClickHouse restart and every self-audit interval and, when configured, the shadow verifier (`member/shadow`). `Role.Ready` is true once the startup audit is clean, every ready copy was audited since the start or the last restart, and every peer's copies reached a manifest; `Role.Stats` carries every series sentio-node exports, with zeros.

The member's ClickHouse users are confined by their grants, not by the role's code. `member/confinement` is a test-only suite that proves it on a real server with `access_control_improvements.table_engines_require_grant` (`scripts/ci/clickhouse-member-access-control.xml`): as `si_member`, every `Replicated*MergeTree` engine, `Buffer`, `Distributed`, `Merge` and `Alias`, a `Replicated` database, `ATTACH PARTITION … FROM`, `MOVE PARTITION … TO TABLE`, a materialized view or an `INSERT` into `hg_member_unsafe`, a write through `remote()` or to S3, a named collection override, `SYSTEM DROP REPLICA`, a dictionary, a view with another definer or with `SQL SECURITY NONE`, `SYSTEM STOP MERGES` and anything on `hg_safe`, `hg_unsafe` or `hg_promote` are refused, while every statement the role issues works; `si_member_ddl`'s settings profile refuses a replica created with merges allowed, and that user can neither write, alter nor optimize. Run it with:

```bash
bash scripts/ci/clickhouse-member-confinement.sh clickhouse/clickhouse-server:26.3.37.3 26.3.37.3
```

The script starts, on one Docker network, the fenced server (the file above with `scripts/ci/clickhouse-keeper.xml` and `scripts/ci/clickhouse-mechanics-users.xml`), an unfenced peer of the same image and MinIO with one bucket and a key generated for the run, runs `//member/confinement:confinement_test` and then the ClickHouse tests of `//member/sources:sources_test` against the same servers, fails if any of them is skipped or does not pass, prints what it measured (`confinement: …` lines) and `confinement: <n> test(s) passed on <image>`, and removes everything it started (`--cleanup` removes what an interrupted run left). The suite is opt-in (`ARBITER_CH_MEMBER_CONFINEMENT=1`); once opted in, a missing `CH_ADDR`, `CH_PEER_ADDR`, `CH_PEER_ADDR_FROM_SERVER`, `ARBITER_CH_EXPECT_VERSION_PREFIX`, `ARBITER_CH_IMAGE`, `S3_ENDPOINT`, `S3_BUCKET`, `S3_ACCESS_KEY_ID` or `S3_SECRET` fails it. `ARBITER_CH_MEMBER_CONFINEMENT_FILTER` limits a run to the tests it matches (and skips the `member/sources` run); `ARBITER_CH_PLATFORM` selects the ClickHouse image's platform. CI's `member-confinement` job and Cut Release run it exactly as above; the private storage-integrity build of 26.3 is run by hand with the prefix `26.3.`, like the mechanics suite (§ ClickHouse mechanics), before any si-member runs on it.

### Member serving (stage 3 P3)

`member/serve` lets a member's host serve a peer owner's database read-only from the member's copy while that owner is quarantined or unreachable (housegate spec 2026-10-14 §11). With `member.Config.Serve` set, `member.Role` runs a `serve.Server` over its install journal; without it the role is the P2 member, byte for byte. Every `PollInterval` (5 s) the server evaluates each database the member holds a copy of. A database of owner O serves only while all of these hold, and otherwise reports the first that fails as one reason of a closed set:

- serving is enabled (`serving_disabled`), and no install of the database is pending (`installing`);
- the latest published manifest is read from the arbiter in this poll and carries O's `track_frontiers` entry (`feed_unavailable`);
- O is quarantined in the table registry, or every TCP probe of O's HouseGate failed since a first failure at least `OwnerUnreachableAfter` ago (t_unreach: 60 s by default, at least 15 s; a probe every `OwnerProbeInterval`, 5 s, each bounded by 3 s) (`owner_available`);
- O's entry has been the same over two polls, counted again from zero after every install (`entry_unconfirmed`);
- the audits are clean (`audit_unclean`), the copy of every Active incarnation of the database is installed (`table_foreign`, `table_diverged`, `table_excluded`), the copy is at O's entry (`copy_behind`), and the copies are exactly the registry's Active incarnations of the database, all of O, with their seq, schema hash and `sitable.CopyTable` name (`table_missing`).

The host takes one lease per served database through the endpoint the si-member process mounts (`DefaultListen` `:9096`): `GET /v1/serving` (the `ServingView`), `POST /v1/serving/acquire` (`{"database","snapshot_id"}` at the view's snapshot), `POST /v1/serving/renew` and `POST /v1/serving/release` (`{"lease_id"}`), each with `Authorization: Bearer <token>` (401 otherwise); `serve.Client` is the host's side. A lease expires `LeaseTTL` (30 s, at least 10 s) after its grant or last renew, and a database that stops serving revokes it: renew then answers 409. The installer calls `BeforeInstall` before it changes any copy of a database, which stops serving, revokes the lease and waits until the host released it or it expired, and `AfterInstall` once the owner's journal records the pass, so a host never reads a copy while it changes. The server records every lease in `serve-leases.json` (`serve.LeaseFile`) in the member's state directory before it grants or renews one (500 if it cannot); a member that restarts restores the leases that have not expired, revoked, so its installs of their databases, the startup audit's included, wait for the host's release or the expiry as before the restart, and an unreadable record fails startup. The series `sentio_node_si_member_serve_databases{state}`, `sentio_node_si_member_serve_not_serving{reason}`, `sentio_node_si_member_serve_owner_unreachable{owner}`, `sentio_node_si_member_serve_lease_waits_total{outcome}` and `sentio_node_si_member_serve_revoking_seconds` are exported from startup, zeros included (`Role.ServeMetrics`).

`TestServeHoldsInstallsOfAServedDatabase` runs the lease protocol against ClickHouse with the other ClickHouse-gated tests (`ARBITER_CH_INTEGRATION=1`, `CH_ADDR`, target `//member/serve:serve_test`).

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

Multi-source storage integrity, stage 3 P1 (housegate spec 2026-10-14 §16 P1, §8.4–§8.6), changes no consensus format: no Raft command, snapshot version or feature string is added and `arbiter.LocalNodeFeatures()` is unchanged, so every binary built from this release can be rolled back to the previous one. `dataplane/chparts` holds the SNode's partition helpers (the SNode delegates to it with no behaviour change), `member/gate` is the pure install gate of spec §8.5, and `dataplane/objstore` holds the object-store backups and the restore-and-gate library. `objstore` reaches the bucket only through ClickHouse statements that name it inline with `S3(...)` / `s3(...)` and the key pair, so the backup and restore users need `READ` / `WRITE ON S3` and no server configuration changes; ClickHouse masks the secret in its system tables and logs (M14), and every error `objstore` returns has the secret removed. Each backup is a `BACKUP DATABASE hg_safe` under `owner-<indexer id>/` with an index object beside it that names the manifest published when it started, and is never overwritten; a restore drill gates every partition against exactly that manifest (spec §8.4's accept rule, §8.5's plan, classifier and closure).

Multi-source storage integrity, stage 3 P2 (housegate spec 2026-10-14 §8, §16 row P2), changes no consensus format: no Raft command, snapshot version or feature string is added and `arbiter.LocalNodeFeatures()` is unchanged, so every binary built from this release can be rolled back to the previous one. `dataplane.Client` gains `GetSafeWatermark`, `GetManifest` and `GetBlockVerification` (`ErrManifestNotFound`, `ErrBlockNotSealed`; `ErrStage3ReadsUnavailable` from an arbiter that predates the last), `wire` the canonical `BlockVerification` and `TableRegistrySnapshot.ManifestOwner`, `dataplane/ddl` the member intent set (`MemberPinned`, `CopyIntent`, `ScratchIntent` and `MemberUnsafeIntent`, which nothing calls yet) and `dataplane` the exported `IncarnationSchema`. The `member` packages are new and run only in the si-member process (§ Member role (stage 3 P2)); the SNode alone additionally ignores zero-row cleanup markers during fresh and recovered statement attribution (the shared metadata query and missing-nonempty-part refusal stay unchanged), and the member's ClickHouse users `si_member` and `si_member_ddl` exist only where an operator creates them.

Multi-source storage integrity, stage 3 P3 (housegate spec 2026-10-14 §11, §16 row P3), changes no consensus format either: no Raft command, field, snapshot version or feature string is added and `arbiter.LocalNodeFeatures()` is unchanged, so every binary built from this release can be rolled back to the previous one. `member/serve` is new and runs only in the si-member process; `install.Deps` gains `AfterInstall` beside `BeforeInstall`, and `member.Config` gains `Serve` (nil: the P2 member). The module now requires `github.com/prometheus/client_golang` directly, at the version the Housegate pin already selects.

Multi-source storage integrity, stage 3 P4 (housegate spec 2026-10-14 §14, §16 P4) changes no consensus format either: no Raft command, snapshot version or feature string is added and `arbiter.LocalNodeFeatures()` is unchanged. The root package adds `SettleTrackCommand`, the canonical form of an authority-signed settle of a quarantined track; `authority` signs and checks it under purpose `arbiter-settle-track-v1`, bound to the network, genesis snapshot and authority epoch (`AuthorizeSettleTrack`; there is no `Verify` variant, because no Apply reads it), pinned by `authority/settle_golden_test.go`; `wire` adds `SettleTrack` with its request converters and `SettlePromotionCommandPB`. Every promotion subscription now sends `SNodeHello.settle`, and the SNode answers a promotion streamed with `PromotionCommand.settle` from its journal only (its journaled acknowledgement, its published intent finished, or a journaled `Applied:false` with `settle: promotion <seq> was not applied on this SNode`), never applying it; `(*snode.Role).SettleStats` counts the answers by outcome. A leader older than this release ignores the hello field and never sends a settle, and a leader of this release sends one only to an SNode whose hello carried it. `state.json` gains `acked_safe_inventories[*].rebuilt`, omitted while false, so a state without a rebuild keeps the previous shape; `snode.RecordRebuiltSafeInventories` writes it for the same-identity owner rebuild (`snode/rebuild`), and the owner audit then takes the rebuilt inventory as the reference when it equals the arbiter's `SafeParts` by row LtHash and `hash_of_all_files`, until the partition's next applied promotion replaces it. An earlier release reads a rebuilt inventory as an ordinary one whose part names differ from the arbiter's, so its owner audit answers `fsm_disagrees` and, in repair mode, holds the table: never downgrade an SNode between a rebuild and each rebuilt partition's next applied promotion. `dataplane/ddl` adds `ReplicaIsActive` and `ScopedUnsafeRootChildren` for the rebuild's Keeper fence.
