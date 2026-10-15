#!/usr/bin/env bash
# Run the ClickHouse mechanics suite (//dataplane/chmechanics) on one image.
#
# Usage:
#   bash scripts/ci/clickhouse-mechanics.sh <image> <version prefix>
#   bash scripts/ci/clickhouse-mechanics.sh --cleanup
#
# Starts two servers of <image> on one Docker network. Node A hosts the shared
# Keeper (clickhouse-shared-keeper-server.xml). Node B is a Keeper client that
# runs with --restart always on a fixed host port, so the suite can restart it
# from SQL (SYSTEM KILL, SYSTEM SHUTDOWN) and reconnect. Both mount the backup
# disk and the default user's access-management settings. Then runs the suite
# and, against node B, the SNode's owner-audit restart test, and removes the
# containers and the network. Every TestMechanics_ function and the restart
# test must report PASS; a skipped test fails the run.
#
# Environment: ARBITER_CH_PLATFORM (for example linux/amd64) is passed to
# docker pull and docker run. ARBITER_CH_MECHANICS_FILTER (a Go regexp)
# limits the suite to the TestMechanics_ functions it matches and skips the
# SNode run.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

suffix="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}"
network="arbiter-core-mech-${suffix}"
ch_a="arbiter-core-mech-a-${suffix}"
ch_b="arbiter-core-mech-b-${suffix}"

cleanup() {
  docker rm -f "${ch_a}" "${ch_b}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
}

if [ "${1:-}" = "--cleanup" ]; then
  cleanup
  exit 0
fi
if [ "$#" -ne 2 ]; then
  echo "usage: $0 <image> <version prefix> | --cleanup" >&2
  exit 2
fi
image="$1"
version_prefix="$2"
platform_flag=""
if [ -n "${ARBITER_CH_PLATFORM:-}" ]; then
  platform_flag="--platform=${ARBITER_CH_PLATFORM}"
fi

on_exit() {
  status=$?
  if [ "${status}" -ne 0 ]; then
    # ClickHouse logs to the container's stderr, so both streams go to ours.
    docker logs --tail 100 "${ch_a}" >&2 2>&1 || true
    docker logs --tail 100 "${ch_b}" >&2 2>&1 || true
  fi
  cleanup
  exit "${status}"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cleanup
docker pull ${platform_flag:+"${platform_flag}"} "${image}" >/dev/null
docker network create "${network}" >/dev/null

common=(
  --network "${network}"
  -e CLICKHOUSE_SKIP_USER_SETUP=1
  -v "${repo_root}/scripts/ci/clickhouse-mechanics-backups.xml:/etc/clickhouse-server/config.d/backups.xml:ro"
  -v "${repo_root}/scripts/ci/clickhouse-mechanics-users.xml:/etc/clickhouse-server/users.d/mechanics.xml:ro"
)

docker run -d ${platform_flag:+"${platform_flag}"} \
  --name "${ch_a}" --hostname "${ch_a}" \
  --network-alias arbiter-core-clickhouse-a \
  -p 127.0.0.1::9000 \
  -v "${repo_root}/scripts/ci/clickhouse-shared-keeper-server.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  "${common[@]}" "${image}" >/dev/null
port_a="$(docker inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' "${ch_a}")"

# Node B is the server the suite restarts. Docker may move an ephemeral host
# port when it restarts a container, so B gets a fixed one, below the usual
# ephemeral range, retried when taken.
port_b=""
run_output=""
for _ in $(seq 1 20); do
  candidate=$((20000 + RANDOM % 10000))
  if run_output="$(docker run -d ${platform_flag:+"${platform_flag}"} --restart always \
    --name "${ch_b}" --hostname "${ch_b}" \
    --network-alias arbiter-core-clickhouse-b \
    -p "127.0.0.1:${candidate}:9000" \
    -v "${repo_root}/scripts/ci/clickhouse-shared-keeper-client.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
    "${common[@]}" "${image}" 2>&1)"; then
    port_b="${candidate}"
    break
  fi
  docker rm -f "${ch_b}" >/dev/null 2>&1 || true
done
if [ -z "${port_b}" ]; then
  echo "node B did not start on any of 20 host ports; the last attempt said: ${run_output}" >&2
  exit 1
fi

ready=false
for _ in $(seq 1 120); do
  if docker exec "${ch_a}" clickhouse-client --query "SELECT 1" >/dev/null 2>&1 &&
    docker exec "${ch_b}" clickhouse-client --query "SELECT 1" >/dev/null 2>&1 &&
    docker exec "${ch_b}" clickhouse-client --query "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [ "${ready}" != true ]; then
  echo "the ClickHouse pair did not become ready" >&2
  exit 1
fi

export ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1 ARBITER_CH_MECHANICS=1 ARBITER_CH_RESTART=1
export CH_ADDR="127.0.0.1:${port_a}"
export CH_REPLICA_ADDR="127.0.0.1:${port_b}"
export CH_RESTART_ADDR="127.0.0.1:${port_b}"
export CH_PEER_ADDR="arbiter-core-clickhouse-a:9000"
export ARBITER_CH_EXPECT_VERSION_PREFIX="${version_prefix}"
export ARBITER_CH_IMAGE="${image}"
env_flags=(
  --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=ARBITER_CH_REPLICA
  --test_env=ARBITER_CH_MECHANICS --test_env=ARBITER_CH_RESTART
  --test_env=CH_ADDR --test_env=CH_REPLICA_ADDR --test_env=CH_RESTART_ADDR --test_env=CH_PEER_ADDR
  --test_env=ARBITER_CH_EXPECT_VERSION_PREFIX --test_env=ARBITER_CH_IMAGE
)

required="$(grep -ho '^func TestMechanics_[A-Za-z0-9_]*' dataplane/chmechanics/*_test.go | sed 's/^func //' | sort -u || true)"
filter_flag=""
if [ -n "${ARBITER_CH_MECHANICS_FILTER:-}" ]; then
  required="$(printf '%s\n' "${required}" | grep -E -- "${ARBITER_CH_MECHANICS_FILTER}" || true)"
  filter_flag="--test_filter=${ARBITER_CH_MECHANICS_FILTER}"
fi
if [ -z "${required}" ]; then
  echo "no TestMechanics_ function to run" >&2
  exit 1
fi

bazel test //dataplane/chmechanics:chmechanics_test "${env_flags[@]}" ${filter_flag:+"${filter_flag}"} \
  --test_arg=-test.v --nocache_test_results --test_timeout=1800 --test_output=errors

log="$(bazel info bazel-testlogs)/dataplane/chmechanics/chmechanics_test/test.log"
# A misspelled filter or a skipped test must not turn this gate green.
for name in ${required}; do
  if ! grep -Eq -- "^--- PASS: ${name} \(" "${log}"; then
    echo "mechanics test ${name} did not run and pass" >&2
    exit 1
  fi
done
if grep -q -- "--- SKIP:" "${log}"; then
  echo "a mechanics test was skipped:" >&2
  grep -- "--- SKIP:" "${log}" >&2
  exit 1
fi
grep -F 'mechanics server ' "${log}" || true
echo "mechanics: $(printf '%s\n' "${required}" | wc -l | tr -d ' ') test(s) passed on ${image}"

if [ -z "${ARBITER_CH_MECHANICS_FILTER:-}" ]; then
  # The SNode's owner audit across a ClickHouse-only restart (Task 5). It
  # restarts node B, so it runs only here, on its own, after the suite. A
  # filter that matches nothing, or a test that skips, passes silently, hence
  # the grep.
  snode_test="TestOwnerAuditRepairsResurrectedPartsAcrossAClickHouseRestart"
  bazel test //snode:snode_test "${env_flags[@]}" "--test_filter=^${snode_test}\$" \
    --test_arg=-test.v --nocache_test_results --test_timeout=1800 --test_output=errors
  snode_log="$(bazel info bazel-testlogs)/snode/snode_test/test.log"
  for name in "${snode_test}" "${snode_test}/repair" "${snode_test}/report"; do
    if ! grep -Eq -- "^ *--- PASS: ${name} \(" "${snode_log}"; then
      echo "SNode restart test ${name} did not run and pass" >&2
      exit 1
    fi
  done
  echo "snode: ${snode_test} passed on ${image}"
fi
