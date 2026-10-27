#!/usr/bin/env bash
# Run the stage-3 member confinement suite (//member/confinement) and the
# member byte sources' ClickHouse tests (//member/sources) on one image.
#
# Usage:
#   bash scripts/ci/clickhouse-member-confinement.sh <image> <version prefix>
#   bash scripts/ci/clickhouse-member-confinement.sh --cleanup
#
# Starts, on one Docker network, the fenced server si-confined (<image> with
# scripts/ci/clickhouse-member-access-control.xml, an embedded Keeper and an
# admin default user that manages users and named collections), the unfenced
# peer si-peer (<image>, the owner's and the verifiers' ClickHouse) and MinIO
# si-minio with one bucket and a key generated for this run. Runs the suite,
# then the tests of member/sources against the same servers, and removes
# everything it started. Every TestConfinement_ function and every test of
# member/sources/*_ch_test.go must report PASS; a skipped test of either
# target fails the run.
#
# Environment: ARBITER_CH_PLATFORM (for example linux/amd64) is passed to
# docker pull and docker run of <image>. ARBITER_CH_MEMBER_CONFINEMENT_FILTER
# (a Go regexp) limits the suite to the TestConfinement_ functions it matches
# and skips the member/sources run.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

suffix="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}"
network="arbiter-core-member-${suffix}"
confined="arbiter-core-member-confined-${suffix}"
peer="arbiter-core-member-peer-${suffix}"
minio="arbiter-core-member-minio-${suffix}"
minio_image="bitnamilegacy/minio:2024.12.18-debian-12-r1@sha256:c0ede65eb88e47138470202c517bacf55ab3c27df848030b7b46e69d34b7d192"
bucket="si-confinement"

cleanup() {
  docker rm -f "${confined}" "${peer}" "${minio}" >/dev/null 2>&1 || true
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
    # ClickHouse and MinIO log to the container's stderr, so both streams go
    # to ours.
    for c in "${confined}" "${peer}" "${minio}"; do
      docker logs --tail 100 "${c}" >&2 2>&1 || true
    done
  fi
  cleanup
  exit "${status}"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# The bucket key exists only in this run's environment: docker and bazel read
# it from there, never from their arguments.
S3_ACCESS_KEY_ID="confinement-$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')"
S3_SECRET="$(od -An -N16 -tx1 /dev/urandom | tr -d ' \n')"
export S3_ACCESS_KEY_ID S3_SECRET

cleanup
docker pull ${platform_flag:+"${platform_flag}"} "${image}" >/dev/null
docker pull "${minio_image}" >/dev/null
docker network create "${network}" >/dev/null

docker run -d ${platform_flag:+"${platform_flag}"} \
  --name "${confined}" --hostname si-confined \
  --network "${network}" --network-alias si-confined \
  -p 127.0.0.1::9000 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -v "${repo_root}/scripts/ci/clickhouse-member-access-control.xml:/etc/clickhouse-server/config.d/zz-si-access-control.xml:ro" \
  -v "${repo_root}/scripts/ci/clickhouse-keeper.xml:/etc/clickhouse-server/config.d/keeper.xml:ro" \
  -v "${repo_root}/scripts/ci/clickhouse-mechanics-users.xml:/etc/clickhouse-server/users.d/zz-confinement-admin.xml:ro" \
  "${image}" >/dev/null
docker run -d ${platform_flag:+"${platform_flag}"} \
  --name "${peer}" --hostname si-peer \
  --network "${network}" --network-alias si-peer \
  -p 127.0.0.1::9000 \
  -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
  -v "${repo_root}/scripts/ci/clickhouse-mechanics-users.xml:/etc/clickhouse-server/users.d/zz-confinement-admin.xml:ro" \
  "${image}" >/dev/null
MINIO_ROOT_USER="${S3_ACCESS_KEY_ID}" MINIO_ROOT_PASSWORD="${S3_SECRET}" docker run -d \
  --name "${minio}" --network "${network}" --network-alias si-minio \
  -e MINIO_ROOT_USER -e MINIO_ROOT_PASSWORD \
  --entrypoint minio "${minio_image}" server /bitnami/minio/data >/dev/null

port_confined="$(docker inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' "${confined}")"
port_peer="$(docker inspect --format '{{(index (index .NetworkSettings.Ports "9000/tcp") 0).HostPort}}' "${peer}")"

ready=false
for _ in $(seq 1 120); do
  if docker exec "${confined}" clickhouse-client --query "SELECT count() FROM system.zookeeper WHERE path = '/'" >/dev/null 2>&1 &&
    docker exec "${peer}" clickhouse-client --query "SELECT 1" >/dev/null 2>&1 &&
    MC_HOST_local="http://${S3_ACCESS_KEY_ID}:${S3_SECRET}@127.0.0.1:9000" \
      docker exec -e MC_HOST_local "${minio}" mc mb --ignore-existing "local/${bucket}" >/dev/null 2>&1; then
    ready=true
    break
  fi
  sleep 1
done
if [ "${ready}" != true ]; then
  echo "the confinement servers did not become ready" >&2
  exit 1
fi

export CH_USER=default CH_PASSWORD=""
export ARBITER_CH_MEMBER_CONFINEMENT=1 ARBITER_CH_INTEGRATION=1
export CH_ADDR="127.0.0.1:${port_confined}"
export CH_PEER_ADDR="127.0.0.1:${port_peer}"
export CH_PEER_ADDR_FROM_SERVER="si-peer:9000"
export ARBITER_CH_EXPECT_VERSION_PREFIX="${version_prefix}"
export ARBITER_CH_IMAGE="${image}"
export S3_ENDPOINT="http://si-minio:9000"
export S3_BUCKET="${bucket}"
env_flags=(
  --test_env=ARBITER_CH_MEMBER_CONFINEMENT --test_env=ARBITER_CH_INTEGRATION
  --test_env=CH_ADDR --test_env=CH_PEER_ADDR --test_env=CH_PEER_ADDR_FROM_SERVER
  --test_env=CH_USER --test_env=CH_PASSWORD
  --test_env=ARBITER_CH_EXPECT_VERSION_PREFIX --test_env=ARBITER_CH_IMAGE
  --test_env=S3_ENDPOINT --test_env=S3_BUCKET --test_env=S3_ACCESS_KEY_ID --test_env=S3_SECRET
)

required="$(grep -ho '^func TestConfinement_[A-Za-z0-9_]*' member/confinement/*_test.go | sed 's/^func //' | sort -u || true)"
filter_flag=""
if [ -n "${ARBITER_CH_MEMBER_CONFINEMENT_FILTER:-}" ]; then
  required="$(printf '%s\n' "${required}" | grep -E -- "${ARBITER_CH_MEMBER_CONFINEMENT_FILTER}" || true)"
  filter_flag="--test_filter=${ARBITER_CH_MEMBER_CONFINEMENT_FILTER}"
fi
if [ -z "${required}" ]; then
  echo "no TestConfinement_ function to run" >&2
  exit 1
fi

bazel test //member/confinement:confinement_test "${env_flags[@]}" ${filter_flag:+"${filter_flag}"} \
  --test_arg=-test.v --nocache_test_results --test_timeout=900 --test_output=all

log="$(bazel info bazel-testlogs)/member/confinement/confinement_test/test.log"
# A misspelled filter or a skipped test must not turn this gate green.
for name in ${required}; do
  if ! grep -Eq -- "^--- PASS: ${name} \(" "${log}"; then
    echo "confinement test ${name} did not run and pass" >&2
    exit 1
  fi
done
if grep -q -- "--- SKIP:" "${log}"; then
  echo "a confinement test was skipped:" >&2
  grep -- "--- SKIP:" "${log}" >&2
  exit 1
fi
grep -o 'confinement server .*' "${log}" || true
grep -o 'confinement: .*' "${log}" || true
echo "confinement: $(printf '%s\n' "${required}" | wc -l | tr -d ' ') test(s) passed on ${image}"

if [ -z "${ARBITER_CH_MEMBER_CONFINEMENT_FILTER:-}" ]; then
  # The byte sources' two-server and object-store tests need exactly this
  # topology (Plan S3-C2 contract §22.1), so they run here, after the suite.
  sources_required="$(grep -ho '^func Test[A-Za-z0-9_]*' member/sources/*_ch_test.go | sed 's/^func //' | sort -u || true)"
  if [ -z "${sources_required}" ]; then
    echo "no ClickHouse test in member/sources/*_ch_test.go" >&2
    exit 1
  fi
  bazel test //member/sources:sources_test "${env_flags[@]}" \
    --test_arg=-test.v --nocache_test_results --test_timeout=900 --test_output=errors
  sources_log="$(bazel info bazel-testlogs)/member/sources/sources_test/test.log"
  for name in ${sources_required}; do
    if ! grep -Eq -- "^--- PASS: ${name} \(" "${sources_log}"; then
      echo "member/sources test ${name} did not run and pass" >&2
      exit 1
    fi
  done
  if grep -q -- "--- SKIP:" "${sources_log}"; then
    echo "a member/sources test was skipped:" >&2
    grep -- "--- SKIP:" "${sources_log}" >&2
    exit 1
  fi
  echo "sources: $(printf '%s\n' "${sources_required}" | wc -l | tr -d ' ') ClickHouse test(s) passed on ${image}"
fi
