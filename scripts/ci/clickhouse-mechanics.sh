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
# disk and the default user's access-management settings. A pinned MinIO on
# the same network (alias arbiter-core-minio) is the S3 endpoint of M12-M14
# and of the objstore tests: one bucket and three keys, one that may not
# delete, one that may delete only */.lock and one that may only read. Then
# runs the suite, prints the delete right M13 measured and, against node B,
# the SNode's owner-audit restart test, then the objstore TestS3_ tests,
# greps both servers' logs for the three S3 secrets, and removes the
# containers and the network. Every TestMechanics_ function, the restart test
# and every TestS3_ function must report PASS; a skipped test fails the run.
#
# Environment: ARBITER_CH_PLATFORM (for example linux/amd64) is passed to
# docker pull and docker run of the ClickHouse image.
# ARBITER_CH_MECHANICS_FILTER (a Go regexp) limits the suite to the
# TestMechanics_ functions it matches and skips the SNode and objstore runs.
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "${repo_root}"

suffix="${GITHUB_RUN_ID:-local}-${GITHUB_RUN_ATTEMPT:-0}"
network="arbiter-core-mech-${suffix}"
ch_a="arbiter-core-mech-a-${suffix}"
ch_b="arbiter-core-mech-b-${suffix}"
minio="arbiter-core-mech-minio-${suffix}"

# The S3 endpoint of M12-M14 and the objstore tests (plan S3-C1 contract
# §17). These keys are fixed CI-only literals of an ephemeral container on a
# private Docker network, never a real credential; M14 and the log grep below
# search for exactly these secrets. MINIO_IMAGE is Bitnami's build of MinIO
# RELEASE.2024-12-18T13-15-44Z with its mc, pinned by digest: MinIO's own
# images can no longer be pulled.
MINIO_IMAGE="bitnamilegacy/minio:2024.12.18-debian-12-r1@sha256:c0ede65eb88e47138470202c517bacf55ab3c27df848030b7b46e69d34b7d192"
s3_bucket="si-mechanics"
s3_root_user="mech-root"
s3_root_secret="mech-root-secret-0000"
s3_nodelete_key="mech-nodelete"
s3_nodelete_secret="mech-nodelete-secret-0001"
s3_lockdelete_key="mech-lockdelete"
s3_lockdelete_secret="mech-lockdelete-secret-0002"
s3_readonly_key="mech-readonly"
s3_readonly_secret="mech-readonly-secret-0003"
tmp_dir=""

cleanup() {
  docker rm -f "${ch_a}" "${ch_b}" "${minio}" >/dev/null 2>&1 || true
  docker network rm "${network}" >/dev/null 2>&1 || true
  if [ -n "${tmp_dir}" ]; then
    rm -rf "${tmp_dir}"
  fi
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
    docker logs --tail 50 "${minio}" >&2 2>&1 || true
  fi
  cleanup
  exit "${status}"
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

cleanup
docker pull ${platform_flag:+"${platform_flag}"} "${image}" >/dev/null
docker pull "${MINIO_IMAGE}" >/dev/null
tmp_dir="$(mktemp -d)"
docker network create "${network}" >/dev/null

# The image's entrypoint is a setup script and its user (uid 1001) has no
# /data, so the server is started directly on the image's data directory.
docker run -d --name "${minio}" --hostname "${minio}" \
  --network "${network}" --network-alias arbiter-core-minio \
  -e MINIO_ROOT_USER="${s3_root_user}" -e MINIO_ROOT_PASSWORD="${s3_root_secret}" \
  --entrypoint minio "${MINIO_IMAGE}" server /bitnami/minio/data >/dev/null

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

# The MinIO image's bundled mc reaches the server through MC_HOST_mech and
# keeps its own state in /tmp.
mc_env=(-e "MC_HOST_mech=http://${s3_root_user}:${s3_root_secret}@127.0.0.1:9000" -e MC_CONFIG_DIR=/tmp/mc)
minio_ready=false
for _ in $(seq 1 60); do
  if docker exec "${mc_env[@]}" "${minio}" mc ls mech >/dev/null 2>&1; then
    minio_ready=true
    break
  fi
  sleep 1
done
if [ "${minio_ready}" != true ]; then
  echo "MinIO did not become ready" >&2
  exit 1
fi
cat > "${tmp_dir}/si-backup-nodelete.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], "Resource": ["arn:aws:s3:::si-mechanics/*"]},
    {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetBucketLocation"], "Resource": ["arn:aws:s3:::si-mechanics"]}
  ]
}
JSON
cat > "${tmp_dir}/si-backup-lockdelete.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject", "s3:PutObject", "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"], "Resource": ["arn:aws:s3:::si-mechanics/*"]},
    {"Effect": "Allow", "Action": ["s3:ListBucket", "s3:ListBucketMultipartUploads", "s3:GetBucketLocation"], "Resource": ["arn:aws:s3:::si-mechanics"]},
    {"Effect": "Allow", "Action": ["s3:DeleteObject"], "Resource": ["arn:aws:s3:::si-mechanics/*/.lock"]}
  ]
}
JSON
cat > "${tmp_dir}/si-restore-readonly.json" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::si-mechanics/*"]},
    {"Effect": "Allow", "Action": ["s3:ListBucket"], "Resource": ["arn:aws:s3:::si-mechanics"]}
  ]
}
JSON
docker exec "${mc_env[@]}" "${minio}" mc mb "mech/${s3_bucket}" >/dev/null
add_s3_user() {
  docker cp "${tmp_dir}/$3.json" "${minio}:/tmp/$3.json" >/dev/null
  docker exec "${mc_env[@]}" "${minio}" mc admin policy create mech "$3" "/tmp/$3.json" >/dev/null
  docker exec "${mc_env[@]}" "${minio}" mc admin user add mech "$1" "$2" >/dev/null
  docker exec "${mc_env[@]}" "${minio}" mc admin policy attach mech "$3" --user "$1" >/dev/null
}
add_s3_user "${s3_nodelete_key}" "${s3_nodelete_secret}" si-backup-nodelete
add_s3_user "${s3_lockdelete_key}" "${s3_lockdelete_secret}" si-backup-lockdelete
add_s3_user "${s3_readonly_key}" "${s3_readonly_secret}" si-restore-readonly

export ARBITER_CH_INTEGRATION=1 ARBITER_CH_KEEPER=1 ARBITER_CH_REPLICA=1 ARBITER_CH_MECHANICS=1 ARBITER_CH_RESTART=1
export CH_ADDR="127.0.0.1:${port_a}"
export CH_REPLICA_ADDR="127.0.0.1:${port_b}"
export CH_RESTART_ADDR="127.0.0.1:${port_b}"
export CH_PEER_ADDR="arbiter-core-clickhouse-a:9000"
export ARBITER_CH_EXPECT_VERSION_PREFIX="${version_prefix}"
export ARBITER_CH_IMAGE="${image}"
export ARBITER_CH_S3=1
export CH_S3_ENDPOINT="http://arbiter-core-minio:9000"
export CH_S3_BUCKET="${s3_bucket}"
export CH_S3_NODELETE_ACCESS_KEY="${s3_nodelete_key}" CH_S3_NODELETE_SECRET_KEY="${s3_nodelete_secret}"
export CH_S3_LOCKDELETE_ACCESS_KEY="${s3_lockdelete_key}" CH_S3_LOCKDELETE_SECRET_KEY="${s3_lockdelete_secret}"
export CH_S3_READONLY_ACCESS_KEY="${s3_readonly_key}" CH_S3_READONLY_SECRET_KEY="${s3_readonly_secret}"
env_flags=(
  --test_env=ARBITER_CH_INTEGRATION --test_env=ARBITER_CH_KEEPER --test_env=ARBITER_CH_REPLICA
  --test_env=ARBITER_CH_MECHANICS --test_env=ARBITER_CH_RESTART
  --test_env=CH_ADDR --test_env=CH_REPLICA_ADDR --test_env=CH_RESTART_ADDR --test_env=CH_PEER_ADDR
  --test_env=ARBITER_CH_EXPECT_VERSION_PREFIX --test_env=ARBITER_CH_IMAGE
  --test_env=ARBITER_CH_S3 --test_env=CH_S3_ENDPOINT --test_env=CH_S3_BUCKET
  --test_env=CH_S3_NODELETE_ACCESS_KEY --test_env=CH_S3_NODELETE_SECRET_KEY
  --test_env=CH_S3_LOCKDELETE_ACCESS_KEY --test_env=CH_S3_LOCKDELETE_SECRET_KEY
  --test_env=CH_S3_READONLY_ACCESS_KEY --test_env=CH_S3_READONLY_SECRET_KEY
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

# M13 measures what a backup key must be allowed to delete (plan S3-C1
# clarification K24) and logs it as exactly one line; the release ledger
# records it as P1_BACKUP_DELETE.
nl=$'\n'
case "${nl}${required}${nl}" in
*"${nl}TestMechanics_S3BackupWithAKeyThatCannotDelete${nl}"*)
  rights="$(grep -Eo 'M13 backup delete right: (none|lock)$' "${log}" || true)"
  if [ -z "${rights}" ] || [ "$(printf '%s\n' "${rights}" | wc -l | tr -d ' ')" -ne 1 ]; then
    echo "M13 must log exactly one 'M13 backup delete right: none|lock' line" >&2
    exit 1
  fi
  echo "mechanics: S3 backup delete right: ${rights##*: }"
  ;;
esac

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

  # The object-store library against the same MinIO (plan S3-C1 contract
  # §17): every TestS3_ function must report PASS.
  bazel test //dataplane/objstore:objstore_test "${env_flags[@]}" \
    --test_arg=-test.v --nocache_test_results --test_timeout=1800 --test_output=errors
  objstore_log="$(bazel info bazel-testlogs)/dataplane/objstore/objstore_test/test.log"
  s3_required="$(grep -ho '^func TestS3_[A-Za-z0-9_]*' dataplane/objstore/*_test.go | sed 's/^func //' | sort -u || true)"
  if [ -z "${s3_required}" ]; then
    echo "no TestS3_ function to run" >&2
    exit 1
  fi
  for name in ${s3_required}; do
    if ! grep -Eq -- "^--- PASS: ${name} \(" "${objstore_log}"; then
      echo "objstore test ${name} did not run and pass" >&2
      exit 1
    fi
  done
  if grep -q -- "--- SKIP:" "${objstore_log}"; then
    echo "an objstore test was skipped:" >&2
    grep -- "--- SKIP:" "${objstore_log}" >&2
    exit 1
  fi
  echo "objstore: $(printf '%s\n' "${s3_required}" | wc -l | tr -d ' ') S3 test(s) passed on ${image}"
fi

# M14's server-log half: no S3 secret may reach a server log. ClickHouse
# writes its log to files in the container; the grep proves something only if
# it read the query lines (executeQuery).
printf '%s\n' "${s3_nodelete_secret}" "${s3_lockdelete_secret}" "${s3_readonly_secret}" > "${tmp_dir}/secrets"
for container in "${ch_a}" "${ch_b}"; do
  server_log="${tmp_dir}/${container}.log"
  docker logs "${container}" > "${server_log}" 2>&1
  if ! docker exec "${container}" sh -c 'cat /var/log/clickhouse-server/*.log' >> "${server_log}"; then
    echo "cannot read /var/log/clickhouse-server/*.log in ${container}" >&2
    exit 1
  fi
  if ! grep -q 'executeQuery' "${server_log}"; then
    echo "no executeQuery line in the server log of ${container}: the secret grep would prove nothing" >&2
    exit 1
  fi
  if grep -q -F -f "${tmp_dir}/secrets" "${server_log}"; then
    echo "an S3 secret reached the server log of ${container}" >&2
    exit 1
  fi
done
echo "mechanics: no S3 secret in the server logs on ${image}"
