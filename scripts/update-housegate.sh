#!/usr/bin/env bash
#
# Update Housegate's Go and Bzlmod pins from a release tag or commit SHA.
#
# Usage:
#   bash scripts/update-housegate.sh v0.7.1
#   bash scripts/update-housegate.sh 4dd088f4fe17d7bf13ba2c2e2311d72d0b97cd54
set -euo pipefail

usage() {
	echo "usage: $0 <housegate-tag-or-commit-sha>" >&2
	exit 2
}

if [ "$#" -ne 1 ]; then
	usage
fi

selector="$1"
if ! [[ "$selector" =~ ^v[0-9]+\.[0-9]+\.[0-9]+([+-][0-9A-Za-z][0-9A-Za-z.+-]*)?$ ]] &&
	! [[ "$selector" =~ ^[0-9A-Fa-f]{7,40}$ ]]; then
	echo "error: expected a Housegate vX.Y.Z tag or a 7-40 character commit SHA, got '${selector}'" >&2
	exit 2
fi

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

go_bin="${GO_BIN:-go}"
bazel_bin="${BAZEL_BIN:-bazel}"
housegate_module="github.com/housegate/housegate"

resolved="$(
	GOWORK=off "$go_bin" list -m \
		-f '{{.Version}} {{with .Origin}}{{.Hash}}{{end}}' \
		"${housegate_module}@${selector}"
)"
read -r go_version commit extra <<<"$resolved"

if [ -z "${go_version:-}" ] || [ -n "${extra:-}" ]; then
	echo "error: could not parse the resolved Housegate version and commit from: ${resolved}" >&2
	exit 1
fi
if ! [[ "$go_version" =~ ^v[0-9] ]]; then
	echo "error: Go resolved an unexpected Housegate version: ${go_version}" >&2
	exit 1
fi
if ! [[ "${commit:-}" =~ ^[0-9a-f]{40}$ ]]; then
	echo "error: Go did not report a full Housegate commit SHA: ${commit:-<empty>}" >&2
	exit 1
fi

bazel_version="${go_version#v}"
pin_comment="Resolved Housegate ${go_version}; source is pinned by the commit below."
module_tmp="$(mktemp "${repo_root}/MODULE.bazel.tmp.XXXXXX")"
readme_tmp="$(mktemp "${repo_root}/README.md.tmp.XXXXXX")"

cleanup() {
	rm -f "$module_tmp" "$readme_tmp"
}
trap cleanup EXIT

rewrite_pins() {
	local input="$1"
	local output="$2"

	awk \
		-v version="$bazel_version" \
		-v commit="$commit" \
		-v pin_comment="$pin_comment" '
		function indentation(line) {
			match(line, /^[[:space:]]*/)
			return substr(line, RSTART, RLENGTH)
		}

		/^[[:space:]]*bazel_dep[[:space:]]*\(/ {
			in_dep = 1
			housegate_dep = 0
		}
		in_dep && /^[[:space:]]*name[[:space:]]*=[[:space:]]*"housegate",[[:space:]]*$/ {
			housegate_dep = 1
		}
		in_dep && housegate_dep && /^[[:space:]]*version[[:space:]]*=/ {
			$0 = indentation($0) "version = \"" version "\","
			dep_versions++
		}

		/^[[:space:]]*git_override[[:space:]]*\(/ {
			in_override = 1
			housegate_override = 0
		}
		in_override && /^[[:space:]]*module_name[[:space:]]*=[[:space:]]*"housegate",[[:space:]]*$/ {
			housegate_override = 1
		}
		in_override && housegate_override && /^[[:space:]]*#[[:space:]]*(Dereferenced|Resolved)[[:space:]]/ {
			$0 = indentation($0) "# " pin_comment
			override_comments++
		}
		in_override && housegate_override && /^[[:space:]]*commit[[:space:]]*=/ {
			$0 = indentation($0) "commit = \"" commit "\","
			override_commits++
		}

		{
			print
		}

		/^[[:space:]]*\)[[:space:]]*$/ {
			if (in_dep) {
				in_dep = 0
				housegate_dep = 0
			}
			if (in_override) {
				in_override = 0
				housegate_override = 0
			}
		}

		END {
			if (dep_versions != 1 || override_comments != 1 || override_commits != 1) {
				printf "error: expected one Housegate version, comment, and commit in %s; found %d, %d, %d\n", FILENAME, dep_versions, override_comments, override_commits > "/dev/stderr"
				exit 1
			}
		}
	' "$input" >"$output"
}

rewrite_pins MODULE.bazel "$module_tmp"
rewrite_pins README.md "$readme_tmp"

GOWORK=off "$go_bin" get "${housegate_module}@${go_version}"
GOWORK=off "$go_bin" mod tidy

chmod 0644 "$module_tmp" "$readme_tmp"
mv "$module_tmp" MODULE.bazel
mv "$readme_tmp" README.md

"$bazel_bin" mod deps --lockfile_mode=update >/dev/null

echo "Updated Housegate dependency:"
echo "  input:          ${selector}"
echo "  Go version:     ${go_version}"
echo "  Bzlmod version: ${bazel_version}"
echo "  commit:         ${commit}"
