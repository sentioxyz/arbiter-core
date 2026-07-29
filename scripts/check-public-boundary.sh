#!/usr/bin/env bash
set -euo pipefail

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

if matches="$(git grep -n -E '"github\.com/sentioxyz/arbiter(/|")' -- '*.go')"; then
  echo "arbiter-core must not import the private github.com/sentioxyz/arbiter repository:" >&2
  echo "$matches" >&2
  exit 1
fi
