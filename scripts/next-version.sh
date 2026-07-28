#!/usr/bin/env bash
#
# Prints the version the next Cut Release should carry, as vX.Y.Z.
#
#   X  is the release line, 0 unless RELEASE_MAJOR says otherwise.
#   Y  increments when this cut lands on a different UTC calendar day than
#      the previous cut.
#   Z  increments for further cuts on the same UTC day and resets when Y moves.
#
# The first cut of a release line is v<X>.0.0. Annotated release tags are the
# ledger: their version and tagger date determine the next version.
#
# Overrides used by tests:
#   RELEASE_MAJOR      X, default 0
#   RELEASE_TODAY_UTC  today as YYYY-MM-DD, default `date -u`
#   RELEASE_LAST_TAG   previous tag, default discovered from git
set -euo pipefail

major="${RELEASE_MAJOR:-0}"
today="${RELEASE_TODAY_UTC:-$(date -u +%Y-%m-%d)}"

if ! [[ "${major}" =~ ^[0-9]+$ ]]; then
	echo "error: RELEASE_MAJOR must be a non-negative integer, got '${major}'" >&2
	exit 2
fi
if ! [[ "${today}" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]]; then
	echo "error: RELEASE_TODAY_UTC must be YYYY-MM-DD, got '${today}'" >&2
	exit 2
fi

last_tag="${RELEASE_LAST_TAG-}"
if [ -z "${last_tag}" ]; then
	last_tag="$(git tag --list "v${major}.*" --sort=-v:refname |
		grep -E "^v${major}\.[0-9]+\.[0-9]+$" |
		head -n 1 || true)"
fi

if [ -z "${last_tag}" ]; then
	echo "v${major}.0.0"
	exit 0
fi
if ! [[ "${last_tag}" =~ ^v${major}\.([0-9]+)\.([0-9]+)$ ]]; then
	echo "error: previous tag is not v${major}.Y.Z: ${last_tag}" >&2
	exit 2
fi

last_minor="${BASH_REMATCH[1]}"
last_patch="${BASH_REMATCH[2]}"

# Force UTC even on self-hosted or developer machines with another local zone.
last_day="$(TZ=UTC0 git for-each-ref \
	--format='%(taggerdate:format-local:%Y-%m-%d)' \
	"refs/tags/${last_tag}")"
if [ -z "${last_day}" ]; then
	# Lightweight tags are not produced by the workflow, but accepting one
	# makes recovery from an older/manual tag deterministic.
	last_day="$(TZ=UTC0 git for-each-ref \
		--format='%(creatordate:format-local:%Y-%m-%d)' \
		"refs/tags/${last_tag}")"
fi
if [ -z "${last_day}" ]; then
	echo "error: could not read creation date for ${last_tag}" >&2
	exit 2
fi

if [ "${today}" = "${last_day}" ]; then
	echo "v${major}.${last_minor}.$((10#${last_patch} + 1))"
else
	echo "v${major}.$((10#${last_minor} + 1)).0"
fi
