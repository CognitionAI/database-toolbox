#!/usr/bin/env bash
# Smoke-test a built image: the pinned toolbox base version must be the one
# that actually runs through the custom entrypoint, and the entrypoint's
# default invocation must still be accepted by the toolbox binary.
#
# Usage: verify-image.sh <image-ref> <dockerfile> [platform]
set -euo pipefail

image="$1"
dockerfile="$2"
platform="${3:-}"

run_args=(--rm)
if [[ -n "$platform" ]]; then
    run_args+=(--platform "$platform")
fi

expected="$(sed -nE 's#^FROM us-central1-docker\.pkg\.dev/database-toolbox/toolbox/toolbox:([0-9]+\.[0-9]+\.[0-9]+)(@sha256:[0-9a-f]{64})?[[:space:]]*$#\1#p' "$dockerfile" | head -n1)"
if [[ -z "$expected" ]]; then
    echo "::notice::$dockerfile does not use a pinned toolbox base; skipping toolbox checks"
    exit 0
fi

echo "Checking $image (${platform:-default platform}) reports toolbox $expected"
version_output="$(docker run "${run_args[@]}" "$image" --version 2>/dev/null | tail -n1)"
echo "$version_output"
if [[ "$version_output" != "toolbox version ${expected}+"* ]]; then
    echo "::error::expected 'toolbox version ${expected}+...' from $image, got '${version_output}'"
    exit 1
fi

echo "Checking $image accepts its default entrypoint arguments"
if ! docker run "${run_args[@]}" "$image" --help >/dev/null 2>&1; then
    echo "::error::$image failed to run its entrypoint with --help"
    exit 1
fi

echo "OK: $image"
