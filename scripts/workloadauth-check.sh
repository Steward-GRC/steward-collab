#!/usr/bin/env bash
# Fails when internal/workloadauth differs by a byte from steward-core's copy
# at STEWARD_CORE_REF in proto-refs.env, the core commit collab already pins
# for its protos. Never edit the copy here: change core's and bump the pin.
# A pinned commit without core's copy fails too.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$root/proto-refs.env"

src="$(mktemp -d)"
trap 'rm -rf "$src"' EXIT
curl -sSfL "https://codeload.github.com/Steward-GRC/steward-core/tar.gz/$STEWARD_CORE_REF" -o "$src/core.tar.gz"
if ! tar -xzf "$src/core.tar.gz" -C "$src" --strip-components=1 --wildcards '*/internal/workloadauth/*'; then
  echo "::error file=proto-refs.env::workloadauth: steward-core $STEWARD_CORE_REF has no internal/workloadauth" >&2
  exit 1
fi

if diff -r "$src/internal/workloadauth" "$root/internal/workloadauth"; then
  echo "workloadauth: identical to steward-core@$STEWARD_CORE_REF"
else
  echo "workloadauth: internal/workloadauth differs from steward-core@$STEWARD_CORE_REF" >&2
  exit 1
fi
