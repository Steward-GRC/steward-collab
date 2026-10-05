#!/usr/bin/env bash
# Regenerates gen/ from this repo's proto/ and from the callee protos pinned in
# proto-refs.env. The callee protos are fetched into .protos/ (git-ignored) and
# never committed; only the generated stubs are.
#
# STEWARD_AUDIT_PROTO_DIR, STEWARD_CORE_PROTO_DIR and STEWARD_IDENTITY_PROTO_DIR
# point at a local proto/ directory instead, for trying an unmerged proto
# change.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
# shellcheck source=/dev/null
source "$root/proto-refs.env"

protos="$root/.protos"
rm -rf "$protos" "$root/gen/go/thirdparty"

# fetch <repo> <ref> <local proto dir> <proto package dir or file>...
fetch() {
  local repo="$1" ref="$2" local_dir="$3"
  shift 3
  local dest="$protos/$repo" path
  local patterns=()
  mkdir -p "$dest"
  if [[ -n "$local_dir" ]]; then
    echo "proto: $repo from $local_dir"
    for path in "$@"; do
      mkdir -p "$(dirname "$dest/$path")"
      cp -R "$local_dir/$path" "$dest/$path"
    done
    return
  fi
  echo "proto: $repo at $ref"
  for path in "$@"; do
    patterns+=("*/proto/$path")
  done
  curl -sSfL "https://codeload.github.com/Steward-GRC/$repo/tar.gz/$ref" |
    tar -xz -C "$dest" --strip-components=2 --wildcards "${patterns[@]}"
}

fetch steward-audit "$STEWARD_AUDIT_REF" "${STEWARD_AUDIT_PROTO_DIR:-}" steward/audit
# Only the two core files collab calls; policy.proto imports category.proto.
fetch steward-core "$STEWARD_CORE_REF" "${STEWARD_CORE_PROTO_DIR:-}" \
  steward/core/v1/category.proto steward/core/v1/policy.proto
# The read service only; read.proto imports types.proto.
fetch steward-identity "$STEWARD_IDENTITY_REF" "${STEWARD_IDENTITY_PROTO_DIR:-}" \
  steward/identity/v1/read.proto steward/identity/v1/types.proto

cd "$root"
buf generate
