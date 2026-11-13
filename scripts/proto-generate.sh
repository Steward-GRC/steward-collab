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
# A path starting with "?" is optional: it is skipped, with a note, when the ref
# does not have it, so a pin that predates a new import still regenerates.
fetch() {
  local repo="$1" ref="$2" local_dir="$3"
  shift 3
  local dest="$protos/$repo" path want archive
  local patterns=()
  mkdir -p "$dest"
  if [[ -n "$local_dir" ]]; then
    echo "proto: $repo from $local_dir"
    for path in "$@"; do
      want="${path#\?}"
      if [[ "$path" == \?* && ! -e "$local_dir/$want" ]]; then
        echo "proto: $repo has no $want, skipping (optional)"
        continue
      fi
      mkdir -p "$(dirname "$dest/$want")"
      cp -R "$local_dir/$want" "$dest/$want"
    done
    return
  fi
  echo "proto: $repo at $ref"
  archive="$(mktemp)"
  curl -sSfL "https://codeload.github.com/Steward-GRC/$repo/tar.gz/$ref" -o "$archive"
  for path in "$@"; do
    want="${path#\?}"
    if [[ "$path" == \?* ]] && ! tar -tzf "$archive" | grep -qx "[^/]*/proto/$want"; then
      echo "proto: $repo at $ref has no $want, skipping (optional)"
      continue
    fi
    patterns+=("*/proto/$want")
  done
  tar -xz -C "$dest" --strip-components=2 --wildcards "${patterns[@]}" <"$archive"
  rm -f "$archive"
}

fetch steward-audit "$STEWARD_AUDIT_REF" "${STEWARD_AUDIT_PROTO_DIR:-}" steward/audit
# Only the core files collab calls, plus what they import: policy.proto imports
# category.proto and semver.proto, and semver.proto imports category.proto.
# Add a file here when a listed core proto starts importing a new one.
# semver.proto is optional because pins from before core#117 do not have it.
fetch steward-core "$STEWARD_CORE_REF" "${STEWARD_CORE_PROTO_DIR:-}" \
  steward/core/v1/category.proto steward/core/v1/policy.proto \
  steward/core/v1/policy_appendix.proto "?steward/core/v1/semver.proto"
# The read service only; read.proto imports types.proto.
fetch steward-identity "$STEWARD_IDENTITY_REF" "${STEWARD_IDENTITY_PROTO_DIR:-}" \
  steward/identity/v1/read.proto steward/identity/v1/types.proto

cd "$root"
buf generate
