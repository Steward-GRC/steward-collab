#!/usr/bin/env bash
# Runs govulncheck and fails on any vulnerability the code calls, except the
# IDs listed in allowed below. Imported-but-uncalled findings never fail,
# matching govulncheck's own exit code. Needs jq.
#
# GOVULNCHECK names the binary (default: govulncheck on PATH). Arguments are
# the packages to scan (default ./...).
set -euo pipefail

# GO-2026-6443: gRPC xDS-routing panic; Steward uses no xDS routing. Tracked in
# Steward-GRC/steward-core#13. Remove when grpc 1.84.1+ or 1.85 ships.
allowed="GO-2026-6443"

bin="${GOVULNCHECK:-govulncheck}"
if [ "$#" -eq 0 ]; then
  set -- ./...
fi

out="$(mktemp)"
trap 'rm -f "$out"' EXIT
"$bin" -format json "$@" > "$out"

called="$(jq -r 'select(.finding != null) | .finding | select(.trace[0].function != null) | .osv' "$out" | sort -u)"

failed=0
for id in $called; do
  skip=0
  for a in $allowed; do
    if [ "$id" = "$a" ]; then
      skip=1
    fi
  done
  if [ "$skip" -eq 1 ]; then
    echo "govulncheck: $id is called but excluded (see the comment in this script)"
  else
    echo "govulncheck: $id is called by this code" >&2
    failed=1
  fi
done

if [ "$failed" -ne 0 ]; then
  echo "govulncheck: run '$bin ./...' for the call traces" >&2
  exit 1
fi
echo "govulncheck: no called vulnerabilities outside the exclusion list"
