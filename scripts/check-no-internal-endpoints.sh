#!/usr/bin/env bash
#
# Fails when a tree that is about to be published contains a publicly routable
# IP literal or an internal name. Publishing either bakes one deployment's
# infrastructure into every installation, so it belongs in configuration, not
# in source.
#
# Reachable addresses are rejected by default and only the ranges that cannot
# identify real infrastructure are allowed: loopback, link-local, the RFC 1918
# private ranges, and the RFC 5737 documentation ranges used in examples.
#
# Internal names come from BEAM_PUBLISH_DENYLIST, an extended regular expression
# held in a repository secret. This script is published, so it cannot list the
# names itself, and a hit is reported by file and line only. With
# --require-denylist an unset BEAM_PUBLISH_DENYLIST is an error; without it the
# name check is skipped.
#
# Usage: scripts/check-no-internal-endpoints.sh [--require-denylist] <path> [<path>...]

set -euo pipefail

require_denylist=0
if [ "${1:-}" = "--require-denylist" ]; then
  require_denylist=1
  shift
fi

if [ "$#" -eq 0 ]; then
  echo "usage: $0 [--require-denylist] <path> [<path>...]" >&2
  exit 2
fi

denylist="${BEAM_PUBLISH_DENYLIST:-}"
if [ -z "$denylist" ] && [ "$require_denylist" -eq 1 ]; then
  echo "check-no-internal-endpoints: BEAM_PUBLISH_DENYLIST is not set" >&2
  exit 2
fi

is_allowed() {
  case "$1" in
    0.*|127.*|255.255.255.255) return 0 ;;                        # loopback / unspecified / broadcast
    10.*|192.168.*) return 0 ;;                                   # RFC 1918
    172.1[6-9].*|172.2[0-9].*|172.3[01].*) return 0 ;;            # RFC 1918
    169.254.*) return 0 ;;                                        # RFC 3927 link-local
    192.0.2.*|198.51.100.*|203.0.113.*) return 0 ;;               # RFC 5737 documentation
    *) return 1 ;;
  esac
}

publishable_files() {
  if git -C "$(dirname "$1")" rev-parse --git-dir >/dev/null 2>&1; then
    git ls-files -z -- "$1" | tr '\0' '\n'
  else
    find "$1" -type f -not -path '*/.git/*' 2>/dev/null
  fi | grep -vE '(^|/)(package-lock\.json|Cargo\.lock|go\.sum|pnpm-lock\.yaml|yarn\.lock)$' \
     | grep -vE "${BEAM_PUBLISH_PRIVATE_PATHS:-^$}" || true
}

addresses=0
names=0

for target in "$@"; do
  if [ ! -e "$target" ]; then
    echo "check-no-internal-endpoints: no such path: $target" >&2
    exit 2
  fi

  files=$(publishable_files "$target")

  while IFS= read -r hit; do
    [ -n "$hit" ] || continue
    file="${hit%%:*}"
    rest="${hit#*:}"
    line="${rest%%:*}"
    for candidate in $(printf '%s' "$rest" | grep -oE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' || true); do
      # A dotted quad whose octets exceed 255 is a version string, not an address.
      if printf '%s' "$candidate" | awk -F. '{for(i=1;i<=4;i++) if ($i>255) exit 1}'; then
        if ! is_allowed "$candidate"; then
          echo "  $file:$line  $candidate"
          addresses=1
        fi
      fi
    done
  done < <(
    printf '%s\n' "$files" \
      | tr '\n' '\0' \
      | xargs -0 -r grep -nE '\b([0-9]{1,3}\.){3}[0-9]{1,3}\b' --binary-files=without-match 2>/dev/null \
      || true
  )

  if [ -n "$denylist" ]; then
    while IFS= read -r hit; do
      [ -n "$hit" ] || continue
      echo "  $hit  internal name"
      names=1
    done < <(
      printf '%s\n' "$files" \
        | tr '\n' '\0' \
        | xargs -0 -r grep -nEi -e "$denylist" --binary-files=without-match 2>/dev/null \
        | cut -d: -f1,2 \
        || true
    )
  fi
done

if [ "$addresses" -ne 0 ]; then
  cat >&2 <<'EOF'

Refusing to publish: the addresses above are publicly routable and would ship
to every installation of this package.

Read the endpoint from the environment with a local fallback instead. If an
address is genuinely safe to publish, add its range to is_allowed() in this
script with a comment saying why.
EOF
fi

if [ "$names" -ne 0 ]; then
  cat >&2 <<'EOF'

Refusing to publish: the lines above contain names of internal repositories,
fixtures or infrastructure that would ship to every installation.

Read them from the environment instead. The matched text is withheld because
the list itself is confidential.
EOF
fi

if [ "$addresses" -ne 0 ] || [ "$names" -ne 0 ]; then
  exit 1
fi

echo "check-no-internal-endpoints: no publishable-tree endpoint leaks found in $*"
