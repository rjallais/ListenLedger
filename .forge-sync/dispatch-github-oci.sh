#!/usr/bin/env bash
set -euo pipefail

: "${GITHUB_OWNER:?GITHUB_OWNER is required}"
: "${GITHUB_REPO:?GITHUB_REPO is required}"
: "${GITHUB_TOKEN:?GITHUB_TOKEN is required}"
: "${SOURCE_SHA:?SOURCE_SHA is required}"

SOURCE_REF="${SOURCE_REF:-refs/heads/main}"

payload=$(cat <<JSON
{
  "event_type": "forgejo-merge",
  "client_payload": {
    "sha": "${SOURCE_SHA}",
    "ref": "${SOURCE_REF}",
    "source": "forgejo"
  }
}
JSON
)

curl --fail-with-body -sS \
  -X POST \
  -H "Accept: application/vnd.github+json" \
  -H "Authorization: Bearer ${GITHUB_TOKEN}" \
  "https://api.github.com/repos/${GITHUB_OWNER}/${GITHUB_REPO}/dispatches" \
  -d "${payload}"

echo "Dispatched GitHub OCI build for ${SOURCE_SHA}"
