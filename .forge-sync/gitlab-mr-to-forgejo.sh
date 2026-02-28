#!/usr/bin/env bash
set -euo pipefail

: "${FORGEJO_PUSH_URL:?FORGEJO_PUSH_URL is required}"

MR_IID="${CI_MERGE_REQUEST_IID:-}"
if [[ -z "${MR_IID}" ]]; then
  echo "No merge request context found; skipping."
  exit 0
fi

TARGET_BRANCH="${FORGEJO_TARGET_BRANCH:-main}"
BRANCH_PREFIX="${FORGEJO_BRANCH_PREFIX:-gitlab/mr}"
BRANCH_NAME="${BRANCH_PREFIX}-${MR_IID}"

echo "Preparing Forgejo branch ${BRANCH_NAME} from MR !${MR_IID}..."

git config user.name "${FORGEJO_BOT_NAME:-gitlab-bridge-bot}"
git config user.email "${FORGEJO_BOT_EMAIL:-gitlab-bridge-bot@example.invalid}"

if ! git remote get-url forgejo >/dev/null 2>&1; then
  git remote add forgejo "${FORGEJO_PUSH_URL}"
else
  git remote set-url forgejo "${FORGEJO_PUSH_URL}"
fi

# The checked-out MR pipeline commit is pushed to Forgejo PR branch.
git push forgejo "HEAD:refs/heads/${BRANCH_NAME}" --force

echo "Pushed ${BRANCH_NAME}. Open/update PR against ${TARGET_BRANCH} in Forgejo."
