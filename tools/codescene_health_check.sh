#!/usr/bin/env bash
set -euo pipefail
# Batch CodeScene code health check via MCP JSON-RPC
# Usage: CS_ACCESS_TOKEN=pat_... bash tools/codescene_health_check.sh
if command -v cs-mcp &>/dev/null; then
  CS_MCP=(cs-mcp)
else
  CS_MCP=(npx -y -p @codescene/codehealth-mcp@latest cs-mcp)
fi
PROJECT="${PROJECT:-$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")}"

# json_escape safely embeds a filesystem path in a JSON string value using
# Python's json.dumps to handle all control characters and special characters.
json_escape() {
  python3 -c 'import json, sys; print(json.dumps(sys.argv[1]))' "$1"
}

# Files to check (relative paths)
FILES=(
  "main.go"
  "config/config.go"
  "internal/app/app.go"
  "internal/app/hooks.go"
  "internal/app/nats.go"
  "internal/fetcher/fetcher.go"
  "internal/handlers/handlers.go"
  "internal/handlers/albums.go"
  "internal/handlers/artists.go"
  "internal/handlers/artist_refresh.go"
  "internal/handlers/artist_update.go"
  "internal/handlers/batch_progress.go"
  "internal/handlers/queue.go"
  "internal/handlers/routes.go"
  "internal/handlers/shared.go"
  "internal/handlers/songs.go"
  "internal/handlers/sse.go"
  "internal/messaging/messages.go"
  "internal/messaging/jetstream.go"
  "internal/quota/quota.go"
  "internal/songbackfill/backfill_test.go"
  "internal/spotify/apify.go"
  "internal/spotify/client.go"
  "internal/spotify/local.go"
  "internal/worker/worker.go"
  "internal/worker/dispatch.go"
  "internal/worker/processing.go"
  "internal/worker/jobs.go"
  "internal/worker/artist_updates.go"
  "internal/worker/metrics.go"
  "cmd/backfill_song_artists/main.go"
  "cmd/backfill_song_artists/review_queue.go"
  "cmd/update_listeners/main.go"
  "cmd/seed/main.go"
)

score_for_file() {
  local full_path="$1"
  local escaped_path
  escaped_path="$(json_escape "$full_path")"

  local payload
  payload=''
  payload+='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"health-check","version":"0.1"}}}'
  payload+=$'\n'
  payload+='{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'
  payload+=$'\n'
  payload+="{\"jsonrpc\":\"2.0\",\"id\":2,\"method\":\"tools/call\",\"params\":{\"name\":\"code_health_score\",\"arguments\":{\"file_path\":$escaped_path}}}"
  payload+=$'\n'

  printf '%s' "$payload" | "${CS_MCP[@]}" 2>>"${DEBUG:-/dev/null}" | python3 -c "
import json
import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        continue
    if obj.get('id') != 2:
        continue
    if obj.get('error'):
        print('ERROR: {}'.format(obj['error']))
        sys.exit(0)
    result = obj.get('result', {})
    for c in result.get('content', []):
        if c.get('type') == 'text':
            print(c.get('text', '').strip())
            sys.exit(0)
print('NO_RESPONSE')
"
}

echo "$(printf '%-55s SCORE' 'FILE')"
echo "-----------------------------------------------------------------"

for f in "${FILES[@]}"; do
  full_path="$PROJECT/$f"
  if [[ ! -f "$full_path" ]]; then
    echo "[codescene] Skipping missing file: $f" >&2
    continue
  fi

  raw_score="$(score_for_file "$full_path")"
  score="$(printf '%s\n' "$raw_score" | python3 -c "
import re
import sys

text = sys.stdin.read().strip()
match = re.search(r'Code Health score:\\s*([0-9]+(?:\\.[0-9]+)?)', text)
if match:
    print(match.group(1))
elif text:
    print(text.splitlines()[-1].strip())
else:
    print('NO_RESPONSE')
")"
  flag=''
  if [[ "$score" =~ ^[0-9]+(\.[0-9]+)?$ ]]; then
    is_low="$(python3 -c 'import sys; print(1 if float(sys.argv[1]) < 7.0 else 0)' "$score")"
    is_warn="$(python3 -c 'import sys; s=float(sys.argv[1]); print(1 if 7.0 <= s < 9.0 else 0)' "$score")"
    if [[ "$is_low" == "1" ]]; then
      flag=' X'
    elif [[ "$is_warn" == "1" ]]; then
      flag=' !'
    else
      flag=' OK'
    fi
  fi

  printf '%-55s %s%s\n' "$f" "$score" "$flag"
done
