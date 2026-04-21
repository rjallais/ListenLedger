#!/usr/bin/env bash
set -euo pipefail
# Batch CodeScene code health check via MCP JSON-RPC
# Usage: CS_ACCESS_TOKEN=pat_... bash tools/codescene_health_check.sh
CS_MCP="${CS_MCP:-$(command -v cs-mcp 2>/dev/null || echo 'npx @codescene/codehealth-mcp@latest cs-mcp')}"
PROJECT="${PROJECT:-$(git rev-parse --show-toplevel 2>/dev/null || echo "$PWD")}"

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
  "internal/songbackfill/backfill.go"
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

# Build JSON-RPC messages
ID=2
MESSAGES=''
MESSAGES+='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"health-check","version":"0.1"}}}'
MESSAGES+=$'\n'
MESSAGES+='{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}'
MESSAGES+=$'\n'

# Build a parallel array mapping ID -> filename for output
declare -A ID_TO_FILE
for f in "${FILES[@]}"; do
  FULL_PATH="$PROJECT/$f"
  MESSAGES+="{\"jsonrpc\":\"2.0\",\"id\":$ID,\"method\":\"tools/call\",\"params\":{\"name\":\"code_health_score\",\"arguments\":{\"file_path\":\"$FULL_PATH\"}}}"
  MESSAGES+=$'\n'
  ID_TO_FILE[$ID]="$f"
  ID=$((ID+1))
done

# Build the file map into env
FILE_MAP=""
for id in "${!ID_TO_FILE[@]}"; do
  FILE_MAP+="$id:${ID_TO_FILE[$id]}"$'\n'
done

# Pass FILE_MAP in the command environment so python can read it
FILE_MAP="$FILE_MAP" CS_ACCESS_TOKEN="${CS_ACCESS_TOKEN:-}" \
  echo "$MESSAGES" | $CS_MCP 2>"${DEBUG:-/dev/null}" | \
  python3 -c "
import sys, json, os

file_map = {}
for line in os.environ.get('FILE_MAP', '').strip().splitlines():
    if ':' in line:
        id_, name = line.split(':', 1)
        file_map[int(id_)] = name

results = []
for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
        if 'result' in obj and 'content' in obj['result']:
            for c in obj['result']['content']:
                if c.get('type') == 'text':
                    results.append((obj['id'], c['text']))
    except (json.JSONDecodeError, KeyError, ValueError):
        pass

results.sort(key=lambda x: x[0])
print('{:<55} SCORE'.format('FILE'))
print('-' * 65)
for id_, text in results:
    fname = file_map.get(id_, 'id={}'.format(id_))
    score = text.replace('Code Health score: ', '').strip()
    flag = ''
    try:
        s = float(score)
        if s < 7.0: flag = ' X'
        elif s < 9.0: flag = ' !'
        else: flag = ' OK'
    except ValueError:
        pass
    print('{:<55} {}{}'.format(fname, score, flag))
"
