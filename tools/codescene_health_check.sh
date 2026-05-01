#!/usr/bin/env bash
set -euo pipefail
# Batch CodeScene code health check via MCP JSON-RPC
# Usage: CODESCENE_PAT=pat_... bash tools/codescene_health_check.sh
#
# Uses a single long-lived cs-mcp process (via named pipe) instead of
# spawning a new MCP session per file. The initialize + notifications/initialized
# handshake happens once; then tools/call messages are streamed to the same
# process. A poll loop waits for all responses before tearing down the session.
if command -v cs-mcp &>/dev/null; then
	CS_MCP=(cs-mcp)
elif command -v npx &>/dev/null; then
	CS_MCP=(npx -y -p @codescene/codehealth-mcp@1.1.5 cs-mcp)
else
	echo "[codescene] Missing dependency: install cs-mcp or npx" >&2
	exit 1
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
	"internal/handlers/songs_create.go"
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

# Build the list of valid files upfront (skip missing ones).
valid_files=()
for f in "${FILES[@]}"; do
	if [[ -f "$PROJECT/$f" ]]; then
		valid_files+=("$f")
	else
		echo "[codescene] Skipping missing file: $f" >&2
	fi
done

if [[ ${#valid_files[@]} -eq 0 ]]; then
	echo "No files to check." >&2
	exit 0
fi

# --- Single-session MCP via named pipe ---
TMP_DIR="$(mktemp -d "${TMPDIR:-/tmp}/cs_mcp.XXXXXX")"
FIFO="$TMP_DIR/in"
OUT="$TMP_DIR/out"
cleanup() {
	exec 3>&- 2>/dev/null || true
	if [[ -n "${MCP_PID:-}" ]]; then
		kill "$MCP_PID" 2>/dev/null || true
		wait "$MCP_PID" 2>/dev/null || true
	fi
	rm -rf "$TMP_DIR"
}
trap cleanup EXIT
mkfifo "$FIFO"
touch "$OUT"

# Start cs-mcp reading from the FIFO, writing stdout to a temp file.
"${CS_MCP[@]}" < "$FIFO" > "$OUT" 2>>"${DEBUG:-/dev/null}" &
MCP_PID=$!

# Open the FIFO for writing on fd 3 so it stays open across multiple printf calls.
exec 3>"$FIFO"

# 1) Initialize handshake (once per session).
printf '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"health-check","version":"0.1"}}}\n' >&3

# Wait for initialize response (id=1) before proceeding
timeout=30
elapsed=0
while [ $elapsed -lt $timeout ]; do
	if python3 -c "import json, sys; [print(1) for line in open(sys.argv[1]) if (obj := json.loads(line.strip() or '{}')) and obj.get('id') == 1]" "$OUT" 2>/dev/null | grep -q 1; then
		break
	fi
	sleep 0.1
	elapsed=$((elapsed + 1))
done

printf '{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}\n' >&3

# 2) Send one tools/call per file (id starting at 2).
msg_id=2
for f in "${valid_files[@]}"; do
	escaped_path="$(json_escape "$PROJECT/$f")"
	printf '{"jsonrpc":"2.0","id":%d,"method":"tools/call","params":{"name":"code_health_score","arguments":{"file_path":%s}}}\n' "$msg_id" "$escaped_path" >&3
	msg_id=$((msg_id + 1))
done

# 3) Poll the output file until all expected responses arrive or we timeout.
expected=${#valid_files[@]}
timeout=120
elapsed=0
while [ $elapsed -lt $timeout ]; do
	count=$(python3 -c '
import json, sys
c = 0
for line in open(sys.argv[1]):
    line = line.strip()
    if not line: continue
    try: obj = json.loads(line)
    except: continue
    if obj.get("id") is not None and obj.get("id") >= 2:
        c += 1
print(c)
' "$OUT" 2>/dev/null || echo 0)
	if [ "$count" -ge "$expected" ]; then
		break
	fi
	sleep 1
	elapsed=$((elapsed + 1))
done

if [ "${count:-0}" -lt "$expected" ]; then
	echo "[codescene] Timed out waiting for MCP responses: got ${count:-0}/${expected}" >&2
	exit 1
fi

# Close the FIFO (sends EOF to cs-mcp) and wait for it to exit.
exec 3>&- 2>/dev/null || true

# 4) Parse responses: extract scores keyed by message id (id >= 2).
mapfile -t score_rows < <(python3 -c '
import json, re, sys

scores = {}
for line in open(sys.argv[1]):
    line = line.strip()
    if not line:
        continue
    try:
        obj = json.loads(line)
    except json.JSONDecodeError:
        continue
    mid = obj.get("id")
    if mid is None or mid < 2:
        continue
    text = None
    if obj.get("error"):
        text = "ERROR: " + str(obj["error"])
    else:
        for c in obj.get("result", {}).get("content", []):
            if c.get("type") == "text":
                text = c.get("text", "").strip()
                break
    if text:
        m = re.search(r"Code Health score:\s*([0-9]+(?:\.[0-9]+)?)", text)
        if m:
            scores[mid] = m.group(1)
        else:
            scores[mid] = text.splitlines()[-1].strip() if text.strip() else "NO_RESPONSE"
    else:
        scores[mid] = "NO_RESPONSE"

for mid in sorted(scores):
    print(f"{mid}\t{scores[mid]}")
' "$OUT")

# Build associative array to map message IDs to scores
declare -A score_by_id=()
for row in "${score_rows[@]}"; do
	mid="${row%%$'\t'*}"
	val="${row#*$'\t'}"
	score_by_id["$mid"]="$val"
done

# 5) Print results table.
printf '%-55s SCORE\n' 'FILE'
echo "-----------------------------------------------------------------"

for i in "${!valid_files[@]}"; do
	f="${valid_files[$i]}"
	msg_id=$((i + 2))
	score="${score_by_id[$msg_id]:-NO_RESPONSE}"
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
