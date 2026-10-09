-- Command sourcing: durable, append-only command history.
-- Every accepted scrape command (single refresh, batch refresh) is logged here
-- once under its request_id. Retries and redrives are RE-EXECUTIONS of the
-- stored command (same request_id), never new rows: the worker treats
-- request_id as the idempotency key (already-succeeded dispatches ack-skip).
-- Operational lifecycle (queued/processing/succeeded/failed) stays in
-- scrape_jobs; this table answers "what was commanded, when, for whom" and
-- redrive replays it. No purge: ~1800 commands/day at ~200 bytes is ~130 MB/yr.
-- (Events use RON; command payloads stay JSON per the HTTP-boundary convention.)

CREATE TABLE IF NOT EXISTS commands (
    request_id TEXT PRIMARY KEY,
    command_type TEXT NOT NULL CHECK(command_type IN ('refresh', 'batch', 'backfilled')),
    artist_id TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    queued_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_commands_artist ON commands(artist_id, queued_at DESC);
CREATE INDEX IF NOT EXISTS idx_commands_queued ON commands(queued_at DESC);
CREATE INDEX IF NOT EXISTS idx_commands_type ON commands(command_type);

-- Backfill: pre-log commands from surviving scrape_jobs rows. Origin is
-- unknown for these, hence 'backfilled'. (In practice the SQLite scrape_jobs
-- table starts empty on most deployments since handler mirroring is new, so
-- this is a no-op there and a safety net elsewhere.)
INSERT OR IGNORE INTO commands (request_id, command_type, artist_id, payload, queued_at)
SELECT j.request_id, 'backfilled', j.artist_id,
    json_object('spotify_id', COALESCE(a.spotify_id, ''), 'artist_name', COALESCE(a.name, '')),
    j.queued_at
FROM scrape_jobs j LEFT JOIN artists a ON a.id = j.artist_id;
