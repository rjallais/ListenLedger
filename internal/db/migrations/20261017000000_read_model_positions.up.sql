-- Read-model positions: per-stream projected version, advanced in the same
-- transaction as the projection writes. The healer reactor compares incoming
-- event versions against these positions to skip streams already caught up
-- (no blind re-projection, no stale regression) and to find streams the
-- inline path left behind.

CREATE TABLE IF NOT EXISTS read_model_positions (
    stream_id TEXT PRIMARY KEY,
    stream_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_read_model_positions_type ON read_model_positions(stream_type, updated_at DESC);

-- Seed each stream's tip from the event log. Without this, a first boot replays
-- every retained DOMAIN_EVENTS message and re-projects all history in one
-- burst. The seed declares pre-migration history converged (run cmd/audit
-- before deploying, same discipline as the jetstream flip). In jetstream mode
-- the SQLite log is frozen at the flip, so post-flip appends carry higher
-- versions and still heal normally.

INSERT INTO read_model_positions (stream_id, stream_type, version)
SELECT stream_id, stream_type, MAX(version)
FROM events
GROUP BY stream_id, stream_type;
