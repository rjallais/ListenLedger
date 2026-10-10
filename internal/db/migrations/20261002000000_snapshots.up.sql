-- Aggregate snapshots (closing the books).
-- Per the Datastar YouTube canon (Immutability & Event Sourcing):
-- snapshots are aggregate freeze-frames for write validation (right side of
-- CQRS), while projections (artists, albums, songs tables) are rebuildable
-- perfect indexes for queries (left side). Streams stay short: replay from
-- the snapshot version instead of from v1 once history grows.
-- Events-only pass: snapshots are optional; projections remain the default
-- read path. Command-sourcing (reproducible command history) is deferred.

CREATE TABLE IF NOT EXISTS snapshots (
    stream_id TEXT PRIMARY KEY,
    stream_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_snapshots_type ON snapshots(stream_type);
