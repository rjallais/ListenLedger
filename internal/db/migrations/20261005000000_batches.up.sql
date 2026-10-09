-- Batch aggregate projection (restart-safe batch progress).
-- Replaces the in-memory batches/artistBatch maps: BatchStarted writes one
-- batches row + one member row per artist; BatchArtistCompleted flips a member
-- and recomputes completed/done. SSE lanes and /api/queue read snapshots from
-- here instead of process memory, so progress survives restarts.
-- completed is always recomputed from members (never incremented blindly),
-- which makes duplicate completion events harmless.

CREATE TABLE IF NOT EXISTS batches (
    id TEXT PRIMARY KEY,
    total INTEGER NOT NULL DEFAULT 0,
    completed INTEGER NOT NULL DEFAULT 0,
    done INTEGER NOT NULL DEFAULT 0,
    stats TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS batch_members (
    batch_id TEXT NOT NULL REFERENCES batches(id) ON DELETE CASCADE,
    artist_id TEXT NOT NULL,
    done INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (batch_id, artist_id)
);

CREATE INDEX IF NOT EXISTS idx_batch_members_artist ON batch_members(artist_id);
CREATE INDEX IF NOT EXISTS idx_batches_done_updated ON batches(done, updated_at DESC);
