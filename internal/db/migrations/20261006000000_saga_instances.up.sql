-- Saga orchestrator instances (durable scrapejob→artist saga tracking).
-- One row per saga instance (request_id): the job event stream stays the
-- authority for lifecycle facts, this table is the queryable state index the
-- orchestrator Tick refreshes from those streams. Terminal states (done/dead)
-- are pruned after 7 days, mirroring the scrape_jobs succeeded retention.
-- created_at is set once on first sight; updated_at on every refresh.

CREATE TABLE IF NOT EXISTS saga_instances (
    request_id TEXT PRIMARY KEY,
    artist_id TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'requested',
    attempts INTEGER NOT NULL DEFAULT 0,
    last_event TEXT NOT NULL DEFAULT '',
    error TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_saga_instances_state_updated ON saga_instances(state, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_saga_instances_artist ON saga_instances(artist_id);
