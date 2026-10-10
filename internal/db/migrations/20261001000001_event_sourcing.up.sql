-- Event Sourcing Store and Historical Listener Snapshots

CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    stream_id TEXT NOT NULL,
    stream_type TEXT NOT NULL,
    version INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload TEXT NOT NULL,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    CONSTRAINT uq_stream_version UNIQUE (stream_id, version)
);

CREATE INDEX IF NOT EXISTS idx_events_stream ON events(stream_id, version ASC);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at ASC);

-- Time-Series Listener History Read Model (powers sparklines, historical charts, and audit provenance)
CREATE TABLE IF NOT EXISTS artist_listener_history (
    id TEXT PRIMARY KEY,
    artist_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    monthly_listeners INTEGER NOT NULL,
    previous_listeners INTEGER NOT NULL DEFAULT 0,
    delta INTEGER NOT NULL DEFAULT 0,
    provider TEXT NOT NULL DEFAULT '',
    duration_ms INTEGER NOT NULL DEFAULT 0,
    scraped_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE,
    CONSTRAINT uq_artist_listener_snapshot UNIQUE (artist_id, version)
);

CREATE INDEX IF NOT EXISTS idx_artist_listener_history ON artist_listener_history(artist_id, scraped_at DESC);
