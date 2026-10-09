-- Core domain query tables for ListenLedger

CREATE TABLE IF NOT EXISTS artists (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    spotify_id TEXT NOT NULL DEFAULT '',
    monthly_listeners INTEGER NOT NULL DEFAULT 0,
    genre_group TEXT NOT NULL CHECK(genre_group IN ('rock_metal', 'everything_else')),
    list_status TEXT NOT NULL CHECK(list_status IN ('included', 'recently_added', 'not_added', 'waiting')),
    fetch_status TEXT NOT NULL DEFAULT 'idle' CHECK(fetch_status IN ('idle', 'pending', 'failed')),
    collection_songs INTEGER NOT NULL DEFAULT 0,
    total_songs INTEGER NOT NULL DEFAULT 0,
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_artists_genre_status_listeners ON artists(genre_group, list_status, monthly_listeners DESC);
CREATE INDEX IF NOT EXISTS idx_artists_spotify_id ON artists(spotify_id) WHERE spotify_id != '';
CREATE INDEX IF NOT EXISTS idx_artists_list_status ON artists(list_status);
CREATE INDEX IF NOT EXISTS idx_artists_fetch_status ON artists(fetch_status);

CREATE TABLE IF NOT EXISTS albums (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_name TEXT NOT NULL,
    collection_songs INTEGER NOT NULL DEFAULT 0,
    total_songs INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK(status IN ('full', 'processed_once', 'waiting')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_albums_status ON albums(status);
CREATE INDEX IF NOT EXISTS idx_albums_artist ON albums(artist_name);

CREATE TABLE IF NOT EXISTS songs (
    id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    artist_name TEXT NOT NULL,
    album TEXT NOT NULL DEFAULT '',
    release_date TEXT NOT NULL DEFAULT '',
    release_year INTEGER NOT NULL DEFAULT 0,
    release_type TEXT NOT NULL DEFAULT '',
    spotify_id TEXT NOT NULL DEFAULT '',
    is_recent INTEGER NOT NULL DEFAULT 0,
    recent_batch_seq INTEGER NOT NULL DEFAULT 0,
    recent_batch_pos INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_songs_recent ON songs(is_recent, release_date);
CREATE INDEX IF NOT EXISTS idx_songs_artist_name ON songs(artist_name);
CREATE INDEX IF NOT EXISTS idx_songs_spotify ON songs(spotify_id) WHERE spotify_id != '';

CREATE TABLE IF NOT EXISTS scrape_jobs (
    id TEXT PRIMARY KEY,
    request_id TEXT NOT NULL UNIQUE,
    artist_id TEXT NOT NULL,
    status TEXT NOT NULL CHECK(status IN ('queued', 'processing', 'succeeded', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT NOT NULL DEFAULT '',
    queued_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    finished_at TEXT,
    FOREIGN KEY (artist_id) REFERENCES artists(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_scrape_jobs_request ON scrape_jobs(request_id);
CREATE INDEX IF NOT EXISTS idx_scrape_jobs_artist ON scrape_jobs(artist_id);
CREATE INDEX IF NOT EXISTS idx_scrape_jobs_status ON scrape_jobs(status);
