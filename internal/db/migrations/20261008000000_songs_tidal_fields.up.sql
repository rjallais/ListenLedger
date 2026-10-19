ALTER TABLE songs ADD COLUMN tidal_id TEXT NOT NULL DEFAULT '';
ALTER TABLE songs ADD COLUMN duplicate_of TEXT NOT NULL DEFAULT '';
CREATE INDEX IF NOT EXISTS idx_songs_tidal_id ON songs(tidal_id) WHERE tidal_id != '';
CREATE INDEX IF NOT EXISTS idx_songs_duplicate_of ON songs(duplicate_of) WHERE duplicate_of != '';
