DROP INDEX IF EXISTS idx_songs_duplicate_of;
DROP INDEX IF EXISTS idx_songs_tidal_id;
ALTER TABLE songs DROP COLUMN duplicate_of;
ALTER TABLE songs DROP COLUMN tidal_id;
