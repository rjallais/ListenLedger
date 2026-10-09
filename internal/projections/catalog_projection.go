// Package projections folds event streams into SQLite read models and publishes
// to NATS for live SSE morphing.
package projections

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
)

// CatalogProjection projects album and song events into the albums/songs read models.
type CatalogProjection struct {
	log *slog.Logger
	db  *toolbeltdb.Database
}

// NewCatalogProjection creates a CatalogProjection.
func NewCatalogProjection(log *slog.Logger, db *toolbeltdb.Database) *CatalogProjection {
	return &CatalogProjection{log: log, db: db}
}

// ResetForReplay clears selected catalog read models before their events are replayed.
func (p *CatalogProjection) ResetForReplay(ctx context.Context, streamTypes ...string) error {
	queries := make([]string, 0, len(streamTypes))
	for _, streamType := range streamTypes {
		switch streamType {
		case album.StreamTypeAlbum:
			queries = append(queries, "DELETE FROM albums;")
		case song.StreamTypeSong:
			queries = append(queries, "DELETE FROM songs;")
		default:
			return fmt.Errorf("resetting catalog projection: unsupported stream type %q", streamType)
		}
	}
	if len(queries) == 0 {
		return nil
	}

	err := p.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		for i, query := range queries {
			stmt := tx.Prep(query)
			_, err := stmt.Step()
			_ = stmt.Reset()
			if err != nil {
				return fmt.Errorf("clearing %s read model: %w", streamTypes[i], err)
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("resetting catalog projections for replay: %w", err)
	}
	return nil
}

// Project applies album/song events to the read models.
func (p *CatalogProjection) Project(ctx context.Context, streamType string, events []eventsourcing.Event) error {
	if len(events) == 0 {
		return nil
	}

	err := p.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		for _, evt := range events {
			var err error
			switch evt.StreamType {
			case album.StreamTypeAlbum:
				err = projectAlbumEvent(tx, evt)
			case song.StreamTypeSong:
				err = projectSongEvent(tx, evt)
			default:
				err = fmt.Errorf("unknown stream type %q", evt.StreamType)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("projecting %s events: %w", streamType, err)
	}
	return nil
}

func projectAlbumEvent(tx *sqlite.Conn, evt eventsourcing.Event) error {
	switch evt.EventType {
	case album.EventTypeAlbumCreated:
		var pl album.CreatedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}
		// ON CONFLICT DO NOTHING: a redelivered Created must not clobber
		// later count/status changes; subsequent events converge the row.
		stmt := tx.Prep(`INSERT INTO albums (id, title, artist_name, collection_songs, total_songs, status, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO NOTHING;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, pl.ID)
		stmt.BindText(2, pl.Title)
		stmt.BindText(3, pl.ArtistName)
		stmt.BindInt64(4, pl.CollectionSongs)
		stmt.BindInt64(5, pl.TotalSongs)
		stmt.BindText(6, pl.Status)
		stmt.BindText(7, evt.CreatedAt.Format(time.RFC3339Nano))
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("inserting album read model: %w", err)
		}
	case album.EventTypeAlbumStatusChanged:
		var pl album.StatusChangedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}
		stmt := tx.Prep("UPDATE albums SET status = ? WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, pl.NewStatus)
		stmt.BindText(2, pl.AlbumID)
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("updating album status: %w", err)
		}
	case album.EventTypeAlbumSongCountsAdjusted:
		var pl album.SongCountsAdjustedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}
		stmt := tx.Prep("UPDATE albums SET collection_songs = ?, total_songs = ? WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindInt64(1, pl.CollectionSongs)
		stmt.BindInt64(2, pl.TotalSongs)
		stmt.BindText(3, pl.AlbumID)
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("updating album song counts: %w", err)
		}
	}
	return nil
}

func projectSongEvent(tx *sqlite.Conn, evt eventsourcing.Event) error {
	switch evt.EventType {
	case song.EventTypeSongCreated:
		var pl song.CreatedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}
		isRecent := int64(0)
		if pl.IsRecent {
			isRecent = 1
		}
		// ON CONFLICT DO NOTHING: same redelivery idempotency as albums above.
		stmt := tx.Prep(`INSERT INTO songs (id, title, artist_name, album, release_date, release_year, release_type, spotify_id, is_recent, recent_batch_seq, recent_batch_pos, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO NOTHING;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, pl.ID)
		stmt.BindText(2, pl.Title)
		stmt.BindText(3, pl.ArtistName)
		stmt.BindText(4, pl.Album)
		stmt.BindText(5, pl.ReleaseDate)
		stmt.BindInt64(6, pl.ReleaseYear)
		stmt.BindText(7, pl.ReleaseType)
		stmt.BindText(8, pl.SpotifyID)
		stmt.BindInt64(9, isRecent)
		stmt.BindInt64(10, pl.RecentBatchSeq)
		stmt.BindInt64(11, pl.RecentBatchPos)
		stmt.BindText(12, evt.CreatedAt.Format(time.RFC3339Nano))
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("inserting song read model: %w", err)
		}
	case song.EventTypeRecentFlagChanged:
		var pl song.RecentFlagChangedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}
		isRecent := int64(0)
		if pl.IsRecent {
			isRecent = 1
		}
		stmt := tx.Prep("UPDATE songs SET is_recent = ?, recent_batch_seq = ?, recent_batch_pos = ? WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindInt64(1, isRecent)
		stmt.BindInt64(2, pl.RecentBatchSeq)
		stmt.BindInt64(3, pl.RecentBatchPos)
		stmt.BindText(4, pl.SongID)
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("updating song recent flag: %w", err)
		}
	}
	return nil
}
