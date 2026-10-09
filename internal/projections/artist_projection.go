package projections

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
)

// ListenerSnapshot represents a point-in-time monthly listener record for trend analysis.
type ListenerSnapshot struct {
	ID                string    `json:"id"`
	ArtistID          string    `json:"artist_id"`
	Version           int64     `json:"version"`
	MonthlyListeners  int64     `json:"monthly_listeners"`
	PreviousListeners int64     `json:"previous_listeners"`
	Delta             int64     `json:"delta"`
	Provider          string    `json:"provider"`
	DurationMs        int64     `json:"duration_ms"`
	ScrapedAt         time.Time `json:"scraped_at"`
}

// ArtistProjection projects Artist events into SQLite read models and publishes to NATS JetStream.
type ArtistProjection struct {
	log *slog.Logger
	db  *toolbeltdb.Database
	nc  *nats.Conn
	js  jetstream.JetStream
}

// NewArtistProjection creates a new ArtistProjection instance.
func NewArtistProjection(log *slog.Logger, db *toolbeltdb.Database, nc *nats.Conn) *ArtistProjection {
	return &ArtistProjection{
		log: log,
		db:  db,
		nc:  nc,
	}
}

// SetJetStream wires the durable JetStream context for domain.events.>
// publishing. When set, Project publishes each event durably (MsgID=event ID)
// in addition to the ephemeral core-NATS fanout for live Datastar SSE morphing.
func (p *ArtistProjection) SetJetStream(js jetstream.JetStream) {
	p.js = js
}

// ResetForReplay clears event-derived artist history and stale artist rows.
// Artist rows referenced by scrape jobs remain so replay does not cascade-delete
// operational queue state; matching event streams will update those rows below.
func (p *ArtistProjection) ResetForReplay(ctx context.Context) error {
	err := p.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		clearHistory := tx.Prep("DELETE FROM artist_listener_history;")
		if _, err := clearHistory.Step(); err != nil {
			_ = clearHistory.Reset()
			return fmt.Errorf("clearing artist listener history: %w", err)
		}
		_ = clearHistory.Reset()

		deleteStaleArtists := tx.Prep(`DELETE FROM artists
			WHERE NOT EXISTS (
				SELECT 1 FROM scrape_jobs WHERE scrape_jobs.artist_id = artists.id
			);`)
		defer func() { _ = deleteStaleArtists.Reset() }()
		if _, err := deleteStaleArtists.Step(); err != nil {
			return fmt.Errorf("clearing stale artist read models: %w", err)
		}

		return nil
	})
	if err != nil {
		return fmt.Errorf("resetting artist projections for replay: %w", err)
	}
	return nil
}

// Project synchronously applies events to SQLite read models and publishes to NATS.
func (p *ArtistProjection) Project(ctx context.Context, agg *artist.Artist, events []eventsourcing.Event) error {
	if len(events) == 0 {
		return nil
	}

	err := p.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		for _, evt := range events {
			if err := p.projectEvent(tx, agg, evt); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("projecting artist events: %w", err)
	}

	// Publish durable domain events to JetStream (append-only log, source of
	// truth). Ephemeral UI fanout lives on artist.updated / queue.updated via
	// the PocketBase hooks path with structured ArtistUpdated payloads — the
	// former core-NATS domain.events.* duplicate and the raw-bytes legacy
	// alias are gone: no subscriber reads core domain.events.*, and both SSE
	// and batch subscribers unmarshal artist.updated as ArtistUpdated.
	if p.js != nil {
		for _, evt := range events {
			subject := messaging.SubjectDomainEvent(evt.StreamType, evt.StreamID, evt.EventType)
			data, err := evt.Bytes()
			if err != nil {
				p.log.Error("failed encoding domain event as RON", "subject", subject, "error", err)
				continue
			}
			pubCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_, pubErr := messaging.PublishDomainEvent(pubCtx, p.js, subject, evt.ID, data)
			cancel()
			if pubErr != nil {
				p.log.Error("failed publishing domain event to JetStream", "subject", subject, "error", pubErr)
			}
		}
	}

	return nil
}

func (p *ArtistProjection) projectEvent(tx *sqlite.Conn, _ *artist.Artist, evt eventsourcing.Event) error {
	switch evt.EventType {
	case artist.EventTypeArtistCreated:
		var pl artist.CreatedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}

		// ON CONFLICT DO NOTHING: a Created replay must never reset an
		// existing row's counters, statuses, or timestamps (e.g. artists kept
		// by ResetForReplay for their scrape jobs). Later events in this same
		// batch converge the row. Fresh inserts still seed zero counters,
		// matching the aggregate's initial state.
		stmt := tx.Prep(`INSERT INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated, created_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(id) DO NOTHING;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, pl.ID)
		stmt.BindText(2, pl.Name)
		stmt.BindText(3, pl.SpotifyID)
		stmt.BindInt64(4, 0)
		stmt.BindText(5, pl.GenreGroup)
		stmt.BindText(6, pl.ListStatus)
		stmt.BindText(7, "idle")
		stmt.BindInt64(8, 0)
		stmt.BindInt64(9, 0)
		stmt.BindText(10, evt.CreatedAt.Format(time.RFC3339Nano))
		stmt.BindText(11, evt.CreatedAt.Format(time.RFC3339Nano))

		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("inserting artist read model: %w", err)
		}

	case artist.EventTypeArtistMonthlyListenersScraped:
		var pl artist.ListenersScrapedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}

		// 1. Update artists table
		updateStmt := tx.Prep("UPDATE artists SET monthly_listeners = ?, last_updated = ?, fetch_status = 'idle' WHERE id = ?;")
		defer func() { _ = updateStmt.Reset() }()
		updateStmt.BindInt64(1, pl.MonthlyListeners)
		updateStmt.BindText(2, pl.ScrapedAt.Format(time.RFC3339Nano))
		updateStmt.BindText(3, pl.ArtistID)
		if _, err := updateStmt.Step(); err != nil {
			return fmt.Errorf("updating artist monthly listeners: %w", err)
		}

		// 2. Insert into artist_listener_history time-series table
		histStmt := tx.Prep(`INSERT INTO artist_listener_history (id, artist_id, version, monthly_listeners, previous_listeners, delta, provider, duration_ms, scraped_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
			ON CONFLICT(artist_id, version) DO UPDATE SET
				id = excluded.id,
				monthly_listeners = excluded.monthly_listeners,
				previous_listeners = excluded.previous_listeners,
				delta = excluded.delta,
				provider = excluded.provider,
				duration_ms = excluded.duration_ms,
				scraped_at = excluded.scraped_at;`)
		defer func() { _ = histStmt.Reset() }()
		histStmt.BindText(1, evt.ID)
		histStmt.BindText(2, pl.ArtistID)
		histStmt.BindInt64(3, evt.Version)
		histStmt.BindInt64(4, pl.MonthlyListeners)
		histStmt.BindInt64(5, pl.PreviousListeners)
		histStmt.BindInt64(6, pl.Delta)
		histStmt.BindText(7, pl.Provider)
		histStmt.BindInt64(8, pl.DurationMs)
		histStmt.BindText(9, pl.ScrapedAt.Format(time.RFC3339Nano))

		if _, err := histStmt.Step(); err != nil {
			return fmt.Errorf("inserting artist listener history: %w", err)
		}

	case artist.EventTypeArtistStatusChanged:
		var pl artist.StatusChangedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}

		stmt := tx.Prep("UPDATE artists SET list_status = ? WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, pl.NewStatus)
		stmt.BindText(2, pl.ArtistID)
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("updating artist list status: %w", err)
		}

	case artist.EventTypeArtistFetchStatusChanged:
		var pl artist.FetchStatusChangedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}

		stmt := tx.Prep("UPDATE artists SET fetch_status = ? WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, pl.FetchStatus)
		stmt.BindText(2, pl.ArtistID)
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("updating artist fetch status: %w", err)
		}

	case artist.EventTypeArtistCollectionSongsAdjusted:
		var pl artist.CollectionSongsAdjustedPayload
		if err := eventsourcing.DecodeEventPayload(evt, &pl); err != nil {
			return err
		}

		stmt := tx.Prep("UPDATE artists SET collection_songs = ?, total_songs = ? WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindInt64(1, pl.CollectionSongs)
		stmt.BindInt64(2, pl.TotalSongs)
		stmt.BindText(3, pl.ArtistID)
		if _, err := stmt.Step(); err != nil {
			return fmt.Errorf("updating artist song counts: %w", err)
		}
	}

	return nil
}

// GetListenerHistory returns historical listener snapshots for an artist, ordered newest to oldest.
func (p *ArtistProjection) GetListenerHistory(ctx context.Context, artistID string, limit int) ([]ListenerSnapshot, error) {
	if limit <= 0 {
		limit = 50
	}

	var snapshots []ListenerSnapshot
	err := p.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT id, artist_id, version, monthly_listeners, previous_listeners, delta, provider, duration_ms, scraped_at FROM artist_listener_history WHERE artist_id = ? ORDER BY version DESC LIMIT ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		stmt.BindInt64(2, int64(limit))

		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}

			tStr := stmt.ColumnText(8)
			t, _ := time.Parse(time.RFC3339Nano, tStr)
			if t.IsZero() {
				t, _ = time.Parse("2006-01-02 15:04:05", tStr)
			}

			snapshots = append(snapshots, ListenerSnapshot{
				ID:                stmt.ColumnText(0),
				ArtistID:          stmt.ColumnText(1),
				Version:           stmt.ColumnInt64(2),
				MonthlyListeners:  stmt.ColumnInt64(3),
				PreviousListeners: stmt.ColumnInt64(4),
				Delta:             stmt.ColumnInt64(5),
				Provider:          stmt.ColumnText(6),
				DurationMs:        stmt.ColumnInt64(7),
				ScrapedAt:         t,
			})
		}
		return nil
	})

	return snapshots, err
}
