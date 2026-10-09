package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"log/slog"

	toolbeltdb "github.com/delaneyj/toolbelt/db"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"
)

// eventLog folds standalone scrape results into the event-sourced log so
// update_listeners results stay auditable like worker scrapes. Best-effort:
// the PocketBase write already committed, so failures only warn — audit flags
// rows without streams for convergence on the next run.
type eventLog struct {
	database *toolbeltdb.Database
	store    *eventsourcing.SQLiteStore
	repo     *artist.Repository
	proj     *projections.ArtistProjection
}

// openEventLog opens the SQLite ledger beside the PocketBase data dir. The
// caller closes the database; a nil log disables event emission.
func openEventLog(ctx context.Context, dataDir string) (*eventLog, error) {
	database, err := db.SetupDB(ctx, slog.Default(), dataDir, false)
	if err != nil {
		return nil, fmt.Errorf("opening event log: %w", err)
	}
	store := eventsourcing.NewSQLiteStore(database)
	return &eventLog{
		database: database,
		store:    store,
		repo:     artist.NewRepository(store),
		proj:     projections.NewArtistProjection(slog.Default(), database, nil),
	}, nil
}

func (l *eventLog) close() {
	if l == nil || l.database == nil {
		return
	}
	if err := l.database.Close(); err != nil {
		log.Printf("[events] warning: closing event log: %v", err)
	}
}

// recordListeners appends an ArtistMonthlyListenersScraped fact for a
// successful scrape and projects it. Seeded streams start from the
// PocketBase row for pre-log artists.
func (l *eventLog) recordListeners(ctx context.Context, artistID, name, spotifyID, genreGroup, listStatus string, listeners int64, durationMs int64) {
	if l == nil {
		return
	}
	agg, err := l.loadOrCreate(ctx, artistID, name, spotifyID, genreGroup, listStatus)
	if err != nil {
		log.Printf("[events] Warning: loading artist %s: %v", artistID, err)
		return
	}
	if err := agg.RecordMonthlyListeners(listeners, "local_headless", durationMs); err != nil {
		log.Printf("[events] Warning: recording listeners for %s: %v", artistID, err)
		return
	}
	if err := l.saveAndProject(ctx, agg); err != nil {
		log.Printf("[events] Warning: saving listeners for %s: %v", artistID, err)
	}
}

// recordFetchFailed appends an ArtistFetchStatusChanged fact for a failed
// scrape so the stream tracks the PocketBase fetch_status.
func (l *eventLog) recordFetchFailed(ctx context.Context, artistID, name, spotifyID, genreGroup, listStatus, errMsg string) {
	if l == nil {
		return
	}
	agg, err := l.loadOrCreate(ctx, artistID, name, spotifyID, genreGroup, listStatus)
	if err != nil {
		log.Printf("[events] Warning: loading artist %s: %v", artistID, err)
		return
	}
	if err := agg.SetFetchStatus("failed", errMsg); err != nil {
		log.Printf("[events] Warning: recording fetch failure for %s: %v", artistID, err)
		return
	}
	if err := l.saveAndProject(ctx, agg); err != nil {
		log.Printf("[events] Warning: saving fetch failure for %s: %v", artistID, err)
	}
}

func (l *eventLog) loadOrCreate(ctx context.Context, artistID, name, spotifyID, genreGroup, listStatus string) (*artist.Artist, error) {
	agg, err := l.repo.Load(ctx, artistID)
	if err == nil {
		return agg, nil
	}
	if !errors.Is(err, eventsourcing.ErrStreamNotFound) {
		return nil, fmt.Errorf("loading artist stream %s: %w", artistID, err)
	}
	seed, err := artist.NewArtist(artistID, name, spotifyID, genreGroup, listStatus)
	if err != nil {
		return nil, fmt.Errorf("seeding artist %s: %w", artistID, err)
	}
	return seed, nil
}

func (l *eventLog) saveAndProject(ctx context.Context, agg *artist.Artist) error {
	events, err := l.repo.Save(ctx, agg)
	if err != nil {
		return fmt.Errorf("saving artist stream %s: %w", agg.AggregateID(), err)
	}
	if l.proj != nil && len(events) > 0 {
		if err := l.proj.Project(ctx, agg, events); err != nil {
			return fmt.Errorf("projecting artist %s: %w", agg.AggregateID(), err)
		}
	}
	return nil
}
