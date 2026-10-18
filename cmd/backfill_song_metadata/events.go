package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"github.com/pocketbase/pocketbase"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"
	"ListenLedger/internal/storeutil"
	_ "ListenLedger/migrations"
)

// loadSongsMissingMetadata reads the songs read model for rows TIDAL could
// fill: missing album, release date, release year, or release type. Songs
// without a title cannot be searched and stay out of scope.
func loadSongsMissingMetadata(ctx context.Context, database *toolbeltdb.Database, limit int) ([]songRow, error) {
	query := `SELECT id, title, artist_name, album, release_date, release_type, spotify_id, release_year
		FROM songs
		WHERE (album = '' OR release_date = '' OR release_year = 0 OR release_type = '') AND TRIM(title) != ''
		ORDER BY id`
	var rows []songRow
	err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep(query)
		defer func() { _ = stmt.Reset() }()
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				return nil
			}
			if limit > 0 && len(rows) >= limit {
				return nil
			}
			rows = append(rows, songRow{
				id:          stmt.ColumnText(0),
				title:       stmt.ColumnText(1),
				artistName:  stmt.ColumnText(2),
				album:       stmt.ColumnText(3),
				releaseDate: stmt.ColumnText(4),
				releaseType: stmt.ColumnText(5),
				spotifyID:   stmt.ColumnText(6),
				releaseYear: stmt.ColumnInt64(7),
			})
		}
	})
	if err != nil {
		return nil, fmt.Errorf("querying songs: %w", err)
	}
	return rows, nil
}

// loadPocketBaseReleaseTypes reads the release_type labels the PB→ledger
// migration dropped from the PocketBase songs table (data.db in the same
// data dir), keyed by song id — the migration copies PB ids verbatim. A
// missing data.db (PocketBase retired) yields no labels.
func loadPocketBaseReleaseTypes(dataDir string) (map[string]string, error) {
	path := filepath.Join(dataDir, "data.db")
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("checking PocketBase data.db: %w", err)
	}
	conn, err := sqlite.OpenConn(path, sqlite.OpenReadOnly)
	if err != nil {
		return nil, fmt.Errorf("opening PocketBase data.db: %w", err)
	}
	defer func() { _ = conn.Close() }()
	types := make(map[string]string)
	stmt := conn.Prep("SELECT id, release_type FROM songs WHERE TRIM(release_type) != '';")
	defer func() { _ = stmt.Reset() }()
	for {
		hasRow, err := stmt.Step()
		if err != nil {
			return nil, fmt.Errorf("reading PocketBase release types: %w", err)
		}
		if !hasRow {
			return types, nil
		}
		types[stmt.ColumnText(0)] = strings.TrimSpace(stmt.ColumnText(1))
	}
}

// applyResolutions appends a SongMetadataEnriched event for every planned
// update and projects it, returning the applied count and the number of rows
// that failed (per-row errors stay on the resolution for the report — the
// events that did land stand). Rows predating the event log get their stream
// seeded first (same shape as the live handlers), so the enrichment fact is
// replayable like any other.
func applyResolutions(ctx context.Context, dataDir string, database *toolbeltdb.Database, resolutions []resolution) (int, int, error) {
	store, cleanup, err := storeutil.OpenEventStore(ctx, dataDir, database)
	if err != nil {
		return 0, 0, err
	}
	defer cleanup()

	repo := song.NewRepository(store)
	proj := projections.NewCatalogProjection(slog.Default(), database)

	var pbApp *pocketbase.PocketBase
	if _, err := os.Stat(filepath.Join(dataDir, "data.db")); err == nil {
		app := pocketbase.NewWithConfig(pocketbase.Config{DefaultDataDir: dataDir})
		if err := app.Bootstrap(); err != nil {
			slog.Warn("bootstrapping PocketBase mirror failed", "error", err)
		} else {
			pbApp = app
			defer func() {
				if err := pbApp.ClearBootstrap(); err != nil {
					slog.Warn("clearing PocketBase bootstrap state", "error", err)
				}
			}()
		}
	}

	applied := 0
	for i := range resolutions {
		res := &resolutions[i]
		if res.Action != actionUpdate {
			continue
		}
		agg, err := loadOrCreateSongStream(ctx, database, repo, res.row.id)
		if err != nil {
			res.Action = actionSkipError
			res.Note = err.Error()
			continue
		}
		// The event carries one dominant source; a PocketBase-only fill is
		// the only update without a TIDAL candidate behind it.
		source := sourceTidal
		if res.Candidate == nil {
			source = sourcePocketbase
		}
		changed, err := agg.EnrichMetadata(res.Fill.Album, res.Fill.ReleaseDate, res.Fill.ReleaseType, source)
		if err != nil {
			res.Action = actionSkipError
			res.Note = err.Error()
			continue
		}
		if !changed {
			// The stream already holds the merged state. If the read model
			// lags (e.g. an earlier projection failed), re-project the stream.
			if agg.Album != res.row.album || agg.ReleaseDate != res.row.releaseDate ||
				agg.ReleaseYear != res.row.releaseYear || agg.ReleaseType != res.row.releaseType {
				evts, lerr := store.Load(ctx, agg.AggregateID())
				if lerr == nil {
					lerr = proj.Project(ctx, song.StreamTypeSong, evts)
				}
				if lerr != nil {
					res.Action = actionSkipError
					res.Note = fmt.Sprintf("read model lags stream; re-projection failed: %v", lerr)
					continue
				}
			}
			res.Action = actionSkipNoMissing
			continue
		}
		events, err := repo.Save(ctx, agg)
		if err != nil {
			res.Action = actionSkipError
			res.Note = err.Error()
			continue
		}
		if err := proj.Project(ctx, song.StreamTypeSong, events); err != nil {
			res.Action = actionSkipError
			res.Note = fmt.Sprintf("event appended but projection failed: %v", err)
			continue
		}
		if pbApp != nil {
			if err := storeutil.MirrorSongToPB(ctx, pbApp, agg); err != nil {
				slog.Warn("mirroring song to PocketBase failed", "song_id", agg.AggregateID(), "error", err)
			}
		}
		res.Applied = true
		applied++
	}

	failed := 0
	for _, res := range resolutions {
		if res.Action == actionSkipError {
			failed++
		}
	}
	return applied, failed, nil
}

// loadOrCreateSongStream loads the song's event stream, seeding legacy
// read-model rows (songs created before the event log) from the full row
// so replay reproduces it (recent flag, batch position, tidal and duplicate
// links). Same shape as the live handlers: the seed is unsaved, so callers
// apply their change and save once, projecting creation and the change
// together.
func loadOrCreateSongStream(ctx context.Context, database *toolbeltdb.Database, repo *song.Repository, id string) (*song.Song, error) {
	agg, err := repo.Load(ctx, id)
	if !errors.Is(err, eventsourcing.ErrStreamNotFound) {
		return agg, err
	}
	return storeutil.SeedSongFromRow(ctx, database, id)
}
