package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/pocketbase/pocketbase"
	"github.com/pocketbase/pocketbase/core"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/appdir"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/messaging"
	"ListenLedger/internal/projections"
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

// applyResolutions appends SongMetadataEnriched events for every update
// resolution and projects them into the SQLite read model. Rows predating
// the event log get their stream seeded first (same shape as the live
// handlers), so the enrichment fact is replayable like any other.
// applyResolutions appends a SongMetadataEnriched event for every planned
// update and projects it, returning the applied count and the number of rows
// that failed (per-row errors stay on the resolution for the report — the
// events that did land stand).
func applyResolutions(ctx context.Context, dataDir string, database *toolbeltdb.Database, resolutions []resolution) (int, int, error) {
	store, cleanup, err := openEventStore(ctx, dataDir, database)
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
		agg, err := loadOrCreateSongStream(ctx, repo, res.row)
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
			// The stream already knows more than the stale row we planned
			// from; converged, nothing to record.
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
			if err := mirrorSongToPB(pbApp, agg); err != nil {
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

// openEventStore builds the authoritative store for the current mode. In
// jetstream mode it boots an embedded server on the data dir's NATS store
// (never ensuring stream config — the app owns that); in sqlite mode it
// refuses to run after the one-way flip, which would fork the frozen log.
func openEventStore(ctx context.Context, dataDir string, database *toolbeltdb.Database) (eventsourcing.Store, func(), error) {
	mode, err := eventsourcing.SelectedStoreName()
	if err != nil {
		return nil, nil, err
	}
	sqliteStore := eventsourcing.NewSQLiteStore(database)
	marked, err := sqliteStore.GetCheckpoint(ctx, eventsourcing.CheckpointJetStreamAuthority)
	if err != nil {
		return nil, nil, fmt.Errorf("reading jetstream authority marker: %w", err)
	}
	if mode == eventsourcing.EventStoreJetStream {
		// Pre-flip, JetStream mirrors SQLite via the outbox relay; writing
		// JetStream directly would put it ahead of the log and fail the
		// parity gate at flip time. Only post-flip runs may append here.
		if marked == 0 {
			return nil, nil, fmt.Errorf("%s=%q but the flip has not happened (no authority marker); appending now would break the parity gate — run in %s=%q mode until the flip",
				eventsourcing.EventStoreEnv, eventsourcing.EventStoreJetStream, eventsourcing.EventStoreEnv, eventsourcing.EventStoreSQLite)
		}
		js, cleanup, err := openStoreJetStream(ctx, dataDir)
		if err != nil {
			return nil, nil, err
		}
		store, err := eventsourcing.SelectedStore(database, js)
		if err != nil {
			cleanup()
			return nil, nil, err
		}
		return store, cleanup, nil
	}

	// Post-flip, the SQLite log is frozen: appending would fork it with
	// versions JetStream already holds.
	if marked != 0 {
		return nil, nil, fmt.Errorf("jetstream authority already established; writing the frozen SQLite log would fork it (run with %s=%q)",
			eventsourcing.EventStoreEnv, eventsourcing.EventStoreJetStream)
	}
	return sqliteStore, func() {}, nil
}

// openStoreJetStream boots an embedded NATS server on the data dir's
// existing NATS store and returns a JetStream context over DOMAIN_EVENTS.
// Same rule as every CLI that touches the store: the live app must be
// stopped first — two servers on one store dir corrupt it.
func openStoreJetStream(ctx context.Context, dataDir string) (jetstream.JetStream, func(), error) {
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1, NoSigs: true, NoLog: true,
		JetStream: true, StoreDir: filepath.Join(dataDir, "nats"),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("starting embedded NATS: %w", err)
	}
	go ns.Start()
	cleanup := func() { ns.Shutdown() }
	if !ns.ReadyForConnections(10 * time.Second) {
		cleanup()
		return nil, nil, fmt.Errorf("embedded NATS not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		cleanup()
		return nil, nil, fmt.Errorf("connecting embedded NATS: %w", err)
	}
	cleanupBoth := func() { nc.Close(); ns.Shutdown() }
	js, err := messaging.NewJetStream(nc)
	if err != nil {
		cleanupBoth()
		return nil, nil, fmt.Errorf("creating JetStream context: %w", err)
	}
	if _, err := js.Stream(ctx, messaging.DomainEventsStreamName); err != nil {
		cleanupBoth()
		return nil, nil, fmt.Errorf("DOMAIN_EVENTS stream: %w", err)
	}
	return js, cleanupBoth, nil
}

// loadOrCreateSongStream loads the song's event stream, seeding legacy
// read-model rows (songs created before the event log) with a SongCreated
// fact first — same shape as the live handlers' loadOrCreateSong.
func loadOrCreateSongStream(ctx context.Context, repo *song.Repository, row songRow) (*song.Song, error) {
	agg, err := repo.Load(ctx, row.id)
	if !errors.Is(err, eventsourcing.ErrStreamNotFound) {
		return agg, err
	}
	seed, err := song.NewSong(row.id, row.title, row.artistName, row.album,
		row.releaseDate, row.releaseType, row.spotifyID, row.releaseYear, 0, 0, false)
	if err != nil {
		return nil, err
	}
	if _, err := repo.Save(ctx, seed); err != nil {
		return nil, err
	}
	return repo.Load(ctx, row.id)
}

// isLiveDataDir reports whether dataDir is the live default (pb_data via
// PB_DATA_DIR or repo root) rather than an explicit staging/backup copy.
// Paths are cleaned to absolute form so relative paths and trailing slashes
// cannot bypass the --apply guard.
func isLiveDataDir(dataDir string) bool {
	abs, err := absPath(dataDir)
	if err != nil {
		return true
	}
	liveAbs, err := absPath(appdir.ResolveDataDir())
	if err != nil {
		return true
	}
	return abs == liveAbs
}

func absPath(path string) (string, error) {
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", err
	}
	// Resolve symlinks (e.g. /tmp on some systems) before comparing, so
	// equivalent paths cannot bypass the guard. Unresolvable paths
	// (not-yet-created staging dirs) fall back to lexical form.
	if resolved, err := filepath.EvalSymlinks(abs); err == nil {
		abs = resolved
	}
	return abs, nil
}

func mirrorSongToPB(app *pocketbase.PocketBase, songAgg *song.Song) error {
	if app == nil || songAgg == nil {
		return nil
	}
	record, err := app.FindRecordById("songs", songAgg.AggregateID())
	if err != nil {
		collection, err := app.FindCollectionByNameOrId("songs")
		if err != nil {
			return fmt.Errorf("finding songs collection: %w", err)
		}
		record = core.NewRecord(collection)
		record.Id = songAgg.AggregateID()
	}
	record.Set("title", songAgg.Title)
	record.Set("artist_name", songAgg.ArtistName)
	record.Set("album", songAgg.Album)
	record.Set("release_date", songAgg.ReleaseDate)
	record.Set("release_year", songAgg.ReleaseYear)
	record.Set("release_type", songAgg.ReleaseType)
	if songAgg.SpotifyID != "" {
		record.Set("spotify_id", songAgg.SpotifyID)
	}
	record.Set("is_recent", songAgg.IsRecent)
	record.Set("tidal_id", songAgg.TidalID)
	record.Set("duplicate_of", songAgg.DuplicateOf)
	return app.Save(record)
}
