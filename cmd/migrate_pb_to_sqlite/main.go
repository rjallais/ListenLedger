package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/artist"
)

func main() {
	var (
		pbDataPath  string
		destDataDir string
		apply       bool
	)

	flag.StringVar(&pbDataPath, "pb-db", "pb_data/data.db", "Path to existing PocketBase data.db")
	flag.StringVar(&destDataDir, "dest-dir", "data", "Destination directory for new SQLite database")
	flag.BoolVar(&apply, "apply", false, "Apply migration changes (defaults to dry-run)")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	ctx := context.Background()

	if _, err := os.Stat(pbDataPath); err != nil {
		logger.Error("PocketBase data.db not found", "path", pbDataPath, "error", err)
		os.Exit(1)
	}

	logger.Info("Starting PocketBase to Pure SQLite migration", "src", pbDataPath, "destDir", destDataDir, "apply", apply)

	// 1. Open Source PocketBase DB (read-only)
	srcConn, err := sqlite.OpenConn(pbDataPath, sqlite.OpenReadOnly)
	if err != nil {
		logger.Error("failed opening source PocketBase db", "error", err)
		os.Exit(1)
	}
	defer func() { _ = srcConn.Close() }()

	// 2. Open Destination SQLite DB with schema & event store migrations
	destDB, err := db.SetupDB(ctx, logger, destDataDir, false)
	if err != nil {
		logger.Error("failed setting up destination database", "error", err)
		os.Exit(1)
	}
	defer func() { _ = destDB.Close() }()

	// 3. Migrate Artists
	migratedArtists := 0
	migratedEvents := 0
	migratedSnapshots := 0

	artistStmt := srcConn.Prep("SELECT id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated FROM artists;")
	defer func() { _ = artistStmt.Reset() }()

	err = destDB.WriteTX(ctx, func(destTx *sqlite.Conn) error {
		insertArtist := destTx.Prep("INSERT OR REPLACE INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = insertArtist.Reset() }()

		insertHist := destTx.Prep("INSERT OR REPLACE INTO artist_listener_history (id, artist_id, version, monthly_listeners, previous_listeners, delta, provider, duration_ms, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = insertHist.Reset() }()

		evtStmt := destTx.Prep("INSERT OR IGNORE INTO events (id, stream_id, stream_type, version, event_type, payload, metadata, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = evtStmt.Reset() }()

		for {
			hasRow, err := artistStmt.Step()
			if err != nil {
				return fmt.Errorf("reading source artist: %w", err)
			}
			if !hasRow {
				break
			}

			id := artistStmt.ColumnText(0)
			name := artistStmt.ColumnText(1)
			spotifyID := artistStmt.ColumnText(2)
			listeners := artistStmt.ColumnInt64(3)
			genreGroup := artistStmt.ColumnText(4)
			listStatus := artistStmt.ColumnText(5)
			fetchStatus := artistStmt.ColumnText(6)
			collectionSongs := artistStmt.ColumnInt64(7)
			totalSongs := artistStmt.ColumnInt64(8)
			lastUpdated := artistStmt.ColumnText(9)
			if lastUpdated == "" {
				lastUpdated = time.Now().UTC().Format(time.RFC3339Nano)
			}

			if genreGroup != "rock_metal" && genreGroup != "everything_else" {
				genreGroup = "everything_else"
			}
			if listStatus == "" {
				listStatus = "waiting"
			}
			if fetchStatus == "" {
				fetchStatus = "idle"
			}

			migratedArtists++

			if apply {
				_ = insertArtist.Reset()
				insertArtist.BindText(1, id)
				insertArtist.BindText(2, name)
				insertArtist.BindText(3, spotifyID)
				insertArtist.BindInt64(4, listeners)
				insertArtist.BindText(5, genreGroup)
				insertArtist.BindText(6, listStatus)
				insertArtist.BindText(7, fetchStatus)
				insertArtist.BindInt64(8, collectionSongs)
				insertArtist.BindInt64(9, totalSongs)
				insertArtist.BindText(10, lastUpdated)
				insertArtist.BindText(11, lastUpdated)

				if _, err := insertArtist.Step(); err != nil {
					return fmt.Errorf("inserting artist %s: %w", id, err)
				}

				// If listeners > 0, create historical snapshot
				if listeners > 0 {
					migratedSnapshots++
					histID := fmt.Sprintf("lh_init_%s", id)
					_ = insertHist.Reset()
					insertHist.BindText(1, histID)
					insertHist.BindText(2, id)
					insertHist.BindInt64(3, 1)
					insertHist.BindInt64(4, listeners)
					insertHist.BindInt64(5, 0)
					insertHist.BindInt64(6, listeners)
					insertHist.BindText(7, "migration:pb")
					insertHist.BindInt64(8, 0)
					insertHist.BindText(9, lastUpdated)
					if _, err := insertHist.Step(); err != nil {
						return fmt.Errorf("inserting history snapshot %s: %w", id, err)
					}
				}

				// Seed Event Store stream
				agg, err := artist.NewArtist(id, name, spotifyID, genreGroup, listStatus)
				if err == nil {
					if listeners > 0 {
						_ = agg.RecordMonthlyListeners(listeners, "migration:pb", 0)
					}
					evts := agg.UncommittedEvents()
					for _, evt := range evts {
						migratedEvents++
						_ = evtStmt.Reset()
						evtStmt.BindText(1, evt.ID)
						evtStmt.BindText(2, evt.StreamID)
						evtStmt.BindText(3, evt.StreamType)
						evtStmt.BindInt64(4, evt.Version)
						evtStmt.BindText(5, evt.EventType)
						evtStmt.BindText(6, string(evt.Payload))
						evtStmt.BindText(7, string(evt.Metadata))
						evtStmt.BindText(8, evt.CreatedAt.Format(time.RFC3339Nano))
						_, _ = evtStmt.Step()
					}
				}
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("error migrating artists", "error", err)
		os.Exit(1)
	}

	// 4. Migrate Albums
	migratedAlbums := 0
	albumStmt := srcConn.Prep("SELECT id, title, artist_name, collection_songs, total_songs, status FROM albums;")
	defer func() { _ = albumStmt.Reset() }()

	err = destDB.WriteTX(ctx, func(destTx *sqlite.Conn) error {
		insertAlbum := destTx.Prep("INSERT OR REPLACE INTO albums (id, title, artist_name, collection_songs, total_songs, status) VALUES (?, ?, ?, ?, ?, ?);")
		defer func() { _ = insertAlbum.Reset() }()

		for {
			hasRow, err := albumStmt.Step()
			if err != nil {
				return fmt.Errorf("reading source album: %w", err)
			}
			if !hasRow {
				break
			}

			id := albumStmt.ColumnText(0)
			title := albumStmt.ColumnText(1)
			artistName := albumStmt.ColumnText(2)
			collectionSongs := albumStmt.ColumnInt64(3)
			totalSongs := albumStmt.ColumnInt64(4)
			status := albumStmt.ColumnText(5)
			if status == "" {
				status = "waiting"
			}

			migratedAlbums++

			if apply {
				_ = insertAlbum.Reset()
				insertAlbum.BindText(1, id)
				insertAlbum.BindText(2, title)
				insertAlbum.BindText(3, artistName)
				insertAlbum.BindInt64(4, collectionSongs)
				insertAlbum.BindInt64(5, totalSongs)
				insertAlbum.BindText(6, status)
				if _, err := insertAlbum.Step(); err != nil {
					return fmt.Errorf("inserting album %s: %w", id, err)
				}
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("error migrating albums", "error", err)
		os.Exit(1)
	}

	// 5. Migrate Songs
	migratedSongs := 0
	songStmt := srcConn.Prep("SELECT id, title, artist_name, is_recent, release_date, recent_batch_seq, recent_batch_pos FROM songs;")
	defer func() { _ = songStmt.Reset() }()

	err = destDB.WriteTX(ctx, func(destTx *sqlite.Conn) error {
		insertSong := destTx.Prep("INSERT OR REPLACE INTO songs (id, title, artist_name, is_recent, release_date, recent_batch_seq, recent_batch_pos) VALUES (?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = insertSong.Reset() }()

		for {
			hasRow, err := songStmt.Step()
			if err != nil {
				return fmt.Errorf("reading source song: %w", err)
			}
			if !hasRow {
				break
			}

			id := songStmt.ColumnText(0)
			title := songStmt.ColumnText(1)
			artistName := songStmt.ColumnText(2)
			isRecent := songStmt.ColumnInt64(3)
			releaseDate := songStmt.ColumnText(4)
			batchSeq := songStmt.ColumnInt64(5)
			batchPos := songStmt.ColumnInt64(6)

			migratedSongs++

			if apply {
				_ = insertSong.Reset()
				insertSong.BindText(1, id)
				insertSong.BindText(2, title)
				insertSong.BindText(3, artistName)
				insertSong.BindInt64(4, isRecent)
				insertSong.BindText(5, releaseDate)
				insertSong.BindInt64(6, batchSeq)
				insertSong.BindInt64(7, batchPos)
				if _, err := insertSong.Step(); err != nil {
					return fmt.Errorf("inserting song %s: %w", id, err)
				}
			}
		}
		return nil
	})
	if err != nil {
		logger.Error("error migrating songs", "error", err)
		os.Exit(1)
	}

	// 6. Summary Report
	modeStr := "DRY-RUN (no changes made, rerun with --apply to commit)"
	if apply {
		modeStr = "APPLIED SUCCESSFULLY"
	}

	logger.Info("Migration completed",
		"status", modeStr,
		"artists", migratedArtists,
		"albums", migratedAlbums,
		"songs", migratedSongs,
		"events_created", migratedEvents,
		"history_snapshots", migratedSnapshots,
	)
}
