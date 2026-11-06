package projections_test

import (
	"os"
	"slices"
	"testing"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"

	"log/slog"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"
)

func setupProjectionDB(t *testing.T) (*toolbeltdb.Database, *eventsourcing.SQLiteStore, *projections.CatalogProjection) {
	t.Helper()
	dir, err := os.MkdirTemp("", "catalog_proj_*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	database, err := db.SetupDB(t.Context(), slog.Default(), dir, true)
	if err != nil {
		t.Fatalf("setup db: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	store := eventsourcing.NewSQLiteStore(database)
	return database, store, projections.NewCatalogProjection(slog.Default(), database)
}

func albumCount(t *testing.T, database *toolbeltdb.Database, id string) (status string, colSongs int64) {
	t.Helper()
	err := database.ReadTX(t.Context(), func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status, collection_songs FROM albums WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			status = stmt.ColumnText(0)
			colSongs = stmt.ColumnInt64(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read album: %v", err)
	}
	return status, colSongs
}

func TestCatalogProjectionAlbumAndReplay(t *testing.T) {
	database, store, proj := setupProjectionDB(t)
	ctx := t.Context()

	agg, err := album.NewAlbum("al_1", "Dirt", "Alice in Chains", "waiting", 0, 13)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := agg.ChangeStatus("full"); err != nil {
		t.Fatalf("status: %v", err)
	}
	events := agg.UncommittedEvents()
	if err := store.Append(ctx, "al_1", 0, events...); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := proj.Project(ctx, album.StreamTypeAlbum, events); err != nil {
		t.Fatalf("project: %v", err)
	}

	status, _ := albumCount(t, database, "al_1")
	if status != "full" {
		t.Fatalf("projected status %q", status)
	}

	// Rebuild the read model from the event log: delete then replay. The
	// position goes with the row: a live position tells Project the row is
	// already current and the replay would be skipped.
	stream, err := store.Load(ctx, "al_1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("DELETE FROM albums WHERE id = 'al_1';")
		if _, err := stmt.Step(); err != nil {
			_ = stmt.Reset()
			return err
		}
		_ = stmt.Reset()
		stmt = tx.Prep("DELETE FROM read_model_positions WHERE stream_id = 'al_1';")
		_, err := stmt.Step()
		_ = stmt.Reset()
		return err
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if err := proj.Project(ctx, album.StreamTypeAlbum, stream); err != nil {
		t.Fatalf("replay: %v", err)
	}

	rebuilt, _ := albumCount(t, database, "al_1")
	if rebuilt != "full" {
		t.Fatalf("replayed status %q", rebuilt)
	}
}

func TestCatalogProjectionSongRecentToggle(t *testing.T) {
	database, store, proj := setupProjectionDB(t)
	ctx := t.Context()

	agg, err := song.NewSong("sg_1", "Rooster", "Alice in Chains", "Dirt", "1992-09-29", "album", "", 1992, 3, 1, true)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	events := agg.UncommittedEvents()
	if err := store.Append(ctx, "sg_1", 0, events...); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := proj.Project(ctx, song.StreamTypeSong, events); err != nil {
		t.Fatalf("project: %v", err)
	}

	var isRecent int64
	if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT is_recent FROM songs WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, "sg_1")
		_, err := stmt.Step()
		isRecent = stmt.ColumnInt64(0)
		return err
	}); err != nil {
		t.Fatalf("read song: %v", err)
	}
	if isRecent != 1 {
		t.Fatalf("expected recent song, got %d", isRecent)
	}
}

func TestCatalogProjectionSongMetadataEnriched(t *testing.T) {
	database, store, proj := setupProjectionDB(t)
	ctx := t.Context()

	agg, err := song.NewSong("sg_2", "Them Bones", "Alice in Chains", "", "", "", "", 0, 3, 1, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.Append(ctx, "sg_2", 0, agg.UncommittedEvents()...); err != nil {
		t.Fatalf("append created: %v", err)
	}
	if err := proj.Project(ctx, song.StreamTypeSong, agg.UncommittedEvents()); err != nil {
		t.Fatalf("project created: %v", err)
	}

	changed, err := agg.EnrichMetadata("Dirt", "1992-09-29", "album", "tidal")
	if err != nil || !changed {
		t.Fatalf("enrich: changed=%v err=%v", changed, err)
	}
	enriched := agg.UncommittedEvents()[1:]
	if err := store.Append(ctx, "sg_2", 1, enriched...); err != nil {
		t.Fatalf("append enriched: %v", err)
	}
	if err := proj.Project(ctx, song.StreamTypeSong, enriched); err != nil {
		t.Fatalf("project enriched: %v", err)
	}

	readRow := func() (album, releaseDate, releaseType string, releaseYear int64) {
		t.Helper()
		if err := database.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep("SELECT album, release_date, release_type, release_year FROM songs WHERE id = ?;")
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, "sg_2")
			if _, err := stmt.Step(); err != nil {
				return err
			}
			album, releaseDate, releaseType, releaseYear = stmt.ColumnText(0), stmt.ColumnText(1), stmt.ColumnText(2), stmt.ColumnInt64(3)
			return nil
		}); err != nil {
			t.Fatalf("read song: %v", err)
		}
		return album, releaseDate, releaseType, releaseYear
	}

	album, releaseDate, releaseType, releaseYear := readRow()
	if album != "Dirt" || releaseDate != "1992-09-29" || releaseYear != 1992 || releaseType != "album" {
		t.Fatalf("enriched row = (%q, %q, %q, %d)", album, releaseDate, releaseType, releaseYear)
	}

	// A pre-field (v1) event without release_type keeps the projected type.
	v1 := eventsourcing.Event{
		StreamID:   "sg_2",
		StreamType: song.StreamTypeSong,
		Version:    3,
		EventType:  song.EventTypeSongMetadataEnriched,
		Payload:    []byte(`{"song_id":"sg_2","album":"Dirt","release_date":"1992-09-29","release_year":1992,"source":"tidal"}`),
	}
	if err := proj.Project(ctx, song.StreamTypeSong, []eventsourcing.Event{v1}); err != nil {
		t.Fatalf("project v1: %v", err)
	}
	if album, _, releaseType, _ := readRow(); releaseType != "album" {
		t.Fatalf("v1 projection wiped release_type: %q (album %q)", releaseType, album)
	}
}

func TestCatalogProjectionSongTidalEvents(t *testing.T) {
	database, store, proj := setupProjectionDB(t)
	ctx := t.Context()

	agg, err := song.NewSong("sg_t1", "Cais", "Milton Nascimento, ...", "Clube da Esquina - Single", "1972-01-01", "single", "spot_t1", 1972, 0, 0, false)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := store.Append(ctx, "sg_t1", 0, agg.UncommittedEvents()...); err != nil {
		t.Fatalf("append created: %v", err)
	}
	if err := proj.Project(ctx, song.StreamTypeSong, agg.UncommittedEvents()); err != nil {
		t.Fatalf("project created: %v", err)
	}

	// Link Tidal ID
	if _, err := agg.LinkTidalID("tidal_cais", "fuzzy", 0.95); err != nil {
		t.Fatalf("link tidal: %v", err)
	}
	// Correct metadata
	if _, err := agg.CorrectMetadata("Clube da Esquina", "1972-03-01", "album", "tidal"); err != nil {
		t.Fatalf("correct metadata: %v", err)
	}
	// Correct artist credits
	if _, err := agg.CorrectArtistCredits("Milton Nascimento, Lô Borges", "tidal"); err != nil {
		t.Fatalf("correct credits: %v", err)
	}
	// Mark duplicate
	if _, err := agg.MarkDuplicate("sg_canonical_cais", "tidal"); err != nil {
		t.Fatalf("mark duplicate: %v", err)
	}

	newEvents := agg.UncommittedEvents()[1:]
	if err := store.Append(ctx, "sg_t1", 1, newEvents...); err != nil {
		t.Fatalf("append: %v", err)
	}
	if err := proj.Project(ctx, song.StreamTypeSong, newEvents); err != nil {
		t.Fatalf("project: %v", err)
	}

	var tidalID, albumName, artistName, duplicateOf string
	err = database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT tidal_id, album, artist_name, duplicate_of FROM songs WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, "sg_t1")
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatalf("row not found")
		}
		tidalID = stmt.ColumnText(0)
		albumName = stmt.ColumnText(1)
		artistName = stmt.ColumnText(2)
		duplicateOf = stmt.ColumnText(3)
		return nil
	})
	if err != nil {
		t.Fatalf("readTX: %v", err)
	}

	if tidalID != "tidal_cais" {
		t.Errorf("expected tidal_id 'tidal_cais', got %q", tidalID)
	}
	if albumName != "Clube da Esquina" {
		t.Errorf("expected album 'Clube da Esquina', got %q", albumName)
	}
	if artistName != "Milton Nascimento, Lô Borges" {
		t.Errorf("expected artist 'Milton Nascimento, Lô Borges', got %q", artistName)
	}
	if duplicateOf != "sg_canonical_cais" {
		t.Errorf("expected duplicate_of 'sg_canonical_cais', got %q", duplicateOf)
	}
}

// TestCatalogProjectionHandlesAllEventTypes keeps both catalog switches in
// step with the album and song event contracts: a projection that does not
// know an event type would skip it silently, leaving the read model behind
// the log.
func TestCatalogProjectionHandlesAllEventTypes(t *testing.T) {
	ctx := t.Context()
	_, _, proj := setupProjectionDB(t)

	albumAgg, err := album.NewAlbum("al_contract", "Contract", "Artist", "waiting", 0, 0)
	if err != nil {
		t.Fatalf("NewAlbum: %v", err)
	}
	if err := albumAgg.ChangeStatus("full"); err != nil {
		t.Fatalf("ChangeStatus: %v", err)
	}
	if err := albumAgg.AdjustSongCounts(1, 2); err != nil {
		t.Fatalf("AdjustSongCounts: %v", err)
	}
	albumEvents := albumAgg.UncommittedEvents()
	if err := proj.Project(ctx, album.StreamTypeAlbum, albumEvents); err != nil {
		t.Fatalf("Project(album events): %v", err)
	}
	wantAlbumTypes := []string{
		album.EventTypeAlbumCreated,
		album.EventTypeAlbumSongCountsAdjusted,
		album.EventTypeAlbumStatusChanged,
	}
	if got := eventsourcing.EventTypesOf(albumEvents); !slices.Equal(got, wantAlbumTypes) {
		t.Fatalf("album walk recorded %v, want %v", got, wantAlbumTypes)
	}

	// Metadata starts empty so enrichment has something to fill.
	songAgg, err := song.NewSong("sg_contract", "Contract", "Artist", "", "", "", "sp_contract", 0, 0, 0, false)
	if err != nil {
		t.Fatalf("NewSong: %v", err)
	}
	if err := songAgg.SetRecent(true, 1, 1); err != nil {
		t.Fatalf("SetRecent: %v", err)
	}
	if _, err := songAgg.EnrichMetadata("Contract Album", "2020-06-06", "album", "contract"); err != nil {
		t.Fatalf("EnrichMetadata: %v", err)
	}
	if _, err := songAgg.LinkTidalID("tidal_contract", "exact", 0.9); err != nil {
		t.Fatalf("LinkTidalID: %v", err)
	}
	if _, err := songAgg.CorrectMetadata("Corrected Album", "2021-07-07", "single", "contract"); err != nil {
		t.Fatalf("CorrectMetadata: %v", err)
	}
	if _, err := songAgg.CorrectArtistCredits("Other Artist", "contract"); err != nil {
		t.Fatalf("CorrectArtistCredits: %v", err)
	}
	if _, err := songAgg.MarkDuplicate("sg_canonical_contract", "contract"); err != nil {
		t.Fatalf("MarkDuplicate: %v", err)
	}
	songEvents := songAgg.UncommittedEvents()
	if err := proj.Project(ctx, song.StreamTypeSong, songEvents); err != nil {
		t.Fatalf("Project(song events): %v", err)
	}
	wantSongTypes := []string{
		song.EventTypeRecentFlagChanged,
		song.EventTypeSongArtistCreditsCorrected,
		song.EventTypeSongCreated,
		song.EventTypeSongMarkedDuplicate,
		song.EventTypeSongMetadataCorrected,
		song.EventTypeSongMetadataEnriched,
		song.EventTypeTidalIDLinked,
	}
	if got := eventsourcing.EventTypesOf(songEvents); !slices.Equal(got, wantSongTypes) {
		t.Fatalf("song walk recorded %v, want %v", got, wantSongTypes)
	}

	unknown, err := eventsourcing.NewEvent("sg_contract", song.StreamTypeSong, int64(len(songEvents)+1), "SongNotAnEvent", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("NewEvent(unknown): %v", err)
	}
	if err := proj.Project(ctx, song.StreamTypeSong, []eventsourcing.Event{unknown}); err == nil {
		t.Fatal("Project(unknown event type) = nil, want an error")
	}
}
