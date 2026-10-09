package projections_test

import (
	"os"
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

	// Rebuild the read model from the event log: delete then replay.
	stream, err := store.Load(ctx, "al_1")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := database.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("DELETE FROM albums WHERE id = 'al_1';")
		defer func() { _ = stmt.Reset() }()
		_, err := stmt.Step()
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
