package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/song"
	"ListenLedger/internal/eventsourcing"

	"github.com/go-chi/chi/v5"
	"zombiezen.com/go/sqlite"
)

// TestHandlers_CatalogEventSourcingAndReplay exercises the user path: create an
// album and toggle a song's recent flag over HTTP, then proves the read models
// can be rebuilt from the event log alone.
func TestHandlers_CatalogEventSourcingAndReplay(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := context.Background()

	// Seed a song directly (legacy row, no stream yet).
	if err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO songs (id, title, artist_name, album, release_date, release_year, is_recent) VALUES ('sg_seed', 'Battery', 'Metallica', 'Master of Puppets', '1986-03-03', 1986, 0);")
		_, err := stmt.Step()
		return err
	}); err != nil {
		t.Fatalf("seed song: %v", err)
	}

	r := chi.NewRouter()
	r.Post("/api/albums", h.HandleCreateAlbum)
	r.Post("/api/songs/{songId}/recent/{value}", h.HandleUpdateSongRecent)

	// 1. Create an album over HTTP.
	form := url.Values{"title": {"Powerslave"}, "artist_name": {"Iron Maiden"}, "status": {"waiting"}, "collection_songs": {"0"}, "total_songs": {"8"}}
	req := httptest.NewRequest(http.MethodPost, "/api/albums", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create album: got %d: %s", rec.Code, rec.Body.String())
	}

	// Find the created album ID from the read model.
	var albumID string
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT id FROM albums WHERE title = 'Powerslave';")
		defer func() { _ = stmt.Reset() }()
		hasRow, err := stmt.Step()
		if hasRow {
			albumID = stmt.ColumnText(0)
		}
		return err
	})
	if err != nil || albumID == "" {
		t.Fatalf("album read model missing: %v (id=%q)", err, albumID)
	}

	// 2. Toggle the seeded song's recent flag over HTTP.
	reqRecent := httptest.NewRequest(http.MethodPost, "/api/songs/sg_seed/recent/true", nil)
	recRecent := httptest.NewRecorder()
	r.ServeHTTP(recRecent, reqRecent)
	if recRecent.Code != http.StatusOK {
		t.Fatalf("recent toggle: got %d: %s", recRecent.Code, recRecent.Body.String())
	}

	// 3. Both streams exist in the event store.
	for _, tc := range []struct{ streamType, prefix string }{{album.StreamTypeAlbum, "al_"}, {song.StreamTypeSong, "sg_"}} {
		stream, err := h.albumStream(ctx, tc.prefix)
		if err != nil || len(stream) == 0 {
			t.Fatalf("expected %s events, err=%v len=%d", tc.streamType, err, len(stream))
		}
	}

	// 4. Wipe the read models and rebuild them from the event log only.
	if err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("DELETE FROM albums WHERE id = ?;")
		stmt.BindText(1, albumID)
		if _, err := stmt.Step(); err != nil {
			return err
		}
		stmt = tx.Prep("DELETE FROM songs WHERE id = 'sg_seed';")
		if _, err := stmt.Step(); err != nil {
			return err
		}
		return nil
	}); err != nil {
		t.Fatalf("wipe read models: %v", err)
	}

	rawAlbumStream, err := h.albumStream(ctx, "al_")
	if err != nil {
		t.Fatalf("load album stream: %v", err)
	}
	if err := h.catalogProjection.Project(ctx, album.StreamTypeAlbum, rawAlbumStream); err != nil {
		t.Fatalf("replay album: %v", err)
	}
	songStream, err := h.albumSongStream(ctx, "sg_seed")
	if err != nil {
		t.Fatalf("load song stream: %v", err)
	}
	if err := h.catalogProjection.Project(ctx, song.StreamTypeSong, songStream); err != nil {
		t.Fatalf("replay song: %v", err)
	}

	// 5. Read models match what the user actions produced.
	var status string
	if err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status FROM albums WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, albumID)
		hasRow, err := stmt.Step()
		if hasRow {
			status = stmt.ColumnText(0)
		}
		return err
	}); err != nil {
		t.Fatalf("read rebuilt album: %v", err)
	}
	if status != "waiting" {
		t.Fatalf("rebuilt album status=%s, want waiting", status)
	}

	var isRecent int64
	if err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT is_recent FROM songs WHERE id = 'sg_seed';")
		defer func() { _ = stmt.Reset() }()
		hasRow, _ := stmt.Step()
		if hasRow {
			isRecent = stmt.ColumnInt64(0)
		}
		return nil
	}); err != nil {
		t.Fatalf("read rebuilt song: %v", err)
	}
	if isRecent != 1 {
		t.Fatalf("rebuilt song is_recent=%d, want 1", isRecent)
	}

	// 6. History correction: rewrite the album's create event in place and replay.
	// Replace is gated to offline repair (LISTENLEDGER_ALLOW_EVENT_REWRITE=1).
	t.Setenv("LISTENLEDGER_ALLOW_EVENT_REWRITE", "1")
	stream, err := h.albumStream(ctx, "al_")
	if err != nil {
		t.Fatalf("reload album stream: %v", err)
	}
	corrected, err := eventsourcing.NewEvent(albumID, album.StreamTypeAlbum, stream[0].Version,
		album.EventTypeAlbumCreated, album.CreatedPayload{
			ID: albumID, Title: "Powerslave", ArtistName: "Iron Maiden",
			Status: "full", TotalSongs: 8,
		}, nil)
	if err != nil {
		t.Fatalf("corrected event: %v", err)
	}
	stream[0] = corrected
	if err := h.store.Replace(ctx, albumID, stream); err != nil {
		t.Fatalf("replace stream: %v", err)
	}
	if err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("DELETE FROM albums WHERE id = ?;")
		stmt.BindText(1, albumID)
		_, err := stmt.Step()
		return err
	}); err != nil {
		t.Fatalf("delete for re-replay: %v", err)
	}
	if err := h.catalogProjection.Project(ctx, album.StreamTypeAlbum, stream); err != nil {
		t.Fatalf("re-replay: %v", err)
	}
	if err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status FROM albums WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, albumID)
		hasRow, _ := stmt.Step()
		if hasRow {
			status = stmt.ColumnText(0)
		}
		return nil
	}); err != nil {
		t.Fatalf("read corrected album: %v", err)
	}
	if status != "full" {
		t.Fatalf("corrected album status=%s, want full", status)
	}
}

// albumStream loads raw events for the first stream with the given ID prefix.
func (h *Handler) albumStream(ctx context.Context, prefix string) ([]eventsourcing.Event, error) {
	ids, err := h.store.StreamIDs(ctx)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		if strings.HasPrefix(id, prefix) {
			return h.store.Load(ctx, id)
		}
	}
	return nil, nil
}

// albumSongStream loads raw events for a specific song stream.
func (h *Handler) albumSongStream(ctx context.Context, songID string) ([]eventsourcing.Event, error) {
	return h.store.Load(ctx, songID)
}
