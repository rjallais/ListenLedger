package handlers

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"

	"zombiezen.com/go/sqlite"

	"ListenLedger/config"
	"ListenLedger/internal/db"
	"ListenLedger/internal/projections"
)

func setupTestSQLiteDB(t *testing.T) (*Handler, string) {
	t.Helper()
	tempDir, err := os.MkdirTemp("", "handler_sqlite_test_*")
	if err != nil {
		t.Fatalf("failed creating temp dir: %v", err)
	}

	testDB, err := db.SetupDB(context.Background(), slog.Default(), tempDir, true)
	if err != nil {
		_ = os.RemoveAll(tempDir)
		t.Fatalf("failed setting up test DB: %v", err)
	}

	staticDir := "static"
	if _, err := os.Stat(staticDir); err != nil {
		if _, err := os.Stat("../../static"); err == nil {
			staticDir = "../../static"
		}
	}

	cfg := &config.Config{
		StaticDir: staticDir,
	}

	h := New(nil, nil, nil, cfg, WithDatabase(testDB))
	return h, tempDir
}

func TestHandlers_SQLiteArtistQueries(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	ctx := context.Background()

	// Seed artists directly into SQLite read model and history
	err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO artists (id, name, spotify_id, monthly_listeners, genre_group, list_status, fetch_status, collection_songs, total_songs, last_updated) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = stmt.Reset() }()

		artistsData := [][]any{
			{"art_1", "Iron Maiden", "sp_1", int64(12000000), "rock_metal", "included", "idle", int64(50), int64(100), "2026-09-20 12:00:00"},
			{"art_2", "Metallica", "sp_2", int64(24000000), "rock_metal", "included", "idle", int64(80), int64(100), "2026-09-20 12:00:00"},
			{"art_3", "Dua Lipa", "sp_3", int64(65000000), "everything_else", "included", "idle", int64(20), int64(50), "2026-09-20 12:00:00"},
			{"art_4", "Waiting Band", "sp_4", int64(500000), "rock_metal", "waiting", "idle", int64(5), int64(5), "2026-09-20 12:00:00"},
		}

		for _, row := range artistsData {
			_ = stmt.Reset()
			stmt.BindText(1, row[0].(string))
			stmt.BindText(2, row[1].(string))
			stmt.BindText(3, row[2].(string))
			stmt.BindInt64(4, row[3].(int64))
			stmt.BindText(5, row[4].(string))
			stmt.BindText(6, row[5].(string))
			stmt.BindText(7, row[6].(string))
			stmt.BindInt64(8, row[7].(int64))
			stmt.BindInt64(9, row[8].(int64))
			stmt.BindText(10, row[9].(string))
			if _, err := stmt.Step(); err != nil {
				return err
			}
		}

		// Seed listener history
		histStmt := tx.Prep("INSERT INTO artist_listener_history (id, artist_id, version, monthly_listeners, previous_listeners, delta, provider, duration_ms, scraped_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = histStmt.Reset() }()
		histData := [][]any{
			{"lh_1", "art_1", int64(1), int64(10000000), int64(0), int64(10000000), "browserless", int64(350), "2026-09-10T10:00:00Z"},
			{"lh_2", "art_1", int64(2), int64(12000000), int64(10000000), int64(2000000), "browserless", int64(280), "2026-09-20T10:00:00Z"},
		}
		for _, row := range histData {
			_ = histStmt.Reset()
			histStmt.BindText(1, row[0].(string))
			histStmt.BindText(2, row[1].(string))
			histStmt.BindInt64(3, row[2].(int64))
			histStmt.BindInt64(4, row[3].(int64))
			histStmt.BindInt64(5, row[4].(int64))
			histStmt.BindInt64(6, row[5].(int64))
			histStmt.BindText(7, row[6].(string))
			histStmt.BindInt64(8, row[7].(int64))
			histStmt.BindText(9, row[8].(string))
			if _, err := histStmt.Step(); err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		t.Fatalf("failed seeding test data: %v", err)
	}

	// 1. Test countArtistsByGenreExcludingWaiting
	rockCount, err := h.countArtistsByGenreExcludingWaiting(ctx, "rock_metal")
	if err != nil {
		t.Fatalf("countArtistsByGenreExcludingWaiting failed: %v", err)
	}
	if rockCount != 2 {
		t.Fatalf("expected rock_metal count 2, got %d", rockCount)
	}

	// 2. Test countWaitingArtists
	waitCount, err := h.countWaitingArtists(ctx)
	if err != nil {
		t.Fatalf("countWaitingArtists failed: %v", err)
	}
	if waitCount != 1 {
		t.Fatalf("expected waiting count 1, got %d", waitCount)
	}

	// 3. Test fetchArtistGenrePage
	artists, total, err := h.fetchArtistGenrePage(ctx, "rock_metal", 1, 10)
	if err != nil {
		t.Fatalf("fetchArtistGenrePage failed: %v", err)
	}
	if total != 2 || len(artists) != 2 {
		t.Fatalf("expected 2 artists, got total %d, slice %d", total, len(artists))
	}
	if artists[0].Name != "Metallica" {
		t.Errorf("expected first artist Metallica (24M listeners), got %s", artists[0].Name)
	}
	if artists[1].Name != "Iron Maiden" {
		t.Errorf("expected second artist Iron Maiden (12M listeners), got %s", artists[1].Name)
	}

	// 4. Test HandleArtistListenerHistory endpoint
	r := h.Routes()
	req := httptest.NewRequest(http.MethodGet, "/api/artists/art_1/history", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 from history endpoint, got %d: %s", rec.Code, rec.Body.String())
	}
	var history []projections.ListenerSnapshot
	if err := json.Unmarshal(rec.Body.Bytes(), &history); err != nil {
		t.Fatalf("failed decoding history JSON: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 history snapshots, got %d", len(history))
	}
	if history[0].MonthlyListeners != 12000000 || history[0].Delta != 2000000 {
		t.Errorf("unexpected latest snapshot: %+v", history[0])
	}

	// 5. Test HandleArtistListenerHistoryDrawer endpoint
	reqDrawer := httptest.NewRequest(http.MethodGet, "/api/artists/art_1/history/drawer", nil)
	recDrawer := httptest.NewRecorder()
	r.ServeHTTP(recDrawer, reqDrawer)

	if recDrawer.Code != http.StatusOK {
		t.Fatalf("expected status 200 from drawer endpoint, got %d: %s", recDrawer.Code, recDrawer.Body.String())
	}
	bodyStr := recDrawer.Body.String()
	if !strings.Contains(bodyStr, "artist-history-dialog") {
		t.Errorf("expected drawer dialog in response, got %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "Iron Maiden") {
		t.Errorf("expected artist name Iron Maiden in drawer, got %s", bodyStr)
	}
	if !strings.Contains(bodyStr, "sparkline-svg") {
		t.Errorf("expected sparkline SVG in drawer, got %s", bodyStr)
	}

	// 6. Test HandleArtistListenerHistoryClose endpoint
	reqClose := httptest.NewRequest(http.MethodGet, "/api/artists/history/close", nil)
	recClose := httptest.NewRecorder()
	r.ServeHTTP(recClose, reqClose)

	if recClose.Code != http.StatusOK {
		t.Fatalf("expected status 200 from close endpoint, got %d: %s", recClose.Code, recClose.Body.String())
	}
	if !strings.Contains(recClose.Body.String(), `id="artist-history-drawer-container"`) {
		t.Errorf("expected empty drawer container in close response, got %s", recClose.Body.String())
	}
}

func TestHandlers_SQLiteAlbumQueries(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	ctx := context.Background()

	// Seed albums into SQLite
	err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO albums (id, title, artist_name, collection_songs, total_songs, status) VALUES (?, ?, ?, ?, ?, ?);")
		defer func() { _ = stmt.Reset() }()

		albumsData := [][]any{
			{"alb_1", "Master of Puppets", "Metallica", int64(8), int64(8), "full"},
			{"alb_2", "Ride the Lightning", "Metallica", int64(6), int64(8), "processed_once"},
			{"alb_3", "Kill 'Em All", "Metallica", int64(2), int64(10), "waiting"},
		}

		for _, row := range albumsData {
			_ = stmt.Reset()
			stmt.BindText(1, row[0].(string))
			stmt.BindText(2, row[1].(string))
			stmt.BindText(3, row[2].(string))
			stmt.BindInt64(4, row[3].(int64))
			stmt.BindInt64(5, row[4].(int64))
			stmt.BindText(6, row[5].(string))
			if _, err := stmt.Step(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed seeding album test data: %v", err)
	}

	r := h.Routes()

	// 1. Test HandleAlbums HTML view
	req := httptest.NewRequest(http.MethodGet, "/albums", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 from /albums, got %d", rec.Code)
	}

	// 2. Test HandleAlbumsAPI for "full"
	reqAPI := httptest.NewRequest(http.MethodGet, "/api/albums/full", nil)
	recAPI := httptest.NewRecorder()
	r.ServeHTTP(recAPI, reqAPI)

	if recAPI.Code != http.StatusOK {
		t.Fatalf("expected status 200 from /api/albums/full, got %d", recAPI.Code)
	}
	body := recAPI.Body.String()
	if !strings.Contains(body, "Master of Puppets") {
		t.Errorf("expected body to contain 'Master of Puppets', got: %s", body)
	}

	// 3. Test HandleCreateAlbum with JSON header
	formData := url.Values{
		"title":            {"Powerslave"},
		"artist_name":      {"Iron Maiden"},
		"status":           {"full"},
		"collection_songs": {"8"},
		"total_songs":      {"8"},
	}
	reqCreate := httptest.NewRequest(http.MethodPost, "/api/albums", strings.NewReader(formData.Encode()))
	reqCreate.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	reqCreate.Header.Set("Accept", "application/json")
	recCreate := httptest.NewRecorder()
	r.ServeHTTP(recCreate, reqCreate)

	if recCreate.Code != http.StatusCreated {
		t.Fatalf("expected status 201 from create album, got %d: %s", recCreate.Code, recCreate.Body.String())
	}
}

func TestHandlers_SQLiteSongQueries(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	ctx := context.Background()

	// Seed songs into SQLite
	err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO songs (id, title, artist_name, album, release_date, release_year, spotify_id, is_recent, recent_batch_seq, recent_batch_pos) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = stmt.Reset() }()

		songsData := [][]any{
			{"sng_1", "Battery", "Metallica", "Master of Puppets", "1986-03-03", int64(1986), "sp_s1", int64(1), int64(1), int64(13)},
			{"sng_2", "Master of Puppets", "Metallica", "Master of Puppets", "1986-03-03", int64(1986), "sp_s2", int64(1), int64(1), int64(12)},
			{"sng_3", "Old Song", "Old Band", "Old Album", "1970-01-01", int64(1970), "sp_s3", int64(0), int64(0), int64(0)},
		}

		for _, row := range songsData {
			_ = stmt.Reset()
			stmt.BindText(1, row[0].(string))
			stmt.BindText(2, row[1].(string))
			stmt.BindText(3, row[2].(string))
			stmt.BindText(4, row[3].(string))
			stmt.BindText(5, row[4].(string))
			stmt.BindInt64(6, row[5].(int64))
			stmt.BindText(7, row[6].(string))
			stmt.BindInt64(8, row[7].(int64))
			stmt.BindInt64(9, row[8].(int64))
			stmt.BindInt64(10, row[9].(int64))
			if _, err := stmt.Step(); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("failed seeding song test data: %v", err)
	}

	entries, err := h.listSongEntries(ctx)
	if err != nil {
		t.Fatalf("listSongEntries failed: %v", err)
	}
	if len(entries) != 3 {
		t.Fatalf("expected 3 songs, got %d", len(entries))
	}

	r := h.Routes()
	req := httptest.NewRequest(http.MethodGet, "/songs", nil)
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200 from /songs, got %d", rec.Code)
	}
}

func TestHandlers_CreateArtistWithAggregate(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	r := h.Routes()

	formData := url.Values{
		"name":             {"Black Sabbath"},
		"spotify_id":       {"5M52tdi3196eUebVv3bJbH"},
		"genre_group":      {"rock_metal"},
		"list_status":      {"included"},
		"monthly_listeners": {"16000000"},
		"collection_songs": {"45"},
	}
	req := httptest.NewRequest(http.MethodPost, "/api/artists", strings.NewReader(formData.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("expected status 201 from create artist, got %d: %s", rec.Code, rec.Body.String())
	}

	// Verify aggregate was stored in Event Store
	ctx := context.Background()
	var artistID string
	var createdArtist struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &createdArtist)
	artistID = createdArtist.ID

	if artistID == "" {
		t.Fatal("empty artist ID in response")
	}

	agg, err := h.artistRepo.Load(ctx, artistID)
	if err != nil {
		t.Fatalf("failed loading artist aggregate %s from event store: %v", artistID, err)
	}
	if agg.Name != "Black Sabbath" {
		t.Errorf("expected aggregate name Black Sabbath, got %s", agg.Name)
	}
	if agg.GenreGroup != "rock_metal" {
		t.Errorf("expected aggregate genre rock_metal, got %s", agg.GenreGroup)
	}
	if agg.MonthlyListeners != 16000000 {
		t.Errorf("expected aggregate monthly listeners 16000000, got %d", agg.MonthlyListeners)
	}
}

func TestHandlers_SQLiteAlbumSongFieldUpdates(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	ctx := context.Background()

	// Seed one album
	err := h.db.WriteTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO albums (id, title, artist_name, collection_songs, total_songs, status) VALUES ('alb_test', 'Piece of Mind', 'Iron Maiden', 5, 9, 'processed_once');")
		defer func() { _ = stmt.Reset() }()
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("failed seeding album: %v", err)
	}

	// Increment collection_songs
	r := h.Routes()
	reqInc := httptest.NewRequest(http.MethodPost, "/api/albums/alb_test/collection/inc", nil)
	recInc := httptest.NewRecorder()
	r.ServeHTTP(recInc, reqInc)

	// Check SQLite table directly
	var colSongs, totSongs int
	_ = h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT collection_songs, total_songs FROM albums WHERE id = 'alb_test';")
		defer func() { _ = stmt.Reset() }()
		if hasRow, _ := stmt.Step(); hasRow {
			colSongs = int(stmt.ColumnInt64(0))
			totSongs = int(stmt.ColumnInt64(1))
		}
		return nil
	})

	if colSongs != 6 {
		t.Errorf("expected collection_songs to increment to 6, got %d", colSongs)
	}
	if totSongs != 9 {
		t.Errorf("expected total_songs to remain 9, got %d", totSongs)
	}

	// Increment total_songs
	reqTotInc := httptest.NewRequest(http.MethodPost, "/api/albums/alb_test/total/inc", nil)
	recTotInc := httptest.NewRecorder()
	r.ServeHTTP(recTotInc, reqTotInc)

	_ = h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT collection_songs, total_songs FROM albums WHERE id = 'alb_test';")
		defer func() { _ = stmt.Reset() }()
		if hasRow, _ := stmt.Step(); hasRow {
			colSongs = int(stmt.ColumnInt64(0))
			totSongs = int(stmt.ColumnInt64(1))
		}
		return nil
	})

	if totSongs != 10 {
		t.Errorf("expected total_songs to increment to 10, got %d", totSongs)
	}
}
