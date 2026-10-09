package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/album"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/templates"
)

// TestSongSideEffects_AlbumSeedAndAdjust proves song-path album deltas become
// facts: a legacy row seeds AlbumCreated, later bumps append
// AlbumSongCountsAdjusted, and the SQLite row converges both times.
func TestSongSideEffects_AlbumSeedAndAdjust(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := t.Context()

	view := albumView{id: "al_se", title: "Powerslave", artistName: "Iron Maiden", status: "waiting", collectionSongs: 1, totalSongs: 8}
	if err := h.foldAlbumCounts(ctx, view); err != nil {
		t.Fatalf("foldAlbumCounts(seed): %v", err)
	}
	events, err := h.store.Load(ctx, "al_se")
	if err != nil || len(events) != 1 {
		t.Fatalf("seeded stream events = %d, %v; want 1 Created", len(events), err)
	}
	assertAlbumRow(t, ctx, h, "al_se", "waiting", 1, 8)

	view.collectionSongs = 2
	if err := h.foldAlbumCounts(ctx, view); err != nil {
		t.Fatalf("foldAlbumCounts(bump): %v", err)
	}
	events, err = h.store.Load(ctx, "al_se")
	if err != nil || len(events) != 2 {
		t.Fatalf("bumped stream events = %d, %v; want Created + Adjusted", len(events), err)
	}
	assertAlbumRow(t, ctx, h, "al_se", "waiting", 2, 8)

	// Idempotent re-fold: same post-state appends nothing.
	if err := h.foldAlbumCounts(ctx, view); err != nil {
		t.Fatalf("foldAlbumCounts(idempotent): %v", err)
	}
	events, err = h.store.Load(ctx, "al_se")
	if err != nil || len(events) != 2 {
		t.Fatalf("re-fold stream events = %d, %v; want still 2", len(events), err)
	}
}

func assertAlbumRow(t *testing.T, ctx context.Context, h *Handler, id, status string, collection, total int64) {
	t.Helper()
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status, collection_songs, total_songs FROM albums WHERE id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatalf("albums row %s missing", id)
		}
		if got := stmt.ColumnText(0); got != status {
			t.Fatalf("albums row %s status = %s, want %s", id, got, status)
		}
		if got := stmt.ColumnInt64(1); got != collection {
			t.Fatalf("albums row %s collection = %d, want %d", id, got, collection)
		}
		if got := stmt.ColumnInt64(2); got != total {
			t.Fatalf("albums row %s total = %d, want %d", id, got, total)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read albums row: %v", err)
	}
}

// TestSongSideEffects_ArtistCountsNeverLogRank proves the rank column is
// never logged as a fact: seeds start counts at stream zero and later bumps
// advance only the collection count, whatever the row's rank cache holds.
func TestSongSideEffects_ArtistCountsNeverLogRank(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := t.Context()

	view := artistView{id: "ar_se", name: "Iron Maiden", spotifyID: "sp_se", genreGroup: "rock_metal", listStatus: "not_added", collectionSongs: 1}
	if err := h.foldArtistCounts(ctx, view); err != nil {
		t.Fatalf("foldArtistCounts(seed): %v", err)
	}
	agg, err := artist.Replay("ar_se", mustLoad(t, ctx, h, "ar_se"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if agg.CollectionSongs != 1 || agg.TotalSongs != 0 {
		t.Fatalf("seeded aggregate = (%d songs, total %d), want (1, 0)", agg.CollectionSongs, agg.TotalSongs)
	}

	// A later bump advances only the count, even though the row cache moved.
	view.collectionSongs = 2
	if err := h.foldArtistCounts(ctx, view); err != nil {
		t.Fatalf("foldArtistCounts(bump): %v", err)
	}
	agg, err = artist.Replay("ar_se", mustLoad(t, ctx, h, "ar_se"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if agg.CollectionSongs != 2 || agg.TotalSongs != 0 {
		t.Fatalf("bumped aggregate = (%d songs, total %d), want (2, 0): rank must stay derived", agg.CollectionSongs, agg.TotalSongs)
	}
	assertArtistCounts(t, ctx, h, "ar_se", 2, 0)
}

func mustLoad(t *testing.T, ctx context.Context, h *Handler, streamID string) []eventsourcing.Event {
	t.Helper()
	events, err := h.store.Load(ctx, streamID)
	if err != nil {
		t.Fatalf("load %s: %v", streamID, err)
	}
	out := make([]eventsourcing.Event, len(events))
	copy(out, events)
	return out
}

func assertArtistCounts(t *testing.T, ctx context.Context, h *Handler, id string, collection, total int64) {
	t.Helper()
	err := h.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT collection_songs, total_songs FROM artists WHERE id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, id)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatalf("artists row %s missing", id)
		}
		if got := stmt.ColumnInt64(0); got != collection {
			t.Fatalf("artists row %s collection = %d, want %d", id, got, collection)
		}
		if got := stmt.ColumnInt64(1); got != total {
			t.Fatalf("artists row %s total = %d, want %d", id, got, total)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read artists row: %v", err)
	}
}

// TestSongSideEffects_AlbumStatusOnLegacySeed proves a legacy stream-less
// album seeds AlbumCreated before a status transition, so the status fact is
// never lost (recordAlbumStatusEvent path).
func TestSongSideEffects_AlbumStatusOnLegacySeed(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := t.Context()

	agg, err := h.loadOrCreateAlbum(ctx, albumView{id: "al_legacy", title: "Killers", artistName: "Iron Maiden", status: "waiting", collectionSongs: 2, totalSongs: 9})
	if err != nil {
		t.Fatalf("loadOrCreateAlbum: %v", err)
	}
	if err := agg.ChangeStatus("full"); err != nil {
		t.Fatalf("ChangeStatus: %v", err)
	}
	if err := h.saveAndProjectAlbum(ctx, agg); err != nil {
		t.Fatalf("saveAndProjectAlbum: %v", err)
	}
	events, err := h.store.Load(ctx, "al_legacy")
	if err != nil || len(events) != 2 {
		t.Fatalf("stream events = %d, %v; want Created + StatusChanged", len(events), err)
	}
	replayed, err := album.Replay("al_legacy", events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.Status != "full" || replayed.CollectionSongs != 2 {
		t.Fatalf("replayed = %+v, want full/(2,9)", replayed)
	}
	assertAlbumRow(t, ctx, h, "al_legacy", "full", 2, 9)
}

// TestRecordAlbumStatusEvent_SeedsLegacy proves a status transition on a
// legacy stream-less album seeds from the committed view (no PocketBase
// read): Created + StatusChanged with counts preserved.
func TestRecordAlbumStatusEvent_SeedsLegacy(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := t.Context()

	committed := templates.Album{ID: "al_stev", Title: "Piece of Mind", ArtistName: "Iron Maiden", Status: "waiting", CollectionSongs: 4, TotalSongs: 9}
	if err := h.recordAlbumStatusEvent(ctx, "al_stev", "full", committed, "waiting"); err != nil {
		t.Fatalf("recordAlbumStatusEvent: %v", err)
	}
	events, err := h.store.Load(ctx, "al_stev")
	if err != nil || len(events) != 2 {
		t.Fatalf("stream events = %d, %v; want Created + StatusChanged", len(events), err)
	}
	replayed, err := album.Replay("al_stev", events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if replayed.Status != "full" || replayed.CollectionSongs != 4 || replayed.TotalSongs != 9 {
		t.Fatalf("replayed = %+v, want full/(4,9)", replayed)
	}
	assertAlbumRow(t, ctx, h, "al_stev", "full", 4, 9)
}

// TestSongRecentErrorRestoresSections proves a failed recent-toggle restores
// the current sections on browser requests (morph resets the optimistically
// disabled checkbox) instead of returning a bare JSON error.
func TestSongRecentErrorRestoresSections(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()

	req := httptest.NewRequest(http.MethodPost, "/api/songs/missing/recent/true", nil)
	req.SetPathValue("songId", "missing")
	req.SetPathValue("value", "true")
	rec := httptest.NewRecorder()
	h.HandleUpdateSongRecent(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("error-path status = %d, want 200 with restored sections: %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); !strings.Contains(body, "songs-") {
		t.Fatalf("error path should patch song sections, got: %.200s", body)
	}
}

// TestSongSideEffects_AlbumReplayConverges guards the audit invariant: a
// folded album stream replays to the projected row values.
func TestSongSideEffects_AlbumReplayConverges(t *testing.T) {
	h, tempDir := setupTestSQLiteDB(t)
	defer func() { _ = os.RemoveAll(tempDir) }()
	defer func() { _ = h.db.Close() }()
	ctx := t.Context()

	view := albumView{id: "al_rb", title: "Seventh Son", artistName: "Iron Maiden", status: "waiting", collectionSongs: 3, totalSongs: 8}
	if err := h.foldAlbumCounts(ctx, view); err != nil {
		t.Fatalf("foldAlbumCounts: %v", err)
	}
	agg, err := album.Replay("al_rb", mustLoad(t, ctx, h, "al_rb"))
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if agg.CollectionSongs != 3 || agg.TotalSongs != 8 || agg.Status != "waiting" {
		t.Fatalf("replayed = %+v, want counts (3,8) waiting", agg)
	}
	assertAlbumRow(t, ctx, h, "al_rb", "waiting", 3, 8)
}
