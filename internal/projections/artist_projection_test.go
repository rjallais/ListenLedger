package projections_test

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"testing"

	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/artist"
	"ListenLedger/internal/eventsourcing"
	"ListenLedger/internal/projections"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"
)

func setupTestDB(t *testing.T) (*toolbeltdb.Database, *eventsourcing.SQLiteStore, *projections.ArtistProjection, func()) {
	t.Helper()
	db, err := db.SetupDB(context.Background(), slog.Default(), t.TempDir(), false)
	if err != nil {
		t.Fatalf("failed to create test db: %v", err)
	}

	store := eventsourcing.NewSQLiteStore(db)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	projection := projections.NewArtistProjection(logger, db, nil)

	cleanup := func() {
		_ = db.Close()
	}
	return db, store, projection, cleanup
}

func TestArtistProjection_FullLifecycleAndHistory(t *testing.T) {
	ctx := context.Background()
	db, store, proj, cleanup := setupTestDB(t)
	defer cleanup()

	repo := artist.NewRepository(store)
	artistID := "artist_radiohead"

	// 1. Create Artist
	agg, err := artist.NewArtist(artistID, "Radiohead", "4Z8W4fKeB5YxbusRsdQVPb", "rock_metal", "included")
	if err != nil {
		t.Fatalf("failed creating artist: %v", err)
	}

	evts, err := repo.Save(ctx, agg)
	if err != nil {
		t.Fatalf("failed saving artist: %v", err)
	}
	if err := proj.Project(ctx, agg, evts); err != nil {
		t.Fatalf("failed projecting artist: %v", err)
	}

	// Verify in read table
	var count int
	_ = db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT count(*) FROM artists WHERE id = ? AND name = 'Radiohead';")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		if hasRow, _ := stmt.Step(); hasRow {
			count = stmt.ColumnInt(0)
		}
		return nil
	})
	if count != 1 {
		t.Fatalf("expected artist in artists table, got count %d", count)
	}

	// 2. Scrape Listeners (v2): 15,000,000
	if err := agg.RecordMonthlyListeners(15000000, "local_headless", 412); err != nil {
		t.Fatalf("failed recording listeners: %v", err)
	}
	evts, err = repo.Save(ctx, agg)
	if err != nil {
		t.Fatalf("failed saving scrape v2: %v", err)
	}
	if err := proj.Project(ctx, agg, evts); err != nil {
		t.Fatalf("failed projecting scrape v2: %v", err)
	}

	// 3. Second Scrape (v3): 15,250,000 (delta +250,000)
	if err := agg.RecordMonthlyListeners(15250000, "scrapingant", 380); err != nil {
		t.Fatalf("failed recording listeners: %v", err)
	}
	evts, err = repo.Save(ctx, agg)
	if err != nil {
		t.Fatalf("failed saving scrape v3: %v", err)
	}
	if err := proj.Project(ctx, agg, evts); err != nil {
		t.Fatalf("failed projecting scrape v3: %v", err)
	}

	// Verify read table has latest listeners
	var currentListeners int64
	_ = db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT monthly_listeners FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		if hasRow, _ := stmt.Step(); hasRow {
			currentListeners = stmt.ColumnInt64(0)
		}
		return nil
	})
	if currentListeners != 15250000 {
		t.Fatalf("expected 15250000 listeners in read table, got %d", currentListeners)
	}

	// Verify historical snapshots
	history, err := proj.GetListenerHistory(ctx, artistID, 10)
	if err != nil {
		t.Fatalf("failed getting listener history: %v", err)
	}
	if len(history) != 2 {
		t.Fatalf("expected 2 historical snapshots, got %d", len(history))
	}

	// Check ordering (newest first)
	if history[0].Version != 3 || history[0].MonthlyListeners != 15250000 || history[0].Delta != 250000 {
		t.Fatalf("unexpected newest snapshot: %+v", history[0])
	}
	if history[1].Version != 2 || history[1].MonthlyListeners != 15000000 || history[1].Delta != 15000000 {
		t.Fatalf("unexpected older snapshot: %+v", history[1])
	}

	// 4. Change Status (v4)
	if err := agg.ChangeListStatus("recently_added", "manual promote"); err != nil {
		t.Fatalf("failed changing status: %v", err)
	}
	evts, err = repo.Save(ctx, agg)
	if err != nil {
		t.Fatalf("failed saving status change: %v", err)
	}
	if err := proj.Project(ctx, agg, evts); err != nil {
		t.Fatalf("failed projecting status change: %v", err)
	}

	var status string
	_ = db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT list_status FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		if hasRow, _ := stmt.Step(); hasRow {
			status = stmt.ColumnText(0)
		}
		return nil
	})
	if status != "recently_added" {
		t.Fatalf("expected list_status recently_added, got %s", status)
	}
}

// TestArtistProjectionHandlesAllEventTypes keeps the projection switch in
// step with the artist event contract: an event type the projection does not
// know would be skipped silently, leaving the read model behind the log.
func TestArtistProjectionHandlesAllEventTypes(t *testing.T) {
	ctx := t.Context()
	_, _, proj, cleanup := setupTestDB(t)
	defer cleanup()

	agg, err := artist.NewArtist("ar_contract", "Contract", "sp_contract", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}
	if err := agg.RecordMonthlyListeners(1000, "contract", 1); err != nil {
		t.Fatalf("RecordMonthlyListeners: %v", err)
	}
	if err := agg.ChangeListStatus("recently_added", "contract"); err != nil {
		t.Fatalf("ChangeListStatus: %v", err)
	}
	if err := agg.SetFetchStatus("pending", ""); err != nil {
		t.Fatalf("SetFetchStatus: %v", err)
	}
	if err := agg.AdjustCollectionSongs(1, 2); err != nil {
		t.Fatalf("AdjustCollectionSongs: %v", err)
	}

	events := agg.UncommittedEvents()
	if err := proj.Project(ctx, agg, events); err != nil {
		t.Fatalf("Project(one event of each declared type): %v", err)
	}
	want := []string{
		artist.EventTypeArtistCollectionSongsAdjusted,
		artist.EventTypeArtistCreated,
		artist.EventTypeArtistFetchStatusChanged,
		artist.EventTypeArtistMonthlyListenersScraped,
		artist.EventTypeArtistStatusChanged,
	}
	if got := eventsourcing.EventTypesOf(events); !slices.Equal(got, want) {
		t.Fatalf("walk recorded %v, want %v", got, want)
	}

	unknown, err := eventsourcing.NewEvent("ar_contract", artist.StreamTypeArtist, int64(len(events)+1), "ArtistNotAnEvent", map[string]any{}, nil)
	if err != nil {
		t.Fatalf("NewEvent(unknown): %v", err)
	}
	if err := proj.Project(ctx, agg, []eventsourcing.Event{unknown}); err == nil {
		t.Fatal("Project(unknown event type) = nil, want an error")
	}
}

// TestArtistProjectionSkipsStaleEvents reproduces the healer race: the healer
// loads a stream outside its transaction, so a batch it re-projects can be
// older than what a concurrent inline projection already committed. Project
// must skip events at or below the recorded position instead of regressing
// rows while the monotonic position stays ahead (drift the audit could then
// never see, let alone repair).
func TestArtistProjectionSkipsStaleEvents(t *testing.T) {
	ctx := t.Context()
	db, store, proj, cleanup := setupTestDB(t)
	defer cleanup()

	repo := artist.NewRepository(store)
	agg, err := artist.NewArtist("ar_stale", "Stale", "sp_stale", "rock_metal", "included")
	if err != nil {
		t.Fatalf("NewArtist: %v", err)
	}

	var all []eventsourcing.Event
	appendBatch := func() {
		t.Helper()
		evts, err := repo.Save(ctx, agg)
		if err != nil {
			t.Fatalf("repo.Save: %v", err)
		}
		if err := proj.Project(ctx, agg, evts); err != nil {
			t.Fatalf("proj.Project: %v", err)
		}
		all = append(all, evts...)
	}

	// v1 ArtistCreated, then v2..v5 listener scrapes with distinct values.
	appendBatch()
	for _, listeners := range []int64{100, 200, 300, 400} {
		if err := agg.RecordMonthlyListeners(listeners, "test", 1); err != nil {
			t.Fatalf("RecordMonthlyListeners: %v", err)
		}
		appendBatch()
	}

	readValue := func(query string) int64 {
		t.Helper()
		var got int64 = -1
		_ = db.ReadTX(ctx, func(tx *sqlite.Conn) error {
			stmt := tx.Prep(query)
			defer func() { _ = stmt.Reset() }()
			stmt.BindText(1, "ar_stale")
			if hasRow, _ := stmt.Step(); hasRow {
				got = stmt.ColumnInt64(0)
			}
			return nil
		})
		return got
	}

	if got := readValue("SELECT monthly_listeners FROM artists WHERE id = ?;"); got != 400 {
		t.Fatalf("monthly_listeners = %d, want 400", got)
	}
	if got := readValue("SELECT version FROM read_model_positions WHERE stream_id = ?;"); got != 5 {
		t.Fatalf("read_model_positions.version = %d, want 5", got)
	}

	// The healer's stale batch: everything except the newest event. Without
	// the in-transaction position check this regressed the row to v4 (300).
	stale := slices.Clone(all[:len(all)-1])
	if err := proj.Project(ctx, agg, stale); err != nil {
		t.Fatalf("Project(stale batch): %v", err)
	}
	if got := readValue("SELECT monthly_listeners FROM artists WHERE id = ?;"); got != 400 {
		t.Fatalf("stale batch regressed monthly_listeners to %d, want 400", got)
	}
	if got := readValue("SELECT version FROM read_model_positions WHERE stream_id = ?;"); got != 5 {
		t.Fatalf("read_model_positions.version = %d, want 5 after stale batch", got)
	}
}
