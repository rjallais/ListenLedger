package projections_test

import (
	"context"
	"io"
	"log/slog"
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
