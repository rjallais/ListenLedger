package worker

import (
	"log/slog"
	"testing"
	"time"

	"zombiezen.com/go/sqlite"

	"ListenLedger/config"
	"ListenLedger/internal/db"
	"ListenLedger/internal/messaging"
)

func TestWorker_EventSourcingAndProjection(t *testing.T) {
	ctx := t.Context()
	tmpDir := t.TempDir()

	sqliteDB, err := db.SetupDB(ctx, slog.Default(), tmpDir, false)
	if err != nil {
		t.Fatalf("failed to setup sqlite db: %v", err)
	}
	defer func() { _ = sqliteDB.Close() }()

	ns := startTestWorkerNATS(t, tmpDir)
	defer ns.Shutdown()
	nc := connectTestWorkerNATS(t, ns.ClientURL())
	defer nc.Close()

	js, err := messaging.NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream error: %v", err)
	}

	cfg := config.DefaultConfig()
	w := New(nil, nc, js, cfg, WithDatabase(sqliteDB))

	artistID := "art_test_123"

	// 1. Record first scrape
	err = w.updateArtistListeners(ctx, artistID, 5000000, "spotify-mobile-ssr", 250)
	if err != nil {
		t.Fatalf("updateArtistListeners error: %v", err)
	}

	// Verify events table: event 1 (artist.created) and event 2 (artist.monthly_listeners_scraped)
	err = sqliteDB.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT event_type, version FROM events WHERE stream_id = ? ORDER BY version ASC;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)

		var versions []int64
		var types []string
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			types = append(types, stmt.ColumnText(0))
			versions = append(versions, stmt.ColumnInt64(1))
		}

		if len(versions) != 2 {
			t.Fatalf("expected 2 events, got %d (%v)", len(versions), types)
		}
		if types[0] != "ArtistCreated" || versions[0] != 1 {
			t.Errorf("event 1: expected ArtistCreated v1, got %s v%d", types[0], versions[0])
		}
		if types[1] != "ArtistMonthlyListenersScraped" || versions[1] != 2 {
			t.Errorf("event 2: expected ArtistMonthlyListenersScraped v2, got %s v%d", types[1], versions[1])
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query events error: %v", err)
	}

	// Verify artists read-model table in SQLite
	err = sqliteDB.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT monthly_listeners, fetch_status FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatalf("expected artist row in artists table")
		}
		listeners := stmt.ColumnInt64(0)
		status := stmt.ColumnText(1)
		if listeners != 5000000 {
			t.Errorf("expected 5000000 listeners, got %d", listeners)
		}
		if status != "idle" {
			t.Errorf("expected idle fetch_status, got %s", status)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query artists error: %v", err)
	}

	// Verify artist_listener_history time-series snapshot table in SQLite
	err = sqliteDB.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT monthly_listeners, previous_listeners, delta, provider, duration_ms FROM artist_listener_history WHERE artist_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatalf("expected history snapshot in artist_listener_history")
		}
		if stmt.ColumnInt64(0) != 5000000 {
			t.Errorf("expected 5000000 monthly_listeners, got %d", stmt.ColumnInt64(0))
		}
		if stmt.ColumnInt64(1) != 0 {
			t.Errorf("expected 0 previous_listeners, got %d", stmt.ColumnInt64(1))
		}
		if stmt.ColumnInt64(2) != 5000000 {
			t.Errorf("expected 5000000 delta, got %d", stmt.ColumnInt64(2))
		}
		if stmt.ColumnText(3) != "spotify-mobile-ssr" {
			t.Errorf("expected spotify-mobile-ssr provider, got %s", stmt.ColumnText(3))
		}
		if stmt.ColumnInt64(4) != 250 {
			t.Errorf("expected 250 duration_ms, got %d", stmt.ColumnInt64(4))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query artist_listener_history error: %v", err)
	}

	// 2. Second scrape with listener increase to verify delta calculation
	err = w.updateArtistListeners(ctx, artistID, 5200000, "local-headless", 310)
	if err != nil {
		t.Fatalf("second updateArtistListeners error: %v", err)
	}

	// Verify second history snapshot with calculated delta
	err = sqliteDB.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT version, monthly_listeners, previous_listeners, delta, provider FROM artist_listener_history WHERE artist_id = ? AND version = 3;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatalf("expected snapshot for version 3")
		}
		if stmt.ColumnInt64(1) != 5200000 {
			t.Errorf("expected 5200000 listeners, got %d", stmt.ColumnInt64(1))
		}
		if stmt.ColumnInt64(2) != 5000000 {
			t.Errorf("expected 5000000 previous_listeners, got %d", stmt.ColumnInt64(2))
		}
		if stmt.ColumnInt64(3) != 200000 {
			t.Errorf("expected 200000 delta, got %d", stmt.ColumnInt64(3))
		}
		if stmt.ColumnText(4) != "local-headless" {
			t.Errorf("expected local-headless provider, got %s", stmt.ColumnText(4))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query second snapshot error: %v", err)
	}

	// 3. Test scrape_jobs SQLite state persistence
	reqID := "req_test_xyz"
	err = sqliteDB.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO scrape_jobs (id, request_id, artist_id, status, attempts, error, queued_at) VALUES (?, ?, ?, ?, ?, ?, ?);")
		defer func() { _ = stmt.Reset() }()
		now := time.Now().UTC().Format(time.RFC3339Nano)
		stmt.BindText(1, "job_1")
		stmt.BindText(2, reqID)
		stmt.BindText(3, artistID)
		stmt.BindText(4, "queued")
		stmt.BindInt64(5, 0)
		stmt.BindText(6, "")
		stmt.BindText(7, now)
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("insert scrape job error: %v", err)
	}

	// Set processing
	w.setScrapeJobProcessing(ctx, reqID)

	// Verify processing
	err = sqliteDB.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status, attempts FROM scrape_jobs WHERE request_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, reqID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatalf("expected scrape job row")
		}
		if stmt.ColumnText(0) != "processing" || stmt.ColumnInt64(1) != 1 {
			t.Errorf("expected processing attempts=1, got %s attempts=%d", stmt.ColumnText(0), stmt.ColumnInt64(1))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query processing error: %v", err)
	}

	// Set finished
	err = w.setScrapeJobFinishedWithContext(ctx, reqID, "succeeded", "")
	if err != nil {
		t.Fatalf("setScrapeJobFinishedWithContext error: %v", err)
	}

	// Verify finished
	err = sqliteDB.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status, finished_at FROM scrape_jobs WHERE request_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, reqID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatalf("expected scrape job row")
		}
		if stmt.ColumnText(0) != "succeeded" {
			t.Errorf("expected succeeded status, got %s", stmt.ColumnText(0))
		}
		if stmt.ColumnText(1) == "" {
			t.Errorf("expected non-empty finished_at")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("query succeeded error: %v", err)
	}
}
