package main

import (
	"testing"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/artist"
)

// TestEventLogRecordListeners proves standalone scrapes fold as facts: two
// scrapes append Created + two scraped events, and the SQLite row converges
// to the latest count with listener history.
func TestEventLogRecordListeners(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()

	elog, err := openEventLog(ctx, dataDir)
	if err != nil {
		t.Fatalf("openEventLog: %v", err)
	}
	defer elog.close()

	elog.recordListeners(ctx, "ar_standalone", "Standalone", "sp_standalone", "rock_metal", "included", 1000, 120)
	elog.recordListeners(ctx, "ar_standalone", "Standalone", "sp_standalone", "rock_metal", "included", 1500, 130)

	events, err := elog.store.Load(ctx, "ar_standalone")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("stream events = %d, want Created + 2 scraped", len(events))
	}
	agg, err := artist.Replay("ar_standalone", events)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if agg.MonthlyListeners != 1500 || agg.FetchStatus != "idle" {
		t.Fatalf("aggregate = listeners %d fetch %s, want 1500 idle", agg.MonthlyListeners, agg.FetchStatus)
	}

	err = elog.database.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT monthly_listeners, fetch_status FROM artists WHERE id = ? LIMIT 1;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, "ar_standalone")
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			t.Fatal("artists row missing after projection")
		}
		if got := stmt.ColumnInt64(0); got != 1500 {
			t.Fatalf("row listeners = %d, want 1500", got)
		}
		hist := tx.Prep("SELECT COUNT(*) FROM artist_listener_history WHERE artist_id = ?;")
		defer func() { _ = hist.Reset() }()
		hist.BindText(1, "ar_standalone")
		if hasRow, err := hist.Step(); err != nil || !hasRow {
			t.Fatalf("history read: %v", err)
		} else if got := hist.ColumnInt64(0); got != 2 {
			t.Fatalf("history rows = %d, want 2", got)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read projection: %v", err)
	}
}

// TestEventLogRecordFetchFailed proves failures track fetch_status in the
// stream (nil log is a no-op for PocketBase-only mode).
func TestEventLogRecordFetchFailed(t *testing.T) {
	ctx := t.Context()
	dataDir := t.TempDir()

	var nilLog *eventLog
	nilLog.recordListeners(ctx, "ghost", "Ghost", "", "rock_metal", "included", 1, 1)
	nilLog.recordFetchFailed(ctx, "ghost", "Ghost", "", "rock_metal", "included", "boom")

	elog, err := openEventLog(ctx, dataDir)
	if err != nil {
		t.Fatalf("openEventLog: %v", err)
	}
	defer elog.close()

	elog.recordFetchFailed(ctx, "ar_fail", "Failer", "sp_fail", "rock_metal", "included", "timeout")
	events, err := elog.store.Load(ctx, "ar_fail")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("stream events = %d, want Created + FetchStatusChanged", len(events))
	}
	agg, err := artist.Replay("ar_fail", events)
	if err != nil {
		t.Fatalf("Replay: %v", err)
	}
	if agg.FetchStatus != "failed" {
		t.Fatalf("fetch status = %s, want failed", agg.FetchStatus)
	}
}
