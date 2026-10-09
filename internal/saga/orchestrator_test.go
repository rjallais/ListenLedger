package saga

import (
	"context"
	"testing"

	toolbeltdb "github.com/delaneyj/toolbelt/db"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
)

func TestDeriveState(t *testing.T) {
	if got := DeriveState(nil); got != StateRequested {
		t.Fatalf("nil job = %q, want requested", got)
	}

	newJob := func(t *testing.T) *scrapejob.Job {
		t.Helper()
		j, err := scrapejob.NewScrapeJob("req_state", "ar_state", "refresh")
		if err != nil {
			t.Fatalf("NewScrapeJob() error = %v", err)
		}
		return j
	}

	if got := DeriveState(newJob(t)); got != StateRequested {
		t.Fatalf("queued = %q, want requested", got)
	}

	started := newJob(t)
	if _, _, err := started.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted() error = %v", err)
	}
	if got := DeriveState(started); got != StateProcessing {
		t.Fatalf("processing = %q, want processing", got)
	}

	failed := newJob(t)
	if _, _, err := failed.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted() error = %v", err)
	}
	if _, _, err := failed.RecordFailed("boom"); err != nil {
		t.Fatalf("RecordFailed() error = %v", err)
	}
	if got := DeriveState(failed); got != StateFailed {
		t.Fatalf("failed = %q, want failed", got)
	}

	done := newJob(t)
	if _, _, err := done.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted() error = %v", err)
	}
	if _, _, err := done.RecordSucceeded("test", 5); err != nil {
		t.Fatalf("RecordSucceeded() error = %v", err)
	}
	if got := DeriveState(done); got != StateDone {
		t.Fatalf("succeeded = %q, want done", got)
	}

	dead := newJob(t)
	if _, _, err := dead.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted() error = %v", err)
	}
	if _, _, err := dead.RecordFailed("poison"); err != nil {
		t.Fatalf("RecordFailed() error = %v", err)
	}
	if _, _, err := dead.RecordDeadLettered("poison"); err != nil {
		t.Fatalf("RecordDeadLettered() error = %v", err)
	}
	if got := DeriveState(dead); got != StateDead {
		t.Fatalf("dead-lettered = %q, want dead", got)
	}
}

func seedSagaRow(t *testing.T, db *toolbeltdb.Database, requestID, artistID, status string) {
	t.Helper()
	ctx := context.Background()
	if err := db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		artist := tx.Prep(`INSERT OR IGNORE INTO artists (id, name, spotify_id, genre_group, list_status)
			VALUES (?, 'Saga Artist', 'sp_saga', 'rock_metal', 'included');`)
		defer func() { _ = artist.Reset() }()
		artist.BindText(1, artistID)
		if _, err := artist.Step(); err != nil {
			return err
		}
		job := tx.Prep(`INSERT OR IGNORE INTO scrape_jobs (id, request_id, artist_id, status, queued_at)
			VALUES (?, ?, ?, ?, '2026-09-30 10:00:00.000Z');`)
		defer func() { _ = job.Reset() }()
		job.BindText(1, requestID)
		job.BindText(2, requestID)
		job.BindText(3, artistID)
		job.BindText(4, status)
		_, err := job.Step()
		return err
	}); err != nil {
		t.Fatalf("seed saga row: %v", err)
	}
}

func sagaInstanceState(t *testing.T, db *toolbeltdb.Database, requestID string) (state string, attempts int64, lastEvent, failure string) {
	t.Helper()
	if err := db.ReadTX(context.Background(), func(tx *sqlite.Conn) error {
		stmt := tx.Prep(`SELECT state, attempts, last_event, error FROM saga_instances WHERE request_id = ?;`)
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if !hasRow {
			return nil
		}
		state = stmt.ColumnText(0)
		attempts = stmt.ColumnInt64(1)
		lastEvent = stmt.ColumnText(2)
		failure = stmt.ColumnText(3)
		return nil
	}); err != nil {
		t.Fatalf("read saga instance: %v", err)
	}
	return state, attempts, lastEvent, failure
}

func TestTickTracksLifecycle(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	store := eventsourcing.NewSQLiteStore(db)

	seedSagaRow(t, db, "req_tick", "ar_tick", "queued")

	// Row ahead of stream reads as requested.
	o := NewOrchestrator(db)
	if n, err := o.Tick(ctx); err != nil {
		t.Fatalf("Tick() error = %v", err)
	} else if n == 0 {
		t.Fatal("Tick() refreshed 0, want >0")
	}
	if state, _, _, _ := sagaInstanceState(t, db, "req_tick"); state != StateRequested {
		t.Fatalf("state = %q, want requested", state)
	}

	// Open the stream: requested + started → processing, attempts 1.
	j, err := scrapejob.NewScrapeJob("req_tick", "ar_tick", "refresh")
	if err != nil {
		t.Fatalf("NewScrapeJob() error = %v", err)
	}
	if uncommitted := j.UncommittedEvents(); len(uncommitted) > 0 {
		if err := store.Append(ctx, "req_tick", 0, uncommitted...); err != nil {
			t.Fatalf("Append requested error = %v", err)
		}
	}
	evts, err := store.Load(ctx, "req_tick")
	if err != nil {
		t.Fatalf("Load error = %v", err)
	}
	j2, err := scrapejob.Replay("req_tick", evts)
	if err != nil {
		t.Fatalf("Replay error = %v", err)
	}
	if _, _, err := j2.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted() error = %v", err)
	}
	if uncommitted := j2.UncommittedEvents(); len(uncommitted) > 0 {
		if err := store.Append(ctx, "req_tick", j2.Version()-int64(len(uncommitted)), uncommitted...); err != nil {
			t.Fatalf("Append started error = %v", err)
		}
	}
	if _, err := o.Tick(ctx); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	if state, attempts, lastEvent, _ := sagaInstanceState(t, db, "req_tick"); state != StateProcessing || attempts != 1 || lastEvent != scrapejob.EventTypeScrapeStarted {
		t.Fatalf("got state=%q attempts=%d last=%q, want processing/1/ScrapeStarted", state, attempts, lastEvent)
	}

	// Close the stream: succeeded → done, History joins the state.
	evts, err = store.Load(ctx, "req_tick")
	if err != nil {
		t.Fatalf("Load error = %v", err)
	}
	j3, err := scrapejob.Replay("req_tick", evts)
	if err != nil {
		t.Fatalf("Replay error = %v", err)
	}
	if _, _, err := j3.RecordSucceeded("test", 9); err != nil {
		t.Fatalf("RecordSucceeded() error = %v", err)
	}
	if uncommitted := j3.UncommittedEvents(); len(uncommitted) > 0 {
		if err := store.Append(ctx, "req_tick", j3.Version()-int64(len(uncommitted)), uncommitted...); err != nil {
			t.Fatalf("Append succeeded error = %v", err)
		}
	}
	if _, err := o.Tick(ctx); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	if state, _, _, _ := sagaInstanceState(t, db, "req_tick"); state != StateDone {
		t.Fatalf("state = %q, want done", state)
	}
	h, err := NewLoader(db, store).LoadByRequest(ctx, "req_tick")
	if err != nil {
		t.Fatalf("LoadByRequest() error = %v", err)
	}
	if h.State != StateDone {
		t.Fatalf("History.State = %q, want done", h.State)
	}
}

func TestTickRecordsFailure(t *testing.T) {
	db := setupTestDB(t)
	ctx := context.Background()
	store := eventsourcing.NewSQLiteStore(db)

	seedSagaRow(t, db, "req_fail", "ar_fail", "processing")

	j, err := scrapejob.NewScrapeJob("req_fail", "ar_fail", "refresh")
	if err != nil {
		t.Fatalf("NewScrapeJob() error = %v", err)
	}
	if _, _, err := j.RecordStarted(); err != nil {
		t.Fatalf("RecordStarted() error = %v", err)
	}
	if _, _, err := j.RecordFailed("stale_timeout"); err != nil {
		t.Fatalf("RecordFailed() error = %v", err)
	}
	if uncommitted := j.UncommittedEvents(); len(uncommitted) > 0 {
		if err := store.Append(ctx, "req_fail", 0, uncommitted...); err != nil {
			t.Fatalf("Append error = %v", err)
		}
	}

	if _, err := NewOrchestrator(db).Tick(ctx); err != nil {
		t.Fatalf("Tick() error = %v", err)
	}
	state, attempts, lastEvent, failure := sagaInstanceState(t, db, "req_fail")
	if state != StateFailed || attempts != 1 || lastEvent != scrapejob.EventTypeScrapeFailed || failure != "stale_timeout" {
		t.Fatalf("got state=%q attempts=%d last=%q err=%q, want failed/1/ScrapeFailed/stale_timeout",
			state, attempts, lastEvent, failure)
	}
}
