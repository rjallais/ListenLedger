package worker

import (
	"context"
	"log/slog"
	"testing"

	"zombiezen.com/go/sqlite"

	"ListenLedger/config"
	"ListenLedger/internal/db"
	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/messaging"
)

func setupTestJobWorker(t *testing.T) (*Worker, context.Context) {
	t.Helper()
	ctx := context.Background()
	tmpDir := t.TempDir()

	sqliteDB, err := db.SetupDB(ctx, slog.Default(), tmpDir, false)
	if err != nil {
		t.Fatalf("SetupDB: %v", err)
	}
	t.Cleanup(func() { _ = sqliteDB.Close() })

	ns := startTestWorkerNATS(t, tmpDir)
	t.Cleanup(ns.Shutdown)
	nc := connectTestWorkerNATS(t, ns.ClientURL())
	t.Cleanup(func() { nc.Close() })

	js, err := messaging.NewJetStream(nc)
	if err != nil {
		t.Fatalf("NewJetStream: %v", err)
	}

	w := New(nil, nc, js, config.DefaultConfig(), WithDatabase(sqliteDB))
	return w, ctx
}

func jobStreamTypes(t *testing.T, ctx context.Context, w *Worker, requestID string) []string {
	t.Helper()
	var types []string
	err := w.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT event_type FROM events WHERE stream_id = ? ORDER BY version ASC;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		for {
			hasRow, err := stmt.Step()
			if err != nil {
				return err
			}
			if !hasRow {
				break
			}
			types = append(types, stmt.ColumnText(0))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("eventLog: %v", err)
	}
	return types
}

func TestJobEvents_TransitionsAndDedup(t *testing.T) {
	w, ctx := setupTestJobWorker(t)

	// Started seeds the stream (Requested + Started atomically).
	w.recordJobEventWarn(ctx, "req_j1", "ar_j1", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})
	got := jobStreamTypes(t, ctx, w, "req_j1")
	if len(got) != 2 || got[0] != scrapejob.EventTypeScrapeRequested || got[1] != scrapejob.EventTypeScrapeStarted {
		t.Fatalf("stream = %v", got)
	}

	// Redelivery appends another Started (attempts preserved, nothing lost).
	w.recordJobEventWarn(ctx, "req_j1", "ar_j1", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})
	if got := jobStreamTypes(t, ctx, w, "req_j1"); len(got) != 3 {
		t.Fatalf("stream after redelivery = %v", got)
	}

	// Not terminal: dedup stays false without touching PocketBase (app is nil).
	if w.isRequestAlreadySucceeded(ctx, "req_j1") {
		t.Fatal("processing job should not dedup")
	}

	w.recordJobEventWarn(ctx, "req_j1", "ar_j1", "succeeded", func(j *scrapejob.Job) error {
		_, _, err := j.RecordSucceeded("mobile-ssr", 250)
		return err
	})
	got = jobStreamTypes(t, ctx, w, "req_j1")
	if len(got) != 4 || got[3] != scrapejob.EventTypeScrapeSucceeded {
		t.Fatalf("stream = %v", got)
	}

	// Terminal success dedups purely from the stream (restart-safe).
	if !w.isRequestAlreadySucceeded(ctx, "req_j1") {
		t.Fatal("succeeded job should dedup from its stream")
	}

	// Post-terminal transitions record nothing.
	w.recordJobEventWarn(ctx, "req_j1", "ar_j1", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})
	if got := jobStreamTypes(t, ctx, w, "req_j1"); len(got) != 4 {
		t.Fatalf("stream after post-terminal = %v", got)
	}
}

func TestJobEvents_FailedAndDeadLettered(t *testing.T) {
	w, ctx := setupTestJobWorker(t)

	w.recordJobEventWarn(ctx, "req_j2", "ar_j2", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})
	w.recordJobEventWarn(ctx, "req_j2", "ar_j2", "failed", func(j *scrapejob.Job) error {
		_, _, err := j.RecordFailed("stale_timeout")
		return err
	})
	// Failed re-opens on retry.
	w.recordJobEventWarn(ctx, "req_j2", "ar_j2", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})
	w.recordJobEventWarn(ctx, "req_j2", "ar_j2", "dead-lettered", func(j *scrapejob.Job) error {
		if _, _, err := j.RecordFailed("retry_exhausted"); err != nil {
			return err
		}
		_, _, err := j.RecordDeadLettered("retry_exhausted")
		return err
	})
	got := jobStreamTypes(t, ctx, w, "req_j2")
	want := []string{"ScrapeRequested", "ScrapeStarted", "ScrapeFailed", "ScrapeStarted", "ScrapeFailed", "ScrapeDeadLettered"}
	if len(got) != len(want) {
		t.Fatalf("stream = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("stream = %v, want %v", got, want)
		}
	}
	if w.isRequestAlreadySucceeded(ctx, "req_j2") {
		t.Fatal("dead-lettered job should not dedup as succeeded")
	}
}
