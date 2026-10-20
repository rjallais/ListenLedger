package worker

import (
	"context"
	"testing"
	"time"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/scrapejob"
)

func TestReconcileFailedJobsSQLiteGuardsEventAppend(t *testing.T) {
	w, ctx := setupTestJobWorker(t)
	queuedAt := time.Now().UTC().Format(jobTimestampFormat)
	seedSweepArtist(t, ctx, w, "ar_reconcile", "pending")
	seedSweepQueuedRow(t, ctx, w, "req_reconcile_failed", "ar_reconcile", queuedAt)
	seedSweepQueuedRow(t, ctx, w, "req_reconcile_processing", "ar_reconcile", queuedAt)
	setReconcileJobStatus(t, ctx, w, "req_reconcile_failed", "failed")
	setReconcileJobStatus(t, ctx, w, "req_reconcile_processing", "processing")

	// Simulate a retry that started after the failed-row SELECT but before the
	// guarded reconciliation update reaches this row.
	w.recordJobEventWarn(ctx, "req_reconcile_processing", "ar_reconcile", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})
	before := jobStreamTypes(t, ctx, w, "req_reconcile_processing")

	w.reconcileFailedJobsSQLite(ctx, "ar_reconcile", []failedJob{
		{requestID: "req_reconcile_failed", artistID: "ar_reconcile"},
		{requestID: "req_reconcile_processing", artistID: "ar_reconcile"},
	}, "recovered_by_retry:req_source")

	if status, _ := sweepJobRow(t, ctx, w, "req_reconcile_failed"); status != "succeeded" {
		t.Fatalf("failed row status = %q, want succeeded", status)
	}
	if status, _ := sweepJobRow(t, ctx, w, "req_reconcile_processing"); status != "processing" {
		t.Fatalf("processing row status = %q, want processing", status)
	}

	after := jobStreamTypes(t, ctx, w, "req_reconcile_processing")
	if len(after) != len(before) {
		t.Fatalf("processing stream changed = %v, want %v", after, before)
	}
	for _, eventType := range after {
		if eventType == scrapejob.EventTypeScrapeSucceeded {
			t.Fatalf("processing stream gained ScrapeSucceeded: %v", after)
		}
	}

	failedStream := jobStreamTypes(t, ctx, w, "req_reconcile_failed")
	foundSucceeded := false
	for _, eventType := range failedStream {
		if eventType == scrapejob.EventTypeScrapeSucceeded {
			foundSucceeded = true
			break
		}
	}
	if !foundSucceeded {
		t.Fatalf("failed stream missing ScrapeSucceeded: %v", failedStream)
	}
}

func setReconcileJobStatus(t *testing.T, ctx context.Context, w *Worker, requestID, status string) {
	t.Helper()
	if err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("UPDATE scrape_jobs SET status = ? WHERE request_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, status)
		stmt.BindText(2, requestID)
		_, err := stmt.Step()
		return err
	}); err != nil {
		t.Fatalf("set scrape_jobs %s status %s: %v", requestID, status, err)
	}
}
