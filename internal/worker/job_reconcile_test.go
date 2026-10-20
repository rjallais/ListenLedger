package worker

import (
	"context"
	"sync"
	"testing"
	"time"

	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/scrapejob"
	"ListenLedger/internal/eventsourcing"
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

type cancelOnLoadStore struct {
	eventsourcing.Store
	once   sync.Once
	cancel context.CancelFunc
}

func (s *cancelOnLoadStore) Load(ctx context.Context, streamID string) ([]eventsourcing.Event, error) {
	s.once.Do(s.cancel)
	return s.Store.Load(ctx, streamID)
}

func TestReconcileFailedJobsSQLiteCompletesAppendsAfterCancellation(t *testing.T) {
	w, ctx := setupTestJobWorker(t)
	queuedAt := time.Now().UTC().Format(jobTimestampFormat)
	seedSweepArtist(t, ctx, w, "ar_reconcile_cancel", "pending")
	jobs := []failedJob{
		{requestID: "req_reconcile_cancel_a", artistID: "ar_reconcile_cancel"},
		{requestID: "req_reconcile_cancel_b", artistID: "ar_reconcile_cancel"},
	}
	for _, job := range jobs {
		seedSweepQueuedRow(t, ctx, w, job.requestID, job.artistID, queuedAt)
		setReconcileJobStatus(t, ctx, w, job.requestID, "failed")
	}

	callerCtx, cancel := context.WithCancel(ctx)
	w.jobStore = &cancelOnLoadStore{Store: w.jobStore, cancel: cancel}

	w.reconcileFailedJobsSQLite(callerCtx, "ar_reconcile_cancel", jobs, "recovered_by_retry:req_cancel_source")

	if callerCtx.Err() == nil {
		t.Fatal("expected the store load to cancel the caller context")
	}
	for _, job := range jobs {
		if status, _ := sweepJobRow(t, ctx, w, job.requestID); status != "succeeded" {
			t.Fatalf("%s status = %q, want succeeded", job.requestID, status)
		}
		if !jobStreamHasType(t, ctx, w, job.requestID, scrapejob.EventTypeScrapeSucceeded) {
			t.Fatalf("%s stream missing ScrapeSucceeded", job.requestID)
		}
	}
}

func jobStreamHasType(t *testing.T, ctx context.Context, w *Worker, requestID, eventType string) bool {
	t.Helper()
	for _, got := range jobStreamTypes(t, ctx, w, requestID) {
		if got == eventType {
			return true
		}
	}
	return false
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
