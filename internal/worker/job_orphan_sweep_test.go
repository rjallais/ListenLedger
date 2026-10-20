package worker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
	"zombiezen.com/go/sqlite"

	"ListenLedger/internal/domain/scrapejob"
)

// TestOrphanedQueuedSweep covers the never-dispatched crash window: a queued
// row with no job stream is swept to failed with a ScrapeFailed fact and its
// artist leaves pending, while dispatched (stream exists) and young rows stay
// untouched.
func TestOrphanedQueuedSweep(t *testing.T) {
	w, ctx := setupTestJobWorker(t)
	now := time.Now().UTC()
	cutoff := now.Add(-orphanedQueuedGrace).Format(jobTimestampFormat)
	old := now.Add(-time.Hour).Format(jobTimestampFormat)
	fresh := now.Format(jobTimestampFormat)

	seedSweepArtist(t, ctx, w, "ar_orphan", "pending")
	seedSweepArtist(t, ctx, w, "ar_dispatched", "pending")
	seedSweepArtist(t, ctx, w, "ar_young", "pending")
	seedSweepQueuedRow(t, ctx, w, "req_orphan", "ar_orphan", old)
	seedSweepQueuedRow(t, ctx, w, "req_dispatched", "ar_dispatched", old)
	seedSweepQueuedRow(t, ctx, w, "req_young", "ar_young", fresh)

	w.recordJobEventWarn(ctx, "req_dispatched", "ar_dispatched", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})

	w.markOrphanedQueuedJobs(ctx, cutoff)

	status, errText := sweepJobRow(t, ctx, w, "req_orphan")
	if status != "failed" || errText != orphanedQueuedReason {
		t.Fatalf("orphan row = %q/%q, want failed/%q", status, errText, orphanedQueuedReason)
	}
	types := jobStreamTypes(t, ctx, w, "req_orphan")
	if len(types) != 2 || types[0] != scrapejob.EventTypeScrapeRequested || types[1] != scrapejob.EventTypeScrapeFailed {
		t.Fatalf("orphan stream = %v, want [ScrapeRequested ScrapeFailed]", types)
	}
	if got := sweepArtistFetchStatus(t, ctx, w, "ar_orphan"); got != "failed" {
		t.Fatalf("orphan artist fetch_status = %q, want failed", got)
	}

	if status, _ := sweepJobRow(t, ctx, w, "req_dispatched"); status != "queued" {
		t.Fatalf("dispatched row = %q, want queued", status)
	}
	if status, _ := sweepJobRow(t, ctx, w, "req_young"); status != "queued" {
		t.Fatalf("young row = %q, want queued", status)
	}
	if got := sweepArtistFetchStatus(t, ctx, w, "ar_dispatched"); got != "pending" {
		t.Fatalf("dispatched artist fetch_status = %q, want pending", got)
	}
	if got := sweepArtistFetchStatus(t, ctx, w, "ar_young"); got != "pending" {
		t.Fatalf("young artist fetch_status = %q, want pending", got)
	}

	// Second sweep over the same cutoff is a no-op: the row already left
	// queued and no second verdict is appended.
	w.markOrphanedQueuedJobs(ctx, cutoff)
	if types := jobStreamTypes(t, ctx, w, "req_orphan"); len(types) != 2 {
		t.Fatalf("orphan stream after second sweep = %v, want unchanged", types)
	}
}

// TestOrphanedQueuedSweepRepairsVerdict covers a partial sweep: the verdict
// pair exists but the row flip did not land, so the next sweep must repair the
// row without appending a third event.
func TestOrphanedQueuedSweepRepairsVerdict(t *testing.T) {
	w, ctx := setupTestJobWorker(t)
	now := time.Now().UTC()
	cutoff := now.Add(-orphanedQueuedGrace).Format(jobTimestampFormat)
	old := now.Add(-time.Hour).Format(jobTimestampFormat)

	seedSweepArtist(t, ctx, w, "ar_repair", "pending")
	seedSweepQueuedRow(t, ctx, w, "req_repair", "ar_repair", old)
	w.recordJobEventWarn(ctx, "req_repair", "ar_repair", "failed", func(j *scrapejob.Job) error {
		_, _, err := j.RecordFailed(orphanedQueuedReason)
		return err
	})
	if types := jobStreamTypes(t, ctx, w, "req_repair"); len(types) != 2 {
		t.Fatalf("seeded verdict stream = %v, want the seed pair", types)
	}

	w.markOrphanedQueuedJobs(ctx, cutoff)

	status, errText := sweepJobRow(t, ctx, w, "req_repair")
	if status != "failed" || errText != orphanedQueuedReason {
		t.Fatalf("repair row = %q/%q, want failed/%q", status, errText, orphanedQueuedReason)
	}
	if types := jobStreamTypes(t, ctx, w, "req_repair"); len(types) != 2 {
		t.Fatalf("repair stream = %v, want the existing verdict pair unchanged", types)
	}
	if got := sweepArtistFetchStatus(t, ctx, w, "ar_repair"); got != "failed" {
		t.Fatalf("repair artist fetch_status = %q, want failed", got)
	}
}

// TestOrphanedQueuedSweepKeepsArtistForNewerJob covers the artist guard: a
// swept orphan must not fail the artist while a newer queued/processing job
// still owns the live scrape.
func TestOrphanedQueuedSweepKeepsArtistForNewerJob(t *testing.T) {
	w, ctx := setupTestJobWorker(t)
	now := time.Now().UTC()
	cutoff := now.Add(-orphanedQueuedGrace).Format(jobTimestampFormat)
	old := now.Add(-time.Hour).Format(jobTimestampFormat)
	fresh := now.Format(jobTimestampFormat)

	seedSweepArtist(t, ctx, w, "ar_guarded", "pending")
	seedSweepQueuedRow(t, ctx, w, "req_guarded_old", "ar_guarded", old)
	seedSweepQueuedRow(t, ctx, w, "req_guarded_new", "ar_guarded", fresh)

	w.markOrphanedQueuedJobs(ctx, cutoff)

	if status, _ := sweepJobRow(t, ctx, w, "req_guarded_old"); status != "failed" {
		t.Fatalf("old orphan row = %q, want failed", status)
	}
	if status, _ := sweepJobRow(t, ctx, w, "req_guarded_new"); status != "queued" {
		t.Fatalf("newer row = %q, want queued", status)
	}
	if got := sweepArtistFetchStatus(t, ctx, w, "ar_guarded"); got != "pending" {
		t.Fatalf("artist fetch_status = %q, want pending while a newer job is live", got)
	}
}

// TestOrphanedQueuedSweepPaginatesPastDispatchedRows covers the backlog case:
// a full page of dispatched-but-still-queued rows must not make the sweep
// rescan the same oldest rows forever.
func TestOrphanedQueuedSweepPaginatesPastDispatchedRows(t *testing.T) {
	w, ctx := setupTestJobWorker(t)
	now := time.Now().UTC()
	cutoff := now.Add(-orphanedQueuedGrace).Format(jobTimestampFormat)
	old := now.Add(-time.Hour).Format(jobTimestampFormat)

	for i := range orphanedQueuedScanLimit {
		requestID := fmt.Sprintf("req_disp_%03d", i)
		artistID := fmt.Sprintf("ar_disp_%03d", i)
		seedSweepArtist(t, ctx, w, artistID, "pending")
		seedSweepQueuedRow(t, ctx, w, requestID, artistID, old)
		w.recordJobEventWarn(ctx, requestID, artistID, "started", func(j *scrapejob.Job) error {
			_, _, err := j.RecordStarted()
			return err
		})
	}
	seedSweepArtist(t, ctx, w, "ar_page_orphan", "pending")
	seedSweepQueuedRow(t, ctx, w, "req_z_orphan", "ar_page_orphan", old)

	// First sweep consumes exactly one full page of dispatched rows.
	w.markOrphanedQueuedJobs(ctx, cutoff)
	if status, _ := sweepJobRow(t, ctx, w, "req_z_orphan"); status != "queued" {
		t.Fatalf("orphan behind a full page = %q, want queued until the next page", status)
	}

	// The cursor advances, so the next sweep reaches the real orphan.
	w.markOrphanedQueuedJobs(ctx, cutoff)
	if status, errText := sweepJobRow(t, ctx, w, "req_z_orphan"); status != "failed" || errText != orphanedQueuedReason {
		t.Fatalf("orphan row = %q/%q, want failed/%q", status, errText, orphanedQueuedReason)
	}
	if got := sweepArtistFetchStatus(t, ctx, w, "ar_page_orphan"); got != "failed" {
		t.Fatalf("orphan artist fetch_status = %q, want failed", got)
	}
}

// TestRequestStreamVerdict covers the preflight guard: the sweep's verdict is
// still reported after a retry starts, so callers can compare delivery time.
func TestRequestStreamVerdict(t *testing.T) {
	w, ctx := setupTestJobWorker(t)

	w.recordJobEventWarn(ctx, "req_orphan_guard", "ar_orphan_guard", "failed", func(j *scrapejob.Job) error {
		_, _, err := j.RecordFailed(orphanedQueuedReason)
		return err
	})
	if succeeded, orphanedAt := w.requestStreamVerdict(ctx, "req_orphan_guard"); succeeded || orphanedAt.IsZero() {
		t.Fatalf("orphan verdict = succeeded=%v orphanedAt=%v, want false/non-zero", succeeded, orphanedAt)
	}

	// A retry can append more events to the same stream. The verdict must
	// remain visible so a late original delivery is still recognized.
	w.recordJobEventWarn(ctx, "req_orphan_guard", "ar_orphan_guard", "started", func(j *scrapejob.Job) error {
		_, _, err := j.RecordStarted()
		return err
	})
	if succeeded, orphanedAt := w.requestStreamVerdict(ctx, "req_orphan_guard"); succeeded || orphanedAt.IsZero() {
		t.Fatalf("reopened orphan stream = succeeded=%v orphanedAt=%v, want false/non-zero", succeeded, orphanedAt)
	}

	w.recordJobEventWarn(ctx, "req_ordinary_failure", "ar_ordinary_failure", "failed", func(j *scrapejob.Job) error {
		_, _, err := j.RecordFailed("fetch_failed")
		return err
	})
	if succeeded, orphanedAt := w.requestStreamVerdict(ctx, "req_ordinary_failure"); succeeded || !orphanedAt.IsZero() {
		t.Fatalf("ordinary failure = succeeded=%v orphanedAt=%v, want false/zero", succeeded, orphanedAt)
	}

	w.recordJobEventWarn(ctx, "req_succeeded_guard", "ar_succeeded_guard", "succeeded", func(j *scrapejob.Job) error {
		_, _, err := j.RecordSucceeded("test", 0)
		return err
	})
	if succeeded, orphanedAt := w.requestStreamVerdict(ctx, "req_succeeded_guard"); !succeeded || !orphanedAt.IsZero() {
		t.Fatalf("succeeded = succeeded=%v orphanedAt=%v, want true/zero", succeeded, orphanedAt)
	}

	if succeeded, orphanedAt := w.requestStreamVerdict(ctx, "req_missing"); succeeded || !orphanedAt.IsZero() {
		t.Fatalf("missing stream = succeeded=%v orphanedAt=%v, want false/zero", succeeded, orphanedAt)
	}
}

func TestDeliveredAfterOrphanVerdict(t *testing.T) {
	verdictAt := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	if deliveredAfterOrphanVerdict(nil, verdictAt) {
		t.Fatal("nil metadata must not bypass the orphan guard")
	}
	if deliveredAfterOrphanVerdict(&jetstream.MsgMetadata{Timestamp: verdictAt.Add(-time.Second)}, verdictAt) {
		t.Fatal("message stored before the verdict must be skipped")
	}
	if !deliveredAfterOrphanVerdict(&jetstream.MsgMetadata{Timestamp: verdictAt.Add(time.Second)}, verdictAt) {
		t.Fatal("retry stored after the verdict must be processed")
	}
}

func seedSweepArtist(t *testing.T, ctx context.Context, w *Worker, artistID, fetchStatus string) {
	t.Helper()
	err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT OR IGNORE INTO artists (id, name, genre_group, list_status, fetch_status) VALUES (?, ?, 'rock_metal', 'included', ?);")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		stmt.BindText(2, artistID)
		stmt.BindText(3, fetchStatus)
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("seed artist %s: %v", artistID, err)
	}
}

func seedSweepQueuedRow(t *testing.T, ctx context.Context, w *Worker, requestID, artistID, queuedAt string) {
	t.Helper()
	err := w.db.WriteWithoutTx(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("INSERT INTO scrape_jobs (id, request_id, artist_id, status, attempts, error, queued_at) VALUES (?, ?, ?, 'queued', 0, '', ?);")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		stmt.BindText(2, requestID)
		stmt.BindText(3, artistID)
		stmt.BindText(4, queuedAt)
		_, err := stmt.Step()
		return err
	})
	if err != nil {
		t.Fatalf("seed scrape_jobs %s: %v", requestID, err)
	}
}

func sweepJobRow(t *testing.T, ctx context.Context, w *Worker, requestID string) (string, string) {
	t.Helper()
	var status, errText string
	err := w.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT status, error FROM scrape_jobs WHERE request_id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, requestID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			status = stmt.ColumnText(0)
			errText = stmt.ColumnText(1)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read scrape_jobs %s: %v", requestID, err)
	}
	return status, errText
}

func sweepArtistFetchStatus(t *testing.T, ctx context.Context, w *Worker, artistID string) string {
	t.Helper()
	var status string
	err := w.db.ReadTX(ctx, func(tx *sqlite.Conn) error {
		stmt := tx.Prep("SELECT fetch_status FROM artists WHERE id = ?;")
		defer func() { _ = stmt.Reset() }()
		stmt.BindText(1, artistID)
		hasRow, err := stmt.Step()
		if err != nil {
			return err
		}
		if hasRow {
			status = stmt.ColumnText(0)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("read artist %s: %v", artistID, err)
	}
	return status
}
